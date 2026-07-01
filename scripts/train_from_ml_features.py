"""
train_from_ml_features.py
Entrena XGBoost con Cross-Validation usando datos de ml_features
✅ CORRECCIONES:
├─ Usa Cross-Validation (5 folds) para pesos más estables
├─ Eliminado data leakage de degradation_cycle
├─ Análisis de estabilidad de features
└─ NUEVO: Usa failover_event como target (conteo correcto)
"""
import psycopg2
import pandas as pd
import numpy as np
import logging
from xgboost_optimizer import ScoringWeightOptimizer

logger = logging.getLogger(__name__)


def load_training_data_from_ml_features(
    timescaledb_password,
    days=30,
    timescaledb_host='timescaledb',
    timescaledb_port=5432,
    timescaledb_db='bgp_failover_db',
    timescaledb_user='bgp_app'
):
    """
    Carga datos de entrenamiento desde ml_features
    ✅ ACTUALIZADO: Incluye failover_event como target correcto
    """
    
    logger.info(f"📥 Cargando {days} días de datos de ml_features...")
    
    conn = psycopg2.connect(
        host=timescaledb_host,
        port=timescaledb_port,
        database=timescaledb_db,
        user=timescaledb_user,
        password=timescaledb_password
    )
    
    query = f"""
    SELECT
        time, provider,
        -- Raw metrics
        peer_latency_ms, dns_latency_ms,
        peer_loss_pct, dns_loss_pct,
        peer_jitter_ms, dns_jitter_ms,
        score,
        -- Derived features
        latency_ratio,
        total_loss_pct,
        quality_index,
        -- Temporal features
        COALESCE(latency_trend_5min, 0) as latency_trend_5min,
        COALESCE(latency_trend_15min, 0) as latency_trend_15min,
        COALESCE(latency_velocity, 0) as latency_velocity,
        COALESCE(latency_acceleration, 0) as latency_acceleration,
        COALESCE(loss_spike_detected::int, 0) as loss_spike_detected,
        -- Rolling statistics
        COALESCE(rolling_mean, 0) as rolling_mean,
        COALESCE(rolling_std, 0) as rolling_std,
        COALESCE(rolling_p95, 0) as rolling_p95,
        -- Contextual features
        hour_of_day,
        day_of_week,
        COALESCE(is_business_hours::int, 0) as is_business_hours,
        COALESCE(is_peak_traffic::int, 0) as is_peak_traffic,
        COALESCE(is_weekend::int, 0) as is_weekend,
        -- Degradation tracking (SIN degradation_cycle para evitar leakage)
        COALESCE(provider_changed::int, 0) as provider_changed,
        COALESCE(score_difference, 0) as score_difference,
        COALESCE(margin_exceeds_threshold::int, 0) as margin_exceeds_threshold,
        -- Detección Combinada
        COALESCE(z_score_peer, 0) as z_score_peer,
        COALESCE(z_score_severity, 'normal') as z_score_severity,
        COALESCE(absolute_severity, 'normal') as absolute_severity,
        COALESCE(relative_diff_ms, 0) as relative_diff_ms,
        COALESCE(relative_severity, 'normal') as relative_severity,
        COALESCE(combined_severity, 'normal') as combined_severity,
        COALESCE(is_combined_anomaly, FALSE) as is_combined_anomaly,
        -- ✅ NUEVO: Targets
        should_failover,
        COALESCE(failover_event, 0) as failover_event
    FROM ml_features
    WHERE time >= NOW() - INTERVAL '{days} days'
    ORDER BY time
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"✅ Cargados {len(df)} registros de ml_features")
    logger.info(f"   Fechas: {df['time'].min()} a {df['time'].max()}")
    logger.info(f"   Providers: {df['provider'].unique()}")
    
    # Verificar features de detección combinada
    detection_features = [
        'z_score_peer', 'z_score_severity', 'rolling_mean',
        'absolute_severity', 'relative_diff_ms', 'combined_severity'
    ]
    
    for feature in detection_features:
        if feature in df.columns:
            if df[feature].dtype == 'object':
                unique_vals = df[feature].unique()[:5]
                logger.info(f"   ✓ {feature}: {len(df[feature].unique())} valores únicos")
            else:
                non_zero = (df[feature] != 0).sum()
                logger.info(f"   ✓ {feature}: {non_zero}/{len(df)} con valores calculados")
        else:
            logger.warning(f"   ✗ {feature}: NO ENCONTRADO")
    
    # ✅ NUEVO: Verificar failover_event
    if 'failover_event' in df.columns:
        failover_events = df['failover_event'].sum()
        unique_failover_times = df[df['failover_event'] == 1]['time'].nunique()
        logger.info(f"   ✓ failover_event: {failover_events} eventos únicos en {unique_failover_times} ciclos")
    else:
        logger.warning(f"   ✗ failover_event: NO ENCONTRADO (usando should_failover como fallback)")
    
    logger.info(f"   Target distribution (should_failover):")
    logger.info(f"     - No failover: {(df['should_failover']==0).sum()}")
    logger.info(f"     - Failover: {(df['should_failover']==1).sum()}")
    
    if 'failover_event' in df.columns:
        logger.info(f"   Target distribution (failover_event):")
        logger.info(f"     - No failover: {(df['failover_event']==0).sum()}")
        logger.info(f"     - Failover: {(df['failover_event']==1).sum()}")
    
    # Conteo correcto de failovers únicos
    unique_failover_times = df[df['should_failover'] == 1]['time'].nunique()
    logger.info(f"   Failovers ÚNICOS (ciclos distintos): {unique_failover_times}")
    
    return df


def main():
    """Ejecuta entrenamiento con Cross-Validation desde ml_features"""
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 80)
    print("🚀 ENTRENAMIENTO CON CROSS-VALIDATION (CORREGIDO)")
    print("=" * 80)
    print()
    
    # 1. Cargar datos
    print("PASO 1: Cargar datos de ml_features")
    print("-" * 80)
    
    df = load_training_data_from_ml_features(
        timescaledb_password='bgp_app_password',
        days=30
    )
    
    print(f"\n✅ Dataset cargado: {len(df)} registros")
    print(f"   Características: {len(df.columns)}")
    
    # 2. Entrenar con Cross-Validation
    print("\nPASO 2: Entrenar XGBoost con Cross-Validation")
    print("-" * 80)
    
    optimizer = ScoringWeightOptimizer()
    feature_importance = optimizer.train_with_cv(df, n_splits=5)
    
    # 3. Obtener pesos
    print("\nPASO 3: Extraer pesos optimizados")
    print("-" * 80)
    
    weights = optimizer.get_optimized_weights()
    
    print("\n" + "=" * 80)
    print("✅ ENTRENAMIENTO COMPLETADO")
    print("=" * 80)
    print()
    print(f"Pesos optimizados (promedio de 5 folds):")
    print(f"  Peer latency weight: {weights['peer_latency_weight']:.4f}")
    print(f"  DNS latency weight: {weights['dns_latency_weight']:.4f}")
    print(f"  Loss importance: {weights['loss_importance']:.4f}")
    print(f"  Jitter importance: {weights['jitter_importance']:.4f}")
    print(f"  Rolling importance: {weights['rolling_importance']:.4f}")
    print(f"  Derived importance: {weights['derived_importance']:.4f}")
    print(f"  Degradation importance: {weights['degradation_importance']:.4f}")
    print(f"  Context importance: {weights['context_importance']:.4f}")
    print(f"  Combined detection importance: {weights['combined_detection_importance']:.4f}")
    
    print("\nCross-Validation Metrics:")
    for metric, values in weights['cv_scores'].items():
        mean_val = np.mean(values)
        std_val = np.std(values)
        print(f"  {metric:12s}: {mean_val:.4f} ± {std_val:.4f}")
    
    print("\nRecomendaciones:")
    for key, value in weights['recommendations'].items():
        status = "✅ SÍ" if value else "❌ NO"
        print(f"  {key}: {status}")
    
    print("\n" + "=" * 80)
    print("📋 CORRECCIONES APLICADAS:")
    print("=" * 80)
    print("  ✅ Eliminada degradation_cycle (data leakage)")
    print("  ✅ Cross-Validation con 5 folds")
    print("  ✅ Regularización aumentada (max_depth=3, reg_alpha=0.1)")
    print("  ✅ Análisis de estabilidad de features")
    print("  ✅ Pesos promediados entre folds")
    print("  ✅ NUEVO: failover_event como target (conteo correcto)")


if __name__ == '__main__':
    main()
