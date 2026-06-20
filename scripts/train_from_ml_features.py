"""
train_from_ml_features.py

Entrena XGBoost usando datos directamente de ml_features
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
    Carga datos de entrenamiento desde ml_features (no de ml_training_data)
    
    ✅ VENTAJAS:
    ├─ Obtiene todas las features derivadas
    ├─ Acceso a estadísticas Rolling
    ├─ Features contextuales completas
    └─ Una única fuente de verdad
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
        peer_latency_mean_10,
        peer_latency_std_10,
        peer_latency_min_10,
        peer_latency_max_10,
        peer_latency_p95_10,
        -- Contextual features
        hour_of_day,
        day_of_week,
        COALESCE(is_business_hours::int, 0) as is_business_hours,
        COALESCE(is_peak_traffic::int, 0) as is_peak_traffic,
        COALESCE(is_weekend::int, 0) as is_weekend,
        -- Target
        should_failover
    FROM ml_features
    WHERE time >= NOW() - INTERVAL '{days} days'
    ORDER BY time
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"✅ Cargados {len(df)} registros de ml_features")
    logger.info(f"   Fechas: {df['time'].min()} a {df['time'].max()}")
    logger.info(f"   Providers: {df['provider'].unique()}")
    logger.info(f"   Target distribution:")
    logger.info(f"     - No failover: {(df['should_failover']==0).sum()}")
    logger.info(f"     - Failover: {(df['should_failover']==1).sum()}")
    
    return df

def main():
    """
    Ejecuta entrenamiento desde ml_features
    """
    
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 80)
    print("🚀 ENTRENAMIENTO DESDE ml_features")
    print("=" * 80)
    print()
    
    # 1. Cargar datos desde ml_features
    print("PASO 1: Cargar datos de ml_features")
    print("-" * 80)
    
    df = load_training_data_from_ml_features(
        timescaledb_password='bgp_app_password',
        days=30
    )
    
    print(f"✅ Dataset cargado: {len(df)} registros")
    print(f"   Características: {list(df.columns)}")
    
    # 2. Entrenar XGBoost
    print("\nPASO 2: Entrenar XGBoost")
    print("-" * 80)
    
    optimizer = ScoringWeightOptimizer()
    y_test, y_pred, y_pred_proba = optimizer.train(df)
    
    # 3. Obtener pesos
    print("\nPASO 3: Extraer pesos optimizados")
    print("-" * 80)
    
    weights = optimizer.get_optimized_weights()
    
    print("\n" + "=" * 80)
    print("✅ ENTRENAMIENTO COMPLETADO")
    print("=" * 80)
    print()
    print(f"Pesos optimizados:")
    print(f"  Peer latency weight: {weights['peer_latency_weight']:.4f}")
    print(f"  DNS latency weight: {weights['dns_latency_weight']:.4f}")
    print(f"  Loss importance: {weights['loss_importance']:.4f}")
    print(f"  Context importance: {weights['context_importance']:.4f}")

if __name__ == '__main__':
    main()
