"""
model_utils.py - Utilidades para carga y procesamiento de datos ML
✅ CORREGIDO: Usa rolling_mean/std/p95 en lugar de peer_latency_*_10
"""
import psycopg2
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class MLDataLoader:
    """Carga datos desde ml_features"""
    
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Conectar a TimescaleDB"""
        try:
            self.conn = psycopg2.connect(
                host='timescaledb',
                port=5432,
                database='bgp_failover_db',
                user='bgp_app',
                password='bgp_app_password'
            )
            logger.info("✅ Conectado a TimescaleDB en timescaledb:5432")
        except Exception as e:
            logger.error(f"❌ Error conectando a TimescaleDB: {e}")
            raise
    
    def load_ml_features(self, days=30):
        """
        ✅ CORREGIDO: Carga ml_features con columnas correctas
        """
        logger.info(f"📥 Cargando {days} días de datos de ml_features...")
        
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
            -- ✅ CORREGIDO: Rolling statistics (nuevos nombres)
            COALESCE(rolling_mean, 0) as rolling_mean,
            COALESCE(rolling_std, 0) as rolling_std,
            COALESCE(rolling_p95, 0) as rolling_p95,
            -- Contextual features
            hour_of_day,
            day_of_week,
            COALESCE(is_business_hours::int, 0) as is_business_hours,
            COALESCE(is_peak_traffic::int, 0) as is_peak_traffic,
            COALESCE(is_weekend::int, 0) as is_weekend,
            -- Provider features
            COALESCE(score_difference, 0) as score_difference,
            COALESCE(margin_exceeds_threshold::int, 0) as margin_exceeds_threshold,
            COALESCE(provider_changed::int, 0) as provider_changed,
            -- Detección combinada
            COALESCE(z_score_peer, 0) as z_score_peer,
            COALESCE(z_score_severity, 'normal') as z_score_severity,
            COALESCE(absolute_severity, 'normal') as absolute_severity,
            COALESCE(relative_diff_ms, 0) as relative_diff_ms,
            COALESCE(relative_severity, 'normal') as relative_severity,
            COALESCE(combined_severity, 'normal') as combined_severity,
            COALESCE(is_combined_anomaly::int, 0) as is_combined_anomaly,
            -- Degradation tracking
            COALESCE(degradation_cycle, 0) as degradation_cycle,
            -- Target
            should_failover
        FROM ml_features
        WHERE time >= NOW() - INTERVAL '{days} days'
        ORDER BY time
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            logger.info(f"✅ Cargados {len(df)} registros de ml_features")
            logger.info(f"   Período: {df['time'].min()} a {df['time'].max()}")
            return df
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            raise
    
    def close(self):
        """Cerrar conexión"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Conexión a TimescaleDB cerrada")


class MLPipelineHelper:
    """Helper para pipeline de ML"""
    
    @staticmethod
    def validate_features(df, required_features):
        """Valida que las features requeridas existan"""
        missing = [f for f in required_features if f not in df.columns]
        if missing:
            logger.warning(f"⚠️ Faltan {len(missing)} features: {missing}")
            return False
        return True
    
    @staticmethod
    def encode_categorical_features(df):
        """Codifica features categóricas"""
        severity_map = {'normal': 0, 'warning': 1, 'degraded': 2, 'critical': 3}
        
        categorical_cols = [
            'z_score_severity',
            'absolute_severity',
            'relative_severity',
            'combined_severity'
        ]
        
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].map(severity_map).fillna(0).astype(int)
        
        return df
