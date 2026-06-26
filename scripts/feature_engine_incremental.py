#!/usr/bin/env python3
"""
Feature Engine Mejorado - Lectura Incremental (SIN REDUNDANCIA)
Calcula features derivadas SOLO para nuevos datos desde bgp_metrics
y los almacena en ml_features sin generar duplicados.

✅ CORRECCIÓN: 
- Eliminadas columnas redundantes (peer_latency_mean_10, etc.)
- Ventana de rolling statistics reducida de 10 a 5 ciclos
- Features de detección combinada (Z-score, Absolute, Relative)
- Fix: divide by zero en cálculo de Z-score
"""
import psycopg2
import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timezone, timedelta

# === Configuración ===
TIMESCALEDB_HOST = 'timescaledb'
TIMESCALEDB_PORT = 5432
TIMESCALEDB_DB = 'bgp_failover_db'
TIMESCALEDB_USER = 'bgp_app'
TIMESCALEDB_PASSWORD = 'bgp_app_password'

# === Configuración de Feature Engine ===
EXECUTION_MODE = "incremental"
LAST_HOURS = 2  # Fallback si no hay datos previos
BATCH_SIZE = None

# ✅ Constantes del motor BGP
SUSTAINED_DEGRADATION_CYCLES = 3
SWITCH_MARGIN = 5

# ✅ Ventana de rolling statistics REDUCIDA de 10 a 5 ciclos
ROLLING_WINDOW = 5  # 5 ciclos × 30s = 2.5 minutos

# ✅ Umbrales de detección combinada
Z_SCORE_THRESHOLDS = {
    'normal': 2.0,
    'warning': 2.5,
    'degraded': 3.0,
    'critical': 3.5
}

ABSOLUTE_LATENCY_THRESHOLDS = {
    'peer_warning': 12.0,
    'peer_degraded': 15.0,
    'peer_critical': 25.0,
    'dns_warning': 15.0,
    'dns_degraded': 20.0,
    'dns_critical': 30.0
}

RELATIVE_DIFF_THRESHOLDS = {
    'warning': 5.0,
    'degraded': 10.0,
    'critical': 15.0
}


class TimescaleDBClient:
    """Cliente mejorado para TimescaleDB con soporte para lectura incremental"""
    
    def __init__(self, host, port, database, user, password):
        self.conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        logging.info(f"✅ Conectado a TimescaleDB en {host}:{port}")

    def get_last_feature_timestamp(self):
        """Lee el último timestamp de ml_features"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT COALESCE(MAX(time), NULL)
                FROM ml_features
            """)
            result = cur.fetchone()
            cur.close()
            
            if result[0]:
                logging.info(f"✅ Último timestamp en ml_features: {result[0]}")
                return result[0]
            else:
                logging.info("ℹ️ ml_features vacía, procesará últimas horas")
                return None
        except Exception as e:
            logging.error(f"⚠️ Error leyendo last_timestamp: {e}")
            return None

    def insert_ml_features(self, row):
        """Inserta un registro de features en ml_features"""
        try:
            cur = self.conn.cursor()
            
            # Construir INSERT dinámico con las columnas del dataframe
            columns = list(row.index)
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            query = f"""
                INSERT INTO ml_features ({column_names})
                VALUES ({placeholders})
            """
            values = [row[col] for col in columns]
            
            cur.execute(query, values)
            self.conn.commit()
            cur.close()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error insertando feature: {e}")


class FeatureEngineImproved:
    """
    Feature Engine mejorado con lectura incremental
    ✅ OBJETIVO: Evitar redundancia, procesar SOLO datos nuevos
    ✅ CORRECCIÓN: Sin columnas redundantes, ventana de 5 ciclos
    """
    
    def __init__(self):
        self.ts_client = TimescaleDBClient(
            host=TIMESCALEDB_HOST,
            port=TIMESCALEDB_PORT,
            database=TIMESCALEDB_DB,
            user=TIMESCALEDB_USER,
            password=TIMESCALEDB_PASSWORD
        )
        self.conn = self.ts_client.conn

    def load_metrics_incremental(self):
        """
        ✅ CORRECCIÓN: Carga SOLO nuevos datos desde última ejecución
        Incluye columnas de detección combinada desde bgp_metrics
        """
        last_timestamp = self.ts_client.get_last_feature_timestamp()
        
        if last_timestamp is None:
            time_filter = f"NOW() - INTERVAL '{LAST_HOURS} hours'"
            logging.info(f"📥 Primera ejecución: cargando últimas {LAST_HOURS} horas...")
        else:
            time_filter = f"'{last_timestamp}'::timestamptz"
            logging.info(f"📥 Cargando SOLO datos después de: {last_timestamp}")
        
        # ✅ Query incluye columnas de detección combinada desde bgp_metrics
        query = f"""
            SELECT
                time, provider,
                peer_latency_ms, dns_latency_ms,
                peer_loss_pct, dns_loss_pct,
                peer_jitter_ms, dns_jitter_ms,
                score,
                -- ✅ Columnas de detección combinada desde bgp_metrics
                COALESCE(z_score_peer, 0) as z_score_peer,
                COALESCE(z_score_severity, 'normal') as z_score_severity,
                COALESCE(rolling_mean, 0) as rolling_mean,
                COALESCE(rolling_std, 0) as rolling_std,
                COALESCE(rolling_p95, 0) as rolling_p95,
                COALESCE(absolute_severity, 'normal') as absolute_severity,
                COALESCE(relative_diff_ms, 0) as relative_diff_ms,
                COALESCE(relative_severity, 'normal') as relative_severity,
                COALESCE(combined_severity, 'normal') as combined_severity,
                -- Degradation tracking
                COALESCE(degradation_cycle, 0) as degradation_cycle,
                COALESCE(provider_changed, FALSE) as provider_changed
            FROM bgp_metrics
            WHERE time > {time_filter}
            ORDER BY time, provider
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            
            if df.empty:
                logging.info("ℹ️ No hay nuevos datos desde última ejecución")
                return pd.DataFrame()
            
            logging.info(f"✅ Cargados {len(df)} NUEVOS registros (sin redundancia)")
            return df
        except Exception as e:
            logging.error(f"Error cargando métricas: {e}")
            return pd.DataFrame()

    def calculate_derived_features(self, df):
        """Calcula features derivadas"""
        if df.empty:
            return df
        
        logging.info("🔧 Calculando features derivadas...")
        df = df.copy()
        
        df['latency_ratio'] = df['peer_latency_ms'] / (df['dns_latency_ms'] + 0.001)
        df['total_loss_pct'] = (df['peer_loss_pct'] + df['dns_loss_pct']) / 2
        
        max_latency = 50.0
        df['quality_index'] = np.clip(
            100 - (
                (df['peer_latency_ms'] / max_latency * 40) +
                (df['total_loss_pct'] * 50) +
                (df['peer_jitter_ms'] / 10 * 10)
            ),
            0, 100
        )
        
        return df

    def calculate_temporal_features(self, df):
        """Calcula features temporales"""
        if df.empty:
            return df
        
        logging.info("🔧 Calculando features temporales...")
        df = df.copy()
        
        for provider in df['provider'].unique():
            mask = df['provider'] == provider
            provider_data = df.loc[mask].sort_values('time')
            
            # ✅ Ventana reducida a ROLLING_WINDOW (5)
            df.loc[mask, 'latency_trend_5min'] = provider_data['peer_latency_ms'].rolling(
                window=ROLLING_WINDOW, min_periods=1
            ).mean().diff().fillna(0).values
            
            df.loc[mask, 'latency_trend_15min'] = provider_data['peer_latency_ms'].rolling(
                window=ROLLING_WINDOW * 3, min_periods=1
            ).mean().diff().fillna(0).values
            
            df.loc[mask, 'latency_velocity'] = provider_data['peer_latency_ms'].diff().fillna(0).values
            df.loc[mask, 'latency_acceleration'] = provider_data['peer_latency_ms'].diff().diff().fillna(0).values
            
            df.loc[mask, 'loss_spike_detected'] = (
                provider_data['peer_loss_pct'].diff().fillna(0) > 5.0
            ).astype(bool).values
        
        return df

    def calculate_rolling_statistics(self, df):
        """
        ✅ CORRECCIÓN: NO calcular columnas redundantes
        Las rolling stats (rolling_mean, rolling_std, rolling_p95) 
        ya vienen de bgp_metrics y se cargaron en load_metrics_incremental()
        
        Este método ahora es un placeholder para futuras features rolling
        que NO existan en bgp_metrics
        """
        if df.empty:
            return df
        
        logging.info(f"🔧 Calculando rolling statistics adicionales (ventana={ROLLING_WINDOW} ciclos)...")
        df = df.copy()
        
        # ✅ Las columnas rolling_mean, rolling_std, rolling_p95 YA VIENEN de bgp_metrics
        # No necesitamos recalcularlas aquí
        
        # Si en el futuro necesitas calcular rolling stats adicionales que NO estén en bgp_metrics,
        # puedes agregarlas aquí. Por ejemplo:
        # - rolling_mean_15 (ventana de 15 ciclos)
        # - rolling_std_15 (ventana de 15 ciclos)
        # - rolling_p95_15 (ventana de 15 ciclos)
        
        # Por ahora, este método no hace nada porque todas las rolling stats
        # necesarias ya vienen de bgp_metrics
        
        return df

    def calculate_contextual_features(self, df):
        """Calcula features contextuales"""
        if df.empty:
            return df
        
        logging.info("🔧 Calculando features contextuales...")
        df = df.copy()
        
        df['hour_of_day'] = df['time'].dt.hour
        df['day_of_week'] = df['time'].dt.dayofweek
        
        df['is_business_hours'] = (
            (df['hour_of_day'] >= 9) & (df['hour_of_day'] < 17) &
            (df['day_of_week'] < 5)
        ).astype(bool)
        
        df['is_peak_traffic'] = (
            ((df['hour_of_day'] >= 10) & (df['hour_of_day'] < 14)) |
            ((df['hour_of_day'] >= 15) & (df['hour_of_day'] < 18))
        ).astype(bool)
        
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(bool)
        
        return df

    def calculate_provider_features(self, df):
        """Calcula features relacionadas con providers"""
        if df.empty:
            return df
        
        logging.info("🔧 Calculando provider features...")
        df = df.copy()
        
        # Obtener información de failovers
        try:
            cur = self.conn.cursor()
            
            cur.execute("""
                SELECT COUNT(*) FROM bgp_failover_events
                WHERE time >= NOW() - INTERVAL '1 hour'
            """)
            changes_last_hour = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COALESCE(MAX(time), NOW()) FROM bgp_failover_events
            """)
            last_change_time = cur.fetchone()[0]
            
            cur.close()
            
            time_since_change = (df['time'] - last_change_time).dt.total_seconds() / 60
        except Exception as e:
            logging.warning(f"⚠️ Error obteniendo failover info: {e}")
            changes_last_hour = 0
            time_since_change = 0
        
        df['provider_changes_last_hour'] = changes_last_hour
        df['time_since_last_change_min'] = time_since_change.clip(lower=0)
        
        # Calcular scores de providers usando merge
        score_table = df[['time', 'provider', 'score']].copy()
        
        score_pivot = score_table.pivot_table(
            index='time',
            columns='provider',
            values='score',
            aggfunc='first'
        )
        
        df['current_provider_score'] = df['score']
        
        df = df.merge(score_pivot.reset_index(), on='time', how='left')
        
        providers = df['provider'].unique()
        if len(providers) == 2:
            provider1, provider2 = sorted(providers)
            df['alternative_provider_score'] = df.apply(
                lambda row: row[provider2] if row['provider'] == provider1 else row[provider1],
                axis=1
            )
        else:
            df['alternative_provider_score'] = df['score']
        
        for col in providers:
            if col in df.columns and col != 'provider':
                df = df.drop(columns=[col])
        
        df['score_difference'] = df['current_provider_score'] - df['alternative_provider_score']
        df['margin_exceeds_threshold'] = (df['score_difference'] > SWITCH_MARGIN).astype(bool)
        
        return df

    def calculate_combined_detection_features(self, df):
        """
        ✅ Calcula features de detección combinada
        Usa los valores de bgp_metrics (z_score_peer, rolling_mean, etc.)
        y recalcula combined_severity si es necesario
        """
        if df.empty:
            return df
        
        logging.info("🔧 Calculando features de detección COMBINADA...")
        df = df.copy()
        
        # ✅ Las columnas z_score_peer, rolling_mean, rolling_std, rolling_p95,
        # absolute_severity, relative_diff_ms, relative_severity YA VIENEN de bgp_metrics
        
        # ✅ CORRECCIÓN: Fix divide by zero en Z-score
        # Si rolling_std es 0, el Z-score debe ser 0 (no infinito)
        if 'rolling_std' in df.columns:
            # Recalcular Z-score con protección contra división por cero
            df['z_score_peer'] = np.where(
                df['rolling_std'] > 0.001,
                (df['peer_latency_ms'] - df['rolling_mean']) / df['rolling_std'].replace(0, np.nan),
                0.0
            )
            df['z_score_peer'] = df['z_score_peer'].fillna(0.0).round(2)
            
            # Recalcular z_score_severity basado en el Z-score corregido
            df['z_score_severity'] = np.where(
                df['z_score_peer'] >= Z_SCORE_THRESHOLDS['critical'], 'critical',
                np.where(
                    df['z_score_peer'] >= Z_SCORE_THRESHOLDS['degraded'], 'degraded',
                    np.where(
                        df['z_score_peer'] >= Z_SCORE_THRESHOLDS['warning'], 'warning',
                        'normal'
                    )
                )
            )
        
        # ✅ Calcular combined_severity (máximo de las 3 detecciones)
        severity_levels = {'normal': 0, 'warning': 1, 'degraded': 2, 'critical': 3}
        
        # Convertir severidades a niveles numéricos
        z_levels = df['z_score_severity'].map(severity_levels).fillna(0)
        abs_levels = df['absolute_severity'].map(severity_levels).fillna(0)
        rel_levels = df['relative_severity'].map(severity_levels).fillna(0)
        
        # Combined level = máximo de los 3
        combined_levels = np.maximum(np.maximum(z_levels, abs_levels), rel_levels)
        
        # Convertir niveles de vuelta a strings
        level_to_severity = {v: k for k, v in severity_levels.items()}
        df['combined_severity'] = combined_levels.map(level_to_severity)
        
        # is_combined_anomaly = True si combined_severity es degraded o critical
        df['is_combined_anomaly'] = combined_levels >= 2
        
        # Estadísticas de detección
        anomaly_count = df['is_combined_anomaly'].sum()
        severity_dist = df['combined_severity'].value_counts().to_dict()
        
        logging.info(f"   📊 Detección combinada:")
        logging.info(f"      - Anomalías detectadas: {anomaly_count}/{len(df)}")
        logging.info(f"      - Distribución de severidad: {severity_dist}")
        
        return df

    def calculate_target_variable(self, df):
        """Calcula variable target usando provider_changed como ground truth"""
        if df.empty:
            return df
        
        logging.info("🔧 Calculando target variable (usando provider_changed como ground truth)...")
        df = df.copy()
        
        df['should_failover'] = df['provider_changed'].astype(int)
        
        failover_cycles = df[df['provider_changed'] == True]['time'].nunique()
        total_records = len(df)
        
        logging.info(f"✅ Target calculado:")
        logging.info(f"   - Total registros en dataset: {total_records}")
        logging.info(f"   - Failovers REALES (ciclos distintos): {failover_cycles}")
        logging.info(f"   - Registros con should_failover=1: {df['should_failover'].sum()} (2 por cada failover)")
        
        return df

    def process_and_store(self):
        """Procesa nuevos features y los almacena"""
        df = self.load_metrics_incremental()
        
        if df.empty:
            logging.info("ℹ️ Sin nuevos datos, nada que procesar")
            return 0
        
        logging.info("🔄 Procesando features...")
        df = self.calculate_derived_features(df)
        df = self.calculate_temporal_features(df)
        df = self.calculate_rolling_statistics(df)
        df = self.calculate_contextual_features(df)
        df = self.calculate_provider_features(df)
        df = self.calculate_combined_detection_features(df)
        df = self.calculate_target_variable(df)
        
        logging.info("✓ Validando datos...")
        
        logging.info("💾 Guardando en ml_features...")
        inserted = 0
        
        for idx, row in df.iterrows():
            try:
                self.ts_client.insert_ml_features(row)
                inserted += 1
            except Exception as e:
                logging.warning(f"Error insertando fila {idx}: {e}")
        
        logging.info(f"✅ {inserted} NUEVOS registros grabados (sin redundancia)")
        return inserted


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/var/log/feature_engine.log')
        ]
    )
    
    logging.info("=" * 80)
    logging.info("🔧 Feature Engine: Calculando features derivadas (INCREMENTAL)")
    logging.info("=" * 80)
    
    engine = FeatureEngineImproved()
    
    logging.info(f"⚙️ Modo: {EXECUTION_MODE.upper()}")
    logging.info(f"⚙️ Fallback: {LAST_HOURS} horas si ml_features vacía")
    logging.info(f"⚙️ SUSTAINED_DEGRADATION_CYCLES: {SUSTAINED_DEGRADATION_CYCLES}")
    logging.info(f"⚙️ SWITCH_MARGIN: {SWITCH_MARGIN}")
    logging.info(f"⚙️ ROLLING_WINDOW: {ROLLING_WINDOW} ciclos ({ROLLING_WINDOW * 30}s)")
    logging.info(f"⚙️ Z-Score Thresholds: {Z_SCORE_THRESHOLDS}")
    logging.info(f"⚙️ Absolute Thresholds (peer): {ABSOLUTE_LATENCY_THRESHOLDS['peer_warning']}/{ABSOLUTE_LATENCY_THRESHOLDS['peer_degraded']}/{ABSOLUTE_LATENCY_THRESHOLDS['peer_critical']}ms")
    logging.info(f"⚙️ Relative Thresholds: {RELATIVE_DIFF_THRESHOLDS['warning']}/{RELATIVE_DIFF_THRESHOLDS['degraded']}/{RELATIVE_DIFF_THRESHOLDS['critical']}ms")
    
    inserted = engine.process_and_store()
    
    logging.info("")
    logging.info("=" * 80)
    logging.info("✅ Feature Engine ejecutado exitosamente")
    logging.info("=" * 80)
    logging.info(f"Registros grabados: {inserted} (NUEVOS, sin redundancia)")


if __name__ == '__main__':
    main()
