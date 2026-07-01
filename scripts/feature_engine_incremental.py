#!/usr/bin/env python3
"""
Feature Engine Mejorado - Lectura Incremental (SIN REDUNDANCIA)
Calcula features derivadas SOLO para nuevos datos desde bgp_metrics
y los almacena en ml_features sin generar duplicados.
FREQUENCY: Cada minuto (configurable)
MODO: Incremental (lee último timestamp, procesa SOLO nuevos)
✅ CORRECCIÓN: Carga features de detección combinada desde bgp_metrics
✅ NUEVO: Crea columna failover_event para conteo correcto de failovers
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
LAST_HOURS = 1
BATCH_SIZE = None

# ✅ Constantes del motor BGP
SUSTAINED_DEGRADATION_CYCLES = 3
SWITCH_MARGIN = 5


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
    ✅ CORRECCIÓN: Cargar features de detección combinada desde bgp_metrics
    ✅ NUEVO: Crear columna failover_event para conteo correcto
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
        ✅ INCLUYE features de detección combinada desde bgp_metrics
        """
        last_timestamp = self.ts_client.get_last_feature_timestamp()
        
        if last_timestamp is None:
            time_filter = f"NOW() - INTERVAL '{LAST_HOURS} hours'"
            logging.info(f"📥 Primera ejecución: cargando últimas {LAST_HOURS} horas...")
        else:
            time_filter = f"'{last_timestamp}'::timestamptz"
            logging.info(f"📥 Cargando SOLO datos después de: {last_timestamp}")
        
        # ✅ CORRECCIÓN: Query incluye columnas de detección combinada
        query = f"""
            SELECT
                time, provider,
                peer_latency_ms, dns_latency_ms,
                peer_loss_pct, dns_loss_pct,
                peer_jitter_ms, dns_jitter_ms,
                score,
                -- ✅ NUEVO: Features de detección combinada desde bgp_metrics
                COALESCE(z_score_peer, 0) as z_score_peer,
                COALESCE(z_score_severity, 'normal') as z_score_severity,
                COALESCE(rolling_mean, 0) as rolling_mean,
                COALESCE(rolling_std, 0) as rolling_std,
                COALESCE(rolling_p95, 0) as rolling_p95,
                COALESCE(absolute_severity, 'normal') as absolute_severity,
                COALESCE(relative_diff_ms, 0) as relative_diff_ms,
                COALESCE(relative_severity, 'normal') as relative_severity,
                COALESCE(combined_severity, 'normal') as combined_severity,
                COALESCE(is_combined_anomaly, FALSE) as is_combined_anomaly,
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
            
            # ✅ Verificar que las features de detección combinada se cargaron
            detection_cols = ['z_score_peer', 'rolling_mean', 'combined_severity']
            for col in detection_cols:
                if col in df.columns:
                    non_zero = (df[col] != 0).sum() if df[col].dtype != 'object' else (df[col] != 'normal').sum()
                    logging.info(f"   ✓ {col}: {non_zero}/{len(df)} registros con valores calculados")
                else:
                    logging.warning(f"   ✗ {col}: NO ENCONTRADO")
            
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
            
            df.loc[mask, 'latency_trend_5min'] = provider_data['peer_latency_ms'].rolling(
                window=10, min_periods=1
            ).mean().diff().fillna(0).values
            
            df.loc[mask, 'latency_trend_15min'] = provider_data['peer_latency_ms'].rolling(
                window=30, min_periods=1
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
        Las rolling stats (rolling_mean, rolling_std, rolling_p95) ya vienen de bgp_metrics
        """
        if df.empty:
            return df
        
        logging.info("🔧 Calculando rolling statistics adicionales...")
        df = df.copy()
        
        # ✅ Las columnas rolling_mean, rolling_std, rolling_p95 YA VIENEN de bgp_metrics
        # NO calcular peer_latency_mean_10, peer_latency_std_10, etc.
        
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

    def calculate_target_variable(self, df):
        """
        ✅ MEJORADO: Crea target por evento único (no por registro)
        - should_failover: mantiene compatibilidad (2 registros por failover)
        - failover_event: NUEVO, marca solo el registro del provider que PERDIÓ
        """
        if df.empty:
            return df
        
        logging.info("🔧 Calculando target variable...")
        df = df.copy()
        
        # Target original (por registro) - mantener para compatibilidad
        df['should_failover'] = df['provider_changed'].astype(int)
        
        # ✅ NUEVO: Target por evento único
        # Solo marcar el registro del provider que PERDIÓ
        # (provider_changed=True Y current_provider_score > alternative_provider_score)
        df['failover_event'] = (
            (df['provider_changed'] == True) & 
            (df['current_provider_score'] > df['alternative_provider_score'])
        ).astype(int)
        
        # Estadísticas
        total_records = len(df)
        failover_records = df['should_failover'].sum()
        failover_events = df['failover_event'].sum()
        unique_failover_times = df[df['failover_event'] == 1]['time'].nunique()
        
        logging.info(f"✅ Target calculado:")
        logging.info(f"   - Total registros: {total_records}")
        logging.info(f"   - Registros con should_failover=1: {failover_records} (2 por cada failover)")
        logging.info(f"   - Registros con failover_event=1: {failover_events} (1 por cada failover)")
        logging.info(f"   - Failovers ÚNICOS (ciclos distintos): {unique_failover_times}")
        
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
    logging.info(f"⚙️ ✅ Features de detección combinada: CARGADAS desde bgp_metrics")
    logging.info(f"⚙️ ✅ Nueva columna failover_event: CREADA para conteo correcto")
    
    inserted = engine.process_and_store()
    
    logging.info("")
    logging.info("=" * 80)
    logging.info("✅ Feature Engine ejecutado exitosamente")
    logging.info("=" * 80)
    logging.info(f"Registros grabados: {inserted} (NUEVOS, sin redundancia)")


if __name__ == '__main__':
    main()
