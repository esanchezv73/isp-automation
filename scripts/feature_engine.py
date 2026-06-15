#!/usr/bin/env python3
"""
feature_engine.py

Calcula features derivadas a partir de raw metrics (bgp_metrics)
y las guarda en ml_features (Feature Store)

Uso:
    python3 feature_engine.py
"""

import psycopg2
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FeatureEngine:
    """
    Calcula características derivadas a partir de raw metrics
    y las guarda en ml_features
    """
    
    def __init__(self, timescaledb_password=None):
        if not timescaledb_password:
            timescaledb_password = 'bgp_app_password'
        
        if not timescaledb_password:
            raise ValueError("TIMESCALEDB_PASSWORD requerida")
        
        self.password = timescaledb_password
        self.host = 'timescaledb'
        self.port = 5432
        self.db = 'bgp_failover_db'
        self.user = 'bgp_app'
    
    def load_raw_metrics(self, hours=1):
        """
        Carga raw metrics de bgp_metrics
        
        Args:
            hours: cargar datos de últimas N horas
        
        Returns:
            DataFrame con raw metrics
        """
        
        logger.info(f"📥 Cargando raw metrics de últimas {hours} horas...")
        
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password
        )
        
        query = f"""
        SELECT
            time,
            provider,
            peer_latency_ms,
            peer_jitter_ms,
            peer_loss_pct,
            dns_latency_ms,
            dns_jitter_ms,
            dns_loss_pct,
            score,
            quality_status
        FROM bgp_metrics
        WHERE time >= NOW() - INTERVAL '{hours} hours'
        ORDER BY provider, time
        """
        
        try:
            df = pd.read_sql(query, conn)
            logger.info(f"✅ Cargados {len(df)} registros")
            return df
        finally:
            conn.close()
    
    def calculate_derived_features(self, df):
        """
        Calcula features derivadas a partir de raw metrics
        
        Input: DataFrame con raw metrics
        Output: DataFrame con features derivadas agregadas
        """
        
        logger.info("🔧 Calculando features derivadas...")
        
        df = df.copy()
        df['time'] = pd.to_datetime(df['time'])
        
        # ====================================================================
        # DERIVED FEATURES: Relaciones y combinaciones
        # ====================================================================
        
        # Ratio peer/dns latency
        df['latency_ratio'] = df['peer_latency_ms'] / (df['dns_latency_ms'] + 0.001)
        
        # Pérdida total (promedio)
        df['total_loss_pct'] = (df['peer_loss_pct'] + df['dns_loss_pct']) / 2
        
        # Quality index [0-100]
        # Fórmula: 100 - (weighted_sum of degradations)
        max_latency = 50.0  # ms
        df['quality_index'] = np.clip(
            100 - (
                (df['peer_latency_ms'] / max_latency * 40) +
                (df['total_loss_pct'] * 50) +
                ((df['peer_jitter_ms'] + df['dns_jitter_ms']) / 2 / 10 * 10)
            ),
            0, 100
        )
        
        # ====================================================================
        # TEMPORAL FEATURES: Tendencias
        # ====================================================================
        
        logger.info("🔧 Calculando features temporales...")
        
        for provider in df['provider'].unique():
            mask = df['provider'] == provider
            provider_df = df[mask].sort_values('time').reset_index(drop=True)
            
            # Tendencia últimos 5 minutos (10 muestras × 30s)
            window_5m = min(10, len(provider_df))
            df.loc[mask, 'latency_trend_5min'] = provider_df['peer_latency_ms'].rolling(
                window=window_5m, min_periods=1
            ).mean().diff().fillna(0).values
            
            # Tendencia últimos 15 minutos (30 muestras)
            window_15m = min(30, len(provider_df))
            df.loc[mask, 'latency_trend_15min'] = provider_df['peer_latency_ms'].rolling(
                window=window_15m, min_periods=1
            ).mean().diff().fillna(0).values
            
            # Velocidad (derivative)
            df.loc[mask, 'latency_velocity'] = provider_df['peer_latency_ms'].diff().fillna(0).values
            
            # Aceleración (second derivative)
            df.loc[mask, 'latency_acceleration'] = provider_df['peer_latency_ms'].diff().diff().fillna(0).values
            
            # Spike detection: aumento repentino de pérdida
            df.loc[mask, 'loss_spike_detected'] = (
                provider_df['peer_loss_pct'].diff().fillna(0) > 5.0
            ).astype(bool).values  # ✅ CORRECCIÓN: bool en lugar de int
        
        # ====================================================================
        # ROLLING STATISTICS: Ventanas móviles
        # ====================================================================
        
        logger.info("🔧 Calculando rolling statistics...")
        
        for provider in df['provider'].unique():
            mask = df['provider'] == provider
            provider_df = df[mask].sort_values('time').reset_index(drop=True)
            
            # Últimas 10 muestras (5 minutos = 10 × 30s)
            window = min(10, len(provider_df))
            
            rolling = provider_df['peer_latency_ms'].rolling(window=window, min_periods=1)
            
            df.loc[mask, 'peer_latency_mean_10'] = rolling.mean().values
            df.loc[mask, 'peer_latency_std_10'] = rolling.std().fillna(0).values
            df.loc[mask, 'peer_latency_min_10'] = rolling.min().values
            df.loc[mask, 'peer_latency_max_10'] = rolling.max().values
            df.loc[mask, 'peer_latency_p95_10'] = rolling.apply(
                lambda x: x.quantile(0.95), raw=False
            ).values
        
        return df
    
    def calculate_contextual_features(self, df):
        """
        Calcula features contextuales basadas en tiempo
        """
        
        logger.info("🔧 Calculando features contextuales...")
        
        df = df.copy()
        
        # Extractos de tiempo
        df['hour_of_day'] = df['time'].dt.hour
        df['day_of_week'] = df['time'].dt.weekday
        
        # Business hours: 9 AM - 5 PM, lunes-viernes
        df['is_business_hours'] = (
            (df['hour_of_day'] >= 9) & (df['hour_of_day'] < 17) &
            (df['day_of_week'] < 5)
        ).astype(bool)  # ✅ CORRECCIÓN: bool en lugar de int
        
        # Peak traffic: 10 AM - 2 PM y 3 PM - 6 PM
        df['is_peak_traffic'] = (
            ((df['hour_of_day'] >= 10) & (df['hour_of_day'] < 14)) |
            ((df['hour_of_day'] >= 15) & (df['hour_of_day'] < 18))
        ).astype(bool)  # ✅ CORRECCIÓN: bool en lugar de int
        
        # Weekend
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(bool)  # ✅ CORRECCIÓN: bool en lugar de int
        
        return df
    
    def calculate_target_variable(self, df):
        """
        Calcula target variable para supervised learning
        
        should_failover: ¿debería ocurrir un failover basado en métricas?
        """
        
        logger.info("🔧 Calculando target variable...")
        
        df = df.copy()
        
        # Determinar si debería haber failover
        # Basado en thresholds
        should_failover = (
            (df['peer_loss_pct'] >= 20.0) |  # Pérdida crítica
            (df['peer_latency_ms'] >= 25.0)   # Latencia crítica
        ).astype(int)  # ✅ CORRECCIÓN: int (no bool) - es variable TARGET para ML (0 o 1)
        
        df['should_failover'] = should_failover
        
        # Otros target variables (para evaluación)
        df['was_false_positive'] = 0  # Requeriría datos históricos de decisiones reales
        df['optimal_threshold'] = 0.5  # Requeriría tunning
        
        return df
    
    def process_and_store(self, df):
        """
        Procesa todas las features y guarda en ml_features
        """
        
        logger.info("🔄 Procesando features...")
        
        # 1. Calcular features derivadas
        df = self.calculate_derived_features(df)
        
        # 2. Calcular features contextuales
        df = self.calculate_contextual_features(df)
        
        # 3. Calcular target variable
        df = self.calculate_target_variable(df)
        
        # 4. Validar valores NULL
        logger.info("✓ Validando datos...")
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"⚠️ Encontrados {null_counts.sum()} valores NULL, rellenando...")
            df = df.fillna(0)
        
        # 5. Guardar en ml_features
        logger.info("💾 Guardando en ml_features...")
        self._save_to_ml_features(df)
        
        logger.info(f"✅ Procesados {len(df)} registros")
        return df
    
    def _save_to_ml_features(self, df):
        """
        Guarda datos en tabla ml_features
        """
        
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password
        )
        
        cur = conn.cursor()
        
        columns = [
            'time', 'provider',
            'peer_latency_ms', 'dns_latency_ms', 'peer_loss_pct', 'dns_loss_pct',
            'peer_jitter_ms', 'dns_jitter_ms', 'score',
            'latency_ratio', 'total_loss_pct', 'quality_index',
            'latency_trend_5min', 'latency_trend_15min', 'latency_velocity',
            'latency_acceleration', 'loss_spike_detected',
            'peer_latency_mean_10', 'peer_latency_std_10',
            'peer_latency_min_10', 'peer_latency_max_10', 'peer_latency_p95_10',
            'hour_of_day', 'day_of_week', 'is_business_hours', 'is_peak_traffic',
            'is_weekend',
            'should_failover'
        ]
        
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f"""
        INSERT INTO ml_features ({', '.join(columns)})
        VALUES ({placeholders})
        """
        
        for idx, row in df.iterrows():
            values = [row.get(col, 0) for col in columns]
            
            try:
                cur.execute(insert_query, values)
            except Exception as e:
                logger.warning(f"Error insertando fila {idx}: {e}")
                continue
            
            if (idx + 1) % 100 == 0:
                logger.info(f"  {idx+1}/{len(df)} registros...")
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"✅ {len(df)} registros guardados en ml_features")
    
    def run(self, hours=1):
        """
        Ejecuta el pipeline completo
        """
        
        logger.info("=" * 80)
        logger.info("🔧 Feature Engine: Calculando features derivadas")
        logger.info("=" * 80)
        
        try:
            # 1. Cargar raw metrics
            df = self.load_raw_metrics(hours=hours)
            
            if df.empty:
                logger.warning("⚠️ No hay datos para procesar")
                return
            
            # 2. Procesar y guardar
            df_processed = self.process_and_store(df)
            
            logger.info("\n" + "=" * 80)
            logger.info("✅ Feature Engine ejecutado exitosamente")
            logger.info("=" * 80)
            
            # 3. Mostrar resumen
            logger.info(f"\nResumen:")
            logger.info(f"  Registros procesados: {len(df_processed)}")
            logger.info(f"  Providers: {df_processed['provider'].unique()}")
            logger.info(f"  Fechas: {df_processed['time'].min()} a {df_processed['time'].max()}")
            logger.info(f"  Fallovers detectados: {df_processed['should_failover'].sum()}")
            
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
            raise


def main():
    """
    Punto de entrada
    """
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Feature Engine: Calcula features derivadas para ml_features'
    )
    parser.add_argument(
        '--hours', type=int, default=1,
        help='Cargar datos de últimas N horas (default: 1)'
    )
    parser.add_argument(
        '--password', type=str,
        help='TimescaleDB password (o usar TIMESCALEDB_PASSWORD env var)'
    )
    
    args = parser.parse_args()
    
    try:
        engine = FeatureEngine(timescaledb_password=args.password)
        engine.run(hours=args.hours)
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
