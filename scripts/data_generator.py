#!/usr/bin/env python3
"""
data_generator.py

Módulo para generar datos sintéticos realistas para entrenamiento de modelos ML

Uso:
    from data_generator import generate_realistic_bgp_data, generate_training_dataset
"""

import numpy as np
import pandas as pd
import psycopg2
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_realistic_bgp_data(n_samples=50000, providers=['PROVIDER1', 'PROVIDER2']):
    """
    Genera datos BGP realistas con patrones naturales
    
    Características:
    ├─ Latencia con variación natural (pequeños picos)
    ├─ Pérdida de paquetes ocasional (no constante)
    ├─ Correlación entre providers
    ├─ Patrones de día/noche
    └─ Fallovers realistas
    
    Args:
        n_samples: número de ciclos a generar
        providers: lista de nombres de providers
    
    Returns:
        DataFrame con columnas: time, provider, peer_latency_ms, ..., should_failover
    """
    
    logger.info(f"📊 Generando {n_samples} ciclos de datos BGP realistas...")
    
    data = []
    base_time = datetime.now() - timedelta(days=30)
    
    # Latencias base por provider (típicas)
    base_latency = {
        'PROVIDER1': 10.0,  # 10ms típico
        'PROVIDER2': 20.0   # 20ms típico
    }
    
    for i in range(n_samples):
        current_time = base_time + timedelta(seconds=30*i)
        hour = current_time.hour
        day_of_week = current_time.weekday()
        
        # Horas pico: 9 AM - 6 PM
        is_peak = 9 <= hour <= 18
        is_weekend = day_of_week >= 5
        
        for provider in providers:
            # ====================================================================
            # LATENCIA: Base + variación natural
            # ====================================================================
            base = base_latency[provider]
            
            # Variación natural (±2ms típica)
            natural_variation = np.random.normal(0, 2)
            
            # Picos ocasionales (5% probabilidad)
            # Simula degradación temporal de red
            if np.random.random() < 0.05:
                peak_variation = np.random.exponential(10)
                latency = base + natural_variation + peak_variation
            else:
                latency = base + natural_variation
            
            latency = max(0.1, latency)
            
            # ====================================================================
            # PÉRDIDA DE PAQUETES: Muy rara en condiciones normales
            # ====================================================================
            if np.random.random() < 0.01:  # 1% probabilidad
                loss = np.random.uniform(0.5, 5.0)  # 0.5-5% loss
            else:
                loss = 0.0
            
            # ====================================================================
            # JITTER: Variabilidad en latencia
            # ====================================================================
            jitter = abs(np.random.normal(0, 1))
            
            # ====================================================================
            # SCORE: Fórmula actual
            # ====================================================================
            loss_penalty = loss * 100
            weighted_latency = (latency * 0.7) + (latency * 0.3)  # peer + dns
            jitter_penalty = jitter * 0.5
            score = weighted_latency + loss_penalty + jitter_penalty
            
            # ====================================================================
            # GROUND TRUTH: ¿Debería haber failover?
            # ====================================================================
            should_failover = False
            
            # Pérdida crítica (>20%) siempre causa failover
            if loss >= 20.0:
                should_failover = True
            # Latencia crítica a veces causa failover (30% probabilidad si es sostenida)
            elif latency >= 25.0 and np.random.random() < 0.3:
                should_failover = True
            
            # ====================================================================
            # AGREGAR REGISTRO
            # ====================================================================
            data.append({
                'time': current_time,
                'provider': provider,
                'peer_latency_ms': round(latency, 2),
                'peer_jitter_ms': round(jitter, 2),
                'peer_loss_pct': round(loss, 2),
                'dns_latency_ms': round(latency * 1.5, 2),  # DNS es ~1.5x peer latency
                'dns_jitter_ms': round(jitter * 1.2, 2),
                'dns_loss_pct': round(loss, 2),  # Pérdida similar
                'score': round(score, 2),
                'hour_of_day': hour,
                'day_of_week': day_of_week,
                'is_peak': int(is_peak),
                'is_weekend': int(is_weekend),
                'should_failover': int(should_failover)
            })
    
    df = pd.DataFrame(data)
    logger.info(f"✅ Generados {len(df)} registros")
    logger.info(f"   Fallovers: {df['should_failover'].sum()} ({df['should_failover'].mean()*100:.1f}%)")
    
    return df


def extract_historical_data(
    days=30,
    timescaledb_host='timescaledb',
    timescaledb_port=5432,
    timescaledb_db='bgp_failover_db',
    timescaledb_user='bgp_app',
    timescaledb_password=None
):
    """
    Extrae datos históricos reales de TimescaleDB para entrenamiento
    
    Args:
        days: número de días de histórico a extraer
        timescaledb_*: parámetros de conexión
    
    Returns:
        DataFrame con datos históricos
    """
    
    if not timescaledb_password:
        raise ValueError("timescaledb_password requerida")
    
    logger.info(f"📊 Extrayendo {days} días de datos históricos de TimescaleDB...")
    
    try:
        conn = psycopg2.connect(
            host=timescaledb_host,
            port=timescaledb_port,
            database=timescaledb_db,
            user=timescaledb_user,
            password=timescaledb_password
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
            EXTRACT(HOUR FROM time)::INTEGER as hour_of_day,
            EXTRACT(DOW FROM time)::INTEGER as day_of_week,
            CASE WHEN EXTRACT(HOUR FROM time) BETWEEN 9 AND 18 THEN 1 ELSE 0 END::INTEGER as is_peak,
            CASE WHEN EXTRACT(DOW FROM time) >= 5 THEN 1 ELSE 0 END::INTEGER as is_weekend,
            -- Ground truth: debería haber failover?
            CASE 
                WHEN peer_loss_pct >= 20 THEN 1
                WHEN peer_latency_ms >= 25 AND RANDOM() < 0.3 THEN 1
                ELSE 0
            END::INTEGER as should_failover
        FROM bgp_metrics
        WHERE time >= NOW() - INTERVAL '{days} days'
        ORDER BY time
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        logger.info(f"✅ Extraídos {len(df)} registros históricos")
        logger.info(f"   Fechas: {df['time'].min()} a {df['time'].max()}")
        logger.info(f"   Fallovers: {df['should_failover'].sum()} ({df['should_failover'].mean()*100:.1f}%)")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ Error extrayendo datos históricos: {e}")
        raise


def generate_training_dataset(
    use_synthetic=True,
    synthetic_samples=30000,
    use_historical=False,
    historical_days=15,
    timescaledb_password=None
):
    """
    Genera dataset mixto: sintético + histórico
    
    Estrategia:
    ├─ Sintético (60-80%): base amplia, variedad de escenarios
    └─ Histórico (20-40%): patrones específicos de tu red
    
    Args:
        use_synthetic: incluir datos sintéticos
        synthetic_samples: número de ciclos sintéticos
        use_historical: incluir datos históricos
        historical_days: días de histórico a extraer
        timescaledb_password: contraseña para TimescaleDB
    
    Returns:
        DataFrame combinado y shuffled
    """
    
    frames = []
    
    # Datos sintéticos
    if use_synthetic:
        logger.info("\n📊 Generando datos SINTÉTICOS...")
        synthetic_df = generate_realistic_bgp_data(n_samples=synthetic_samples)
        frames.append(synthetic_df)
    
    # Datos históricos
    if use_historical:
        logger.info("\n📊 Generando datos HISTÓRICOS...")
        try:
            historical_df = extract_historical_data(
                days=historical_days,
                timescaledb_password=timescaledb_password
            )
            frames.append(historical_df)
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron extraer datos históricos: {e}")
    
    # Combinar datasets
    if not frames:
        raise ValueError("Debes usar al menos use_synthetic=True o use_historical=True")
    
    training_df = pd.concat(frames, ignore_index=True)
    
    # Shuffle para mejor entrenamiento
    training_df = training_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Resumen
    logger.info("\n" + "=" * 80)
    logger.info("✅ DATASET DE ENTRENAMIENTO GENERADO")
    logger.info("=" * 80)
    logger.info(f"\nTotal registros: {len(training_df)}")
    logger.info(f"Providers: {', '.join(training_df['provider'].unique())}")
    logger.info(f"Rango temporal: {training_df['time'].min()} a {training_df['time'].max()}")
    logger.info(f"\nDistribución de target (should_failover):")
    logger.info(f"  No failover: {(training_df['should_failover']==0).sum()} ({(training_df['should_failover']==0).mean()*100:.1f}%)")
    logger.info(f"  Failover:    {(training_df['should_failover']==1).sum()} ({(training_df['should_failover']==1).mean()*100:.1f}%)")
    
    return training_df


def save_training_data_to_timescaledb(
    df,
    version='mixed',
    timescaledb_host='timescaledb',
    timescaledb_port=5432,
    timescaledb_db='bgp_failover_db',
    timescaledb_user='bgp_app',
    timescaledb_password=None
):
    """
    Guarda datos de entrenamiento en TimescaleDB
    
    Args:
        df: DataFrame con datos de entrenamiento
        version: etiqueta del dataset ('synthetic', 'historical', 'mixed')
        timescaledb_*: parámetros de conexión
    """
    
    if not timescaledb_password:
        raise ValueError("timescaledb_password requerida")
    
    logger.info(f"\n💾 Guardando {len(df)} registros en TimescaleDB...")
    
    try:
        conn = psycopg2.connect(
            host=timescaledb_host,
            port=timescaledb_port,
            database=timescaledb_db,
            user=timescaledb_user,
            password=timescaledb_password
        )
        
        cur = conn.cursor()
        
        for idx, row in df.iterrows():
            cur.execute("""
                INSERT INTO ml_training_data (
                    time, provider, peer_latency_ms, peer_jitter_ms, peer_loss_pct,
                    dns_latency_ms, dns_jitter_ms, dns_loss_pct, score,
                    hour_of_day, is_peak, is_weekend, should_failover, dataset_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['time'], row['provider'],
                row['peer_latency_ms'], row['peer_jitter_ms'], row['peer_loss_pct'],
                row['dns_latency_ms'], row['dns_jitter_ms'], row['dns_loss_pct'],
                row['score'],
                int(row['hour_of_day']), int(row['is_peak']), int(row['is_weekend']),
                int(row['should_failover']), version
            ))
            
            if (idx + 1) % 5000 == 0:
                logger.info(f"   {idx+1}/{len(df)} registros insertados...")
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"✅ {len(df)} registros guardados en TimescaleDB")
        
    except Exception as e:
        logger.error(f"❌ Error guardando datos: {e}")
        raise
