#!/usr/bin/env python3
"""
Test simple: Enviar datos de prueba a TimescaleDB
"""

from datetime import datetime, timedelta
from timescaledb_client import TimescaleDBClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_basic_insert():
    """Test 1: Inserción básica de una métrica"""
    
    logger.info("=" * 60)
    logger.info("TEST 1: Inserción Básica")
    logger.info("=" * 60)
    
    client = TimescaleDBClient()
    
    # Métrica simple
    metric = {
        'time': datetime.utcnow(),
        'provider': 'PROVIDER1',
        'peer_latency_ms': 10.5,
        'peer_jitter_ms': 2.1,
        'peer_loss_pct': 0.0,
        'dns_latency_ms': 20.3,
        'dns_jitter_ms': 5.2,
        'dns_loss_pct': 0.0,
        'score': 20.8,
        'current_provider': 'PROVIDER1',
        'provider_changed': False,
        'quality_status': 'excellent'
    }
    
    result = client.insert_bgp_metrics(metric)
    logger.info(f"Resultado: {'✅ EXITOSO' if result else '❌ FALLIDO'}")
    
    client.close()


def test_multiple_inserts():
    """Test 2: Inserción de múltiples métricas"""
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Inserción Múltiple (Batch)")
    logger.info("=" * 60)
    
    client = TimescaleDBClient()
    
    # Simular 5 ciclos de métricas
    metrics_list = []
    base_time = datetime.utcnow()
    
    for i in range(5):
        # PROVIDER1
        metrics_list.append({
            'time': base_time - timedelta(seconds=30*i),
            'provider': 'PROVIDER1',
            'peer_latency_ms': 12.5 + (i * 0.5),  # Aumenta gradualmente
            'peer_jitter_ms': 3.2 + (i * 0.1),
            'peer_loss_pct': 0.0,
            'dns_latency_ms': 25.3,
            'dns_jitter_ms': 8.1,
            'dns_loss_pct': 0.0,
            'score': 23.7 + (i * 0.3),
            'current_provider': 'PROVIDER1',
            'provider_changed': False,
            'quality_status': 'excellent' if i < 3 else 'good'
        })
        
        # PROVIDER2
        metrics_list.append({
            'time': base_time - timedelta(seconds=30*i),
            'provider': 'PROVIDER2',
            'peer_latency_ms': 28.4 - (i * 0.2),  # Mejora gradualmente
            'peer_jitter_ms': 10.5 - (i * 0.3),
            'peer_loss_pct': 1.0 - (i * 0.1),
            'dns_latency_ms': 35.2,
            'dns_jitter_ms': 12.3,
            'dns_loss_pct': 0.5,
            'score': 32.1 - (i * 0.5),
            'current_provider': 'PROVIDER1',
            'provider_changed': False,
            'quality_status': 'good'
        })
    
    result = client.insert_batch_metrics(metrics_list)
    logger.info(f"Resultado: {'✅ EXITOSO' if result else '❌ FALLIDO'}")
    
    client.close()


def test_failover_event():
    """Test 3: Registrar evento de failover"""
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Evento de Failover")
    logger.info("=" * 60)
    
    client = TimescaleDBClient()
    
    event = {
        'previous_provider': 'PROVIDER1',
        'new_provider': 'PROVIDER2',
        'change_reason': 'sustained_degradation',
        'previous_provider_score': 45.2,
        'new_provider_score': 28.4,
        'score_improvement': 0,  # Se calcula en la función
        'detected_by': 'sustained_degradation_3_cycles'
    }
    
    result = client.insert_failover_event(event)
    logger.info(f"Resultado: {'✅ EXITOSO' if result else '❌ FALLIDO'}")
    
    client.close()


def test_read_data():
    """Test 4: Leer datos insertados"""
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Lectura de Datos")
    logger.info("=" * 60)
    
    client = TimescaleDBClient()
    
    logger.info("\n📊 Últimas 5 métricas de PROVIDER1:")
    ixa_metrics = client.get_latest_metrics('PROVIDER1', limit=5)
    
    if ixa_metrics:
        for i, metric in enumerate(ixa_metrics, 1):
            logger.info(f"  {i}. {metric[0]} - Score: {metric[3]:.2f} - {metric[4]}")
    else:
        logger.warning("  No hay datos disponibles")
    
    logger.info("\n📊 Últimas 5 métricas de PROVIDER2:")
    ufinet_metrics = client.get_latest_metrics('PROVIDER2', limit=5)
    
    if ufinet_metrics:
        for i, metric in enumerate(ufinet_metrics, 1):
            logger.info(f"  {i}. {metric[0]} - Score: {metric[3]:.2f} - {metric[4]}")
    else:
        logger.warning("  No hay datos disponibles")
    
    client.close()


def main():
    """Ejecutar todos los tests"""
    
    logger.info("🧪 Iniciando tests de TimescaleDB")
    logger.info("=" * 60)
    
    try:
        test_basic_insert()
        test_multiple_inserts()
        test_failover_event()
        test_read_data()
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ TODOS LOS TESTS COMPLETADOS EXITOSAMENTE")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        exit(1)


if __name__ == '__main__':
    main()
