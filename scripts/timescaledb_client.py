#!/usr/bin/env python3
"""
TimescaleDB Client for BGP Failover Metrics

Envía métricas desde el script bgp_failover_engine.py a TimescaleDB
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import json

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimescaleDBClient:
    """Cliente para escribir métricas en TimescaleDB"""
    
    def __init__(self, host='timescaledb', port=5432, database='bgp_failover_db',
                 user='bgp_app', password='bgp_app_password'):
        """
        Inicializar conexión a TimescaleDB
        
        Args:
            host: Hostname de TimescaleDB
            port: Puerto (default 5432)
            database: Nombre de base de datos
            user: Usuario PostgreSQL
            password: Contraseña
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establecer conexión a TimescaleDB"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"✅ Conectado a TimescaleDB en {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"❌ Error conectando a TimescaleDB: {e}")
            raise
    
    def insert_bgp_metrics(self, metrics):
        """
        Insertar métricas BGP en TimescaleDB
        
        Args:
            metrics (dict): Diccionario con métricas BGP
                {
                    'time': datetime,
                    'provider': 'PROVIDER1',
                    'peer_latency_ms': 12.5,
                    'peer_jitter_ms': 3.2,
                    'peer_loss_pct': 0.0,
                    'dns_latency_ms': 25.3,
                    'dns_jitter_ms': 8.1,
                    'dns_loss_pct': 0.0,
                    'score': 23.7,
                    'current_provider': 'PROVIDER1',
                    'provider_changed': False,
                    'quality_status': 'excellent'
                }
        
        Returns:
            bool: True si inserción exitosa, False si error
        """
        try:
            cur = self.conn.cursor()
            
            # Preparar query de inserción
            insert_query = """
                INSERT INTO bgp_metrics (
                    time,
                    provider,
                    peer_latency_ms,
                    peer_jitter_ms,
                    peer_loss_pct,
                    dns_latency_ms,
                    dns_jitter_ms,
                    dns_loss_pct,
                    score,
                    current_provider,
                    provider_changed,
                    quality_status
                ) VALUES (
                    %(time)s,
                    %(provider)s,
                    %(peer_latency_ms)s,
                    %(peer_jitter_ms)s,
                    %(peer_loss_pct)s,
                    %(dns_latency_ms)s,
                    %(dns_jitter_ms)s,
                    %(dns_loss_pct)s,
                    %(score)s,
                    %(current_provider)s,
                    %(provider_changed)s,
                    %(quality_status)s
                )
            """
            
            # Ejecutar inserción
            cur.execute(insert_query, metrics)
            self.conn.commit()
            
            logger.info(f"✅ Métrica insertada: {metrics['provider']} - Score: {metrics['score']:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando métrica: {e}")
            self.conn.rollback()
            return False
        
        finally:
            cur.close()
    
    def insert_failover_event(self, event):
        """
        Insertar evento de failover
        
        Args:
            event (dict): Evento de failover
                {
                    'previous_provider': 'PROVIDER1',
                    'new_provider': 'PROVIDER2',
                    'change_reason': 'sustained_degradation',
                    'previous_provider_score': 45.2,
                    'new_provider_score': 23.7,
                    'detected_by': 'sustained_degradation'
                }
        """
        try:
            cur = self.conn.cursor()
            
            insert_query = """
                INSERT INTO bgp_failover_events (
                    previous_provider,
                    new_provider,
                    change_reason,
                    previous_provider_score,
                    new_provider_score,
                    score_improvement,
                    detected_by
                ) VALUES (
                    %(previous_provider)s,
                    %(new_provider)s,
                    %(change_reason)s,
                    %(previous_provider_score)s,
                    %(new_provider_score)s,
                    %(score_improvement)s,
                    %(detected_by)s
                )
            """
            
            # Calcular mejora en score
            event['score_improvement'] = event['previous_provider_score'] - event['new_provider_score']
            
            cur.execute(insert_query, event)
            self.conn.commit()
            
            logger.info(f"🔄 Failover registrado: {event['previous_provider']} → {event['new_provider']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando failover event: {e}")
            self.conn.rollback()
            return False
        
        finally:
            cur.close()
    
    def insert_batch_metrics(self, metrics_list):
        """
        Insertar múltiples métricas (batch insert, más eficiente)
        
        Args:
            metrics_list: Lista de diccionarios de métricas
        """
        try:
            cur = self.conn.cursor()
            
            # Preparar datos
            data = [
                (
                    m['time'],
                    m['provider'],
                    m['peer_latency_ms'],
                    m['peer_jitter_ms'],
                    m['peer_loss_pct'],
                    m['dns_latency_ms'],
                    m['dns_jitter_ms'],
                    m['dns_loss_pct'],
                    m['score'],
                    m['current_provider'],
                    m['provider_changed'],
                    m['quality_status']
                )
                for m in metrics_list
            ]
            
            insert_query = """
                INSERT INTO bgp_metrics (
                    time, provider, peer_latency_ms, peer_jitter_ms, peer_loss_pct,
                    dns_latency_ms, dns_jitter_ms, dns_loss_pct, score, 
                    current_provider, provider_changed, quality_status
                ) VALUES %s
            """
            
            execute_values(cur, insert_query, data)
            self.conn.commit()
            
            logger.info(f"✅ Batch de {len(metrics_list)} métricas insertadas")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en batch insert: {e}")
            self.conn.rollback()
            return False
        
        finally:
            cur.close()
    
    def get_latest_metrics(self, provider, limit=10):
        """
        Obtener últimas métricas de un provider
        
        Args:
            provider: Nombre del provider (PROVIDER1, PROVIDER2)
            limit: Número de registros a obtener
        
        Returns:
            Lista de tuples con datos
        """
        try:
            cur = self.conn.cursor()
            
            query = """
                SELECT time, provider, peer_latency_ms, score, quality_status
                FROM bgp_metrics
                WHERE provider = %s
                ORDER BY time DESC
                LIMIT %s
            """
            
            cur.execute(query, (provider, limit))
            results = cur.fetchall()
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo métricas: {e}")
            return []
        
        finally:
            cur.close()
    
    def health_check(self):
        """
        Verificar salud de la conexión
        
        Returns:
            bool: True si conexión está activa
        """
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT 1')
            cur.fetchone()
            cur.close()
            return True
        except:
            return False
    
    def close(self):
        """Cerrar conexión"""
        if self.conn:
            self.conn.close()
            logger.info("Conexión a TimescaleDB cerrada")


# ============================================================================
# EJEMPLO DE USO
# ============================================================================

if __name__ == '__main__':
    """Script de prueba"""
    
    # Crear cliente
    client = TimescaleDBClient(
        host='timescaledb',
        database='bgp_failover_db',
        user='bgp_app',
        password='bgp_app_password'
    )
    
    # Verificar salud
    if not client.health_check():
        logger.error("No se pudo conectar a TimescaleDB")
        exit(1)
    
    logger.info("✅ Conexión verificada")
    
    # Insertar métrica simple
    sample_metric = {
        'time': datetime.utcnow(),
        'provider': 'PROVIDER1',
        'peer_latency_ms': 12.5,
        'peer_jitter_ms': 3.2,
        'peer_loss_pct': 0.0,
        'dns_latency_ms': 25.3,
        'dns_jitter_ms': 8.1,
        'dns_loss_pct': 0.0,
        'score': 23.7,
        'current_provider': 'PROVIDER1',
        'provider_changed': False,
        'quality_status': 'excellent'
    }
    
    # Enviar métrica
    client.insert_bgp_metrics(sample_metric)
    
    # Insertar métrica del segundo provider
    sample_metric['provider'] = 'PROVIDER2'
    sample_metric['score'] = 28.4
    sample_metric['quality_status'] = 'good'
    client.insert_bgp_metrics(sample_metric)
    
    # Obtener últimas métricas
    print("\n📊 Últimas métricas de PROVIDER1:")
    metrics = client.get_latest_metrics('PROVIDER1', limit=5)
    for metric in metrics:
        print(f"  {metric[0]} - Score: {metric[3]:.2f} - Status: {metric[4]}")
    
    # Cerrar conexión
    client.close()
    
    print("\n✅ Test completado exitosamente!")
