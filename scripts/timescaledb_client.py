#!/usr/bin/env python3
"""
TimescaleDB Client FINAL - VERSIÓN COMPLETA

Incluye:
✅ peer_asn
✅ provider_change_reason
✅ degradation_cycle
✅ sustained_degradation
✅ detection_cycles en failover events
✅ Validación de cambios reales
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimescaleDBClient:
    """Cliente TimescaleDB FINAL con todos los campos"""
    
    def __init__(self, host='timescaledb', port=5432, database='bgp_failover_db',
                 user='bgp_app', password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password or 'bgp_app_password'
        
        if not self.password:
            raise ValueError("password requerida")
        
        self.conn = None
        self.connect()
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password
            )
            logger.info(f"✅ Conectado a TimescaleDB en {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ Error conectando a TimescaleDB: {e}")
            raise
    
    def insert_bgp_metrics(self, metrics):
        """
        Insertar métricas BGP COMPLETAS con TODOS los campos
        
        Campos:
        ✅ time, provider, peer_ip, peer_asn
        ✅ peer_latency_ms, peer_jitter_ms, peer_loss_pct
        ✅ dns_latency_ms, dns_jitter_ms, dns_loss_pct
        ✅ score, weighted_latency, loss_penalty, jitter_penalty
        ✅ current_provider, provider_changed, provider_change_reason
        ✅ degradation_cycle, sustained_degradation
        ✅ quality_status, cycle_number
        """
        try:
            cur = self.conn.cursor()
            
            insert_query = """
                INSERT INTO bgp_metrics (
                    time,
                    provider,
                    peer_ip,
                    peer_asn,
                    peer_latency_ms,
                    peer_jitter_ms,
                    peer_loss_pct,
                    dns_latency_ms,
                    dns_jitter_ms,
                    dns_loss_pct,
                    score,
                    weighted_latency,
                    loss_penalty,
                    jitter_penalty,
                    current_provider,
                    provider_changed,
                    provider_change_reason,
                    degradation_cycle,
                    sustained_degradation,
                    quality_status,
                    cycle_number
                ) VALUES (
                    %(time)s,
                    %(provider)s,
                    %(peer_ip)s,
                    %(peer_asn)s,
                    %(peer_latency_ms)s,
                    %(peer_jitter_ms)s,
                    %(peer_loss_pct)s,
                    %(dns_latency_ms)s,
                    %(dns_jitter_ms)s,
                    %(dns_loss_pct)s,
                    %(score)s,
                    %(weighted_latency)s,
                    %(loss_penalty)s,
                    %(jitter_penalty)s,
                    %(current_provider)s,
                    %(provider_changed)s,
                    %(provider_change_reason)s,
                    %(degradation_cycle)s,
                    %(sustained_degradation)s,
                    %(quality_status)s,
                    %(cycle_number)s
                )
            """
            
            cur.execute(insert_query, metrics)
            self.conn.commit()
            
            logger.debug(f"✅ {metrics['provider']} Ciclo {metrics.get('cycle_number')} Score: {metrics['score']:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando métrica: {e}")
            self.conn.rollback()
            return False
        
        finally:
            cur.close()
    
    def insert_failover_event(self, event):
        """
        Insertar evento de failover CON detection_cycles
        
        Validación: previous_provider != new_provider
        """
        try:
            # ✅ VALIDACIÓN: No insertar eventos falsos (PROVIDER1 → PROVIDER1)
            if event.get('previous_provider') == event.get('new_provider'):
                logger.warning(f"⚠️ Evento inválido: {event['previous_provider']} → {event['new_provider']} (sin cambio)")
                return False
            
            cur = self.conn.cursor()
            
            insert_query = """
                INSERT INTO bgp_failover_events (
                    previous_provider,
                    new_provider,
                    change_reason,
                    previous_provider_score,
                    new_provider_score,
                    score_improvement,
                    detection_cycles,
                    detected_by
                ) VALUES (
                    %(previous_provider)s,
                    %(new_provider)s,
                    %(change_reason)s,
                    %(previous_provider_score)s,
                    %(new_provider_score)s,
                    %(score_improvement)s,
                    %(detection_cycles)s,
                    %(detected_by)s
                )
            """
            
            event['score_improvement'] = event['previous_provider_score'] - event['new_provider_score']
            
            cur.execute(insert_query, event)
            self.conn.commit()
            
            logger.info(f"🔄 Failover: {event['previous_provider']} → {event['new_provider']} (ciclos: {event.get('detection_cycles', '?')})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando failover event: {e}")
            self.conn.rollback()
            return False
        
        finally:
            cur.close()
    
    def insert_batch_metrics(self, metrics_list):
        """Insertar batch de métricas (más eficiente)"""
        try:
            cur = self.conn.cursor()
            
            data = [
                (
                    m['time'],
                    m['provider'],
                    m.get('peer_ip', ''),
                    m.get('peer_asn'),
                    m['peer_latency_ms'],
                    m['peer_jitter_ms'],
                    m['peer_loss_pct'],
                    m['dns_latency_ms'],
                    m['dns_jitter_ms'],
                    m['dns_loss_pct'],
                    m['score'],
                    m.get('weighted_latency', 0),
                    m.get('loss_penalty', 0),
                    m.get('jitter_penalty', 0),
                    m['current_provider'],
                    m['provider_changed'],
                    m.get('provider_change_reason', ''),
                    m.get('degradation_cycle', 0),
                    m.get('sustained_degradation', False),
                    m['quality_status'],
                    m.get('cycle_number', 0)
                )
                for m in metrics_list
            ]
            
            insert_query = """
                INSERT INTO bgp_metrics (
                    time, provider, peer_ip, peer_asn, peer_latency_ms, peer_jitter_ms,
                    peer_loss_pct, dns_latency_ms, dns_jitter_ms, dns_loss_pct, score,
                    weighted_latency, loss_penalty, jitter_penalty, current_provider,
                    provider_changed, provider_change_reason, degradation_cycle,
                    sustained_degradation, quality_status, cycle_number
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
        """Obtener últimas métricas"""
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
        """Verificar salud de conexión"""
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


if __name__ == '__main__':
    import os
    
    password = os.environ.get('TIMESCALEDB_PASSWORD')
    if not password:
        print("❌ TIMESCALEDB_PASSWORD no definida")
        exit(1)
    
    client = TimescaleDBClient(
        host='timescaledb',
        database='bgp_failover_db',
        user='bgp_app',
        password=password
    )
    
    if not client.health_check():
        logger.error("No se pudo conectar")
        exit(1)
    
    logger.info("✅ Conexión verificada")
    
    # Insertar métrica COMPLETA
    sample_metric = {
        'time': datetime.utcnow(),
        'provider': 'PROVIDER1',
        'peer_ip': '2001:db8:ffaa::255',
        'peer_asn': 65002,
        'peer_latency_ms': 12.5,
        'peer_jitter_ms': 3.2,
        'peer_loss_pct': 0.0,
        'dns_latency_ms': 25.3,
        'dns_jitter_ms': 8.1,
        'dns_loss_pct': 0.0,
        'score': 23.7,
        'weighted_latency': 20.5,
        'loss_penalty': 0.0,
        'jitter_penalty': 2.8,
        'current_provider': 'PROVIDER1',
        'provider_changed': False,
        'provider_change_reason': '',
        'degradation_cycle': 0,
        'sustained_degradation': False,
        'quality_status': 'excellent',
        'cycle_number': 1
    }
    
    client.insert_bgp_metrics(sample_metric)
    
    sample_metric['provider'] = 'PROVIDER2'
    sample_metric['peer_ip'] = '2001:db8:ffac::255'
    sample_metric['peer_asn'] = 65003
    sample_metric['cycle_number'] = 2
    client.insert_bgp_metrics(sample_metric)
    
    print("\n📊 Últimas métricas de PROVIDER1:")
    metrics = client.get_latest_metrics('PROVIDER1', limit=5)
    for metric in metrics:
        print(f"  {metric[0]} - Score: {metric[3]:.2f}")
    
    client.close()
    print("\n✅ Test completado!")
