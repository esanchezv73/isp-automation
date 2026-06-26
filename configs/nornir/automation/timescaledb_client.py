#!/usr/bin/env python3
"""
TimescaleDB Client FINAL - VERSIÓN COMPLETA CON CONVERSIÓN DE TIPOS
✅ Conversión automática de numpy types a Python nativos
✅ INSERT dinámico robusto
✅ Soporte para todas las columnas de detección combinada
✅ Compatible con NumPy 2.0+
"""
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import os
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_numpy_to_python(value):
    """
    ✅ CORREGIDO: Convierte tipos numpy a tipos nativos de Python
    Compatible con NumPy 2.0+ (np.string_ fue removido)
    """
    # Manejar None y NaN primero
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    
    # Tipos enteros de numpy
    if isinstance(value, (np.integer,)):
        return int(value)
    
    # Tipos flotantes de numpy
    if isinstance(value, (np.floating,)):
        return float(value)
    
    # Tipos booleanos de numpy
    if isinstance(value, (np.bool_,)):
        return bool(value)
    
    # Arrays de numpy
    if isinstance(value, np.ndarray):
        return value.tolist()
    
    # Strings de numpy (compatible con NumPy 2.0)
    # En NumPy 2.0, np.string_ fue removido, usar np.bytes_ en su lugar
    if hasattr(np, 'bytes_') and isinstance(value, np.bytes_):
        return str(value)
    elif hasattr(np, 'str_') and isinstance(value, np.str_):
        return str(value)
    
    # Si no es tipo numpy, retornar tal cual
    return value


def sanitize_metrics_dict(metrics_dict):
    """
    Sanitiza un diccionario de métricas convirtiendo todos los numpy types
    a tipos nativos de Python para que psycopg2 pueda insertarlos correctamente
    """
    sanitized = {}
    for key, value in metrics_dict.items():
        sanitized[key] = convert_numpy_to_python(value)
    return sanitized


class TimescaleDBClient:
    """Cliente TimescaleDB FINAL con conversión automática de tipos"""
    
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
        ✅ CORREGIDO: Insertar métricas BGP con conversión automática de tipos numpy
        Manejo robusto de errores y cursores
        """
        cur = None  # ✅ CORRECCIÓN: Inicializar cursor antes del try
        try:
            # ✅ Sanitizar el diccionario antes de insertar
            sanitized_metrics = sanitize_metrics_dict(metrics)
            
            # ✅ INSERT DINÁMICO: Construir query basado en las claves del diccionario
            columns = list(sanitized_metrics.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            insert_query = f"""
                INSERT INTO bgp_metrics ({column_names})
                VALUES ({placeholders})
            """
            
            values = [sanitized_metrics[col] for col in columns]
            
            cur = self.conn.cursor()
            cur.execute(insert_query, values)
            self.conn.commit()
            
            logger.debug(f"✅ {sanitized_metrics['provider']} Ciclo {sanitized_metrics.get('cycle_number')} Score: {sanitized_metrics['score']:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando métrica: {e}")
            logger.error(f"   Columnas intentadas: {list(metrics.keys())}")
            logger.error(f"   Valores problemáticos:")
            for key, value in metrics.items():
                logger.error(f"     {key}: {type(value).__name__} = {value}")
            
            # ✅ CORRECCIÓN: Solo hacer rollback si la conexión está activa
            if self.conn and not self.conn.closed:
                try:
                    self.conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"❌ Error en rollback: {rollback_error}")
            
            return False
        finally:
            # ✅ CORRECCIÓN: Solo cerrar cursor si fue creado
            if cur is not None:
                try:
                    cur.close()
                except Exception as close_error:
                    logger.debug(f"⚠️ Error cerrando cursor: {close_error}")

    def insert_failover_event(self, event):
        """
        ✅ CORREGIDO: Insertar evento de failover con conversión de tipos
        """
        cur = None  # ✅ Inicializar cursor
        try:
            # ✅ Sanitizar el diccionario
            sanitized_event = sanitize_metrics_dict(event)
            
            # Validación: No insertar eventos falsos
            if sanitized_event.get('previous_provider') == sanitized_event.get('new_provider'):
                logger.warning(f"⚠️ Evento inválido: {sanitized_event['previous_provider']} → {sanitized_event['new_provider']}")
                return False
            
            # ✅ INSERT DINÁMICO
            columns = list(sanitized_event.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            insert_query = f"""
                INSERT INTO bgp_failover_events ({column_names})
                VALUES ({placeholders})
            """
            
            values = [sanitized_event[col] for col in columns]
            
            cur = self.conn.cursor()
            cur.execute(insert_query, values)
            self.conn.commit()
            
            logger.info(f"🔄 Failover: {sanitized_event['previous_provider']} → {sanitized_event['new_provider']} (ciclos: {sanitized_event.get('detection_cycles', '?')})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando failover event: {e}")
            if self.conn and not self.conn.closed:
                try:
                    self.conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"❌ Error en rollback: {rollback_error}")
            return False
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception as close_error:
                    logger.debug(f"⚠️ Error cerrando cursor: {close_error}")

    def insert_batch_metrics(self, metrics_list):
        """✅ CORREGIDO: Insertar batch de métricas con conversión de tipos"""
        cur = None  # ✅ Inicializar cursor
        try:
            # ✅ Sanitizar todas las métricas
            sanitized_list = [sanitize_metrics_dict(m) for m in metrics_list]
            
            if not sanitized_list:
                return True
            
            # Usar las claves del primer registro
            columns = list(sanitized_list[0].keys())
            column_names = ", ".join(columns)
            
            # Construir lista de tuplas
            data = [
                tuple(m.get(col) for col in columns)
                for m in sanitized_list
            ]
            
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"""
                INSERT INTO bgp_metrics ({column_names})
                VALUES %s
            """
            
            cur = self.conn.cursor()
            execute_values(cur, insert_query, data)
            self.conn.commit()
            
            logger.info(f"✅ Batch de {len(metrics_list)} métricas insertadas")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en batch insert: {e}")
            if self.conn and not self.conn.closed:
                try:
                    self.conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"❌ Error en rollback: {rollback_error}")
            return False
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception as close_error:
                    logger.debug(f"⚠️ Error cerrando cursor: {close_error}")

    def get_latest_metrics(self, provider, limit=10):
        """Obtener últimas métricas"""
        cur = None
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
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass

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
        if self.conn and not self.conn.closed:
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
    
    # ✅ Test con métrica que incluye numpy types
    sample_metric = {
        'time': datetime.utcnow(),
        'provider': 'PROVIDER1',
        'peer_ip': '2001:db8:ffaa::255',
        'peer_asn': np.int64(65002),  # ← numpy type
        'peer_latency_ms': np.float64(12.5),  # ← numpy type
        'peer_jitter_ms': np.float64(3.2),
        'peer_loss_pct': np.float64(0.0),
        'dns_latency_ms': np.float64(25.3),
        'dns_jitter_ms': np.float64(8.1),
        'dns_loss_pct': np.float64(0.0),
        'score': np.float64(23.7),
        'weighted_latency': np.float64(20.5),
        'loss_penalty': np.float64(0.0),
        'jitter_penalty': np.float64(2.8),
        'current_provider': 'PROVIDER1',
        'provider_changed': np.bool_(False),  # ← numpy bool
        'provider_change_reason': '',
        'degradation_cycle': np.int64(0),
        'sustained_degradation': np.bool_(False),
        'quality_status': 'excellent',
        'cycle_number': np.int64(1),
        # ✅ Nuevas columnas de detección combinada
        'z_score_peer': np.float64(1.5),
        'z_score_severity': 'normal',
        'rolling_mean': np.float64(10.5),
        'rolling_std': np.float64(2.3),
        'rolling_p95': np.float64(15.2),
        'absolute_severity': 'warning',
        'relative_diff_ms': np.float64(5.8),
        'relative_severity': 'warning',
        'combined_severity': 'warning',
        'is_combined_anomaly': np.bool_(False)
    }
    
    result = client.insert_bgp_metrics(sample_metric)
    
    if result:
        print("\n📊 Últimas métricas de PROVIDER1:")
        metrics = client.get_latest_metrics('PROVIDER1', limit=5)
        for metric in metrics:
            print(f"  {metric[0]} - Score: {metric[3]:.2f}")
    
    client.close()
    print("\n✅ Test completado!")
