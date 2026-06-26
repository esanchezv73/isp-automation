#!/usr/bin/env python3
"""
BGP Failover Engine VERSIÓN FINAL CON DETECCIÓN COMBINADA
- Lógica de switch_margin CORRECTA
- Persistencia de cycle_number y current_provider
- TimescaleDB integrado
- ✅ DETECCIÓN COMBINADA: Z-score + Absolute + Relative
- ✅ Conversión automática de tipos numpy
- ✅ INSERT dinámico en TimescaleDB
"""
import requests
import time
import logging
import subprocess
import json
import os
import math
import numpy as np
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

try:
    from timescaledb_client import TimescaleDBClient
    TIMESCALEDB_AVAILABLE = True
except ImportError:
    TIMESCALEDB_AVAILABLE = False

try:
    from bgp_failover_config import (
        LATENCY_THRESHOLDS, MTR_CONFIG, MTR_DESTINATIONS, PEER_IPS, IP_VERSIONS,
        PROVIDERS, CYCLE_INTERVAL, POLICY_RULE_IDS
    )
except ImportError:
    MTR_CONFIG = {'count': 5, 'timeout': 30, 'packet_size': 64, 'interval': 0.5}
    MTR_DESTINATIONS = {'PROVIDER1': '2001:db8:8888::100', 'PROVIDER2': '2001:db8:4444::100'}
    PEER_IPS = {'PROVIDER1': '2001:db8:ffaa::255', 'PROVIDER2': '2001:db8:ffac::255'}
    IP_VERSIONS = {'PROVIDER1': '6', 'PROVIDER2': '6'}
    PROVIDERS = ['PROVIDER1', 'PROVIDER2']
    LATENCY_THRESHOLDS = {
        'peer_warning': 12, 'peer_critical': 25, 'dns_warning': 15,
        'dns_critical': 30, 'switch_margin': 5
    }
    CYCLE_INTERVAL = 30
    POLICY_RULE_IDS = {
        'EXPORT-TO-PROVIDER1': 1, 'EXPORT-TO-PROVIDER2': 2,
        'SET-LOCAL-PREF-PROVIDER1': 3, 'SET-LOCAL-PREF-PROVIDER2': 4
    }

NETBOX_URL = "http://192.168.0.140:8000"
NETBOX_TOKEN = "rDbitCHC2V3fQy2Ksmr1pRuagb7pCc2qXCYz7qEp"
DRY_RUN = True
ELASTICSEARCH_ENABLED = True
ELASTICSEARCH_URL = "http://172.90.90.9:9200"
ELASTICSEARCH_INDEX = "bgp-failover"
TIMESCALEDB_ENABLED = True
TIMESCALEDB_HOST = 'timescaledb'
TIMESCALEDB_PORT = 5432
TIMESCALEDB_DB = 'bgp_failover_db'
TIMESCALEDB_USER = 'bgp_app'
TIMESCALEDB_PASSWORD = 'bgp_app_password'
SUSTAINED_DEGRADATION_CYCLES = 3
IMMEDIATE_FAILOVER_PACKET_LOSS = 20.0

# ✅ Umbrales de Z-score
Z_SCORE_THRESHOLDS = {
    'normal': 2.0,
    'warning': 2.5,
    'degraded': 3.0,
    'critical': 3.5
}

# ✅ Umbrales ABSOLUTOS de latencia
ABSOLUTE_LATENCY_THRESHOLDS = {
    'peer_warning': 12.0,
    'peer_degraded': 15.0,
    'peer_critical': 25.0,
    'dns_warning': 15.0,
    'dns_degraded': 20.0,
    'dns_critical': 30.0
}

# ✅ Umbrales de DIFERENCIA RELATIVA
RELATIVE_DIFF_THRESHOLDS = {
    'warning': 5.0,
    'degraded': 10.0,
    'critical': 15.0
}

ROLLING_HISTORY_SIZE = 10


@dataclass
class LatencyMetrics:
    """Métricas de latencia"""
    peer_avg: float
    peer_loss: float
    dns_avg: float
    dns_loss: float
    peer_stddev: float
    dns_stddev: float

    @property
    def is_healthy(self) -> bool:
        return self.peer_loss < IMMEDIATE_FAILOVER_PACKET_LOSS and self.dns_loss < IMMEDIATE_FAILOVER_PACKET_LOSS

    @property
    def has_latency_warning(self) -> bool:
        return self.peer_avg >= LATENCY_THRESHOLDS['peer_warning'] or self.dns_avg >= LATENCY_THRESHOLDS['dns_warning']

    @property
    def has_latency_critical(self) -> bool:
        return self.peer_avg >= LATENCY_THRESHOLDS['peer_critical'] or self.dns_avg >= LATENCY_THRESHOLDS['dns_critical']

    @property
    def has_packet_loss(self) -> bool:
        return self.peer_loss > 0.0 or self.dns_loss > 0.0

    @property
    def quality_score(self) -> float:
        loss_penalty = (self.peer_loss + self.dns_loss) * 10
        weighted_latency = (self.peer_avg * 0.7) + (self.dns_avg * 0.3)
        jitter_penalty = (self.peer_stddev + self.dns_stddev) * 0.5
        return weighted_latency + loss_penalty + jitter_penalty


class ElasticsearchClient:
    """Cliente para Elasticsearch"""
    def __init__(self, url: str, index_prefix: str):
        self.url = url
        self.index_prefix = index_prefix
        self.enabled = ELASTICSEARCH_ENABLED
        self.session = requests.Session()
        if self.enabled:
            self._verify_connection()

    def _verify_connection(self):
        try:
            response = self.session.get(f"{self.url}/_cluster/health", timeout=5)
            if response.status_code == 200:
                logging.info(f"✅ Conectado a Elasticsearch: {self.url}")
                health_data = response.json()
                logging.info(f"   Cluster: {health_data.get('cluster_name', 'unknown')}, Status: {health_data.get('status', 'unknown')}")
            else:
                logging.warning(f"⚠️ Elasticsearch status: {response.status_code}")
                self.enabled = False
        except requests.exceptions.ConnectionError as e:
            logging.error(f"❌ No se pudo conectar a Elasticsearch: {e}")
            self.enabled = False
        except Exception as e:
            logging.warning(f"⚠️ Error verificando Elasticsearch: {e}")
            self.enabled = False

    def send_unified_metrics(self, cycle_data: Dict[str, Any], all_providers_metrics: Dict[str, Dict]):
        if not self.enabled:
            return
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            doc = {
                "@timestamp": timestamp,
                "cycle": cycle_data["cycle"],
                "current_provider": cycle_data["current_provider"],
                "provider_changed": cycle_data["provider_changed"],
            }
            if cycle_data["provider_changed"]:
                doc["previous_provider"] = cycle_data.get("previous_provider")
                doc["new_provider"] = cycle_data.get("new_provider")
                doc["change_reason"] = cycle_data["change_reason"]

            for provider_name, metrics in all_providers_metrics.items():
                prefix = provider_name.lower()
                doc[f"{prefix}_score"] = metrics["score"]
                doc[f"{prefix}_is_healthy"] = metrics["is_healthy"]
                doc[f"{prefix}_is_primary"] = metrics["is_primary"]
                doc[f"{prefix}_peer_latency_ms"] = metrics["peer_latency_ms"]
                doc[f"{prefix}_peer_jitter_ms"] = metrics["peer_jitter_ms"]
                doc[f"{prefix}_peer_loss_pct"] = metrics["peer_loss_pct"]
                doc[f"{prefix}_dns_latency_ms"] = metrics["dns_latency_ms"]
                doc[f"{prefix}_dns_jitter_ms"] = metrics["dns_jitter_ms"]
                doc[f"{prefix}_dns_loss_pct"] = metrics["dns_loss_pct"]
                doc[f"{prefix}_ip_version"] = metrics["ip_version"]
                # ✅ Métricas de detección COMBINADA
                doc[f"{prefix}_z_score_peer"] = metrics.get("z_score_peer", 0.0)
                doc[f"{prefix}_z_score_severity"] = metrics.get("z_score_severity", "normal")
                doc[f"{prefix}_rolling_mean"] = metrics.get("rolling_mean", 0.0)
                doc[f"{prefix}_rolling_std"] = metrics.get("rolling_std", 0.0)
                doc[f"{prefix}_rolling_p95"] = metrics.get("rolling_p95", 0.0)
                doc[f"{prefix}_absolute_severity"] = metrics.get("absolute_severity", "normal")
                doc[f"{prefix}_relative_diff_ms"] = metrics.get("relative_diff_ms", 0.0)
                doc[f"{prefix}_relative_severity"] = metrics.get("relative_severity", "normal")
                doc[f"{prefix}_combined_severity"] = metrics.get("combined_severity", "normal")
                doc[f"{prefix}_is_combined_anomaly"] = metrics.get("is_combined_anomaly", False)
                doc[f"{prefix}_has_latency_warning"] = metrics["has_latency_warning"]
                doc[f"{prefix}_has_latency_critical"] = metrics["has_latency_critical"]
                doc[f"{prefix}_has_packet_loss"] = metrics["has_packet_loss"]

            fecha = datetime.now(timezone.utc).strftime("%Y.%m.%d")
            index_name = f"{self.index_prefix}-{fecha}"
            url = f"{self.url}/{index_name}/_doc"
            response = self.session.post(url, json=doc, headers={"Content-Type": "application/json"}, timeout=10)
            if response.status_code in [200, 201]:
                logging.info(f"✅ Elasticsearch: Ciclo #{cycle_data['cycle']} enviado a {index_name}")
            else:
                logging.error(f"❌ Error enviando a Elasticsearch ({response.status_code}): {response.text}")
        except Exception as e:
            logging.error(f"❌ Error enviando métricas a Elasticsearch: {e}")


class BGPFailoverEngine:
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer nbt_8gWOf9dUSS7v.{NETBOX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.metrics_history = {provider: [] for provider in PROVIDERS}
        self.es_client = ElasticsearchClient(ELASTICSEARCH_URL, ELASTICSEARCH_INDEX)
        self.ts_client = None
        self.provider_asn_map = {}
        self.provider_peer_ip_map = {}

        if TIMESCALEDB_ENABLED and TIMESCALEDB_AVAILABLE:
            try:
                self.ts_client = TimescaleDBClient(
                    host=TIMESCALEDB_HOST, port=TIMESCALEDB_PORT,
                    database=TIMESCALEDB_DB, user=TIMESCALEDB_USER,
                    password=TIMESCALEDB_PASSWORD
                )
                logging.info("✅ TimescaleDB Client inicializado")
                self._load_provider_config()
            except Exception as e:
                logging.error(f"❌ Error inicializando TimescaleDB: {e}")
                self.ts_client = None

        self.cycle_count = self._load_last_cycle_number()
        self.current_primary_provider = self._load_current_provider()
        self.last_provider = self.current_primary_provider
        self.degradation_counter = 0
        self.better_provider_candidate = None

        logging.info(f"🚀 Engine inicializado:")
        logging.info(f"   Ciclo actual: {self.cycle_count}")
        logging.info(f"   Provider actual: {self.current_primary_provider}")
        logging.info(f"   📊 Z-Score Thresholds: {Z_SCORE_THRESHOLDS}")
        logging.info(f"   📊 Absolute Thresholds (peer): "
                    f"warning={ABSOLUTE_LATENCY_THRESHOLDS['peer_warning']}ms, "
                    f"degraded={ABSOLUTE_LATENCY_THRESHOLDS['peer_degraded']}ms, "
                    f"critical={ABSOLUTE_LATENCY_THRESHOLDS['peer_critical']}ms")
        logging.info(f"   📊 Relative Diff Thresholds: {RELATIVE_DIFF_THRESHOLDS}")
        logging.info(f"   📊 Rolling History: {ROLLING_HISTORY_SIZE} ciclos ({ROLLING_HISTORY_SIZE * CYCLE_INTERVAL}s)")

    def _load_provider_config(self):
        """Carga provider_asn y peer_ip desde TimescaleDB"""
        try:
            if not self.ts_client or not self.ts_client.conn:
                return
            cur = self.ts_client.conn.cursor()
            cur.execute("SELECT provider, peer_asn, peer_ip FROM provider_config")
            for provider, asn, peer_ip in cur.fetchall():
                self.provider_asn_map[provider] = asn
                self.provider_peer_ip_map[provider] = peer_ip
            cur.close()
            logging.info(f"✅ Configuración de {len(self.provider_asn_map)} providers cargada")
        except Exception as e:
            logging.error(f"❌ Error cargando provider_config: {e}")

    def _load_last_cycle_number(self) -> int:
        """Lee el último cycle_number de bgp_metrics"""
        try:
            if not self.ts_client or not self.ts_client.conn:
                logging.warning("⚠️ TimescaleDB no disponible, iniciando desde ciclo 1")
                return 1
            cur = self.ts_client.conn.cursor()
            cur.execute("SELECT COALESCE(MAX(cycle_number), 0) FROM bgp_metrics")
            last_cycle = cur.fetchone()[0]
            cur.close()
            next_cycle = last_cycle + 1
            logging.info(f"✅ Último ciclo en BD: {last_cycle} → Próximo: {next_cycle}")
            return next_cycle
        except Exception as e:
            logging.error(f"⚠️ Error leyendo cycle_number: {e}")
            return 1

    def _load_current_provider(self) -> str:
        """Lee el provider actual del último failover event"""
        try:
            if not self.ts_client or not self.ts_client.conn:
                logging.warning("⚠️ TimescaleDB no disponible, usando PROVIDER1")
                return "PROVIDER1"
            cur = self.ts_client.conn.cursor()
            cur.execute("""
                SELECT new_provider
                FROM bgp_failover_events
                ORDER BY event_id DESC
                LIMIT 1
            """)
            result = cur.fetchone()
            cur.close()
            if result:
                provider = result[0]
                logging.info(f"✅ Provider actual en BD: {provider}")
                return provider
            else:
                logging.info("ℹ️ No hay eventos previos, usando PROVIDER1")
                return "PROVIDER1"
        except Exception as e:
            logging.error(f"⚠️ Error leyendo provider actual: {e}")
            return "PROVIDER1"

    def run_mtr(self, destination: str, ip_version: str) -> Optional[Dict[str, Any]]:
        try:
            cmd = ['mtr', f'-{ip_version}', '-n', '-j', '-c', str(MTR_CONFIG['count']),
                   '-s', str(MTR_CONFIG['packet_size']), '-i', str(MTR_CONFIG['interval']), destination]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=MTR_CONFIG['timeout'])
            if result.returncode == 0:
                return json.loads(result.stdout)
        except Exception as e:
            logging.error(f"Error ejecutando MTR: {e}")
            return None

    def extract_metrics(self, mtr_report: Dict[str, Any], provider: str) -> Optional[LatencyMetrics]:
        try:
            hubs = mtr_report['report']['hubs']
            peer_ip = PEER_IPS[provider]
            peer_hop = None
            dns_hop = None
            for hop in hubs:
                if hop.get('host') == peer_ip:
                    peer_hop = hop
                if hop.get('count') == len(hubs):
                    dns_hop = hop

            if not peer_hop or not dns_hop:
                logging.warning(f"No se encontraron hops para {provider}")
                return None

            dns_loss = float(dns_hop.get('Loss%', 100.0))
            metrics = LatencyMetrics(
                peer_avg=float(peer_hop.get('Avg', float('inf'))),
                peer_loss=dns_loss,
                dns_avg=float(dns_hop.get('Avg', float('inf'))),
                dns_loss=dns_loss,
                peer_stddev=float(peer_hop.get('StDev', 0.0)),
                dns_stddev=float(dns_hop.get('StDev', 0.0))
            )
            logging.info(f"📊 {provider} - Peer: {metrics.peer_avg:.2f}ms | DNS: {metrics.dns_avg:.2f}ms | Score: {metrics.quality_score:.2f}")
            return metrics
        except Exception as e:
            logging.error(f"Error extrayendo métricas: {e}")
            return None

    def measure_provider_latency(self, provider: str) -> Optional[LatencyMetrics]:
        destination = MTR_DESTINATIONS.get(provider)
        ip_version = IP_VERSIONS.get(provider, '6')
        if not destination:
            return None
        mtr_report = self.run_mtr(destination, ip_version)
        return self.extract_metrics(mtr_report, provider) if mtr_report else None

    # ====================================================================
    # ✅ MÉTODOS DE DETECCIÓN COMBINADA DE ANOMALÍAS
    # ====================================================================
    
    def calculate_rolling_stats(self, provider: str) -> Dict[str, float]:
        """Calcula estadísticas rolling (mean, std, p95) del historial del provider."""
        history = self.metrics_history[provider]
        
        if len(history) < 3:
            return {
                'mean': 0.0,
                'std': 0.0,
                'p95': 0.0,
                'count': len(history)
            }
        
        peer_latencies = [m.peer_avg for m in history if m.peer_avg < 900]
        
        if len(peer_latencies) < 3:
            return {
                'mean': 0.0,
                'std': 0.0,
                'p95': 0.0,
                'count': len(peer_latencies)
            }
        
        return {
            'mean': float(np.mean(peer_latencies)),
            'std': float(np.std(peer_latencies)),
            'p95': float(np.percentile(peer_latencies, 95)),
            'count': len(peer_latencies)
        }

    def calculate_z_score(self, current_value: float, mean: float, std: float) -> float:
        """Calcula el Z-score: cuántas desviaciones estándar está el valor actual"""
        if std < 0.001:
            return 0.0
        return (current_value - mean) / std

    def detect_z_score_anomaly(self, provider: str, metrics: LatencyMetrics) -> Dict[str, Any]:
        """✅ DETECCIÓN 1: Anomalía basada en Z-score"""
        rolling_stats = self.calculate_rolling_stats(provider)
        
        if rolling_stats['count'] < 3 or rolling_stats['std'] < 0.001:
            return {
                'z_score': 0.0,
                'severity': 'normal',
                'is_anomaly': False,
                'rolling_mean': rolling_stats['mean'],
                'rolling_std': rolling_stats['std'],
                'rolling_p95': rolling_stats['p95'],
                'history_count': rolling_stats['count']
            }
        
        z_score = self.calculate_z_score(
            metrics.peer_avg,
            rolling_stats['mean'],
            rolling_stats['std']
        )
        
        if z_score >= Z_SCORE_THRESHOLDS['critical']:
            severity = 'critical'
            is_anomaly = True
        elif z_score >= Z_SCORE_THRESHOLDS['degraded']:
            severity = 'degraded'
            is_anomaly = True
        elif z_score >= Z_SCORE_THRESHOLDS['warning']:
            severity = 'warning'
            is_anomaly = True
        else:
            severity = 'normal'
            is_anomaly = False
        
        return {
            'z_score': round(z_score, 2),
            'severity': severity,
            'is_anomaly': is_anomaly,
            'rolling_mean': round(rolling_stats['mean'], 2),
            'rolling_std': round(rolling_stats['std'], 2),
            'rolling_p95': round(rolling_stats['p95'], 2),
            'history_count': rolling_stats['count']
        }

    def detect_absolute_anomaly(self, metrics: LatencyMetrics) -> Dict[str, Any]:
        """✅ DETECCIÓN 2: Anomalía basada en umbrales ABSOLUTOS"""
        peer_latency = metrics.peer_avg
        dns_latency = metrics.dns_avg
        
        if (peer_latency >= ABSOLUTE_LATENCY_THRESHOLDS['peer_critical'] or
            dns_latency >= ABSOLUTE_LATENCY_THRESHOLDS['dns_critical']):
            severity = 'critical'
            is_anomaly = True
        elif (peer_latency >= ABSOLUTE_LATENCY_THRESHOLDS['peer_degraded'] or
              dns_latency >= ABSOLUTE_LATENCY_THRESHOLDS['dns_degraded']):
            severity = 'degraded'
            is_anomaly = True
        elif (peer_latency >= ABSOLUTE_LATENCY_THRESHOLDS['peer_warning'] or
              dns_latency >= ABSOLUTE_LATENCY_THRESHOLDS['dns_warning']):
            severity = 'warning'
            is_anomaly = True
        else:
            severity = 'normal'
            is_anomaly = False
        
        return {
            'severity': severity,
            'is_anomaly': is_anomaly,
            'peer_latency': round(peer_latency, 2),
            'dns_latency': round(dns_latency, 2)
        }

    def detect_relative_anomaly(self, provider: str, metrics: LatencyMetrics) -> Dict[str, Any]:
        """✅ DETECCIÓN 3: Anomalía basada en DIFERENCIA RELATIVA vs provider alternativo"""
        other_provider = [p for p in PROVIDERS if p != provider][0]
        other_history = self.metrics_history[other_provider]
        
        if not other_history:
            return {
                'relative_diff_ms': 0.0,
                'severity': 'normal',
                'is_anomaly': False,
                'other_provider_avg': 0.0
            }
        
        recent_other = other_history[-3:]
        other_avg = np.mean([m.peer_avg for m in recent_other if m.peer_avg < 900])
        
        relative_diff = metrics.peer_avg - other_avg
        
        if relative_diff >= RELATIVE_DIFF_THRESHOLDS['critical']:
            severity = 'critical'
            is_anomaly = True
        elif relative_diff >= RELATIVE_DIFF_THRESHOLDS['degraded']:
            severity = 'degraded'
            is_anomaly = True
        elif relative_diff >= RELATIVE_DIFF_THRESHOLDS['warning']:
            severity = 'warning'
            is_anomaly = True
        else:
            severity = 'normal'
            is_anomaly = False
        
        return {
            'relative_diff_ms': round(relative_diff, 2),
            'severity': severity,
            'is_anomaly': is_anomaly,
            'other_provider': other_provider,
            'other_provider_avg': round(other_avg, 2)
        }

    def detect_combined_anomaly(self, provider: str, metrics: LatencyMetrics) -> Dict[str, Any]:
        """✅ DETECCIÓN COMBINADA: Combina las 3 fuentes de detección"""
        z_info = self.detect_z_score_anomaly(provider, metrics)
        absolute_info = self.detect_absolute_anomaly(metrics)
        relative_info = self.detect_relative_anomaly(provider, metrics)
        
        severity_levels = {'normal': 0, 'warning': 1, 'degraded': 2, 'critical': 3}
        
        z_level = severity_levels[z_info['severity']]
        absolute_level = severity_levels[absolute_info['severity']]
        relative_level = severity_levels[relative_info['severity']]
        
        combined_level = max(z_level, absolute_level, relative_level)
        combined_severity = [k for k, v in severity_levels.items() if v == combined_level][0]
        
        is_combined_anomaly = (
            z_info['is_anomaly'] or
            absolute_info['is_anomaly'] or
            relative_info['is_anomaly']
        )
        
        contributing_sources = []
        if z_info['is_anomaly']:
            contributing_sources.append(f"Z-score({z_info['z_score']:+.2f})")
        if absolute_info['is_anomaly']:
            contributing_sources.append(f"Absolute({absolute_info['peer_latency']:.1f}ms)")
        if relative_info['is_anomaly']:
            contributing_sources.append(f"Relative({relative_info['relative_diff_ms']:+.1f}ms)")
        
        return {
            'z_score': z_info['z_score'],
            'z_score_severity': z_info['severity'],
            'rolling_mean': z_info['rolling_mean'],
            'rolling_std': z_info['rolling_std'],
            'rolling_p95': z_info['rolling_p95'],
            'history_count': z_info['history_count'],
            'absolute_severity': absolute_info['severity'],
            'relative_diff_ms': relative_info['relative_diff_ms'],
            'relative_severity': relative_info['severity'],
            'other_provider_avg': relative_info['other_provider_avg'],
            'combined_severity': combined_severity,
            'is_combined_anomaly': is_combined_anomaly,
            'contributing_sources': contributing_sources,
            'z_level': z_level,
            'absolute_level': absolute_level,
            'relative_level': relative_level
        }

    def should_switch_provider(self) -> Tuple[str, str, Dict[str, Dict]]:
        """✅ MEJORADO: Lógica de failover con DETECCIÓN COMBINADA"""
        provider_scores = {}
        combined_anomaly_data = {}
        
        for provider in PROVIDERS:
            metrics = self.measure_provider_latency(provider)
            if not metrics:
                metrics = LatencyMetrics(
                    peer_avg=999.0, peer_loss=100.0, dns_avg=999.0,
                    dns_loss=100.0, peer_stddev=0.0, dns_stddev=0.0
                )
            
            self.metrics_history[provider].append(metrics)
            if len(self.metrics_history[provider]) > ROLLING_HISTORY_SIZE:
                self.metrics_history[provider].pop(0)
            
            # ✅ Detectar anomalía COMBINADA
            anomaly_info = self.detect_combined_anomaly(provider, metrics)
            combined_anomaly_data[provider] = anomaly_info
            
            recent_metrics = self.metrics_history[provider][-3:]
            score_avg = sum(m.quality_score for m in recent_metrics) / len(recent_metrics)
            
            # ✅ Guardar anomaly_info en provider_scores
            provider_scores[provider] = {
                'score': score_avg,
                'metrics': metrics,
                'is_healthy': metrics.is_healthy,
                'anomaly_info': anomaly_info
            }

        # Log de scores y detección combinada
        logging.info("📈 Scores promedio y detección COMBINADA:")
        for provider in PROVIDERS:
            score = provider_scores[provider]['score']
            health = "✅" if provider_scores[provider]['is_healthy'] else "❌"
            current = "⭐" if provider == self.current_primary_provider else "  "
            a_info = combined_anomaly_data[provider]
            
            severity_emoji = {
                'normal': '🟢',
                'warning': '🟡',
                'degraded': '🟠',
                'critical': '🔴'
            }.get(a_info['combined_severity'], '⚪')
            
            sources_str = ""
            if a_info['contributing_sources']:
                sources_str = " ← " + ", ".join(a_info['contributing_sources'])
            
            logging.info(
                f"   {current} {provider}: score={score:.2f} {health} | "
                f"COMBINADO={a_info['combined_severity'].upper()}{severity_emoji} "
                f"[Z={a_info['z_score']:+.2f}, μ={a_info['rolling_mean']:.1f}ms, "
                f"Rel={a_info['relative_diff_ms']:+.1f}ms]{sources_str}"
            )

        best_provider = min(provider_scores.keys(), key=lambda p: provider_scores[p]['score'])
        best_score = provider_scores[best_provider]['score']
        current_score = provider_scores[self.current_primary_provider]['score']
        score_diff = current_score - best_score

        current_anomaly = combined_anomaly_data[self.current_primary_provider]
        has_severe_anomaly = current_anomaly['combined_severity'] in ['degraded', 'critical']
        
        if best_provider != self.current_primary_provider:
            if score_diff > LATENCY_THRESHOLDS['switch_margin'] or has_severe_anomaly:
                
                if has_severe_anomaly:
                    sources = ", ".join(current_anomaly['contributing_sources']) if current_anomaly['contributing_sources'] else "anomalía combinada"
                    reason_type = f"anomalía COMBINADA [{current_anomaly['combined_severity']}]: {sources}"
                else:
                    reason_type = f"diferencia de score ({score_diff:.2f} > {LATENCY_THRESHOLDS['switch_margin']})"
                
                if self.better_provider_candidate == best_provider:
                    self.degradation_counter += 1
                    logging.info(
                        f"⏱️ Degradación sostenida: {self.degradation_counter}/{SUSTAINED_DEGRADATION_CYCLES} ciclos "
                        f"[{reason_type}]"
                    )
                else:
                    self.degradation_counter = 1
                    self.better_provider_candidate = best_provider
                    logging.info(
                        f"🔄 Nuevo candidato: {best_provider} (contador reiniciado 1/{SUSTAINED_DEGRADATION_CYCLES}) "
                        f"[{reason_type}]"
                    )

                if not provider_scores[self.current_primary_provider]['is_healthy']:
                    return best_provider, f"Cambio inmediato: {best_provider}", provider_scores

                if self.degradation_counter >= SUSTAINED_DEGRADATION_CYCLES:
                    return best_provider, f"{best_provider} mejor por {score_diff:.2f} puntos ({SUSTAINED_DEGRADATION_CYCLES} ciclos)", provider_scores

                return self.current_primary_provider, f"Degradación en progreso: {self.degradation_counter}/{SUSTAINED_DEGRADATION_CYCLES} ciclos", provider_scores
            else:
                self.degradation_counter = 0
                self.better_provider_candidate = None
                logging.info(f"✅ Diferencia insuficiente ({score_diff:.2f} < {LATENCY_THRESHOLDS['switch_margin']}) y sin anomalía combinada")
                return self.current_primary_provider, "Condiciones estables", provider_scores
        else:
            if self.degradation_counter > 0:
                logging.info(f"✅ {self.current_primary_provider} vuelve a ser el mejor")
                self.degradation_counter = 0
                self.better_provider_candidate = None
            return self.current_primary_provider, "Condiciones estables", provider_scores

    def update_netbox_policy(self, rule_id: int, updates: Dict[str, Any]):
        if DRY_RUN:
            logging.info(f"🧪 DRY RUN - Actualizaría regla {rule_id}")
            return

    def switch_to_provider(self, new_provider: str, reason: str):
        if new_provider == self.current_primary_provider:
            return
        self.last_provider = self.current_primary_provider
        logging.info(f"🔄 Cambiando de {self.current_primary_provider} a {new_provider}")
        self.current_primary_provider = new_provider

    def send_metrics_to_timescaledb(self, cycle_data: Dict[str, Any], all_providers_metrics: Dict[str, Dict]):
        """Envía métricas a TimescaleDB incluyendo todas las métricas de detección combinada"""
        if not self.ts_client:
            return
        try:
            timestamp = datetime.now(timezone.utc)
            for provider_name, metrics in all_providers_metrics.items():
                metric = {
                    'time': timestamp,
                    'provider': provider_name,
                    'peer_ip': self.provider_peer_ip_map.get(provider_name, ''),
                    'peer_asn': self.provider_asn_map.get(provider_name),
                    'peer_latency_ms': round(metrics["peer_latency_ms"], 2),
                    'peer_jitter_ms': round(metrics["peer_jitter_ms"], 2),
                    'peer_loss_pct': round(metrics["peer_loss_pct"], 2),
                    'dns_latency_ms': round(metrics["dns_latency_ms"], 2),
                    'dns_jitter_ms': round(metrics["dns_jitter_ms"], 2),
                    'dns_loss_pct': round(metrics["dns_loss_pct"], 2),
                    'score': round(metrics["score"], 2),
                    'weighted_latency': round((metrics["peer_latency_ms"] * 0.7) + (metrics["dns_latency_ms"] * 0.3), 2),
                    'loss_penalty': round(((metrics["peer_loss_pct"] + metrics["dns_loss_pct"]) / 2) * 10, 2),
                    'jitter_penalty': round(((metrics["peer_jitter_ms"] + metrics["dns_jitter_ms"]) / 2) * 0.5, 2),
                    # ✅ Campos de Z-score
                    'z_score_peer': round(metrics.get("z_score_peer", 0.0), 2),
                    'z_score_severity': metrics.get("z_score_severity", "normal"),
                    'rolling_mean': round(metrics.get("rolling_mean", 0.0), 2),
                    'rolling_std': round(metrics.get("rolling_std", 0.0), 2),
                    'rolling_p95': round(metrics.get("rolling_p95", 0.0), 2),
                    # ✅ Campos de detección absoluta
                    'absolute_severity': metrics.get("absolute_severity", "normal"),
                    # ✅ Campos de detección relativa
                    'relative_diff_ms': round(metrics.get("relative_diff_ms", 0.0), 2),
                    'relative_severity': metrics.get("relative_severity", "normal"),
                    # ✅ Campos de detección COMBINADA
                    'combined_severity': metrics.get("combined_severity", "normal"),
                    'is_combined_anomaly': metrics.get("is_combined_anomaly", False),
                    'current_provider': cycle_data["current_provider"],
                    'provider_changed': cycle_data["provider_changed"],
                    'provider_change_reason': cycle_data.get("change_reason", "") if cycle_data["provider_changed"] else "",
                    'degradation_cycle': self.degradation_counter,
                    'sustained_degradation': self.degradation_counter >= SUSTAINED_DEGRADATION_CYCLES,
                    'quality_status': self._determine_quality_status(metrics),
                    'cycle_number': self.cycle_count
                }
                self.ts_client.insert_bgp_metrics(metric)

            if cycle_data["provider_changed"] and cycle_data.get("new_provider") and cycle_data["new_provider"] != cycle_data["previous_provider"]:
                event = {
                    'previous_provider': cycle_data["previous_provider"],
                    'new_provider': cycle_data["new_provider"],
                    'change_reason': cycle_data["change_reason"],
                    'previous_provider_score': round(all_providers_metrics[cycle_data["previous_provider"]]["score"], 2),
                    'new_provider_score': round(all_providers_metrics[cycle_data["new_provider"]]["score"], 2),
                    'detection_cycles': cycle_data["cycle"],
                    'detected_by': 'bgp_failover_engine'
                }
                result = self.ts_client.insert_failover_event(event)
                if result:
                    logging.info(f"✅ TimescaleDB: Failover registrado en ciclo #{cycle_data['cycle']}")
        except Exception as e:
            logging.error(f"❌ Error enviando a TimescaleDB: {e}")

    def _determine_quality_status(self, metrics: Dict) -> str:
        combined_severity = metrics.get("combined_severity", "normal")
        
        if metrics["has_latency_critical"] or metrics["peer_loss_pct"] >= 20:
            return "critical"
        elif combined_severity in ['degraded', 'critical']:
            return "critical"
        elif metrics["has_latency_warning"] or metrics["peer_loss_pct"] > 0:
            return "warning"
        elif combined_severity == 'warning':
            return "warning"
        return "excellent"

    def run_cycle(self):
        try:
            logging.info("=" * 80)
            logging.info(f"🔍 Ciclo #{self.cycle_count} - Primary: {self.current_primary_provider}")
            new_provider, reason, provider_scores = self.should_switch_provider()

            provider_will_change = new_provider != self.current_primary_provider
            cycle_data = {
                "cycle": self.cycle_count,
                "current_provider": self.current_primary_provider,
                "provider_changed": provider_will_change,
                "previous_provider": self.current_primary_provider if provider_will_change else None,
                "new_provider": new_provider if provider_will_change else None,
                "change_reason": reason
            }

            # ✅ Construir all_metrics con TODOS los campos de detección
            all_metrics = {}
            for provider in PROVIDERS:
                metrics = provider_scores[provider]['metrics']
                a_info = provider_scores[provider]['anomaly_info']
                
                all_metrics[provider] = {
                    "score": round(provider_scores[provider]['score'], 2),
                    "is_healthy": provider_scores[provider]['is_healthy'],
                    "is_primary": provider == self.current_primary_provider,
                    # Métricas de Peer
                    "peer_latency_ms": round(metrics.peer_avg, 2),
                    "peer_jitter_ms": round(metrics.peer_stddev, 2),
                    "peer_loss_pct": round(metrics.peer_loss, 2),
                    # Métricas de DNS
                    "dns_latency_ms": round(metrics.dns_avg, 2),
                    "dns_jitter_ms": round(metrics.dns_stddev, 2),
                    "dns_loss_pct": round(metrics.dns_loss, 2),
                    # Metadata
                    "ip_version": f"IPv{IP_VERSIONS.get(provider, '?')}",
                    # ✅ Métricas de detección COMBINADA
                    "z_score_peer": a_info['z_score'],
                    "z_score_severity": a_info['z_score_severity'],
                    "rolling_mean": a_info['rolling_mean'],
                    "rolling_std": a_info['rolling_std'],
                    "rolling_p95": a_info['rolling_p95'],
                    "absolute_severity": a_info['absolute_severity'],
                    "relative_diff_ms": a_info['relative_diff_ms'],
                    "relative_severity": a_info['relative_severity'],
                    "combined_severity": a_info['combined_severity'],
                    "is_combined_anomaly": a_info['is_combined_anomaly'],
                    # Campos de observabilidad
                    "has_latency_warning": metrics.has_latency_warning,
                    "has_latency_critical": metrics.has_latency_critical,
                    "has_packet_loss": metrics.has_packet_loss
                }

            # 1️⃣ Enviar métricas con estado ACTUAL del contador
            self.es_client.send_unified_metrics(cycle_data, all_metrics)
            self.send_metrics_to_timescaledb(cycle_data, all_metrics)

            # 2️⃣ Ejecutar failover si aplica
            if provider_will_change:
                self.switch_to_provider(new_provider, reason)
                logging.info(f"🔄 Failover: {cycle_data['previous_provider']} → {new_provider} (ciclos: {self.cycle_count})")

                self.degradation_counter = 0
                self.better_provider_candidate = None
            else:
                logging.info(f"✓ Sin cambios - {reason}")

            self.cycle_count += 1

        except Exception as e:
            logging.error(f"❌ Error en ciclo: {e}", exc_info=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('/var/log/bgp_failover.log')]
    )

    try:
        subprocess.run(['mtr', '--version'], capture_output=True, check=True)
    except:
        logging.error("❌ MTR no instalado")
        return 1

    engine = BGPFailoverEngine()
    logging.info("🚀 BGP Failover Engine - Versión con Detección Combinada")
    logging.info(f"📍 Providers: {', '.join(PROVIDERS)}")
    logging.info(f"⏱️ Ciclo: {CYCLE_INTERVAL}s")
    logging.info(f"📊 Switch Margin: {LATENCY_THRESHOLDS['switch_margin']} puntos")
    logging.info(f"📊 Z-Score Thresholds: warning={Z_SCORE_THRESHOLDS['warning']}, "
                f"degraded={Z_SCORE_THRESHOLDS['degraded']}, critical={Z_SCORE_THRESHOLDS['critical']}")
    logging.info(f"📊 Absolute Thresholds (peer): "
                f"warning={ABSOLUTE_LATENCY_THRESHOLDS['peer_warning']}ms, "
                f"degraded={ABSOLUTE_LATENCY_THRESHOLDS['peer_degraded']}ms, "
                f"critical={ABSOLUTE_LATENCY_THRESHOLDS['peer_critical']}ms")
    logging.info(f"📊 Relative Diff Thresholds: "
                f"warning={RELATIVE_DIFF_THRESHOLDS['warning']}ms, "
                f"degraded={RELATIVE_DIFF_THRESHOLDS['degraded']}ms, "
                f"critical={RELATIVE_DIFF_THRESHOLDS['critical']}ms")
    logging.info(f"📊 Rolling History: {ROLLING_HISTORY_SIZE} ciclos")
    logging.info("=" * 80)

    while True:
        engine.run_cycle()
        time.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    exit(main())
