#!/usr/bin/env python3
"""
BGP Failover Engine VERSIÓN CORREGIDA
- Lógica de switch_margin CORRECTA
- TimescaleDB integrado
- Logs completos restaurados
"""

import requests
import time
import logging
import subprocess
import json
import os
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

current_primary_provider = "PROVIDER1"


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
        loss_penalty = (self.peer_loss + self.dns_loss) * 100
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
            else:
                logging.warning(f"⚠️ Elasticsearch status: {response.status_code}")
                self.enabled = False
        except:
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
                doc[f"{prefix}_peer_latency_ms"] = metrics["peer_latency_ms"]
                doc[f"{prefix}_peer_loss_pct"] = metrics["peer_loss_pct"]
                doc[f"{prefix}_dns_latency_ms"] = metrics["dns_latency_ms"]
                doc[f"{prefix}_dns_loss_pct"] = metrics["dns_loss_pct"]
            
            fecha = datetime.now(timezone.utc).strftime("%Y.%m.%d")
            index_name = f"{self.index_prefix}-{fecha}"
            url = f"{self.url}/{index_name}/_doc"
            
            response = self.session.post(url, json=doc, headers={"Content-Type": "application/json"}, timeout=10)
            if response.status_code in [200, 201]:
                logging.info(f"✅ Elasticsearch: Ciclo #{cycle_data['cycle']} enviado")
        except Exception as e:
            logging.error(f"❌ Error Elasticsearch: {e}")


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
        
        self.cycle_count = 0
        self.last_provider = current_primary_provider
        self.degradation_counter = 0
        self.better_provider_candidate = None
    
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
    
    def should_switch_provider(self) -> Tuple[str, str, Dict[str, Dict]]:
        """
        ✅ LÓGICA CORREGIDA:
        1. Primero calcula scores
        2. Verifica switch_margin ANTES de contar degradación
        3. Solo incrementa contador si la diferencia es significativa
        """
        provider_scores = {}
        
        for provider in PROVIDERS:
            metrics = self.measure_provider_latency(provider)
            if not metrics:
                metrics = LatencyMetrics(
                    peer_avg=999.0, peer_loss=100.0, dns_avg=999.0,
                    dns_loss=100.0, peer_stddev=0.0, dns_stddev=0.0
                )
            
            self.metrics_history[provider].append(metrics)
            if len(self.metrics_history[provider]) > 3:
                self.metrics_history[provider].pop(0)
            
            score_avg = sum(m.quality_score for m in self.metrics_history[provider]) / len(self.metrics_history[provider])
            provider_scores[provider] = {'score': score_avg, 'metrics': metrics, 'is_healthy': metrics.is_healthy}
        
        logging.info("📈 Scores promedio:")
        for provider in PROVIDERS:
            score = provider_scores[provider]['score']
            health = "✅" if provider_scores[provider]['is_healthy'] else "❌"
            current = "⭐" if provider == current_primary_provider else "  "
            logging.info(f"   {current} {provider}: {score:.2f} {health}")
        
        best_provider = min(provider_scores.keys(), key=lambda p: provider_scores[p]['score'])
        best_score = provider_scores[best_provider]['score']
        current_score = provider_scores[current_primary_provider]['score']
        score_diff = current_score - best_score
        
        if best_provider != current_primary_provider:
            # ✅ CORRECCIÓN: Verificar switch_margin PRIMERO
            if score_diff > LATENCY_THRESHOLDS['switch_margin']:
                # Solo ahora contar como degradación
                if self.better_provider_candidate == best_provider:
                    self.degradation_counter += 1
                    logging.info(f"⏱️ Degradación sostenida: {self.degradation_counter}/{SUSTAINED_DEGRADATION_CYCLES} ciclos")
                else:
                    self.degradation_counter = 1
                    self.better_provider_candidate = best_provider
                    logging.info(f"🔄 Nuevo candidato: {best_provider} (contador reiniciado 1/{SUSTAINED_DEGRADATION_CYCLES})")
                
                # Verificar si cumple para cambiar
                if not provider_scores[current_primary_provider]['is_healthy']:
                    self.degradation_counter = 0
                    self.better_provider_candidate = None
                    return best_provider, f"Cambio inmediato: {best_provider}", provider_scores
                
                if self.degradation_counter >= SUSTAINED_DEGRADATION_CYCLES:
                    self.degradation_counter = 0
                    self.better_provider_candidate = None
                    return best_provider, f"{best_provider} mejor por {score_diff:.2f} puntos ({SUSTAINED_DEGRADATION_CYCLES} ciclos)", provider_scores
            else:
                # ✅ CORRECCIÓN: Diferencia NO significativa → reiniciar contador
                self.degradation_counter = 0
                self.better_provider_candidate = None
                logging.info(f"✅ Diferencia insuficiente ({score_diff:.2f} < {LATENCY_THRESHOLDS['switch_margin']})")
        else:
            if self.degradation_counter > 0:
                logging.info(f"✅ {current_primary_provider} vuelve a ser el mejor")
            self.degradation_counter = 0
            self.better_provider_candidate = None
        
        return current_primary_provider, "Condiciones estables", provider_scores
    
    def update_netbox_policy(self, rule_id: int, updates: Dict[str, Any]):
        if DRY_RUN:
            logging.info(f"🧪 DRY RUN - Actualizaría regla {rule_id}")
            return
    
    def switch_to_provider(self, new_provider: str, reason: str):
        global current_primary_provider
        if new_provider == current_primary_provider:
            return
        self.last_provider = current_primary_provider
        logging.info(f"🔄 Cambiando de {current_primary_provider} a {new_provider}")
        current_primary_provider = new_provider
    
    def send_metrics_to_timescaledb(self, cycle_data: Dict[str, Any], all_providers_metrics: Dict[str, Dict]):
        """Envía métricas a TimescaleDB"""
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
                    'loss_penalty': round(((metrics["peer_loss_pct"] + metrics["dns_loss_pct"]) / 2) * 100, 2),
                    'jitter_penalty': round(((metrics["peer_jitter_ms"] + metrics["dns_jitter_ms"]) / 2) * 0.5, 2),
                    'current_provider': cycle_data["current_provider"],
                    'provider_changed': cycle_data["provider_changed"],
                    'provider_change_reason': cycle_data.get("change_reason", "") if cycle_data["provider_changed"] else "",
                    'degradation_cycle': self.degradation_counter,
                    'sustained_degradation': self.degradation_counter >= 2,
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
        if metrics["has_latency_critical"] or metrics["peer_loss_pct"] >= 20:
            return "critical"
        elif metrics["has_latency_warning"] or metrics["peer_loss_pct"] > 0:
            return "warning"
        return "excellent"
    
    def run_cycle(self):
        try:
            self.cycle_count += 1
            logging.info("=" * 80)
            logging.info(f"🔍 Ciclo #{self.cycle_count} - Primary: {current_primary_provider}")
            
            new_provider, reason, provider_scores = self.should_switch_provider()
            
            provider_will_change = new_provider != current_primary_provider
            
            cycle_data = {
                "cycle": self.cycle_count,
                "current_provider": current_primary_provider,
                "provider_changed": provider_will_change,
                "previous_provider": current_primary_provider if provider_will_change else None,  # ✅ CORREGIDO: usar current, no last
                "new_provider": new_provider if provider_will_change else None,
                "change_reason": reason
            }
            
            all_metrics = {}
            for provider in PROVIDERS:
                metrics = provider_scores[provider]['metrics']
                all_metrics[provider] = {
                    "score": round(provider_scores[provider]['score'], 2),
                    "is_healthy": provider_scores[provider]['is_healthy'],
                    "peer_latency_ms": round(metrics.peer_avg, 2),
                    "peer_jitter_ms": round(metrics.peer_stddev, 2),
                    "peer_loss_pct": round(metrics.peer_loss, 2),
                    "dns_latency_ms": round(metrics.dns_avg, 2),
                    "dns_jitter_ms": round(metrics.dns_stddev, 2),
                    "dns_loss_pct": round(metrics.dns_loss, 2),
                    "has_latency_warning": metrics.has_latency_warning,
                    "has_latency_critical": metrics.has_latency_critical,
                    "has_packet_loss": metrics.has_packet_loss
                }
            
            self.es_client.send_unified_metrics(cycle_data, all_metrics)
            self.send_metrics_to_timescaledb(cycle_data, all_metrics)
            
            if provider_will_change:
                self.switch_to_provider(new_provider, reason)
            else:
                logging.info(f"✓ Sin cambios - {reason}")
                
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
    
    logging.info("🚀 BGP Failover Engine - Versión Corregida")
    logging.info(f"📍 Providers: {', '.join(PROVIDERS)}")
    logging.info(f"⏱️ Ciclo: {CYCLE_INTERVAL}s")
    logging.info(f"📊 Switch Margin: {LATENCY_THRESHOLDS['switch_margin']} puntos")
    logging.info(f"⚠️ Degradación Sostenida: {SUSTAINED_DEGRADATION_CYCLES} ciclos")
    
    while True:
        engine.run_cycle()
        time.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    exit(main())
