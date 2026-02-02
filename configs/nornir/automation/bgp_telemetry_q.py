#!/usr/bin/env python3
"""
BGP Failover Engine con Telemetría a Elasticsearch
Versión con formato plano para mejor compatibilidad con Grafana
"""

import requests
import time
import logging
import subprocess
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

# === CONFIGURACIÓN CALIBRADA AUTOMÁTICAMENTE ===
try:
    from bgp_failover_config import (
        LATENCY_THRESHOLDS,
        MTR_CONFIG,
        MTR_DESTINATIONS,
        PEER_IPS,
        IP_VERSIONS,
        PROVIDERS,
        CYCLE_INTERVAL,
        POLICY_RULE_IDS
    )
    print("✅ Configuración calibrada cargada exitosamente")
except ImportError:
    print("⚠️ Usando configuración por defecto")
    
    MTR_CONFIG = {
        'count': 5,
        'timeout': 30,
        'packet_size': 64,
        'interval': 0.5
    }
    
    MTR_DESTINATIONS = {
        'IXA': '2001:db8:8888::100',
        'UFINET': '2001:db8:4444::100'
    }
    
    PEER_IPS = {
        'IXA': '2001:db8:ffaa::255',
        'UFINET': '2001:db8:ffac::255'
    }
    
    IP_VERSIONS = {
        'IXA': '6',
        'UFINET': '6'
    }
    
    PROVIDERS = ['IXA', 'UFINET']
    
    LATENCY_THRESHOLDS = {
        'peer_warning': 12,
        'peer_critical': 25,
        'dns_warning': 10,
        'dns_critical': 30,
        'switch_margin': 3
    }
    
    CYCLE_INTERVAL = 30
    
    POLICY_RULE_IDS = {
        'EXPORT-TO-IXA': 1,
        'EXPORT-TO-UFINET': 2,
        'SET-LOCAL-PREF-IXA': 3,
        'SET-LOCAL-PREF-UFINET': 4
    }

# === Configuración de NetBox ===
NETBOX_URL = "http://192.168.117.135:8000"
NETBOX_TOKEN = "c889397e6b09cfd1556378047213220b2c47b7e8"
DRY_RUN = True

# === CONFIGURACIÓN DE ELASTICSEARCH ===
ELASTICSEARCH_ENABLED = True
ELASTICSEARCH_URL = "http://172.90.90.9:9200"
ELASTICSEARCH_INDEX = "bgp-failover-metrics"

# Estado actual
current_primary_provider = "IXA"  # Default


@dataclass
class LatencyMetrics:
    """Métricas de latencia extraídas de MTR"""
    peer_avg: float
    peer_loss: float
    dns_avg: float
    dns_loss: float
    peer_stddev: float
    dns_stddev: float
    
    @property
    def is_healthy(self) -> bool:
        """Determina si el enlace está saludable"""
        return (
            self.peer_loss == 0.0 and 
            self.dns_loss == 0.0 and
            self.peer_avg < LATENCY_THRESHOLDS['peer_critical'] and
            self.dns_avg < LATENCY_THRESHOLDS['dns_critical']
        )
    
    @property
    def quality_score(self) -> float:
        """Calcula un score de calidad del enlace"""
        loss_penalty = (self.peer_loss + self.dns_loss) * 100
        weighted_latency = (self.peer_avg * 0.7) + (self.dns_avg * 0.3)
        jitter_penalty = (self.peer_stddev + self.dns_stddev) * 0.5
        score = weighted_latency + loss_penalty + jitter_penalty
        return score


class ElasticsearchClient:
    """Cliente simple para enviar métricas a Elasticsearch"""
    
    def __init__(self, url: str, index: str):
        self.url = url
        self.index = index
        self.enabled = ELASTICSEARCH_ENABLED
        self.session = requests.Session()
        
        if self.enabled:
            self._verify_connection()
    
    def _verify_connection(self):
        """Verifica conectividad con Elasticsearch"""
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
            logging.error(f"❌ No se pudo conectar a Elasticsearch en {self.url}")
            logging.error(f"   Error: {e}")
            self.enabled = False
        except Exception as e:
            logging.warning(f"⚠️ Error verificando Elasticsearch: {e}")
            self.enabled = False
    
    def send_metrics_flat(self, cycle_data: Dict[str, Any], provider_metrics: Dict[str, Any]):
        """Envía métricas en formato plano a Elasticsearch con índice diario"""
        if not self.enabled:
            logging.debug("Elasticsearch deshabilitado, no se envían métricas")
            return
        
        try:
            # Formato de timestamp compatible (sin zona horaria explícita)
            timestamp = datetime.utcnow().isoformat()
            
            # Crear documento plano
            flat_doc = {
                "@timestamp": timestamp,
                "cycle": cycle_data["cycle"],
                "current_provider": cycle_data["current_provider"],
                "provider_changed": cycle_data["provider_changed"],
                "previous_provider": cycle_data.get("previous_provider"),
                "change_reason": cycle_data["change_reason"],
                "provider_name": provider_metrics["name"],
                "score": provider_metrics["score"],
                "is_healthy": provider_metrics["is_healthy"],
                "is_primary": provider_metrics["is_primary"],
                "peer_latency_ms": provider_metrics["peer_latency_ms"],
                "peer_jitter_ms": provider_metrics["peer_jitter_ms"],
                "peer_loss_pct": provider_metrics["peer_loss_pct"],
                "dns_latency_ms": provider_metrics["dns_latency_ms"],
                "dns_jitter_ms": provider_metrics["dns_jitter_ms"],
                "dns_loss_pct": provider_metrics["dns_loss_pct"],
                "ip_version": provider_metrics["ip_version"]
            }
            
            # Índice diario
            fecha = datetime.utcnow().strftime("%Y.%m.%d")
            index_name = f"bgp-failover-{fecha}"
            url = f"{self.url}/{index_name}/_doc"
            
            response = self.session.post(
                url,
                json=flat_doc,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                logging.info(f"✅ Métricas enviadas a Elasticsearch ({index_name}) para {provider_metrics['name']}")
            else:
                logging.error(f"❌ Error enviando a Elasticsearch ({response.status_code}): {response.text}")
                
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error de conexión a Elasticsearch: {e}")
        except Exception as e:
            logging.error(f"❌ Error enviando métricas: {e}")


class BGPFailoverEngine:
    def __init__(self):
        self.headers = {
            "Authorization": f"Token {NETBOX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Historial de métricas para evitar flapping
        self.metrics_history = {provider: [] for provider in PROVIDERS}
        
        # Cliente de Elasticsearch
        self.es_client = ElasticsearchClient(ELASTICSEARCH_URL, ELASTICSEARCH_INDEX)
        
        # Contador de ciclos
        self.cycle_count = 0
        self.last_provider = current_primary_provider
        
    def run_mtr(self, destination: str, ip_version: str) -> Optional[Dict[str, Any]]:
        """Ejecuta MTR hacia un destino"""
        try:
            cmd = [
                'mtr',
                f'-{ip_version}',
                '-n', '-j',
                '-c', str(MTR_CONFIG['count']),
                '-s', str(MTR_CONFIG['packet_size']),
                '-i', str(MTR_CONFIG['interval']),
                destination
            ]
            
            logging.debug(f"Ejecutando MTR (IPv{ip_version}): {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=MTR_CONFIG['timeout']
            )
            
            if result.returncode == 0:
                logging.debug(f"MTR exitoso hacia {destination}")
                return json.loads(result.stdout)
            else:
                logging.error(f"MTR falló: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logging.error(f"Timeout ejecutando MTR hacia {destination}")
            return None
        except Exception as e:
            logging.error(f"Error ejecutando MTR: {e}")
            return None
    
    def extract_metrics(self, mtr_report: Dict[str, Any], provider: str) -> Optional[LatencyMetrics]:
        """Extrae métricas del reporte MTR"""
        try:
            hubs = mtr_report['report']['hubs']
            peer_ip = PEER_IPS[provider]
            
            peer_hop = None
            dns_hop = None
            
            for hub in hubs:
                if hub.get('host') == peer_ip:
                    peer_hop = hub
                if hub.get('count') == len(hubs):
                    dns_hop = hub
            
            if not peer_hop or not dns_hop:
                logging.warning(f"No se encontraron hops para {provider}")
                return None
            
            metrics = LatencyMetrics(
                peer_avg=float(peer_hop.get('Avg', float('inf'))),
                peer_loss=float(peer_hop.get('Loss%', 100.0)),
                dns_avg=float(dns_hop.get('Avg', float('inf'))),
                dns_loss=float(dns_hop.get('Loss%', 100.0)),
                peer_stddev=float(peer_hop.get('StDev', 0.0)),
                dns_stddev=float(dns_hop.get('StDev', 0.0))
            )
            
            weighted_lat = (metrics.peer_avg * 0.7) + (metrics.dns_avg * 0.3)
            loss_pen = (metrics.peer_loss + metrics.dns_loss) * 100
            jitter_pen = (metrics.peer_stddev + metrics.dns_stddev) * 0.5
            
            logging.info(
                f"📊 {provider} - Peer: {metrics.peer_avg:.2f}ms (±{metrics.peer_stddev:.2f}ms, "
                f"loss {metrics.peer_loss}%) | DNS: {metrics.dns_avg:.2f}ms "
                f"(±{metrics.dns_stddev:.2f}ms, loss {metrics.dns_loss}%)"
            )
            logging.info(
                f"   └─ Score: {metrics.quality_score:.2f} = "
                f"Latencia({weighted_lat:.2f}) + Pérdida({loss_pen:.2f}) + Jitter({jitter_pen:.2f})"
            )
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error extrayendo métricas de {provider}: {e}")
            return None
    
    def measure_provider_latency(self, provider: str) -> Optional[LatencyMetrics]:
        """Mide latencia de un proveedor usando MTR"""
        destination = MTR_DESTINATIONS.get(provider)
        ip_version = IP_VERSIONS.get(provider, '6')
        
        if not destination:
            logging.error(f"No hay destino MTR para {provider}")
            return None
        
        logging.debug(f"Medición MTR para {provider} -> {destination} (IPv{ip_version})")
        mtr_report = self.run_mtr(destination, ip_version)
        
        if not mtr_report:
            return None
        
        return self.extract_metrics(mtr_report, provider)
    
    def should_switch_provider(self) -> Tuple[str, str, Dict[str, Any]]:
        """Determina si cambiar de proveedor y prepara métricas"""
        # Medir todos los proveedores
        provider_scores = {}
        
        for provider in PROVIDERS:
            metrics = self.measure_provider_latency(provider)
            
            if not metrics:
                logging.warning(f"⚠️ Medición {provider} falló")
                metrics = LatencyMetrics(
                    peer_avg=999.0, peer_loss=100.0, dns_avg=999.0, 
                    dns_loss=100.0, peer_stddev=0.0, dns_stddev=0.0
                )
            
            self.metrics_history[provider].append(metrics)
            
            if len(self.metrics_history[provider]) > 3:
                self.metrics_history[provider].pop(0)
            
            score_avg = sum(m.quality_score for m in self.metrics_history[provider]) / len(self.metrics_history[provider])
            provider_scores[provider] = {
                'score': score_avg,
                'metrics': metrics,
                'is_healthy': metrics.is_healthy
            }
        
        # Log scores
        logging.info("📈 Scores promedio:")
        for provider in PROVIDERS:
            score = provider_scores[provider]['score']
            health = "✅" if provider_scores[provider]['is_healthy'] else "❌"
            current = "⭐" if provider == current_primary_provider else "  "
            logging.info(f"   {current} {provider}: {score:.2f} {health}")
        
        # Encontrar mejor provider
        best_provider = min(provider_scores.keys(), key=lambda p: provider_scores[p]['score'])
        best_score = provider_scores[best_provider]['score']
        current_score = provider_scores[current_primary_provider]['score']
        
        score_diff = current_score - best_score
        
        if best_provider != current_primary_provider:
            if score_diff > LATENCY_THRESHOLDS['switch_margin']:
                if not provider_scores[current_primary_provider]['is_healthy']:
                    reason = f"{current_primary_provider} no saludable"
                else:
                    reason = f"{best_provider} mejor por {score_diff:.2f} puntos"
                return best_provider, reason, provider_scores
        
        return current_primary_provider, "Condiciones estables", provider_scores
    
    def update_netbox_policy(self, rule_id: int, updates: Dict[str, Any]):
        """Actualiza regla en NetBox"""
        if DRY_RUN:
            logging.info(f"🧪 DRY RUN - Actualizaría regla {rule_id}: {updates}")
            return
        
        url = f"{NETBOX_URL}/api/plugins/bgp/routing-policy-rule/{rule_id}/"
        
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            current_data = resp.json()
            
            update_data = {
                'routing_policy': current_data['routing_policy']['id'],
                'index': current_data['index'],
                'action': current_data['action'],
                'custom_fields': {}
            }
            
            for key, value in current_data['custom_fields'].items():
                update_data['custom_fields'][key] = value
            
            for key, value in updates.items():
                update_data['custom_fields'][key] = value
            
            resp = self.session.patch(url, json=update_data)
            resp.raise_for_status()
            logging.info(f"✅ Actualizada regla {rule_id}: {updates}")
            
        except Exception as e:
            logging.error(f"❌ Error actualizando regla {rule_id}: {e}")
            raise
    
    def switch_to_provider(self, new_provider: str, reason: str):
        """Cambia configuración BGP al nuevo proveedor"""
        global current_primary_provider
        
        if new_provider == current_primary_provider:
            logging.info(f"🔄 No se requiere cambio ({reason})")
            return
        
        self.last_provider = current_primary_provider
        
        logging.info(f"🔄 Cambiando de {current_primary_provider} a {new_provider}")
        logging.info(f"📝 Razón: {reason}")
        
        # Actualizar políticas para todos los providers
        for provider in PROVIDERS:
            provider_key = provider.upper().replace(' ', '_')
            export_key = f'EXPORT-TO-{provider_key}'
            pref_key = f'SET-LOCAL-PREF-{provider_key}'
            
            if export_key not in POLICY_RULE_IDS or pref_key not in POLICY_RULE_IDS:
                logging.warning(f"⚠️ Reglas para {provider} no configuradas")
                continue
            
            if provider == new_provider:
                # Primary
                self.update_netbox_policy(POLICY_RULE_IDS[export_key], {'as_path_prepend_count': 0})
                self.update_netbox_policy(POLICY_RULE_IDS[pref_key], {'local_preference': '200'})
            else:
                # Backup
                self.update_netbox_policy(POLICY_RULE_IDS[export_key], {'as_path_prepend_count': 3})
                self.update_netbox_policy(POLICY_RULE_IDS[pref_key], {'local_preference': '100'})
        
        current_primary_provider = new_provider
        logging.info(f"✅ Cambio completado. Primary: {new_provider}")
    
    def run_cycle(self):
        """Ejecuta un ciclo de monitoreo"""
        try:
            self.cycle_count += 1
            
            logging.info("=" * 80)
            logging.info(f"🔍 Ciclo #{self.cycle_count} - Primary: {current_primary_provider}")
            
            new_provider, reason, provider_scores = self.should_switch_provider()
            
            # Preparar datos del ciclo
            cycle_data = {
                "cycle": self.cycle_count,
                "current_provider": current_primary_provider,
                "provider_changed": new_provider != current_primary_provider,
                "previous_provider": self.last_provider if new_provider != current_primary_provider else None,
                "change_reason": reason
            }
            
            # Enviar métricas planas a Elasticsearch (una por proveedor)
            for provider in PROVIDERS:
                metrics = provider_scores[provider]['metrics']
                provider_data = {
                    "name": provider,
                    "score": round(provider_scores[provider]['score'], 2),
                    "is_healthy": provider_scores[provider]['is_healthy'],
                    "is_primary": provider == current_primary_provider,
                    "peer_latency_ms": round(metrics.peer_avg, 2),
                    "peer_jitter_ms": round(metrics.peer_stddev, 2),
                    "peer_loss_pct": round(metrics.peer_loss, 2),
                    "dns_latency_ms": round(metrics.dns_avg, 2),
                    "dns_jitter_ms": round(metrics.dns_stddev, 2),
                    "dns_loss_pct": round(metrics.dns_loss, 2),
                    "ip_version": f"IPv{IP_VERSIONS.get(provider, 'unknown')}"
                }
                
                self.es_client.send_metrics_flat(cycle_data, provider_data)
            
            if new_provider != current_primary_provider:
                self.switch_to_provider(new_provider, reason)
            else:
                logging.info(f"✓ Sin cambios - {reason}")
                
        except Exception as e:
            logging.error(f"❌ Error en ciclo: {e}", exc_info=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/var/log/bgp_failover.log')
        ]
    )
    
    # Verificar MTR
    try:
        subprocess.run(['mtr', '--version'], capture_output=True, check=True)
    except Exception:
        logging.error("❌ MTR no instalado")
        return 1
    
    engine = BGPFailoverEngine()
    
    logging.info("🚀 BGP Failover Engine con Telemetría")
    logging.info(f"📍 Thresholds: {LATENCY_THRESHOLDS}")
    logging.info(f"📡 Providers: {', '.join(PROVIDERS)}")
    logging.info(f"⏱️ Ciclo: {CYCLE_INTERVAL}s")
    
    if ELASTICSEARCH_ENABLED:
        logging.info(f"📊 Elasticsearch: {ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}")
    
    logging.info("🔧 Versiones IP:")
    for provider in PROVIDERS:
        ip_v = IP_VERSIONS.get(provider, '?')
        logging.info(f"   • {provider}: IPv{ip_v}")
    
    while True:
        logging.info(f"\n{'='*80}\n🔄 Ciclo #{engine.cycle_count + 1}\n{'='*80}")
        engine.run_cycle()
        time.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    exit(main())
