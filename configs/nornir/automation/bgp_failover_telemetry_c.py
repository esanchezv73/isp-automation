#!/usr/bin/env python3
"""
BGP Failover Engine con Telemetría a Elasticsearch
Versión con UN documento unificado por ciclo (no anidado)
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
ELASTICSEARCH_INDEX = "bgp-failover"  # Se agregará la fecha automáticamente

# === CONFIGURACIÓN DE DEGRADACIÓN SOSTENIDA ===
# Número de ciclos consecutivos que un provider debe estar mejor para justificar cambio
SUSTAINED_DEGRADATION_CYCLES = 3  # Cambiar solo si persiste por 3 ciclos (90 segundos con ciclo de 30s)
# Con CYCLE_INTERVAL=30s y SUSTAINED=3 → requiere 90 segundos de degradación sostenida
# Con CYCLE_INTERVAL=30s y SUSTAINED=5 → requiere 150 segundos (2.5 minutos)

# Umbral de pérdida de paquetes para cambio inmediato (sin esperar ciclos)
IMMEDIATE_FAILOVER_PACKET_LOSS = 20.0  # 20% de pérdida → cambio inmediato
# Solo PÉRDIDA DE PAQUETES causa cambio inmediato
# Latencia crítica requiere degradación sostenida (puede ser spike momentáneo)

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
        """
        Determina si el enlace tiene PÉRDIDA CRÍTICA DE PAQUETES.
        Solo pérdida de paquetes ≥20% causa cambio inmediato.
        Latencia crítica requiere degradación sostenida (puede ser spike momentáneo).
        """
        return (
            self.peer_loss < IMMEDIATE_FAILOVER_PACKET_LOSS and 
            self.dns_loss < IMMEDIATE_FAILOVER_PACKET_LOSS
        )
    
    @property
    def has_latency_warning(self) -> bool:
        """Indica si hay latencia en nivel de advertencia (para observabilidad)"""
        return (
            self.peer_avg >= LATENCY_THRESHOLDS['peer_warning'] or
            self.dns_avg >= LATENCY_THRESHOLDS['dns_warning']
        )
    
    @property
    def has_latency_critical(self) -> bool:
        """Indica si hay latencia crítica (para observabilidad)"""
        return (
            self.peer_avg >= LATENCY_THRESHOLDS['peer_critical'] or
            self.dns_avg >= LATENCY_THRESHOLDS['dns_critical']
        )
    
    @property
    def has_packet_loss(self) -> bool:
        """Indica si hay pérdida de paquetes (cualquier cantidad)"""
        return self.peer_loss > 0.0 or self.dns_loss > 0.0
    
    @property
    def quality_score(self) -> float:
        """Calcula un score de calidad del enlace"""
        loss_penalty = (self.peer_loss + self.dns_loss) * 100
        weighted_latency = (self.peer_avg * 0.7) + (self.dns_avg * 0.3)
        jitter_penalty = (self.peer_stddev + self.dns_stddev) * 0.5
        score = weighted_latency + loss_penalty + jitter_penalty
        return score


class ElasticsearchClient:
    """Cliente para enviar métricas a Elasticsearch en formato unificado"""
    
    def __init__(self, url: str, index_prefix: str):
        self.url = url
        self.index_prefix = index_prefix
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
    
    def send_unified_metrics(self, cycle_data: Dict[str, Any], all_providers_metrics: Dict[str, Dict]):
        """
        Envía UN SOLO documento por ciclo con métricas de todos los providers.
        Formato plano: ixa_score, ufinet_score, etc.
        
        Ejemplo de documento:
        {
          "@timestamp": "2026-02-03T02:00:00",
          "cycle": 1,
          "current_provider": "IXA",
          "provider_changed": false,
          "change_reason": "Condiciones estables",
          "ixa_score": 6.72,
          "ixa_is_primary": true,
          "ixa_peer_latency_ms": 6.33,
          "ufinet_score": 10.43,
          "ufinet_is_primary": false,
          "ufinet_peer_latency_ms": 4.60,
          ...
        }
        """
        if not self.enabled:
            logging.debug("Elasticsearch deshabilitado")
            return
        
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Documento unificado - datos globales
            doc = {
                "@timestamp": timestamp,
                "cycle": cycle_data["cycle"],
                "current_provider": cycle_data["current_provider"],
                "provider_changed": cycle_data["provider_changed"],
                "previous_provider": cycle_data.get("previous_provider"),
                "new_provider": cycle_data.get("new_provider"),  # Nuevo campo
                "change_reason": cycle_data["change_reason"]
            }
            
            # Agregar métricas de cada provider con prefijos
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
                # Campos de observabilidad para alertas en Grafana
                doc[f"{prefix}_has_latency_warning"] = metrics["has_latency_warning"]
                doc[f"{prefix}_has_latency_critical"] = metrics["has_latency_critical"]
                doc[f"{prefix}_has_packet_loss"] = metrics["has_packet_loss"]
            
            # Índice diario
            fecha = datetime.now(timezone.utc).strftime("%Y.%m.%d")
            index_name = f"{self.index_prefix}-{fecha}"
            url = f"{self.url}/{index_name}/_doc"
            
            response = self.session.post(
                url,
                json=doc,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                logging.info(f"✅ Métricas del ciclo #{cycle_data['cycle']} enviadas a {index_name}")
            else:
                logging.error(f"❌ Error enviando a Elasticsearch ({response.status_code}): {response.text}")
                
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
        
        # Historial de métricas
        self.metrics_history = {provider: [] for provider in PROVIDERS}
        
        # Cliente de Elasticsearch
        self.es_client = ElasticsearchClient(ELASTICSEARCH_URL, ELASTICSEARCH_INDEX)
        
        # Contadores
        self.cycle_count = 0
        self.last_provider = current_primary_provider
        
        # Tracking de degradación sostenida
        self.degradation_counter = 0  # Ciclos consecutivos donde otro provider es mejor
        self.better_provider_candidate = None  # Qué provider es mejor
        
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
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=MTR_CONFIG['timeout']
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                logging.error(f"MTR falló: {result.stderr}")
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
            
            logging.info(
                f"📊 {provider} - Peer: {metrics.peer_avg:.2f}ms (±{metrics.peer_stddev:.2f}ms, "
                f"loss {metrics.peer_loss}%) | DNS: {metrics.dns_avg:.2f}ms "
                f"(±{metrics.dns_stddev:.2f}ms, loss {metrics.dns_loss}%)"
            )
            logging.info(f"   └─ Score: {metrics.quality_score:.2f}")
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error extrayendo métricas de {provider}: {e}")
            return None
    
    def measure_provider_latency(self, provider: str) -> Optional[LatencyMetrics]:
        """Mide latencia de un proveedor usando MTR"""
        destination = MTR_DESTINATIONS.get(provider)
        ip_version = IP_VERSIONS.get(provider, '6')
        
        if not destination:
            return None
        
        mtr_report = self.run_mtr(destination, ip_version)
        if not mtr_report:
            return None
        
        return self.extract_metrics(mtr_report, provider)
    
    def should_switch_provider(self) -> Tuple[str, str, Dict[str, Dict]]:
        """
        Determina si cambiar de proveedor basado en degradación sostenida.
        Requiere que el problema persista por SUSTAINED_DEGRADATION_CYCLES ciclos.
        """
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
        
        # Lógica de degradación sostenida
        if best_provider != current_primary_provider:
            # El current NO es el mejor
            
            # Verificar si es el mismo candidato que venía siendo mejor
            if self.better_provider_candidate == best_provider:
                self.degradation_counter += 1
                logging.info(
                    f"⏱️ Degradación sostenida: {self.degradation_counter}/{SUSTAINED_DEGRADATION_CYCLES} ciclos "
                    f"({best_provider} mejor que {current_primary_provider} por {score_diff:.2f} puntos)"
                )
            else:
                # Cambió el candidato, reiniciar contador
                self.degradation_counter = 1
                self.better_provider_candidate = best_provider
                logging.info(
                    f"🔄 Nuevo candidato: {best_provider} (contador reiniciado 1/{SUSTAINED_DEGRADATION_CYCLES})"
                )
            
            # Verificar si cumple condiciones para cambiar
            if score_diff > LATENCY_THRESHOLDS['switch_margin']:
                # Caso 1: Provider actual NO saludable (pérdida de paquetes crítica) → Cambio inmediato
                if not provider_scores[current_primary_provider]['is_healthy']:
                    metrics_current = provider_scores[current_primary_provider]['metrics']
                    peer_loss = metrics_current.peer_loss
                    dns_loss = metrics_current.dns_loss
                    self.degradation_counter = 0  # Reiniciar contador
                    self.better_provider_candidate = None
                    return best_provider, f"{current_primary_provider} pérdida crítica de paquetes (peer: {peer_loss:.1f}%, dns: {dns_loss:.1f}%) - cambio inmediato", provider_scores
                
                # Caso 2: Degradación sostenida alcanzada
                if self.degradation_counter >= SUSTAINED_DEGRADATION_CYCLES:
                    self.degradation_counter = 0  # Reiniciar contador
                    self.better_provider_candidate = None
                    return best_provider, f"{best_provider} mejor por {score_diff:.2f} puntos ({SUSTAINED_DEGRADATION_CYCLES} ciclos)", provider_scores
                
                # Caso 3: Aún no alcanza el umbral de sostenimiento
                return current_primary_provider, f"Evaluando cambio a {best_provider} ({self.degradation_counter}/{SUSTAINED_DEGRADATION_CYCLES} ciclos)", provider_scores
            else:
                # Diferencia no supera margin
                return current_primary_provider, f"Diferencia insuficiente ({score_diff:.2f} < {LATENCY_THRESHOLDS['switch_margin']})", provider_scores
        else:
            # El current SÍ es el mejor → Reiniciar contador
            if self.degradation_counter > 0:
                logging.info(f"✅ {current_primary_provider} vuelve a ser el mejor, reiniciando contador de degradación")
            self.degradation_counter = 0
            self.better_provider_candidate = None
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
        
        for provider in PROVIDERS:
            provider_key = provider.upper().replace(' ', '_')
            export_key = f'EXPORT-TO-{provider_key}'
            pref_key = f'SET-LOCAL-PREF-{provider_key}'
            
            if export_key not in POLICY_RULE_IDS or pref_key not in POLICY_RULE_IDS:
                logging.warning(f"⚠️ Reglas para {provider} no configuradas")
                continue
            
            if provider == new_provider:
                self.update_netbox_policy(POLICY_RULE_IDS[export_key], {'as_path_prepend_count': 0})
                self.update_netbox_policy(POLICY_RULE_IDS[pref_key], {'local_preference': '200'})
            else:
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
            
            # IMPORTANTE: Usar current_primary_provider ANTES de cambiar
            provider_will_change = new_provider != current_primary_provider
            
            # Datos globales del ciclo - usar el provider ACTUAL (antes del cambio)
            cycle_data = {
                "cycle": self.cycle_count,
                "current_provider": current_primary_provider,  # Estado ANTES del cambio
                "provider_changed": provider_will_change,
                "previous_provider": self.last_provider if provider_will_change else None,
                "new_provider": new_provider if provider_will_change else None,  # A dónde va a cambiar
                "change_reason": reason
            }
            
            # Preparar métricas de TODOS los providers
            all_metrics = {}
            for provider in PROVIDERS:
                metrics = provider_scores[provider]['metrics']
                all_metrics[provider] = {
                    "score": round(provider_scores[provider]['score'], 2),
                    "is_healthy": provider_scores[provider]['is_healthy'],
                    "is_primary": provider == current_primary_provider,  # Estado ACTUAL
                    # Métricas base
                    "peer_latency_ms": round(metrics.peer_avg, 2),
                    "peer_jitter_ms": round(metrics.peer_stddev, 2),
                    "peer_loss_pct": round(metrics.peer_loss, 2),
                    "dns_latency_ms": round(metrics.dns_avg, 2),
                    "dns_jitter_ms": round(metrics.dns_stddev, 2),
                    "dns_loss_pct": round(metrics.dns_loss, 2),
                    "ip_version": f"IPv{IP_VERSIONS.get(provider, '?')}",
                    # Campos de observabilidad (para Grafana)
                    "has_latency_warning": metrics.has_latency_warning,
                    "has_latency_critical": metrics.has_latency_critical,
                    "has_packet_loss": metrics.has_packet_loss
                }
            
            # Enviar métricas ANTES de cambiar el provider
            self.es_client.send_unified_metrics(cycle_data, all_metrics)
            
            # AHORA SÍ cambiar el provider si es necesario
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
    
    logging.info("🚀 BGP Failover Engine con Telemetría Unificada")
    logging.info(f"📍 Thresholds: {LATENCY_THRESHOLDS}")
    logging.info(f"📡 Providers: {', '.join(PROVIDERS)}")
    logging.info(f"⏱️ Ciclo: {CYCLE_INTERVAL}s")
    
    if ELASTICSEARCH_ENABLED:
        logging.info(f"📊 Elasticsearch: {ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}-YYYY.MM.DD")
        logging.info(f"   Formato: UN documento por ciclo (sin anidaciones)")
    
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
