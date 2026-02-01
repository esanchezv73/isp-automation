#!/usr/bin/env python3
"""
BGP Failover Engine - Automatización basada en latencia con MTR
Optimizado para usar mtr en lugar de ping para mediciones más precisas
VERSIÓN CON INTEGRACIÓN DEL CALIBRADOR
"""

import requests
import time
import logging
import subprocess
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

# === CONFIGURACIÓN CALIBRADA AUTOMÁTICAMENTE ===
# Importar valores optimizados generados por network_calibrator.py
try:
    from bgp_failover_config import (
        LATENCY_THRESHOLDS,
        MTR_CONFIG,
        MTR_DESTINATIONS,
        PEER_IPS,
        CYCLE_INTERVAL
    )
    print("✅ Configuración calibrada cargada exitosamente")
except ImportError:
    # Fallback: Usar configuración por defecto si no existe el archivo calibrado
    print("⚠️ Usando configuración por defecto (ejecuta network_calibrator.py para optimizar)")
    
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
    
    LATENCY_THRESHOLDS = {
        'peer_warning': 12,
        'peer_critical': 25,
        'dns_warning': 10,
        'dns_critical': 30,
        'switch_margin': 3
    }
    
    CYCLE_INTERVAL = 30

# === Configuración de NetBox ===
NETBOX_URL = "http://192.168.117.135:8000"
NETBOX_TOKEN = "c889397e6b09cfd1556378047213220b2c47b7e8"
DRY_RUN = False

# IDs de las reglas de políticas BGP (obtenidos de NetBox API)
POLICY_RULE_IDS = {
    'EXPORT-TO-IXA': 1,
    'EXPORT-TO-UFINET': 2, 
    'SET-LOCAL-PREF-IXA': 3,
    'SET-LOCAL-PREF-UFINET': 4
}

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
        """
        Calcula un score de calidad del enlace.
        Menor es mejor. Considera latencia, pérdida de paquetes y estabilidad.
        
        Fórmula:
        - Latencia ponderada: (peer_avg * 0.7) + (dns_avg * 0.3)
        - Penalización pérdida: (peer_loss + dns_loss) * 100
        - Penalización jitter: (peer_stddev + dns_stddev) * 0.5
        - Score total = latencia + pérdida + jitter
        
        Ejemplo:
        - Peer: 5ms, DNS: 10ms, Sin pérdida, Jitter bajo
        - Score = (5*0.7 + 10*0.3) + 0 + 1 = 6.5 (EXCELENTE)
        
        - Peer: 20ms, DNS: 50ms, 10% pérdida, Jitter alto  
        - Score = (20*0.7 + 50*0.3) + 1000 + 10 = 1039 (MALO)
        """
        # Penalizar pérdida de paquetes severamente
        loss_penalty = (self.peer_loss + self.dns_loss) * 100
        
        # Latencia promedio ponderada (70% peer, 30% DNS)
        weighted_latency = (self.peer_avg * 0.7) + (self.dns_avg * 0.3)
        
        # Penalizar inestabilidad (jitter alto)
        jitter_penalty = (self.peer_stddev + self.dns_stddev) * 0.5
        
        score = weighted_latency + loss_penalty + jitter_penalty
        
        return score


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
        self.metrics_history = {'IXA': [], 'UFINET': []}
        
    def run_mtr(self, destination: str) -> Optional[Dict[str, Any]]:
        """
        Ejecuta MTR hacia un destino y retorna el reporte en formato JSON
        
        Args:
            destination: Dirección IPv6 de destino
            
        Returns:
            Dict con el reporte MTR o None si falla
        """
        try:
            cmd = [
                'mtr',
                '-6',  # IPv6
                '-n',  # No resolver nombres (más rápido)
                '-j',  # Formato JSON
                '-c', str(MTR_CONFIG['count']),
                '-s', str(MTR_CONFIG['packet_size']),
                '-i', str(MTR_CONFIG['interval']),  # Intervalo entre paquetes
                destination
            ]
            
            logging.debug(f"Ejecutando MTR: {' '.join(cmd)}")
            
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
                logging.error(f"MTR falló con código {result.returncode}: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logging.error(f"Timeout ejecutando MTR hacia {destination} (>{MTR_CONFIG['timeout']}s)")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Error parseando JSON de MTR: {e}")
            logging.debug(f"Output recibido: {result.stdout[:500]}")
            return None
        except Exception as e:
            logging.error(f"Error ejecutando MTR hacia {destination}: {e}")
            return None
    
    def extract_metrics(self, mtr_report: Dict[str, Any], provider: str) -> Optional[LatencyMetrics]:
        """
        Extrae métricas relevantes del reporte MTR
        
        Args:
            mtr_report: Reporte JSON de MTR
            provider: Nombre del proveedor (IXA/UFINET)
            
        Returns:
            LatencyMetrics o None si no se pueden extraer
        """
        try:
            hubs = mtr_report['report']['hubs']
            peer_ip = PEER_IPS[provider]
            
            # Buscar el hop del peer (count 2) y el DNS (count 3)
            peer_hop = None
            dns_hop = None
            
            for hub in hubs:
                if hub.get('host') == peer_ip:
                    peer_hop = hub
                # El DNS es el último hop (mayor count)
                if hub.get('count') == len(hubs):
                    dns_hop = hub
            
            if not peer_hop or not dns_hop:
                logging.warning(f"No se encontraron hops necesarios para {provider}")
                logging.debug(f"Hubs encontrados: {[h.get('host') for h in hubs]}")
                return None
            
            metrics = LatencyMetrics(
                peer_avg=float(peer_hop.get('Avg', float('inf'))),
                peer_loss=float(peer_hop.get('Loss%', 100.0)),
                dns_avg=float(dns_hop.get('Avg', float('inf'))),
                dns_loss=float(dns_hop.get('Loss%', 100.0)),
                peer_stddev=float(peer_hop.get('StDev', 0.0)),
                dns_stddev=float(dns_hop.get('StDev', 0.0))
            )
            
            # Calcular componentes del score para logging
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
            
        except (KeyError, ValueError, TypeError) as e:
            logging.error(f"Error extrayendo métricas de {provider}: {e}")
            return None
    
    def measure_provider_latency(self, provider: str) -> Optional[LatencyMetrics]:
        """
        Mide latencia completa de un proveedor usando MTR
        
        Args:
            provider: Nombre del proveedor (IXA/UFINET)
            
        Returns:
            LatencyMetrics o None si falla
        """
        destination = MTR_DESTINATIONS.get(provider)
        if not destination:
            logging.error(f"No hay destino MTR configurado para {provider}")
            return None
        
        logging.debug(f"Iniciando medición MTR para {provider} -> {destination}")
        mtr_report = self.run_mtr(destination)
        
        if not mtr_report:
            return None
        
        return self.extract_metrics(mtr_report, provider)
    
    def should_switch_provider(self) -> Tuple[str, str]:
        """
        Determina si se debe cambiar de proveedor basado en métricas MTR
        
        Returns:
            Tupla (nuevo_proveedor, razón_del_cambio)
        """
        # Medir ambos proveedores
        ixa_metrics = self.measure_provider_latency("IXA")
        ufinet_metrics = self.measure_provider_latency("UFINET")
        
        # Si alguna medición falló, usar valores de penalización
        if not ixa_metrics:
            logging.warning("⚠️ Medición IXA falló, asignando penalización")
            ixa_metrics = LatencyMetrics(
                peer_avg=999.0, peer_loss=100.0, dns_avg=999.0, 
                dns_loss=100.0, peer_stddev=0.0, dns_stddev=0.0
            )
        
        if not ufinet_metrics:
            logging.warning("⚠️ Medición UFINET falló, asignando penalización")
            ufinet_metrics = LatencyMetrics(
                peer_avg=999.0, peer_loss=100.0, dns_avg=999.0,
                dns_loss=100.0, peer_stddev=0.0, dns_stddev=0.0
            )
        
        # Actualizar historial
        self.metrics_history['IXA'].append(ixa_metrics)
        self.metrics_history['UFINET'].append(ufinet_metrics)
        
        # Mantener solo últimas 3 mediciones
        for provider in ['IXA', 'UFINET']:
            if len(self.metrics_history[provider]) > 3:
                self.metrics_history[provider].pop(0)
        
        # Calcular scores promedio (histeresis)
        ixa_score_avg = sum(m.quality_score for m in self.metrics_history['IXA']) / len(self.metrics_history['IXA'])
        ufinet_score_avg = sum(m.quality_score for m in self.metrics_history['UFINET']) / len(self.metrics_history['UFINET'])
        
        logging.info(f"📈 Scores promedio - IXA: {ixa_score_avg:.2f}, UFINET: {ufinet_score_avg:.2f}")
        
        global current_primary_provider
        
        # Lógica de decisión con histeresis
        if current_primary_provider == "IXA":
            # Si UFINET es significativamente mejor, cambiar
            if ufinet_score_avg < (ixa_score_avg - LATENCY_THRESHOLDS['switch_margin']):
                if not ixa_metrics.is_healthy:
                    return "UFINET", "IXA no saludable y UFINET mejor"
                else:
                    return "UFINET", f"UFINET mejor por {ixa_score_avg - ufinet_score_avg:.2f} puntos"
        
        elif current_primary_provider == "UFINET":
            # Si IXA es significativamente mejor, cambiar
            if ixa_score_avg < (ufinet_score_avg - LATENCY_THRESHOLDS['switch_margin']):
                if not ufinet_metrics.is_healthy:
                    return "IXA", "UFINET no saludable y IXA mejor"
                else:
                    return "IXA", f"IXA mejor por {ufinet_score_avg - ixa_score_avg:.2f} puntos"
        
        return current_primary_provider, "Condiciones estables"
    
    def update_netbox_policy(self, rule_id: int, updates: Dict[str, Any]):
        """Actualiza los Custom Fields de una regla de política en NetBox"""
        
        if DRY_RUN:
            logging.info(f"🧪 DRY RUN - Actualizaría regla {rule_id}: {updates}")
            return
        
        url = f"{NETBOX_URL}/api/plugins/bgp/routing-policy-rule/{rule_id}/"
        
        # Obtener regla actual para preservar todos los campos obligatorios
        resp = self.session.get(url)
        resp.raise_for_status()
        current_data = resp.json()
        
        # Crear payload completo con todos los campos requeridos
        update_data = {
            'routing_policy': current_data['routing_policy']['id'],
            'index': current_data['index'],
            'action': current_data['action'],
            'custom_fields': {}
        }
        
        # Copiar todos los custom fields existentes
        for key, value in current_data['custom_fields'].items():
            update_data['custom_fields'][key] = value
        
        # Actualizar solo los campos especificados
        for key, value in updates.items():
            update_data['custom_fields'][key] = value
        
        # Enviar actualización
        try:
            resp = self.session.patch(url, json=update_data)
            resp.raise_for_status()
            logging.info(f"✅ Actualizada regla {rule_id}: {updates}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"❌ Error HTTP al actualizar regla {rule_id}: {e}")
            logging.error(f"❌ Respuesta del servidor: {resp.text}")
            raise
    
    def switch_to_provider(self, new_provider: str, reason: str):
        """Cambia la configuración BGP para usar un nuevo proveedor principal"""
        global current_primary_provider
        
        if new_provider == current_primary_provider:
            logging.info(f"🔄 No se requiere cambio de proveedor ({reason})")
            return
        
        logging.info(f"🔄 Cambiando proveedor principal de {current_primary_provider} a {new_provider}")
        logging.info(f"📝 Razón: {reason}")
        
        if new_provider == "IXA":
            # IXA principal, UFINET backup
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-IXA'], {'as_path_prepend_count': 0})
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-UFINET'], {'as_path_prepend_count': 3})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-IXA'], {'local_preference': '200'})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-UFINET'], {'local_preference': '100'})
            
        elif new_provider == "UFINET":
            # UFINET principal, IXA backup  
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-IXA'], {'as_path_prepend_count': 3})
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-UFINET'], {'as_path_prepend_count': 0})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-IXA'], {'local_preference': '100'})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-UFINET'], {'local_preference': '200'})
        
        current_primary_provider = new_provider
        logging.info(f"✅ Cambio completado. Proveedor principal ahora: {new_provider}")
    
    def run_cycle(self):
        """Ejecuta un ciclo completo de monitoreo y decisión"""
        try:
            logging.info("=" * 80)
            logging.info(f"🔍 Iniciando ciclo de monitoreo - Proveedor actual: {current_primary_provider}")
            
            new_provider, reason = self.should_switch_provider()
            
            if new_provider != current_primary_provider:
                self.switch_to_provider(new_provider, reason)
            else:
                logging.info(f"✓ Sin cambios requeridos - {reason}")
                
        except Exception as e:
            logging.error(f"❌ Error en ciclo de automatización: {e}", exc_info=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/var/log/bgp_failover.log')
        ]
    )
    
    # Verificar que MTR está instalado
    try:
        subprocess.run(['mtr', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.error("❌ MTR no está instalado. Instalar con: apt-get install mtr-tiny")
        return 1
    
    engine = BGPFailoverEngine()
    logging.info("🚀 Iniciando motor de failover BGP con MTR...")
    logging.info(f"📍 Thresholds configurados: {LATENCY_THRESHOLDS}")
    logging.info(f"⏱️ Ciclo de monitoreo: {CYCLE_INTERVAL} segundos")
    
    # Ejecutar continuamente usando intervalo configurado
    cycle_count = 0
    while True:
        cycle_count += 1
        logging.info(f"\n{'='*80}\n🔄 Ciclo #{cycle_count}\n{'='*80}")
        engine.run_cycle()
        time.sleep(CYCLE_INTERVAL)  # ← Usa la variable importada


if __name__ == "__main__":
    exit(main())
