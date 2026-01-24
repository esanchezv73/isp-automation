#!/usr/bin/env python3
"""
BGP Failover Engine - Automatización basada en latencia
"""

import requests
import time
import logging
from typing import Dict, Any

# === Configuración ===
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

# Thresholds de latencia (ms)
LATENCY_THRESHOLDS = {
    'normal': 10,
    'warning': 20,
    'critical': 50
}

# Estado actual
current_primary_provider = "IXA"  # Default

class BGPFailoverEngine:
    def __init__(self):
        self.headers = {
            "Authorization": f"Token {NETBOX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Historial de latencia para evitar flapping
        self.latency_history = {'IXA': [], 'UFINET': []}
    
    def measure_latency(self, provider: str) -> float:
        """Mide latencia real hacia los peers BGP"""
        import subprocess
        
        # Direcciones IP reales de tus peers
        PEER_IPS = {
            'IXA': '2001:db8:ffaa::255',
            'UFINET': '2001:db8:ffac::255'
        }
        
        try:
            peer_ip = PEER_IPS[provider]
            result = subprocess.run(
                ['ping6', '-c', '3', '-W', '2', peer_ip],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                # Extraer latencia promedio
                for line in result.stdout.split('\n'):
                    if 'avg' in line:
                        avg_latency = line.split('/')[4]
                        return float(avg_latency)
                        
        except Exception as e:
            logging.error(f"Error midiendo latencia a {provider}: {e}")
        
        return float('inf')  # Si falla, retornar latencia infinita
    
    def get_average_latency(self, provider: str) -> float:
        """Calcula promedio móvil de latencia"""
        if not self.latency_history[provider]:
            return self.measure_latency(provider)
        
        # Mantener solo últimas 5 mediciones
        if len(self.latency_history[provider]) > 5:
            self.latency_history[provider].pop(0)
            
        return sum(self.latency_history[provider]) / len(self.latency_history[provider])
    
    def should_switch_provider(self) -> str:
        """Determina si se debe cambiar de proveedor"""
        ixa_latency = self.get_average_latency("IXA")
        ufinet_latency = self.get_average_latency("UFINET")
        
        # Actualizar historial
        self.latency_history['IXA'].append(ixa_latency)
        self.latency_history['UFINET'].append(ufinet_latency)
        
        logging.info(f"Latencia - IXA: {ixa_latency:.2f}ms, UFINET: {ufinet_latency:.2f}ms")
        
        # Lógica de decisión con histeresis
        if current_primary_provider == "IXA":
            if ufinet_latency < ixa_latency and ixa_latency > LATENCY_THRESHOLDS['warning']:
                return "UFINET"
        elif current_primary_provider == "UFINET":
            if ixa_latency < ufinet_latency and ufinet_latency > LATENCY_THRESHOLDS['warning']:
                return "IXA"
                
        return current_primary_provider
    
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
        
        # Actualizar solo los campos especificados (mantener tipos originales)
        for key, value in updates.items():
            update_data['custom_fields'][key] = value  # Los valores ya vienen con tipo correcto
        
        # Enviar actualización
        try:
            resp = self.session.patch(url, json=update_data)
            resp.raise_for_status()
            logging.info(f"✅ Actualizada regla {rule_id}: {updates}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"❌ Error HTTP al actualizar regla {rule_id}: {e}")
            logging.error(f"❌ Respuesta del servidor: {resp.text}")
            raise
    
    def switch_to_provider(self, new_provider: str):
        """Cambia la configuración BGP para usar un nuevo proveedor principal"""
        global current_primary_provider
        
        if new_provider == current_primary_provider:
            logging.info("🔄 No se requiere cambio de proveedor")
            return
        
        logging.info(f"🔄 Cambiando proveedor principal de {current_primary_provider} a {new_provider}")
        
        if new_provider == "IXA":
            # IXA principal, UFINET backup
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-IXA'], {'as_path_prepend_count': 0})      # ← Entero
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-UFINET'], {'as_path_prepend_count': 2})   # ← Entero
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-IXA'], {'local_preference': '200'})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-UFINET'], {'local_preference': '100'})
            
        elif new_provider == "UFINET":
            # UFINET principal, IXA backup  
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-IXA'], {'as_path_prepend_count': 2})      # ← Entero
            self.update_netbox_policy(POLICY_RULE_IDS['EXPORT-TO-UFINET'], {'as_path_prepend_count': 0})   # ← Entero
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-IXA'], {'local_preference': '100'})
            self.update_netbox_policy(POLICY_RULE_IDS['SET-LOCAL-PREF-UFINET'], {'local_preference': '200'})
        
        current_primary_provider = new_provider
        logging.info(f"✅ Cambio completado. Proveedor principal ahora: {new_provider}")
    
    def run_cycle(self):
        """Ejecuta un ciclo completo de monitoreo y decisión"""
        try:
            new_provider = self.should_switch_provider()
            if new_provider != current_primary_provider:
                self.switch_to_provider(new_provider)
            else:
                logging.debug("🔍 Sin cambios requeridos en esta iteración")
                
        except Exception as e:
            logging.error(f"❌ Error en ciclo de automatización: {e}")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    engine = BGPFailoverEngine()
    logging.info("🚀 Iniciando motor de failover BGP...")
    
    # Ejecutar continuamente cada 60 segundos
    while True:
        engine.run_cycle()
        time.sleep(60)

if __name__ == "__main__":
    main()
