#!/usr/bin/env python3
"""
BGP Failover Network Calibration Tool - Versión Generalizada
Soporta múltiples providers, IPv4/IPv6, y configuración flexible
"""

import subprocess
import json
import time
import statistics
import ipaddress
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sys

# Configuración de pruebas
CALIBRATION_CONFIG = {
    'cycles': 10,           # Número de ciclos de medición
    'interval': 30,         # Segundos entre mediciones
    'mtr_count': 5,         # Paquetes por medición MTR
    'mtr_interval': 0.5,    # Intervalo entre paquetes MTR
    'mtr_timeout': 30       # Timeout MTR
}


@dataclass
class NetworkMetrics:
    """Métricas agregadas de múltiples mediciones"""
    peer_latency_avg: float
    peer_latency_min: float
    peer_latency_max: float
    peer_latency_std: float
    peer_jitter_avg: float
    peer_jitter_max: float
    peer_loss_max: float
    
    dns_latency_avg: float
    dns_latency_min: float
    dns_latency_max: float
    dns_latency_std: float
    dns_jitter_avg: float
    dns_jitter_max: float
    dns_loss_max: float
    
    measurement_count: int
    success_rate: float


def detect_ip_version(ip_address: str) -> Optional[str]:
    """
    Detecta si una dirección IP es IPv4 o IPv6
    
    Args:
        ip_address: Dirección IP a analizar
        
    Returns:
        '4' para IPv4, '6' para IPv6, None si inválida
    """
    try:
        ip_obj = ipaddress.ip_address(ip_address)
        if isinstance(ip_obj, ipaddress.IPv4Address):
            return '4'
        elif isinstance(ip_obj, ipaddress.IPv6Address):
            return '6'
    except ValueError:
        return None
    return None


def validate_ip_address(ip_str: str) -> Tuple[bool, Optional[str], str]:
    """
    Valida una dirección IP y retorna información sobre ella
    
    Args:
        ip_str: String con la dirección IP
        
    Returns:
        Tupla (es_valida, version, mensaje)
    """
    version = detect_ip_version(ip_str)
    
    if version is None:
        return False, None, "❌ Dirección IP inválida"
    
    ip_type = "IPv4" if version == '4' else "IPv6"
    return True, version, f"✅ {ip_type} válida"


class NetworkCalibrator:
    def __init__(self, provider_name: str, destination: str, peer_ip: str, ip_version: str):
        self.provider_name = provider_name
        self.destination = destination
        self.peer_ip = peer_ip
        self.ip_version = ip_version  # '4' o '6'
        
        # Almacenamiento de mediciones
        self.peer_latencies = []
        self.peer_jitters = []
        self.peer_losses = []
        
        self.dns_latencies = []
        self.dns_jitters = []
        self.dns_losses = []
        
        self.failed_measurements = 0
        
    def run_mtr(self) -> Optional[Dict]:
        """Ejecuta MTR y retorna el reporte JSON"""
        try:
            cmd = [
                'mtr',
                f'-{self.ip_version}',  # -4 para IPv4, -6 para IPv6
                '-n',  # No resolver nombres
                '-j',  # Formato JSON
                '-c', str(CALIBRATION_CONFIG['mtr_count']),
                '-i', str(CALIBRATION_CONFIG['mtr_interval']),
                self.destination
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=CALIBRATION_CONFIG['mtr_timeout']
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                return None
                
        except Exception as e:
            print(f"    ❌ Error MTR: {e}")
            return None
    
    def extract_hop_metrics(self, mtr_report: Dict) -> Optional[Tuple[Dict, Dict]]:
        """Extrae métricas del peer y DNS (último hop)"""
        try:
            hubs = mtr_report['report']['hubs']
            
            # Buscar peer y DNS
            peer_hop = None
            dns_hop = None
            
            for hub in hubs:
                if hub.get('host') == self.peer_ip:
                    peer_hop = hub
                if hub.get('count') == len(hubs):
                    dns_hop = hub
            
            if not peer_hop or not dns_hop:
                return None
            
            return peer_hop, dns_hop
            
        except Exception as e:
            print(f"    ⚠️ Error extrayendo hops: {e}")
            return None
    
    def measure_cycle(self, cycle_num: int) -> bool:
        """Ejecuta un ciclo de medición"""
        print(f"  🔄 Ciclo {cycle_num}/{CALIBRATION_CONFIG['cycles']}...", end=' ', flush=True)
        
        mtr_report = self.run_mtr()
        if not mtr_report:
            print("❌ Falló MTR")
            self.failed_measurements += 1
            return False
        
        hops = self.extract_hop_metrics(mtr_report)
        if not hops:
            print("❌ No se encontraron hops")
            self.failed_measurements += 1
            return False
        
        peer_hop, dns_hop = hops
        
        # Almacenar métricas del peer
        self.peer_latencies.append(float(peer_hop.get('Avg', 0)))
        self.peer_jitters.append(float(peer_hop.get('StDev', 0)))
        self.peer_losses.append(float(peer_hop.get('Loss%', 0)))
        
        # Almacenar métricas del DNS
        self.dns_latencies.append(float(dns_hop.get('Avg', 0)))
        self.dns_jitters.append(float(dns_hop.get('StDev', 0)))
        self.dns_losses.append(float(dns_hop.get('Loss%', 0)))
        
        print(f"✅ Peer: {peer_hop['Avg']:.1f}ms, DNS: {dns_hop['Avg']:.1f}ms")
        
        return True
    
    def calculate_metrics(self) -> NetworkMetrics:
        """Calcula estadísticas agregadas de todas las mediciones"""
        total_measurements = CALIBRATION_CONFIG['cycles']
        successful = total_measurements - self.failed_measurements
        
        return NetworkMetrics(
            # Peer métricas
            peer_latency_avg=statistics.mean(self.peer_latencies) if self.peer_latencies else 0,
            peer_latency_min=min(self.peer_latencies) if self.peer_latencies else 0,
            peer_latency_max=max(self.peer_latencies) if self.peer_latencies else 0,
            peer_latency_std=statistics.stdev(self.peer_latencies) if len(self.peer_latencies) > 1 else 0,
            peer_jitter_avg=statistics.mean(self.peer_jitters) if self.peer_jitters else 0,
            peer_jitter_max=max(self.peer_jitters) if self.peer_jitters else 0,
            peer_loss_max=max(self.peer_losses) if self.peer_losses else 0,
            
            # DNS métricas
            dns_latency_avg=statistics.mean(self.dns_latencies) if self.dns_latencies else 0,
            dns_latency_min=min(self.dns_latencies) if self.dns_latencies else 0,
            dns_latency_max=max(self.dns_latencies) if self.dns_latencies else 0,
            dns_latency_std=statistics.stdev(self.dns_latencies) if len(self.dns_latencies) > 1 else 0,
            dns_jitter_avg=statistics.mean(self.dns_jitters) if self.dns_jitters else 0,
            dns_jitter_max=max(self.dns_jitters) if self.dns_jitters else 0,
            dns_loss_max=max(self.dns_losses) if self.dns_losses else 0,
            
            measurement_count=successful,
            success_rate=(successful / total_measurements * 100) if total_measurements > 0 else 0
        )


class ThresholdCalculator:
    """Calcula thresholds recomendados basados en métricas de red"""
    
    @staticmethod
    def calculate_thresholds(
        provider_metrics: Dict[str, NetworkMetrics]
    ) -> Dict[str, any]:
        """Calcula thresholds óptimos basados en las características de todos los providers"""
        
        # Recopilar todas las latencias base
        all_peer_avg = [m.peer_latency_avg for m in provider_metrics.values()]
        all_peer_max = [m.peer_latency_max for m in provider_metrics.values()]
        all_dns_avg = [m.dns_latency_avg for m in provider_metrics.values()]
        all_dns_max = [m.dns_latency_max for m in provider_metrics.values()]
        
        # Calcular baseline (latencia máxima promedio observada)
        peer_baseline = max(all_peer_avg) if all_peer_avg else 10
        dns_baseline = max(all_dns_avg) if all_dns_avg else 10
        
        # Calcular variabilidad entre providers
        peer_diff = max(all_peer_avg) - min(all_peer_avg) if len(all_peer_avg) > 1 else 0
        dns_diff = max(all_dns_avg) - min(all_dns_avg) if len(all_dns_avg) > 1 else 0
        
        # Calcular jitter típico
        all_peer_jitter = [m.peer_jitter_avg for m in provider_metrics.values()]
        all_dns_jitter = [m.dns_jitter_avg for m in provider_metrics.values()]
        avg_peer_jitter = statistics.mean(all_peer_jitter) if all_peer_jitter else 1
        avg_dns_jitter = statistics.mean(all_dns_jitter) if all_dns_jitter else 1
        
        # Thresholds calculados
        thresholds = {
            'peer_warning': round(peer_baseline * 2, 0),
            'peer_critical': round(peer_baseline * 4, 0),
            'dns_warning': round(dns_baseline * 2.5, 0),
            'dns_critical': round(dns_baseline * 5, 0),
            'switch_margin': max(3, min(10, round(peer_diff + dns_diff + avg_peer_jitter, 0)))
        }
        
        # MTR config basado en latencias observadas
        avg_latency = statistics.mean(all_peer_avg + all_dns_avg)
        
        if avg_latency < 10:
            mtr_count = 5
            mtr_interval = 0.5
        elif avg_latency < 50:
            mtr_count = 5
            mtr_interval = 0.5
        else:
            mtr_count = 3
            mtr_interval = 1.0
        
        mtr_config = {
            'count': mtr_count,
            'timeout': 30,
            'interval': mtr_interval,
            'packet_size': 64
        }
        
        # Ciclo de monitoreo
        max_jitter = max([m.peer_jitter_max for m in provider_metrics.values()])
        cycle_interval = 60 if max_jitter > 10 or avg_latency > 50 else 30
        
        return {
            'latency_thresholds': thresholds,
            'mtr_config': mtr_config,
            'cycle_interval': cycle_interval,
            'metadata': {
                'peer_baseline': peer_baseline,
                'dns_baseline': dns_baseline,
                'avg_peer_jitter': avg_peer_jitter,
                'avg_dns_jitter': avg_dns_jitter,
                'provider_diff': peer_diff + dns_diff
            }
        }
    
    @staticmethod
    def generate_config_file(
        config: Dict,
        providers_info: List[Dict],
        provider_metrics: Dict[str, NetworkMetrics],
        output_file: str = "bgp_failover_config.py"
    ):
        """Genera archivo de configuración Python"""
        
        # Generar diccionarios de destinos y peers
        destinations_dict = "{\n"
        peers_dict = "{\n"
        policy_rules_example = "{\n"
        
        for i, prov in enumerate(providers_info):
            prov_name = prov['name'].upper().replace(' ', '_')
            destinations_dict += f"    '{prov_name}': '{prov['destination']}',\n"
            peers_dict += f"    '{prov_name}': '{prov['peer_ip']}',\n"
            policy_rules_example += f"    'EXPORT-TO-{prov_name}': {i*2+1},\n"
            policy_rules_example += f"    'SET-LOCAL-PREF-{prov_name}': {i*2+2},\n"
        
        destinations_dict += "}"
        peers_dict += "}"
        policy_rules_example += "}"
        
        content = f'''#!/usr/bin/env python3
"""
BGP Failover Configuration - Auto-generated
Generated by Network Calibration Tool (Generalized Version)

Providers analizados: {', '.join([p['name'] for p in providers_info])}
"""

# === CONFIGURACIÓN AUTOMÁTICA BASADA EN ANÁLISIS DE RED ===

# IDs de las reglas de políticas BGP (EDITAR SEGÚN TU NETBOX)
# EJEMPLO - Ajustar según tus reglas reales:
POLICY_RULE_IDS = {policy_rules_example}

# Configuración de MTR - Optimizada para tu red
MTR_CONFIG = {{
    'count': {config['mtr_config']['count']},
    'timeout': {config['mtr_config']['timeout']},
    'interval': {config['mtr_config']['interval']},
    'packet_size': {config['mtr_config']['packet_size']}
}}

# Destinos para MTR
MTR_DESTINATIONS = {destinations_dict}

# IPs de los peers BGP
PEER_IPS = {peers_dict}

# Versiones IP por provider (para MTR)
IP_VERSIONS = {{
'''
        
        for prov in providers_info:
            prov_name = prov['name'].upper().replace(' ', '_')
            content += f"    '{prov_name}': '{prov['ip_version']}',  # {'IPv4' if prov['ip_version'] == '4' else 'IPv6'}\n"
        
        content += f'''}}

# Thresholds de latencia - Calculados automáticamente
# Baseline observado:
#   - Peer: {config['metadata']['peer_baseline']:.2f}ms promedio
#   - DNS: {config['metadata']['dns_baseline']:.2f}ms promedio
#   - Jitter peer: {config['metadata']['avg_peer_jitter']:.2f}ms promedio
#   - Diferencia entre providers: {config['metadata']['provider_diff']:.2f}ms

LATENCY_THRESHOLDS = {{
    'peer_warning': {config['latency_thresholds']['peer_warning']},
    'peer_critical': {config['latency_thresholds']['peer_critical']},
    'dns_warning': {config['latency_thresholds']['dns_warning']},
    'dns_critical': {config['latency_thresholds']['dns_critical']},
    'switch_margin': {config['latency_thresholds']['switch_margin']}
}}

# Intervalo entre ciclos de monitoreo (segundos)
CYCLE_INTERVAL = {config['cycle_interval']}

# Lista de providers configurados
PROVIDERS = {[prov['name'].upper().replace(' ', '_') for prov in providers_info]}

# === MÉTRICAS DE REFERENCIA (Para análisis) ===

PROVIDER_BASELINES = {{
'''
        
        for provider, metrics in provider_metrics.items():
            content += f'''    '{provider.upper().replace(' ', '_')}': {{
        'peer_latency': {{
            'avg': {metrics.peer_latency_avg:.2f},
            'min': {metrics.peer_latency_min:.2f},
            'max': {metrics.peer_latency_max:.2f},
            'std': {metrics.peer_latency_std:.2f}
        }},
        'peer_jitter': {{
            'avg': {metrics.peer_jitter_avg:.2f},
            'max': {metrics.peer_jitter_max:.2f}
        }},
        'dns_latency': {{
            'avg': {metrics.dns_latency_avg:.2f},
            'min': {metrics.dns_latency_min:.2f},
            'max': {metrics.dns_latency_max:.2f},
            'std': {metrics.dns_latency_std:.2f}
        }},
        'dns_jitter': {{
            'avg': {metrics.dns_jitter_avg:.2f},
            'max': {metrics.dns_jitter_max:.2f}
        }},
        'success_rate': {metrics.success_rate:.1f}
    }},
'''
        
        content += '''}

# === NOTAS DE USO ===
# 1. Ajustar IDs de reglas de políticas BGP en POLICY_RULE_IDS según tu NetBox
# 2. Importar esta configuración en bgp_failover_mtr.py
# 3. Ejecutar primero en modo DRY_RUN = True
# 4. Monitorear comportamiento por 24 horas
# 5. Ajustar thresholds si es necesario
'''
        
        with open(output_file, 'w') as f:
            f.write(content)
        
        print(f"\n📄 Archivo de configuración generado: {output_file}")


def print_metrics_table(metrics: NetworkMetrics, provider_name: str):
    """Imprime tabla de métricas de forma legible"""
    print(f"\n  📊 Métricas de {provider_name}:")
    print(f"  {'─' * 70}")
    print(f"  {'Métrica':<30} {'Promedio':<12} {'Mín':<10} {'Máx':<10}")
    print(f"  {'─' * 70}")
    
    # Peer
    print(f"  {'Latencia Peer (ms)':<30} {metrics.peer_latency_avg:>10.2f}  "
          f"{metrics.peer_latency_min:>8.2f}  {metrics.peer_latency_max:>8.2f}")
    print(f"  {'Jitter Peer (ms)':<30} {metrics.peer_jitter_avg:>10.2f}  "
          f"{'─':>8}  {metrics.peer_jitter_max:>8.2f}")
    print(f"  {'Pérdida Peer (%)':<30} {'─':>10}  "
          f"{'─':>8}  {metrics.peer_loss_max:>8.2f}")
    
    print(f"  {'':<30}")
    
    # DNS
    print(f"  {'Latencia DNS (ms)':<30} {metrics.dns_latency_avg:>10.2f}  "
          f"{metrics.dns_latency_min:>8.2f}  {metrics.dns_latency_max:>8.2f}")
    print(f"  {'Jitter DNS (ms)':<30} {metrics.dns_jitter_avg:>10.2f}  "
          f"{'─':>8}  {metrics.dns_jitter_max:>8.2f}")
    print(f"  {'Pérdida DNS (%)':<30} {'─':>10}  "
          f"{'─':>8}  {metrics.dns_loss_max:>8.2f}")
    
    print(f"  {'─' * 70}")
    print(f"  {'Mediciones exitosas':<30} {metrics.measurement_count}/{CALIBRATION_CONFIG['cycles']} "
          f"({metrics.success_rate:.1f}%)")
    print(f"  {'Variabilidad latencia peer':<30} ±{metrics.peer_latency_std:.2f}ms")
    print(f"  {'Variabilidad latencia DNS':<30} ±{metrics.dns_latency_std:.2f}ms")


def print_recommendations(config: Dict, provider_metrics: Dict[str, NetworkMetrics]):
    """Imprime recomendaciones de configuración"""
    
    print("\n" + "=" * 80)
    print("🎯 CONFIGURACIÓN RECOMENDADA")
    print("=" * 80)
    
    thresh = config['latency_thresholds']
    mtr = config['mtr_config']
    meta = config['metadata']
    
    print("\n📊 ANÁLISIS DE RED:")
    print(f"  • Latencia base peer: {meta['peer_baseline']:.2f}ms")
    print(f"  • Latencia base DNS: {meta['dns_baseline']:.2f}ms")
    print(f"  • Jitter promedio: {meta['avg_peer_jitter']:.2f}ms")
    print(f"  • Diferencia entre providers: {meta['provider_diff']:.2f}ms")
    
    print("\n⚙️ THRESHOLDS CALCULADOS:")
    print(f"  LATENCY_THRESHOLDS = {{")
    print(f"      'peer_warning': {thresh['peer_warning']},")
    print(f"      'peer_critical': {thresh['peer_critical']},")
    print(f"      'dns_warning': {thresh['dns_warning']},")
    print(f"      'dns_critical': {thresh['dns_critical']},")
    print(f"      'switch_margin': {thresh['switch_margin']}")
    print(f"  }}")
    
    print("\n🔧 CONFIGURACIÓN MTR:")
    print(f"  MTR_CONFIG = {{")
    print(f"      'count': {mtr['count']},")
    print(f"      'timeout': {mtr['timeout']},")
    print(f"      'interval': {mtr['interval']},")
    print(f"      'packet_size': {mtr['packet_size']}")
    print(f"  }}")
    
    print(f"\n⏱️ CICLO DE MONITOREO: {config['cycle_interval']} segundos")
    
    # Clasificación de la red
    print("\n🏷️ CLASIFICACIÓN DE TU RED:")
    
    avg_latency = (meta['peer_baseline'] + meta['dns_baseline']) / 2
    avg_jitter = meta['avg_peer_jitter']
    
    if avg_latency < 10 and avg_jitter < 2:
        network_class = "🟢 EXCELENTE - Red de baja latencia y muy estable"
    elif avg_latency < 30 and avg_jitter < 5:
        network_class = "🟡 BUENA - Red estable con latencias normales"
    elif avg_latency < 100 and avg_jitter < 15:
        network_class = "🟠 ACEPTABLE - Red con latencias elevadas o jitter moderado"
    else:
        network_class = "🔴 PROBLEMÁTICA - Alta latencia o mucho jitter"
    
    print(f"  {network_class}")
    print(f"  • Latencia promedio: {avg_latency:.2f}ms")
    print(f"  • Jitter promedio: {avg_jitter:.2f}ms")
    
    # Comparación entre providers
    print("\n🔄 COMPARACIÓN ENTRE PROVIDERS:")
    
    providers = list(provider_metrics.keys())
    for i, provider in enumerate(providers):
        metrics = provider_metrics[provider]
        print(f"  • {provider}: {metrics.peer_latency_avg:.2f}ms peer, {metrics.dns_latency_avg:.2f}ms DNS")
    
    if len(providers) >= 2:
        # Comparar todos contra el mejor
        best_provider = min(providers, key=lambda p: provider_metrics[p].peer_latency_avg)
        best_latency = provider_metrics[best_provider].peer_latency_avg
        
        print(f"\n  🏆 Mejor provider (latencia): {best_provider} ({best_latency:.2f}ms)")
        
        for provider in providers:
            if provider != best_provider:
                diff = provider_metrics[provider].peer_latency_avg - best_latency
                print(f"  • {provider} es {diff:.2f}ms más lento")
    
    print("\n" + "=" * 80)


def main():
    """Función principal del calibrador"""
    
    print("=" * 80)
    print("🔬 BGP FAILOVER - HERRAMIENTA DE CALIBRACIÓN DE RED (GENERALIZADA)")
    print("=" * 80)
    print("\nEsta herramienta analizará tus enlaces ISP y generará la configuración")
    print("óptima para el sistema de failover BGP.\n")
    print("✨ Soporta: Múltiples providers, IPv4/IPv6, configuración flexible\n")
    
    # Solicitar número de providers
    print("📝 CONFIGURACIÓN INICIAL")
    print("-" * 80)
    
    while True:
        try:
            num_providers = int(input("\n¿Cuántos providers/ISPs deseas analizar? [2-10]: ").strip())
            if 2 <= num_providers <= 10:
                break
            else:
                print("  ⚠️ Por favor ingresa un número entre 2 y 10")
        except ValueError:
            print("  ⚠️ Por favor ingresa un número válido")
    
    print(f"\n✅ Se analizarán {num_providers} providers\n")
    
    # Recopilar información de cada provider
    providers = []
    
    for i in range(num_providers):
        print("=" * 80)
        print(f"🌐 PROVIDER {i+1} de {num_providers}")
        print("-" * 80)
        
        # Nombre del provider
        while True:
            prov_name = input(f"\n  Nombre del provider (ej: IXA, Cogent, Level3): ").strip()
            if prov_name:
                break
            print("  ⚠️ El nombre no puede estar vacío")
        
        # Destino MTR/DNS
        while True:
            prov_dest = input(f"  Destino MTR/DNS (IPv4 o IPv6): ").strip()
            is_valid, version, msg = validate_ip_address(prov_dest)
            print(f"  {msg}")
            if is_valid:
                dest_version = version
                break
        
        # Peer IP
        while True:
            prov_peer = input(f"  IP del peer BGP (IPv4 o IPv6): ").strip()
            is_valid, version, msg = validate_ip_address(prov_peer)
            print(f"  {msg}")
            
            if is_valid:
                # Verificar que coincida con la versión del destino
                if version != dest_version:
                    print(f"  ⚠️ Advertencia: El peer es IPv{version} pero el destino es IPv{dest_version}")
                    confirm = input("  ¿Continuar de todas formas? [s/N]: ").strip().lower()
                    if confirm == 's':
                        break
                else:
                    break
        
        providers.append({
            'name': prov_name,
            'destination': prov_dest,
            'peer_ip': prov_peer,
            'ip_version': dest_version
        })
        
        print(f"\n  ✅ Provider '{prov_name}' configurado")
    
    # Confirmar configuración
    print("\n" + "=" * 80)
    print("📋 RESUMEN DE CONFIGURACIÓN")
    print("=" * 80)
    
    for i, p in enumerate(providers, 1):
        ip_type = "IPv4" if p['ip_version'] == '4' else "IPv6"
        print(f"\nProvider {i}: {p['name']} ({ip_type})")
        print(f"  • Destino MTR: {p['destination']}")
        print(f"  • Peer BGP: {p['peer_ip']}")
    
    print(f"\n⏱️ Se ejecutarán {CALIBRATION_CONFIG['cycles']} ciclos de medición por provider")
    print(f"   (intervalo: {CALIBRATION_CONFIG['interval']} segundos)")
    total_time = num_providers * CALIBRATION_CONFIG['cycles'] * CALIBRATION_CONFIG['interval'] / 60
    print(f"   Tiempo total estimado: ~{total_time:.0f} minutos")
    
    confirm = input("\n¿Continuar con la calibración? [S/n]: ").strip().lower()
    if confirm and confirm != 's':
        print("Cancelado.")
        return 0
    
    # Ejecutar calibración
    print("\n" + "=" * 80)
    print("🚀 INICIANDO CALIBRACIÓN")
    print("=" * 80)
    
    provider_metrics = {}
    
    for provider in providers:
        print(f"\n📡 Analizando {provider['name']} ({'IPv' + provider['ip_version']})...")
        print("-" * 80)
        
        calibrator = NetworkCalibrator(
            provider['name'],
            provider['destination'],
            provider['peer_ip'],
            provider['ip_version']
        )
        
        # Ejecutar ciclos de medición
        for cycle in range(1, CALIBRATION_CONFIG['cycles'] + 1):
            calibrator.measure_cycle(cycle)
            
            if cycle < CALIBRATION_CONFIG['cycles']:
                print(f"  ⏳ Esperando {CALIBRATION_CONFIG['interval']}s...", end='\r', flush=True)
                time.sleep(CALIBRATION_CONFIG['interval'])
        
        # Calcular métricas
        metrics = calibrator.calculate_metrics()
        provider_metrics[provider['name']] = metrics
        
        # Mostrar resultados
        print_metrics_table(metrics, provider['name'])
    
    # Calcular thresholds recomendados
    print("\n" + "=" * 80)
    print("🧮 CALCULANDO CONFIGURACIÓN ÓPTIMA")
    print("=" * 80)
    
    config = ThresholdCalculator.calculate_thresholds(provider_metrics)
    
    # Mostrar recomendaciones
    print_recommendations(config, provider_metrics)
    
    # Generar archivo de configuración
    print("\n" + "=" * 80)
    print("📝 GENERANDO ARCHIVO DE CONFIGURACIÓN")
    print("=" * 80)
    
    output_file = input("\nNombre del archivo [bgp_failover_config.py]: ").strip()
    if not output_file:
        output_file = "bgp_failover_config.py"
    
    ThresholdCalculator.generate_config_file(config, providers, provider_metrics, output_file)
    
    # Instrucciones finales
    print("\n" + "=" * 80)
    print("✅ CALIBRACIÓN COMPLETADA")
    print("=" * 80)
    print("\n📋 PRÓXIMOS PASOS:")
    print("\n1. Revisar el archivo generado:")
    print(f"   cat {output_file}")
    print("\n2. Ajustar IDs de reglas de políticas en POLICY_RULE_IDS")
    print("\n3. Importar configuración en bgp_failover_mtr.py")
    print("\n4. Ejecutar en modo DRY_RUN primero")
    print("\n5. Monitorear 24 horas y ajustar si es necesario")
    print("\n" + "=" * 80)
    
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Calibración cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
