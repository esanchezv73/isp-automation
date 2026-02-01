#!/usr/bin/env python3
"""
BGP Failover Network Calibration Tool
Analiza las condiciones reales de la red y sugiere configuración óptima
"""

import subprocess
import json
import time
import statistics
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


class NetworkCalibrator:
    def __init__(self, provider_name: str, destination: str, peer_ip: str):
        self.provider_name = provider_name
        self.destination = destination
        self.peer_ip = peer_ip
        
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
                '-6', '-n', '-j',
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
        """Extrae métricas del peer (hop 2) y DNS (último hop)"""
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
        """
        Calcula thresholds óptimos basados en las características de todos los providers
        
        Metodología:
        - peer_warning: 2× la latencia promedio más alta observada
        - peer_critical: 4× la latencia promedio más alta observada
        - dns_warning: 2.5× la latencia promedio más alta observada
        - dns_critical: 5× la latencia promedio más alta observada
        - switch_margin: Basado en la diferencia típica entre providers
        """
        
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
            # Latencia peer
            'peer_warning': round(peer_baseline * 2, 0),
            'peer_critical': round(peer_baseline * 4, 0),
            
            # Latencia DNS (más tolerante porque es end-to-end)
            'dns_warning': round(dns_baseline * 2.5, 0),
            'dns_critical': round(dns_baseline * 5, 0),
            
            # Switch margin: Basado en diferencia típica + margen de seguridad
            # Mínimo 3, máximo 10
            'switch_margin': max(3, min(10, round(peer_diff + dns_diff + avg_peer_jitter, 0)))
        }
        
        # MTR config basado en latencias observadas
        avg_latency = statistics.mean(all_peer_avg + all_dns_avg)
        
        if avg_latency < 10:  # Red muy rápida
            mtr_count = 5
            mtr_interval = 0.5
        elif avg_latency < 50:  # Red normal
            mtr_count = 5
            mtr_interval = 0.5
        else:  # Red lenta
            mtr_count = 3
            mtr_interval = 1.0
        
        mtr_config = {
            'count': mtr_count,
            'timeout': 30,
            'interval': mtr_interval,
            'packet_size': 64
        }
        
        # Ciclo de monitoreo
        # Redes rápidas y estables: 30s
        # Redes lentas o inestables: 60s
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
        provider_metrics: Dict[str, NetworkMetrics],
        output_file: str = "bgp_failover_config.py"
    ):
        """Genera archivo de configuración Python"""
        
        content = f'''#!/usr/bin/env python3
"""
BGP Failover Configuration - Auto-generated
Generated by Network Calibration Tool
"""

# === CONFIGURACIÓN AUTOMÁTICA BASADA EN ANÁLISIS DE RED ===

# IDs de las reglas de políticas BGP (EDITAR SEGÚN TU NETBOX)
POLICY_RULE_IDS = {{
    'EXPORT-TO-IXA': 1,
    'EXPORT-TO-UFINET': 2, 
    'SET-LOCAL-PREF-IXA': 3,
    'SET-LOCAL-PREF-UFINET': 4
}}

# Configuración de MTR - Optimizada para tu red
MTR_CONFIG = {{
    'count': {config['mtr_config']['count']},
    'timeout': {config['mtr_config']['timeout']},
    'interval': {config['mtr_config']['interval']},
    'packet_size': {config['mtr_config']['packet_size']}
}}

# Destinos para MTR (EDITAR SEGÚN TU RED)
MTR_DESTINATIONS = {{
    'IXA': '2001:db8:8888::100',      # Cambiar a tu DNS de prueba
    'UFINET': '2001:db8:4444::100'    # Cambiar a tu DNS de prueba
}}

# IPs de los peers BGP (EDITAR SEGÚN TU RED)
PEER_IPS = {{
    'IXA': '2001:db8:ffaa::255',      # Cambiar a IP de tu peer
    'UFINET': '2001:db8:ffac::255'    # Cambiar a IP de tu peer
}}

# Thresholds de latencia - Calculados automáticamente
# Baseline observado:
#   - Peer: {config['metadata']['peer_baseline']:.2f}ms promedio
#   - DNS: {config['metadata']['dns_baseline']:.2f}ms promedio
#   - Jitter peer: {config['metadata']['avg_peer_jitter']:.2f}ms promedio
#   - Diferencia entre providers: {config['metadata']['provider_diff']:.2f}ms

LATENCY_THRESHOLDS = {{
    'peer_warning': {config['latency_thresholds']['peer_warning']},      # {config['metadata']['peer_baseline']:.1f}ms × 2
    'peer_critical': {config['latency_thresholds']['peer_critical']},     # {config['metadata']['peer_baseline']:.1f}ms × 4
    'dns_warning': {config['latency_thresholds']['dns_warning']},       # {config['metadata']['dns_baseline']:.1f}ms × 2.5
    'dns_critical': {config['latency_thresholds']['dns_critical']},      # {config['metadata']['dns_baseline']:.1f}ms × 5
    'switch_margin': {config['latency_thresholds']['switch_margin']}        # Diferencia típica + jitter
}}

# Intervalo entre ciclos de monitoreo (segundos)
CYCLE_INTERVAL = {config['cycle_interval']}  # {'Recomendado para red rápida/estable' if config['cycle_interval'] == 30 else 'Recomendado para red lenta/inestable'}

# === MÉTRICAS DE REFERENCIA (Para análisis) ===
# Estas son las condiciones observadas durante la calibración:

PROVIDER_BASELINES = {{
'''
        
        for provider, metrics in provider_metrics.items():
            content += f'''    '{provider}': {{
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
# 1. Revisar y ajustar IPs de destinos MTR y peers según tu red
# 2. Revisar IDs de reglas de políticas BGP en NetBox
# 3. Importar esta configuración en bgp_failover_mtr.py
# 4. Ejecutar primero en modo DRY_RUN = True
# 5. Monitorear comportamiento por 24 horas
# 6. Ajustar thresholds si es necesario
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
    if len(providers) >= 2:
        p1, p2 = providers[0], providers[1]
        m1, m2 = provider_metrics[p1], provider_metrics[p2]
        
        diff_peer = abs(m1.peer_latency_avg - m2.peer_latency_avg)
        diff_dns = abs(m1.dns_latency_avg - m2.dns_latency_avg)
        
        if diff_peer < 2 and diff_dns < 3:
            comparison = "Muy similares - switch_margin bajo recomendado"
        elif diff_peer < 5 and diff_dns < 10:
            comparison = "Diferencia moderada - configuración balanceada"
        else:
            comparison = "Diferencia significativa - switch_margin alto recomendado"
        
        print(f"  • {p1}: {m1.peer_latency_avg:.2f}ms peer, {m1.dns_latency_avg:.2f}ms DNS")
        print(f"  • {p2}: {m2.peer_latency_avg:.2f}ms peer, {m2.dns_latency_avg:.2f}ms DNS")
        print(f"  • Diferencia: {diff_peer:.2f}ms peer, {diff_dns:.2f}ms DNS")
        print(f"  → {comparison}")
    
    # Recomendaciones específicas
    print("\n💡 RECOMENDACIONES ESPECÍFICAS:")
    
    recommendations = []
    
    if avg_jitter > 10:
        recommendations.append("⚠️ Jitter alto detectado - considera aumentar historial de histeresis a 5 ciclos")
    
    if avg_latency > 50:
        recommendations.append("⚠️ Latencias altas - aumentar cycle_interval a 60-90 segundos")
    
    if meta['provider_diff'] < 3:
        recommendations.append("✓ Providers muy similares - switch_margin bajo previene flapping")
    
    if meta['provider_diff'] > 10:
        recommendations.append("⚠️ Gran diferencia entre providers - monitorear estabilidad")
    
    # Verificar pérdida de paquetes
    has_loss = any(m.peer_loss_max > 0 or m.dns_loss_max > 0 for m in provider_metrics.values())
    if has_loss:
        recommendations.append("🚨 Pérdida de paquetes detectada - investigar calidad de enlaces")
    else:
        recommendations.append("✓ Sin pérdida de paquetes - enlaces de buena calidad")
    
    if not recommendations:
        recommendations.append("✅ Configuración óptima generada - sin ajustes adicionales necesarios")
    
    for rec in recommendations:
        print(f"  {rec}")
    
    print("\n" + "=" * 80)


def main():
    """Función principal del calibrador"""
    
    print("=" * 80)
    print("🔬 BGP FAILOVER - HERRAMIENTA DE CALIBRACIÓN DE RED")
    print("=" * 80)
    print("\nEsta herramienta analizará tus enlaces ISP y generará la configuración")
    print("óptima para el sistema de failover BGP.\n")
    
    # Solicitar configuración
    print("📝 CONFIGURACIÓN DE PROVEEDORES")
    print("-" * 80)
    
    providers = []
    
    # Provider 1
    print("\n🌐 Provider 1:")
    p1_name = input("  Nombre (ej: IXA): ").strip() or "IXA"
    p1_dest = input("  Destino MTR/DNS (ej: 2001:db8:8888::100): ").strip()
    p1_peer = input("  IP del peer BGP (ej: 2001:db8:ffaa::255): ").strip()
    
    if p1_dest and p1_peer:
        providers.append({
            'name': p1_name,
            'destination': p1_dest,
            'peer_ip': p1_peer
        })
    else:
        print("  ❌ Configuración incompleta para Provider 1")
        return 1
    
    # Provider 2
    print("\n🌐 Provider 2:")
    p2_name = input("  Nombre (ej: UFINET): ").strip() or "UFINET"
    p2_dest = input("  Destino MTR/DNS (ej: 2001:db8:4444::100): ").strip()
    p2_peer = input("  IP del peer BGP (ej: 2001:db8:ffac::255): ").strip()
    
    if p2_dest and p2_peer:
        providers.append({
            'name': p2_name,
            'destination': p2_dest,
            'peer_ip': p2_peer
        })
    else:
        print("  ❌ Configuración incompleta para Provider 2")
        return 1
    
    # Confirmar configuración
    print("\n" + "=" * 80)
    print("📋 RESUMEN DE CONFIGURACIÓN")
    print("=" * 80)
    for i, p in enumerate(providers, 1):
        print(f"\nProvider {i}: {p['name']}")
        print(f"  • Destino MTR: {p['destination']}")
        print(f"  • Peer BGP: {p['peer_ip']}")
    
    print(f"\n⏱️ Se ejecutarán {CALIBRATION_CONFIG['cycles']} ciclos de medición")
    print(f"   (intervalo: {CALIBRATION_CONFIG['interval']} segundos)")
    print(f"   Tiempo total estimado: ~{CALIBRATION_CONFIG['cycles'] * CALIBRATION_CONFIG['interval'] / 60:.0f} minutos")
    
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
        print(f"\n📡 Analizando {provider['name']}...")
        print("-" * 80)
        
        calibrator = NetworkCalibrator(
            provider['name'],
            provider['destination'],
            provider['peer_ip']
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
    
    ThresholdCalculator.generate_config_file(config, provider_metrics, output_file)
    
    # Instrucciones finales
    print("\n" + "=" * 80)
    print("✅ CALIBRACIÓN COMPLETADA")
    print("=" * 80)
    print("\n📋 PRÓXIMOS PASOS:")
    print("\n1. Revisar el archivo generado:")
    print(f"   cat {output_file}")
    print("\n2. Ajustar IPs y IDs de reglas según tu NetBox")
    print("\n3. Importar configuración en bgp_failover_mtr.py:")
    print(f"   from {output_file.replace('.py', '')} import *")
    print("\n4. Ejecutar en modo DRY_RUN primero:")
    print("   DRY_RUN = True")
    print("\n5. Monitorear 24 horas y ajustar si es necesario")
    print("\n6. Activar en producción:")
    print("   DRY_RUN = False")
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
