#!/usr/bin/env python3
"""
Script de diagnóstico para MTR - Detectar problemas
"""

import subprocess
import json
import time
import sys

# Destinos de prueba
DESTINATIONS = {
    'IXA': '2001:db8:8888::100',
    'UFINET': '2001:db8:4444::100'
}

PEER_IPS = {
    'IXA': '2001:db8:ffaa::255',
    'UFINET': '2001:db8:ffac::255'
}

def test_mtr_command(destination, provider, count=5, interval=0.5):
    """Prueba diferentes configuraciones de MTR"""
    
    print(f"\n{'='*80}")
    print(f"🧪 Probando MTR hacia {provider} ({destination})")
    print(f"{'='*80}\n")
    
    # Test 1: Verificar que MTR existe
    print("1️⃣ Verificando instalación de MTR...")
    try:
        result = subprocess.run(['which', 'mtr'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ MTR encontrado en: {result.stdout.strip()}")
        else:
            print("   ❌ MTR no encontrado. Instalar con: sudo apt-get install mtr-tiny")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # Test 2: Verificar versión
    print("\n2️⃣ Verificando versión de MTR...")
    try:
        result = subprocess.run(['mtr', '--version'], capture_output=True, text=True)
        print(f"   ✅ {result.stdout.strip()}")
    except Exception as e:
        print(f"   ⚠️ No se pudo obtener versión: {e}")
    
    # Test 3: Probar conectividad básica (ping6)
    print(f"\n3️⃣ Probando conectividad IPv6 básica...")
    try:
        result = subprocess.run(
            ['ping6', '-c', '2', '-W', '2', destination],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print(f"   ✅ Conectividad IPv6 OK")
        else:
            print(f"   ❌ Ping6 falló: {result.stderr}")
            return False
    except Exception as e:
        print(f"   ❌ Error en ping6: {e}")
        return False
    
    # Test 4: MTR simple sin JSON
    print(f"\n4️⃣ Probando MTR modo texto (sin JSON)...")
    cmd_simple = ['mtr', '-6', '-n', '-c', '3', '-r', destination]
    print(f"   Comando: {' '.join(cmd_simple)}")
    
    start = time.time()
    try:
        result = subprocess.run(
            cmd_simple,
            capture_output=True,
            text=True,
            timeout=30
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:
            print(f"   ✅ MTR completado en {elapsed:.1f} segundos")
            print(f"\n   Output (primeras 10 líneas):")
            for line in result.stdout.split('\n')[:10]:
                if line.strip():
                    print(f"   {line}")
        else:
            print(f"   ❌ MTR falló: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"   ❌ Timeout después de 30 segundos")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # Test 5: MTR con JSON
    print(f"\n5️⃣ Probando MTR con JSON...")
    cmd_json = ['mtr', '-6', '-n', '-j', '-c', str(count), '-i', str(interval), destination]
    print(f"   Comando: {' '.join(cmd_json)}")
    
    start = time.time()
    try:
        result = subprocess.run(
            cmd_json,
            capture_output=True,
            text=True,
            timeout=30
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:
            print(f"   ✅ MTR JSON completado en {elapsed:.1f} segundos")
            
            # Parsear JSON
            try:
                data = json.loads(result.stdout)
                print(f"\n   📊 Análisis del reporte:")
                print(f"   - Source: {data['report']['mtr']['src']}")
                print(f"   - Destination: {data['report']['mtr']['dst']}")
                print(f"   - Tests: {data['report']['mtr']['tests']}")
                print(f"   - Hops encontrados: {len(data['report']['hubs'])}")
                
                print(f"\n   🛣️ Path completo:")
                for hub in data['report']['hubs']:
                    print(f"   Hop {hub['count']}: {hub['host']} - "
                          f"Avg: {hub['Avg']:.2f}ms, Loss: {hub['Loss%']}%, "
                          f"StDev: {hub['StDev']:.2f}ms")
                
                # Verificar si encontramos el peer
                peer_ip = PEER_IPS[provider]
                peer_found = False
                for hub in data['report']['hubs']:
                    if hub['host'] == peer_ip:
                        peer_found = True
                        print(f"\n   ✅ Peer {peer_ip} encontrado en hop {hub['count']}")
                        print(f"      - Avg: {hub['Avg']:.2f}ms")
                        print(f"      - Loss: {hub['Loss%']}%")
                        print(f"      - StDev: {hub['StDev']:.2f}ms")
                        break
                
                if not peer_found:
                    print(f"\n   ⚠️ Peer {peer_ip} NO encontrado en el path")
                    print(f"      Esto puede causar problemas en el script principal")
                
                return True
                
            except json.JSONDecodeError as e:
                print(f"   ❌ Error parseando JSON: {e}")
                print(f"   Output recibido (primeros 500 chars):")
                print(f"   {result.stdout[:500]}")
                return False
        else:
            print(f"   ❌ MTR falló con código {result.returncode}")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ❌ Timeout después de 30 segundos")
        print(f"   💡 Sugerencia: Aumentar MTR_CONFIG['timeout'] o reducir 'count'")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def main():
    print("🔍 DIAGNÓSTICO DE MTR PARA BGP FAILOVER")
    print("="*80)
    
    all_ok = True
    
    for provider, destination in DESTINATIONS.items():
        if not test_mtr_command(destination, provider):
            all_ok = False
    
    print(f"\n{'='*80}")
    if all_ok:
        print("✅ TODAS LAS PRUEBAS PASARON")
        print("\n💡 Recomendaciones:")
        print("   - MTR_CONFIG['count']: 5 (balance velocidad/precisión)")
        print("   - MTR_CONFIG['interval']: 0.5 (segundos entre paquetes)")
        print("   - MTR_CONFIG['timeout']: 30 (segundos)")
    else:
        print("❌ ALGUNAS PRUEBAS FALLARON")
        print("\n🔧 Pasos para resolver:")
        print("   1. Verificar que mtr-tiny esté instalado: sudo apt-get install mtr-tiny")
        print("   2. Verificar conectividad IPv6: ping6 -c 3 2001:db8:8888::100")
        print("   3. Verificar permisos: MTR necesita capacidades de red")
        print("   4. Si sigue fallando, ejecutar con sudo")
    
    print("="*80)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
