#!/usr/bin/env python3
"""
nornir_init.py - Inicialización Nornir + NetBoxInventory2
"""

from nornir import InitNornir

print("=== Inicializando Nornir con NetBoxInventory2 ===\n")

try:
    nr = InitNornir(config_file="config.yaml")

    print(f"✅ Nornir inicializado correctamente")
    print(f"📦 Dispositivos cargados: {len(nr.inventory.hosts)}\n")

    print("📋 Inventario:")
    for name, host in nr.inventory.hosts.items():
        print(f" • {name}")
        print(f"   ├─ hostname : {host.hostname}")
        print(f"   ├─ platform : {host.platform}")
        print(f"   └─ site     : {host.data.get('site', 'N/A')}")
        print()

except Exception as e:
    print(f"❌ Error inicializando Nornir: {e}")
    raise
