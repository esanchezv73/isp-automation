#!/usr/bin/env python3
"""
NetBox Render Context Simulator - Simula el contexto real de renderizado
mostrando cómo se accede a los datos en templates Jinja2.
"""
import json
import sys

def simulate_device_context(device_data):
    """Simula el contexto de un dispositivo en renderizado."""
    print("=== CONTEXTO DE DISPOSITIVO (device) ===")
    print("En templates, puedes acceder a:")
    
    # Atributos básicos
    basic_attrs = ['name', 'description', 'id']
    for attr in basic_attrs:
        if attr in device_data:
            print(f"  {{ {{ device.{attr} }} }} → {device_data[attr]}")
    
    # Custom fields
    if 'custom_fields' in device_data and device_data['custom_fields']:
        print("\n  Custom Fields (dos formas):")
        for cf_name, cf_value in device_data['custom_fields'].items():
            print(f"    {{ {{ device.cf.{cf_name} }} }} → {cf_value}")
            print(f"    {{ {{ device.custom_fields.{cf_name} }} }} → {cf_value}")
    
    # Primary IP
    if 'primary_ip4' in device_data and device_data['primary_ip4']:
        ip_addr = device_data['primary_ip4']['address']
        ip_only = ip_addr.split('/')[0]
        print(f"\n  Primary IP4:")
        print(f"    {{ {{ device.primary_ip4.address }} }} → {ip_addr}")
        print(f"    {{ {{ device.primary_ip4.address.ip }} }} → {ip_only}")

def simulate_interface_context(interfaces_data):
    """Simula el contexto de interfaces en renderizado."""
    print("\n=== CONTEXTO DE INTERFACES (intf) ===")
    print("Cuando iteras sobre interfaces, cada 'intf' tiene:")
    
    if not interfaces_data:
        print("  No hay interfaces en los datos.")
        return
    
    # Tomar la primera interfaz como ejemplo
    sample_intf = interfaces_data[0] if isinstance(interfaces_data, list) else interfaces_data
    
    intf_attrs = ['name', 'description', 'enabled', 'id']
    for attr in intf_attrs:
        if attr in sample_intf:
            print(f"  {{ {{ intf.{attr} }} }} → {sample_intf[attr]}")
    
    print("\n  Relaciones (métodos disponibles):")
    print("    intf.ip_addresses.all() → todas las direcciones IP asignadas")
    print("    intf.ip_addresses.all() | selectattr('family', 'equalto', 4) → solo IPv4")
    print("    intf.ip_addresses.all() | selectattr('family', 'equalto', 6) → solo IPv6")
    
    # Mostrar estructura de IP addresses
    if 'ip_addresses' in sample_intf and sample_intf['ip_addresses']:
        print("\n  Estructura de IP Addresses (cada 'ip' en el bucle):")
        sample_ip = sample_intf['ip_addresses'][0]
        if 'address' in sample_ip:
            full_addr = sample_ip['address']
            addr_only = full_addr.split('/')[0]
            print(f"    {{ {{ ip.address }} }} → {full_addr}")
            print(f"    {{ {{ ip.address.ip }} }} → {addr_only} (requiere filtro ipaddr)")
            print(f"    {{ {{ ip.family.value }} }} → {sample_ip.get('family', {}).get('value', 'N/A')}")

def simulate_bgp_context(bgp_sessions_data):
    """Simula el contexto de sesiones BGP en renderizado."""
    print("\n=== CONTEXTO DE SESIONES BGP (session) ===")
    print("Cuando iteras sobre bgp_sessions, cada 'session' tiene:")
    
    if not bgp_sessions_data:
        print("  No hay sesiones BGP en los datos.")
        return
    
    sample_session = bgp_sessions_data[0] if isinstance(bgp_sessions_data, list) else bgp_sessions_data
    
    session_attrs = ['name', 'description', 'id']
    for attr in session_attrs:
        if attr in sample_session:
            print(f"  {{ {{ session.{attr} }} }} → {sample_session[attr]}")
    
    # Relaciones complejas
    if 'remote_address' in sample_session:
        remote_addr = sample_session['remote_address']['address']
        addr_only = remote_addr.split('/')[0]
        print(f"\n  Remote Address:")
        print(f"    {{ {{ session.remote_address.address }} }} → {remote_addr}")
        print(f"    {{ {{ session.remote_address.address.split('/')[0] }} }} → {addr_only}")
    
    if 'remote_as' in sample_session:
        remote_asn = sample_session['remote_as']['asn']
        print(f"\n  Remote AS:")
        print(f"    {{ {{ session.remote_as.asn }} }} → {remote_asn}")
    
    if 'local_address' in sample_session:
        local_addr = sample_session['local_address']['address']
        print(f"\n  Local Address:")
        print(f"    {{ {{ session.local_address.address }} }} → {local_addr}")
    
    if 'export_policies' in sample_session and sample_session['export_policies']:
        print(f"\n  Export Policies:")
        for i, policy in enumerate(sample_session['export_policies']):
            print(f"    {{ {{ session.export_policies[{i}].name }} }} → {policy['name']}")

def analyze_netbox_render_context(json_file):
    """Analiza el JSON y simula el contexto de renderizado real."""
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    print("=== NETBOX RENDER CONTEXT SIMULATOR ===\n")
    print(f"Analizando: {json_file}\n")
    
    # Detectar tipo de datos
    if isinstance(data, dict):
        if 'results' in data and isinstance(data['results'], list):
            # Es una lista paginada (como /api/dcim/devices/)
            results = data['results']
            if results:
                item = results[0]
                if 'device_type' in item:  # Es un dispositivo
                    simulate_device_context(item)
                elif 'interface' in str(item):  # Es una interfaz
                    simulate_interface_context(results)
                elif 'remote_address' in item:  # Es una sesión BGP
                    simulate_bgp_context(results)
        else:
            # Es un objeto individual
            if 'device_type' in data:  # Dispositivo individual
                simulate_device_context(data)
            elif 'remote_address' in data:  # Sesión BGP individual
                simulate_bgp_context(data)
            elif 'interfaces' in data:  # Dispositivo con interfaces
                simulate_device_context(data)
                if 'interfaces' in data:
                    simulate_interface_context(data['interfaces'])
            elif 'bgp_sessions' in data:  # Dispositivo con sesiones BGP
                simulate_device_context(data)
                simulate_bgp_context(data['bgp_sessions'])
    
    print("\n=== NOTAS IMPORTANTES ===")
    print("1. En templates, las relaciones usan métodos como .all()")
    print("2. Los filtros como selectattr() funcionan en QuerySets")
    print("3. La estructura en templates es más rica que el JSON de la API")
    print("4. Usa este simulador para entender cómo acceder a tus datos")

def main():
    if len(sys.argv) != 2:
        print("Uso: python netbox_render_context_simulator.py <archivo_netbox.json>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    try:
        analyze_netbox_render_context(json_file)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
