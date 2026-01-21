#!/usr/bin/env python3
"""
NetBox API Context Analyzer - Analiza directamente la API de NetBox
y simula el contexto de renderizado para templates Jinja2.
"""
import requests
import sys
import json
from urllib.parse import urljoin

class NetBoxContextAnalyzer:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'Token {token}',
            'Accept': 'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch_endpoint(self, endpoint):
        """Obtiene datos de un endpoint de NetBox."""
        url = urljoin(self.base_url, endpoint)
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error al acceder a {url}: {e}")
            return None
    
        
    def get_device_with_related_data(self, device_id):
        """Obtiene un dispositivo con sus datos relacionados (interfaces, BGP sessions, etc.)."""
        # Obtener dispositivo básico
        device = self.fetch_endpoint(f'/api/dcim/devices/{device_id}/')
        if not device:
            return None
        
        # Obtener interfaces del dispositivo
        interfaces = self.fetch_endpoint(f'/api/dcim/interfaces/?device_id={device_id}')
        if interfaces:
            device['interfaces'] = interfaces.get('results', [])
            
            # Obtener direcciones IP para cada interfaz
            for intf in device['interfaces']:
                ip_addresses = self.fetch_endpoint(f'/api/ipam/ip-addresses/?interface_id={intf["id"]}')
                if ip_addresses:
                    intf['ip_addresses'] = ip_addresses.get('results', [])
        
        # Obtener sesiones BGP del dispositivo
        bgp_sessions = self.fetch_endpoint(f'/api/plugins/bgp/bgpsession/?device_id={device_id}')
        if bgp_sessions:
            device['bgp_sessions'] = bgp_sessions.get('results', [])
        
        # Obtener direcciones IP primarias
        if device.get('primary_ip4'):
            primary_ip4 = self.fetch_endpoint(f'/api/ipam/ip-addresses/{device["primary_ip4"]["id"]}/')
            if primary_ip4:
                device['primary_ip4'] = primary_ip4
        
        return device
    
    def simulate_device_context(self, device_data):
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
    
    def simulate_interface_context(self, interfaces_data):
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
        print("    intf.ip_addresses.all() | selectattr('family.value', 'equalto', 4) → solo IPv4")
        print("    intf.ip_addresses.all() | selectattr('family.value', 'equalto', 6) → solo IPv6")
        
        # Mostrar estructura de IP addresses
        if 'ip_addresses' in sample_intf and sample_intf['ip_addresses']:
            print("\n  Estructura de IP Addresses (cada 'ip' en el bucle):")
            sample_ip = sample_intf['ip_addresses'][0]
            if 'address' in sample_ip:
                full_addr = sample_ip['address']
                addr_only = full_addr.split('/')[0]
                family_value = sample_ip.get('family', {}).get('value', 'N/A')
                print(f"    {{ {{ ip.address }} }} → {full_addr}")
                print(f"    {{ {{ ip.address.split('/')[0] }} }} → {addr_only}")
                print(f"    {{ {{ ip.family.value }} }} → {family_value}")
    
    def simulate_bgp_context(self, bgp_sessions_data):
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
    
    def analyze_device_context(self, device_id):
        """Analiza el contexto completo de un dispositivo."""
        print(f"=== ANALIZANDO DISPOSITIVO ID: {device_id} ===\n")
        
        device_data = self.get_device_with_related_data(device_id)
        if not device_data:
            print("No se pudo obtener el dispositivo.")
            return
        
        print(f"Dispositivo: {device_data.get('name', 'N/A')}\n")
        
        # Simular contexto de dispositivo
        self.simulate_device_context(device_data)
        
        # Simular contexto de interfaces
        if 'interfaces' in device_data:
            self.simulate_interface_context(device_data['interfaces'])
        
        # Simular contexto de BGP sessions
        if 'bgp_sessions' in device_data:
            self.simulate_bgp_context(device_data['bgp_sessions'])
        
        print("\n=== NOTAS IMPORTANTES ===")
        print("1. En templates, las relaciones usan métodos como .all()")
        print("2. Los filtros como selectattr() funcionan en QuerySets")
        print("3. Usa family.value para filtrar por tipo de IP (4=IPv4, 6=IPv6)")
        print("4. Este análisis refleja el contexto real de renderizado de NetBox")

def main():
    if len(sys.argv) != 4:
        print("Uso: python netbox_api_context_analyzer.py <netbox_url> <api_token> <device_id>")
        print("\nEjemplo:")
        print("  python netbox_api_context_analyzer.py http://netbox.example.com 1234567890abcdef 3")
        sys.exit(1)
    
    netbox_url = sys.argv[1]
    api_token = sys.argv[2]
    device_id = sys.argv[3]
    
    try:
        analyzer = NetBoxContextAnalyzer(netbox_url, api_token)
        analyzer.analyze_device_context(device_id)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
