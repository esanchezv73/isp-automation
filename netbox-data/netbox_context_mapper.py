#!/usr/bin/env python3
"""
NetBox Context Mapper - Analiza cualquier respuesta JSON de NetBox
y genera la jerarquía de acceso para templates Jinja2.
"""
import json
import sys
from collections import OrderedDict

def get_jinja2_path(data, parent_path=""):
    """
    Recorre recursivamente la estructura JSON y genera rutas de acceso Jinja2.
    """
    paths = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{parent_path}.{key}" if parent_path else key
            
            if isinstance(value, (dict, list)):
                # Recursión para objetos anidados
                paths.extend(get_jinja2_path(value, current_path))
            else:
                # Valor terminal
                paths.append({
                    'jinja2_path': current_path,
                    'value': value,
                    'type': type(value).__name__
                })
                
    elif isinstance(data, list):
        for i, item in enumerate(data):
            current_path = f"{parent_path}[{i}]"
            if isinstance(item, (dict, list)):
                paths.extend(get_jinja2_path(item, current_path))
            else:
                paths.append({
                    'jinja2_path': current_path,
                    'value': item,
                    'type': type(item).__name__
                })
    
    return paths

def analyze_netbox_json(json_file):
    """Analiza cualquier archivo JSON de NetBox y muestra todas las rutas de acceso."""
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    print("=== NETBOX CONTEXT MAPPER ===\n")
    print(f"Archivo analizado: {json_file}")
    print(f"Estructura raíz: {type(data).__name__}\n")
    
    # Obtener todas las rutas de acceso
    all_paths = get_jinja2_path(data)
    
    if not all_paths:
        print("⚠️  No se encontraron rutas de acceso en el archivo JSON.")
        return
    
    print(f"✅ Encontradas {len(all_paths)} rutas de acceso posibles:\n")
    
    # Agrupar por tipo de dato
    grouped_by_type = {}
    for path_info in all_paths:
        dtype = path_info['type']
        if dtype not in grouped_by_type:
            grouped_by_type[dtype] = []
        grouped_by_type[dtype].append(path_info)
    
    # Mostrar resultados agrupados
    for dtype, paths in grouped_by_type.items():
        print(f"--- {dtype.upper()} ({len(paths)} elementos) ---")
        for path_info in sorted(paths, key=lambda x: x['jinja2_path']):
            value_str = str(path_info['value'])[:50]  # Truncar valores largos
            if len(str(path_info['value'])) > 50:
                value_str += "..."
            print(f"  {{ {{ {path_info['jinja2_path']} }} }} = {value_str}")
        print()
    
    # Mostrar ejemplos de uso comunes
    print("=== EJEMPLOS DE USO COMUNES ===")
    common_examples = [
        ("device.name", "Nombre del dispositivo"),
        ("device.cf.local_asn", "Custom field ASN local"),
        ("bgp_sessions[0].remote_address.address", "Primera sesión BGP - dirección remota"),
        ("interfaces[0].name", "Primera interfaz - nombre"),
        ("ip_addresses[0].address", "Primera dirección IP"),
    ]
    
    for example, description in common_examples:
        print(f"  {example} → {description}")

def main():
    if len(sys.argv) != 2:
        print("Uso: python netbox_context_mapper.py <archivo_netbox.json>")
        print("\nEjemplo:")
        print("  python netbox_context_mapper.py device_export.json")
        sys.exit(1)
    
    json_file = sys.argv[1]
    try:
        analyze_netbox_json(json_file)
    except FileNotFoundError:
        print(f"Error: Archivo '{json_file}' no encontrado.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Archivo JSON inválido - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
