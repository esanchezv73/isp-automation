#!/usr/bin/env python3
"""
Apply BGP policy changes to Huawei router - With undo logic for as-path
"""

import os
from nornir import InitNornir
from nornir_netmiko import netmiko_send_config, netmiko_send_command
from nornir_utils.plugins.functions import print_result
from nornir.core.filter import F
import requests

def get_bgp_policies_from_netbox():
    """Obtiene políticas BGP desde NetBox API"""
    netbox_token = os.getenv('NETBOX_TOKEN', 'rDbitCHC2V3fQy2Ksmr1pRuagb7pCc2qXCYz7qEp')
    netbox_url = os.getenv('NETBOX_URL', 'http://192.168.0.140:8000')
    
    headers = {"Authorization": f"Bearer nbt_8gWOf9dUSS7v.{netbox_token}"}
    url = f"{netbox_url}/api/plugins/bgp/routing-policy-rule/"
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    
    policies = {}
    for rule in resp.json()['results']:
        policy_name = rule['routing_policy']['name']
        policies[policy_name] = {
            'match_ipv6_address': [p['name'] for p in rule['match_ipv6_address']],
            'apply_community': rule['custom_fields'].get('apply_community'),
            'local_preference': rule['custom_fields'].get('local_preference'),
            'as_path_prepend_count': rule['custom_fields'].get('as_path_prepend_count')
        }
    
    return policies

def render_bgp_config(policies, local_asn="65000"):
    """Renderiza configuración BGP incremental con lógica de undo"""
    config_lines = []
    
    for policy_name, policy_data in policies.items():
        config_lines.append(f"route-policy {policy_name} permit node 10")
        
        # Match conditions
        if policy_data['match_ipv6_address']:
            for prefix_list in policy_data['match_ipv6_address']:
                config_lines.append(f" if-match ipv6 address prefix-list {prefix_list}")
        
        # Apply community
        if policy_data['apply_community']:
            config_lines.append(f" apply community {policy_data['apply_community']}")
        
        # Apply local preference
        if policy_data['local_preference']:
            config_lines.append(f" apply local-preference {policy_data['local_preference']}")
        
        # Apply as-path with undo logic
        as_path_count = policy_data['as_path_prepend_count']
        if as_path_count is not None:
            as_path_count = int(as_path_count)
            if as_path_count == 0:
                # Eliminar cualquier as-path existente
                config_lines.append(" undo apply as-path")
            elif as_path_count > 0:
                # Aplicar nuevo as-path
                as_path_cmd = " ".join([local_asn] * as_path_count)
                config_lines.append(f" apply as-path {as_path_cmd} additive")
        # Si as_path_prepend_count es None, no hacer nada
        
        config_lines.append("quit")
    
    return config_lines

def main():
    # Inicializar Nornir
    nr = InitNornir(config_file="config-nornir.yaml")
    huawei_devices = nr.filter(F(groups__contains="huawei"))
    
    # Obtener y renderizar configuración BGP
    policies = get_bgp_policies_from_netbox()
    local_asn = "65000"
    config_commands = render_bgp_config(policies, local_asn)
    
    print("🔧 Aplicando políticas BGP al router Huawei...")
    print("Comandos a aplicar:")
    for cmd in config_commands:
        print(f"  {cmd}")
    
    # 1️⃣ Aplicar configuración BGP
    config_result = huawei_devices.run(
        task=netmiko_send_config,
        config_commands=config_commands,
        exit_config_mode=False
    )
    
    print_result(config_result)
    
    # 2️⃣ Commit usando timing
    commit_result = huawei_devices.run(
        task=netmiko_send_command,
        command_string="commit",
        use_timing=True
    )
    
    print_result(commit_result)
    
    if config_result.failed or commit_result.failed:
        print("❌ Pipeline failed due to configuration errors")
        exit(1)
    else:
        print("✅ BGP policies applied successfully")

if __name__ == "__main__":
    main()
           
        
