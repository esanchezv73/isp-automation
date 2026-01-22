#!/usr/bin/env python3
"""
deploy_huawei_netbox.py - Versión final y limpia
"""

import os
import requests
from netmiko import ConnectHandler

# === Configuración ===
NETBOX_URL = "http://192.168.117.135:8000"
NETBOX_TOKEN = "c889397e6b09cfd1556378047213220b2c47b7e8"
DEVICE_ID = 3  # ¡Usamos ID directo para evitar ambigüedad!

DEVICE = {
    "device_type": "huawei",
    "host": "172.90.90.7",
    "username": "admin",
    "password": "admin",
    "timeout": 10,
}

def get_rendered_config() -> str:
    url = f"{NETBOX_URL}/api/dcim/devices/{DEVICE_ID}/render-config/"
    headers = {
        "Authorization": f"Token {NETBOX_TOKEN}",
        "Accept": "text/plain",
    }
    print("📡 Solicitando configuración renderizada a NetBox...")
    resp = requests.post(url, headers=headers)
    resp.raise_for_status()
    
    # Verificación adicional: asegurarse de que no es JSON
    if resp.text.strip().startswith("{"):
        raise RuntimeError("❌ ¡Se recibió JSON! Algo está mal en la solicitud.")
    
    return resp.text

def main():
    config_text = get_rendered_config()
    print("✅ Configuración recibida. Enviando al dispositivo...\n")

    conn = ConnectHandler(**DEVICE)
    try:
        # Enviar toda la configuración de una vez
        output = conn.send_config_set(
            config_commands=config_text.splitlines(),
            cmd_verify=False,
            exit_config_mode=False  # Huawei necesita 'return' explícito
        )
        print("📤 Salida del dispositivo:")
        print(output)

        # Guardar configuración
        print("\n💾 Guardando configuración...")
        save_out = conn.save_config()
        print(save_out)

    finally:
        conn.disconnect()
        print("\n✅ ¡Proceso completado!")

if __name__ == "__main__":
    main()
