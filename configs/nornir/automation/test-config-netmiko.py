from nornir import InitNornir
from nornir_netmiko import netmiko_send_config, netmiko_send_command
from nornir_utils.plugins.functions import print_result
from nornir.core.filter import F

nr = InitNornir(config_file="config-nornir.yaml")

huawei_devices = nr.filter(F(groups__contains="huawei"))

# 1️⃣ Configuración
config_commands = [
    "interface Ethernet1/0/2",
    "description Uplink-to-Core"
]

config_result = huawei_devices.run(
    task=netmiko_send_config,
    config_commands=config_commands,
    exit_config_mode=False
)

print_result(config_result)

# 2️⃣ Commit (usando timing)
commit_result = huawei_devices.run(
    task=netmiko_send_command,
    command_string="commit",
    use_timing=True
)

print_result(commit_result)
