apt update
sleep 1
apt install curl python3-dev build-essential libssl-dev libffi-dev -y
sleep 3
pip install nornir nornir-netmiko nornir-napalm nornir-utils
sleep 3
pip install pynetbox
sleep 1
pip install nornir-jinja2
sleep 1
pip install nornir-netbox
sleep 1
pip install nornir-scrapli
sleep 1
