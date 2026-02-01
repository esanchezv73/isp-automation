#!/bin/bash

# Crear bridge
brctl addbr br1
ip link set dev br1 up

# Agregar interfaces al bridge
brctl addif br1 eth1
brctl addif br1 eth2
brctl addif br1 eth3
brctl addif br1 eth4
brctl addif br1 eth5
brctl addif br1 eth6
brctl addif br1 eth7
