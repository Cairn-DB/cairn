#!/usr/bin/env bash
# Creates the Cairn scale-test fleet on Hetzner Cloud: a private network, a firewall that only
# admits SSH from this machine's public IPv4, an SSH key, NODES node VMs and one bench VM.
# BILLED FROM CREATION: run teardown.sh when done. Usage: provision.sh
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
MYIP="$(curl -4 -s https://api.ipify.org)"
[ -f "$KEY_FILE" ] || ssh-keygen -t ed25519 -N "" -C cairn-scale -f "$KEY_FILE"
hcloud ssh-key describe "$KEY_NAME" >/dev/null 2>&1 || \
  hcloud ssh-key create --name "$KEY_NAME" --public-key-from-file "$KEY_FILE.pub" --label "$LABEL"
hcloud network describe "$NET_NAME" >/dev/null 2>&1 || {
  hcloud network create --name "$NET_NAME" --ip-range "$NET_RANGE" --label "$LABEL"
  hcloud network add-subnet "$NET_NAME" --type cloud --network-zone eu-central --ip-range 10.77.0.0/24
}
hcloud firewall describe "$FW_NAME" >/dev/null 2>&1 || {
  hcloud firewall create --name "$FW_NAME" --label "$LABEL"
  hcloud firewall add-rule "$FW_NAME" --direction in --protocol tcp --port 22 --source-ips "$MYIP/32" \
    --description "ssh from operator"
}
create() { # name type private_ip
  hcloud server describe "$1" >/dev/null 2>&1 && { echo "$1 exists"; return; }
  hcloud server create --name "$1" --type "$2" --image "$IMAGE" --location "$LOCATION" \
    --ssh-key "$KEY_NAME" --firewall "$FW_NAME" --label "$LABEL" --without-ipv6=false
  hcloud server attach-to-network "$1" --network "$NET_NAME" --ip "$3"
}
for i in $(seq 1 "$NODES"); do create "$(node_name "$i")" "$NODE_TYPE" "$(node_ip "$i")"; done
create "$BENCH_NAME" "$BENCH_TYPE" "$BENCH_IP"
hcloud server list -l "$LABEL"
