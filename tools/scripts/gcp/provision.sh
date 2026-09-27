#!/usr/bin/env bash
# Creates the Cairn scale-test fleet on GCP: a VPC with one subnet, firewall rules (SSH from
# this machine's IPv4 only; all traffic inside the subnet), NODES node VMs and a bench VM.
# BILLED FROM CREATION: run teardown.sh when done.
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
MYIP="$(curl -4 -s https://api.ipify.org)"
[ -f "$KEY_FILE" ] || ssh-keygen -t ed25519 -N "" -C cairn-scale -f "$KEY_FILE"
$G compute networks describe "$NET" >/dev/null 2>&1 || $G compute networks create "$NET" --subnet-mode=custom
$G compute networks subnets describe "$SUBNET" --region "$REGION" >/dev/null 2>&1 || \
  $G compute networks subnets create "$SUBNET" --network "$NET" --region "$REGION" --range "$RANGE"
$G compute firewall-rules describe cairn-ssh >/dev/null 2>&1 || \
  $G compute firewall-rules create cairn-ssh --network "$NET" --allow tcp:22 --source-ranges "$MYIP/32"
$G compute firewall-rules describe cairn-internal >/dev/null 2>&1 || \
  $G compute firewall-rules create cairn-internal --network "$NET" --allow tcp,udp,icmp --source-ranges "$RANGE"
META="ssh-keys=$SSH_USER:$(cat "$KEY_FILE.pub"),enable-oslogin=FALSE"
create() { # name type ip disk_gb
  $G compute instances describe "$1" --zone "$ZONE" >/dev/null 2>&1 && { echo "$1 exists"; return; }
  $G compute instances create "$1" --zone "$ZONE" --machine-type "$2" \
    --network-interface "subnet=$SUBNET,private-network-ip=$3" \
    --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud \
    --boot-disk-size "${4}GB" --boot-disk-type pd-ssd --labels project=cairn --metadata "$META"
}
for i in $(seq 1 "$NODES"); do create "$(node_name "$i")" "$NODE_TYPE" "$(node_ip "$i")" "$NODE_DISK_GB"; done
create "$BENCH_NAME" "$BENCH_TYPE" "$BENCH_IP" 60
# New VMs have new host keys, and GCP may hand out public IPs used by earlier fleets: forget
# the keys recorded for these IPs.
for vm in $(for i in $(seq 1 "$NODES"); do node_name "$i"; done) "$BENCH_NAME"; do
  ssh-keygen -R "$(public_ip "$vm")" -f "$HOME/.ssh/cairn_gcp_known_hosts" >/dev/null 2>&1 || true
done
$G compute instances list --filter="labels.project=cairn" --format="table(name,machineType.basename(),status,networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)"
