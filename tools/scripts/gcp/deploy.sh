#!/usr/bin/env bash
# Installs Rust on node 1, copies the repository (no target/, data/, .git), builds the release
# binaries there, copies them to the other VMs, and starts the dataset download on the bench VM.
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
ROOT="$(cd ../../.. && pwd)"
ssh_to() { ssh $SSH_OPTS "$SSH_USER@$(public_ip "$1")" "${@:2}"; }
# Waits for SSH, at most 10 minutes, and fails loudly (a silent endless retry once billed
# 10 hours of idle VMs: GCP reused public IPs whose old host keys were still known).
wait_ssh() {
  local ip i; ip="$(public_ip "$1")"
  for i in $(seq 120); do
    ssh $SSH_OPTS -o BatchMode=yes "$SSH_USER@$ip" true 2>/dev/null && return 0
    sleep 5
  done
  echo "SSH to $1 ($ip) failed for 10 minutes:" >&2
  ssh $SSH_OPTS -o BatchMode=yes "$SSH_USER@$ip" true >&2 || true
  exit 1
}
for n in $(seq 1 "$NODES"); do wait_ssh "$(node_name "$n")"; done; wait_ssh "$BENCH_NAME"
# Node data on the local NVMe SSD when there is one: formatted, mounted at /mnt/cairn, and
# linked as ~/cairn/data/run (where run-cluster.sh writes).
for n in $(seq 1 "$NODES"); do
  ssh_to "$(node_name "$n")" 'dev=/dev/disk/by-id/google-local-nvme-ssd-0
    if [ -e $dev ] && ! mountpoint -q /mnt/cairn; then
      sudo mkfs.ext4 -q -F $dev && sudo mkdir -p /mnt/cairn && sudo mount -o discard,noatime $dev /mnt/cairn && sudo chown cairn:cairn /mnt/cairn
    fi
    mkdir -p cairn/data && { mountpoint -q /mnt/cairn && mkdir -p /mnt/cairn/run && ln -sfn /mnt/cairn/run cairn/data/run || true; }
    df -h /mnt/cairn 2>/dev/null | tail -1'
done
B="$(node_name 1)"; BIP="$(public_ip "$B")"
ssh_to "$B" 'sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential rsync curl >/dev/null
  [ -x ~/.cargo/bin/cargo ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null'
rsync -az --delete -e "ssh $SSH_OPTS" --exclude target --exclude data --exclude .git "$ROOT/" "$SSH_USER@$BIP:cairn/"
ssh_to "$B" 'cd cairn && ~/.cargo/bin/cargo build --release -p cairn-server -p cairn-bench 2>&1 | tail -1'
# Same image and CPU family everywhere (kernels dispatch at run time): copy the binaries.
mkdir -p /tmp/cairn-gcp-bin && rsync -az -e "ssh $SSH_OPTS" "$SSH_USER@$BIP:cairn/target/release/cairn-server" "$SSH_USER@$BIP:cairn/target/release/cairn-bench" /tmp/cairn-gcp-bin/
for vm in $(for n in $(seq 2 "$NODES"); do node_name "$n"; done) "$BENCH_NAME"; do
  ip="$(public_ip "$vm")"
  ssh $SSH_OPTS "$SSH_USER@$ip" 'sudo apt-get install -y -qq rsync >/dev/null 2>&1; mkdir -p cairn/target/release'
  rsync -az -e "ssh $SSH_OPTS" --exclude target --exclude data --exclude .git "$ROOT/" "$SSH_USER@$ip:cairn/"
  rsync -az -e "ssh $SSH_OPTS" /tmp/cairn-gcp-bin/ "$SSH_USER@$ip:cairn/target/release/"
done
ssh_to "$BENCH_NAME" 'mkdir -p cairn/data/bigann && cd cairn/data/bigann && cp ../../tools/scripts/hcloud/fetch-bigann.sh . && (nohup ./fetch-bigann.sh > download.log 2>&1 &)'
echo deployed
