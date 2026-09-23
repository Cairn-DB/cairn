#!/usr/bin/env bash
# Installs Rust on node 1, copies the repository (no target/, data/, .git), builds the release
# binaries there, copies them to the other VMs, and starts the dataset download on the bench VM.
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
ROOT="$(cd ../../.. && pwd)"
ssh_to() { ssh $SSH_OPTS "$SSH_USER@$(public_ip "$1")" "${@:2}"; }
wait_ssh() { local ip; ip="$(public_ip "$1")"; until ssh $SSH_OPTS "$SSH_USER@$ip" true 2>/dev/null; do sleep 5; done; }
for n in $(seq 1 "$NODES"); do wait_ssh "$(node_name "$n")"; done; wait_ssh "$BENCH_NAME"
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
