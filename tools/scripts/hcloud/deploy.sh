#!/usr/bin/env bash
# Installs Rust, copies the repository (without target/ and data/), builds the release binaries
# on every VM, and starts the dataset download on the bench VM. Usage: deploy.sh
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
ROOT="$(cd ../../.. && pwd)"
setup() { # server
  local ip; ip="$(public_ip "$1")"
  until ssh $SSH_OPTS -o ConnectTimeout=5 root@"$ip" true 2>/dev/null; do sleep 5; done
  ssh $SSH_OPTS root@"$ip" 'apt-get update -qq && apt-get install -y -qq build-essential rsync curl >/dev/null
    [ -x ~/.cargo/bin/cargo ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null'
  rsync -az --delete -e "ssh $SSH_OPTS" --exclude target --exclude data --exclude .git "$ROOT/" root@"$ip":/root/cairn/
  ssh $SSH_OPTS root@"$ip" 'cd /root/cairn && ~/.cargo/bin/cargo build --release -p cairn-server -p cairn-bench 2>&1 | tail -1'
}
pids=()
for i in $(seq 1 "$NODES"); do setup "$(node_name "$i")" & pids+=($!); done
setup "$BENCH_NAME" & pids+=($!)
for p in "${pids[@]}"; do wait "$p"; done
# Datasets straight from the source to the bench VM (datacenter bandwidth).
ssh $SSH_OPTS root@"$(public_ip "$BENCH_NAME")" 'mkdir -p /root/cairn/data/bigann && cd /root/cairn/data/bigann &&
  cp /root/cairn/tools/scripts/hcloud/fetch-bigann.sh . && nohup ./fetch-bigann.sh > download.log 2>&1 &'
echo deployed
