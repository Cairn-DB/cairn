#!/usr/bin/env bash
# Starts cairn-server on every node over the private network (start), or stops it (stop).
# Extra server flags come from $CAIRN_SERVER_FLAGS. Usage: run-cluster.sh start|stop [shards] [cores]
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
PEERS=""; for i in $(seq 1 "$NODES"); do PEERS="$PEERS --peer $i=$(node_ip "$i"):7100"; done
for i in $(seq 1 "$NODES"); do
  ip="$(public_ip "$(node_name "$i")")"
  case "${1:-}" in
    start) ssh $SSH_OPTS root@"$ip" "cd /root/cairn && pkill -x cairn-server; mkdir -p data/run &&
      RUST_LOG=warn nohup target/release/cairn-server --node-id $i --listen $(node_ip "$i"):7100 $PEERS \
        --data data/run/node$i --schema tools/scripts/sift-schema.json --shards ${2:-16} --cores ${3:-14} \
        ${CAIRN_SERVER_FLAGS:-} > data/run/node$i.log 2>&1 & echo started node$i" ;;
    stop) ssh $SSH_OPTS root@"$ip" "pkill -x cairn-server || true" ;;
    *) echo "usage: $0 start|stop [shards] [cores]"; exit 2 ;;
  esac
done
