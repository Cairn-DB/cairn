#!/usr/bin/env bash
# Starts (start) or stops (stop) cairn-server on every node over the private subnet.
# Extra server flags from $CAIRN_SERVER_FLAGS. Usage: run-cluster.sh start|stop [shards] [cores] [memtable_bytes]
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
PEERS=""; for i in $(seq 1 "$NODES"); do PEERS="$PEERS --peer $i=$(node_ip "$i"):7100"; done
for i in $(seq 1 "$NODES"); do
  ip="$(public_ip "$(node_name "$i")")"
  case "${1:-}" in
    start) ssh $SSH_OPTS "$SSH_USER@$ip" "cd cairn && (pkill -x cairn-server || true); mkdir -p data/run &&
      (MALLOC_ARENA_MAX=2 RUST_LOG=\${RUST_LOG:-warn} nohup target/release/cairn-server --node-id $i --listen $(node_ip "$i"):7100 $PEERS \
        --data data/run/node$i --schema tools/scripts/sift-schema.json --shards ${2:-8} --cores ${3:-6} --memtable-bytes ${4:-134217728} \
        ${CAIRN_SERVER_FLAGS:-} > data/run/node$i.log 2>&1 &) ; sleep 1; pgrep -x cairn-server >/dev/null && echo started node$i" ;;
    stop) ssh $SSH_OPTS "$SSH_USER@$ip" "pkill -x cairn-server || true; echo stopped node$i" ;;
    *) echo "usage: $0 start|stop [shards] [cores] [memtable_bytes]"; exit 2 ;;
  esac
done
