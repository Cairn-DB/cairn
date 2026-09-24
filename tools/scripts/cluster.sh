#!/usr/bin/env bash
# Starts (or stops) a local 3-node cluster for the end-to-end benchmark.
# Usage: tools/scripts/cluster.sh start <data_dir> <schema.json> [shards] [cores] [memtable_bytes] [max_segments]
#        tools/scripts/cluster.sh stop
# Extra server flags (e.g. --sq8-only) come from $CAIRN_SERVER_FLAGS.
# With $CAIRN_TLS_DIR (from tools/scripts/gen-certs.sh), every node runs with mutual TLS.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
PIDFILE="data/cluster.pids"
case "${1:-}" in
  start)
    DATA="${2:?data dir}"; SCHEMA="${3:?schema}"; SHARDS="${4:-6}"; CORES="${5:-2}"; MEM="${6:-67108864}"; MAXSEG="${7:-8}"
    cargo build --release -p cairn-server 2>&1 | tail -1
    mkdir -p "$DATA"; : > "$PIDFILE"
    PEERS="--peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103"
    for i in 1 2 3; do
      TLS=""
      if [ -n "${CAIRN_TLS_DIR:-}" ]; then
        TLS="--tls-ca $CAIRN_TLS_DIR/ca.pem --tls-cert $CAIRN_TLS_DIR/node$i.pem --tls-key $CAIRN_TLS_DIR/node$i.key"
      fi
      MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-2} RUST_LOG=${RUST_LOG:-warn} nohup target/release/cairn-server --node-id $i --listen 127.0.0.1:710$i $PEERS \
        --data "$DATA/node$i" --schema "$SCHEMA" --shards "$SHARDS" --cores "$CORES" --memtable-bytes "$MEM" --max-segments "$MAXSEG" ${CAIRN_SERVER_FLAGS:-} $TLS --tick-ms 50 \
        > "$DATA/node$i.log" 2>&1 &
      echo $! >> "$PIDFILE"
    done
    sleep 2; echo "started: $(cat "$PIDFILE" | tr '\n' ' ')"
    ;;
  stop)
    if [ -f "$PIDFILE" ]; then xargs -r kill < "$PIDFILE" 2>/dev/null || true; rm -f "$PIDFILE"; fi
    echo stopped
    ;;
  *) echo "usage: $0 start <data> <schema> [shards] [cores] [mem] [max_segments] | stop"; exit 2 ;;
esac
