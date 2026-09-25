#!/bin/sh
# Maps CAIRN_* environment variables to cairn-server flags; extra arguments are passed through.
#   CAIRN_NODE_ID  this node's id (default 1)
#   CAIRN_PEERS    space-separated id=ip:port, this node included (default: a single node)
#   CAIRN_LISTEN   listen address (default 0.0.0.0:7100)
#   CAIRN_SCHEMA   schema JSON (default /etc/cairn/schema.json)
#   CAIRN_SHARDS   shards (default 4); CAIRN_CORES executor threads (default: all CPUs)
#   CAIRN_TLS_DIR  directory from tools/scripts/gen-certs.sh: ca.pem, node<id>.pem, node<id>.key
set -eu
ID="${CAIRN_NODE_ID:-1}"
LISTEN="${CAIRN_LISTEN:-0.0.0.0:7100}"
PEERS="${CAIRN_PEERS:-$ID=127.0.0.1:${LISTEN##*:}}"
set -- --node-id "$ID" --listen "$LISTEN" --data /data \
  --schema "${CAIRN_SCHEMA:-/etc/cairn/schema.json}" \
  --shards "${CAIRN_SHARDS:-4}" --cores "${CAIRN_CORES:-$(nproc)}" "$@"
for p in $PEERS; do set -- "$@" --peer "$p"; done
if [ -n "${CAIRN_TLS_DIR:-}" ]; then
  set -- "$@" --tls-ca "$CAIRN_TLS_DIR/ca.pem" \
    --tls-cert "$CAIRN_TLS_DIR/node$ID.pem" --tls-key "$CAIRN_TLS_DIR/node$ID.key"
fi
exec /usr/local/bin/cairn-server "$@"
