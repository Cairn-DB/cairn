#!/bin/sh
# Maps CAIRN_* environment variables to cairn-server flags; extra arguments are passed through.
#   CAIRN_NODE_ID  this node's id (default 1)
#   CAIRN_PEERS    space-separated id=ip:port, this node included (default: a single node)
#   CAIRN_LISTEN   listen address (default 0.0.0.0:7100)
#   CAIRN_SCHEMA   schema JSON (default /etc/cairn/schema.json)
#   CAIRN_SHARDS   shards (default 4); CAIRN_CORES executor threads (default: all CPUs)
#   CAIRN_TLS_DIR  directory from tools/scripts/gen-certs.sh: ca.pem, node<id>.pem, node<id>.key
#   CAIRN_HTTP_LISTEN  HTTP/JSON API address (default 0.0.0.0:7200; "off" disables it). With
#                  CAIRN_TLS_DIR the API is off unless set, and needs CAIRN_HTTP_ALLOW_PLAINTEXT=1
#   API keys (ADR 0030), one of:
#     CAIRN_HTTP_ADMIN_KEY  an admin key given in clear (16+ characters; the same on every node)
#     CAIRN_HTTP_KEYS       a keys file (entries from `cairn-server keygen <id> <roles>`)
#     neither: a first start generates an admin key into /data/http-keys.json and prints it once
#   CAIRN_HTTP_TLS_CERT, CAIRN_HTTP_TLS_KEY  PEM files to serve the API over HTTPS
#   CAIRN_HTTP_INSECURE_DEV=1  no authentication at all (development only)
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
HTTP="${CAIRN_HTTP_LISTEN:-}"
[ -z "$HTTP" ] && [ -z "${CAIRN_TLS_DIR:-}" ] && HTTP=0.0.0.0:7200
if [ -n "$HTTP" ] && [ "$HTTP" != off ]; then
  set -- "$@" --http-listen "$HTTP"
  [ "${CAIRN_HTTP_ALLOW_PLAINTEXT:-0}" = 1 ] && set -- "$@" --http-allow-plaintext
  if [ "${CAIRN_HTTP_INSECURE_DEV:-0}" = 1 ]; then
    set -- "$@" --http-insecure-dev
  elif [ -n "${CAIRN_HTTP_KEYS:-}" ]; then
    set -- "$@" --http-keys "$CAIRN_HTTP_KEYS"
  elif [ -z "${CAIRN_HTTP_ADMIN_KEY:-}" ]; then
    set -- "$@" --http-keys /data/http-keys.json --http-generate-admin-key
  fi
  if [ -n "${CAIRN_HTTP_TLS_CERT:-}" ]; then
    set -- "$@" --http-tls-cert "$CAIRN_HTTP_TLS_CERT" --http-tls-key "${CAIRN_HTTP_TLS_KEY:?CAIRN_HTTP_TLS_KEY goes with CAIRN_HTTP_TLS_CERT}"
  fi
fi
exec /usr/local/bin/cairn-server "$@"
