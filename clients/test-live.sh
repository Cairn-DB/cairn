#!/usr/bin/env bash
# Runs both clients' tests against a fresh local node (ADR 0031): builds cairn-server, starts
# one node with an admin key, an unscoped key, a key scoped to tenant "acme" and a read-only
# key, then runs
# the TypeScript and Python suites (unit and live). Needs cargo, node >= 18, python3 with
# httpx and pytest. Usage: clients/test-live.sh [--release]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
profile=debug
[[ "${1:-}" == "--release" ]] && profile=release
cargo build --manifest-path "$root/Cargo.toml" -p cairn-server $([[ $profile == release ]] && echo --release) >&2
bin="$root/target/$profile/cairn-server"
work=$(mktemp -d)
pid=""
cleanup() { [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT
key() { "$bin" keygen "$@" 2>/dev/null; }
field() { python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]] if sys.argv[2]=='key' else json.dumps(json.load(open(sys.argv[1]))['entry']))" "$1" "$2"; }
key app read,write,takedown > "$work/app.json"
key acme-app read,write,takedown --tenant acme > "$work/acme.json"
key viewer read > "$work/read.json"
echo "{\"keys\": [$(field "$work/app.json" entry), $(field "$work/acme.json" entry), $(field "$work/read.json" entry)]}" > "$work/keys.json"
ports=($(python3 -c "import socket
for _ in range(2):
    s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()"))
raft=127.0.0.1:${ports[0]} web=127.0.0.1:${ports[1]}
admin="cairn-live-test-admin-$RANDOM$RANDOM"
CAIRN_HTTP_ADMIN_KEY="$admin" "$bin" --node-id 1 --listen "$raft" --peer "1=$raft" --data "$work/data" \
  --schema "$here/test-schema.json" --shards 3 --cores 2 --http-listen "$web" \
  --http-keys "$work/keys.json" > "$work/node.log" 2>&1 &
pid=$!
for _ in $(seq 100); do curl -sf "http://$web/health" >/dev/null && break; sleep 0.2; done
curl -sf "http://$web/health" >/dev/null || { cat "$work/node.log"; exit 1; }
export CAIRN_URL="http://$web"
export CAIRN_KEY=$(field "$work/app.json" key)
export CAIRN_ACME_KEY=$(field "$work/acme.json" key)
export CAIRN_READ_KEY=$(field "$work/read.json" key)
export CAIRN_ADMIN_KEY="$admin"
status=0
echo "== TypeScript" >&2
(cd "$here/typescript" && { [[ -d node_modules ]] || npm ci --no-audit --no-fund; } && npm test) || status=1
echo "== Python" >&2
(cd "$here/python" && PYTHONPATH=src python3 -m pytest -q -p no:cacheprovider tests) || status=1
[[ $status == 0 ]] || { echo "== node log" >&2; tail -50 "$work/node.log" >&2; }
exit $status
