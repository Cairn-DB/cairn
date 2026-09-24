#!/usr/bin/env bash
# Generates a cluster CA, one certificate per node (DNS name node-<id>.cairn) and client
# certificates for mutual TLS (ADR 0018). Keys are EC P-256 in PKCS#8 PEM.
# Usage: tools/scripts/gen-certs.sh <out_dir> <node_count> [client_name ...]
#   e.g. tools/scripts/gen-certs.sh data/tls 3 bench
# Output: ca.pem (distribute), ca.key (keep offline), node<i>.pem/.key, client-<name>.pem/.key.
# Node keys belong on their node only. Re-running refuses to overwrite an existing CA.
set -euo pipefail
OUT="${1:?out dir}"; N="${2:?node count}"; shift 2
DAYS="${CAIRN_CERT_DAYS:-825}"
mkdir -p "$OUT"; umask 077
if [ ! -f "$OUT/ca.key" ]; then
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$OUT/ca.key"
  openssl req -x509 -new -key "$OUT/ca.key" -sha256 -days "$DAYS" -subj "/CN=cairn cluster CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$OUT/ca.pem"
else
  echo "keeping existing CA in $OUT"
fi
issue() { # name file
  local name="$1" file="$2"
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$OUT/$file.key"
  openssl req -new -key "$OUT/$file.key" -subj "/CN=$name" -out "$OUT/$file.csr"
  openssl x509 -req -in "$OUT/$file.csr" -CA "$OUT/ca.pem" -CAkey "$OUT/ca.key" -CAcreateserial \
    -days "$DAYS" -sha256 -out "$OUT/$file.pem" -extfile <(printf '%s\n' \
      "subjectAltName=DNS:$name" "extendedKeyUsage=serverAuth,clientAuth" \
      "keyUsage=critical,digitalSignature" "basicConstraints=critical,CA:FALSE")
  rm -f "$OUT/$file.csr"
}
for i in $(seq 1 "$N"); do issue "node-$i.cairn" "node$i"; done
for c in "$@"; do issue "$c.client.cairn" "client-$c"; done
chmod 644 "$OUT"/*.pem
echo "wrote $OUT: ca.pem, node1..$N, clients: ${*:-none}"
