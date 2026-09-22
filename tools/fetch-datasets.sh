#!/usr/bin/env bash
# Fetches benchmark datasets into data/ (gitignored). Records sha256 sums in data/CHECKSUMS on
# first download and verifies them afterwards. Usage: tools/fetch-datasets.sh [sift|yfcc|all]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/data"
mkdir -p "$DATA"
SUMS="$DATA/CHECKSUMS"
touch "$SUMS"

fetch() { # url dest
  local url="$1" dest="$2"
  if [ ! -f "$dest" ]; then
    echo "downloading $url"
    curl -L --fail --retry 3 -o "$dest.part" "$url"
    mv "$dest.part" "$dest"
  fi
  local rel="${dest#"$DATA"/}"
  local sum; sum="$(sha256sum "$dest" | cut -d' ' -f1)"
  if grep -q " $rel\$" "$SUMS"; then
    grep " $rel\$" "$SUMS" | grep -q "^$sum " || { echo "CHECKSUM MISMATCH for $rel"; exit 1; }
    echo "verified $rel"
  else
    echo "$sum  $rel" >> "$SUMS"
    echo "recorded $rel $sum"
  fi
}

sift() {
  mkdir -p "$DATA/sift"
  fetch "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz" "$DATA/sift/sift.tar.gz"
  if [ ! -f "$DATA/sift/sift_base.fvecs" ]; then
    tar -xzf "$DATA/sift/sift.tar.gz" -C "$DATA/sift" --strip-components=1
  fi
  ls -la "$DATA/sift"
}

yfcc() {
  mkdir -p "$DATA/yfcc10m"
  local base="https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/yfcc100M"
  for f in base.10M.u8bin query.public.100K.u8bin base.metadata.10M.spmat query.metadata.public.100K.spmat GT.public.ibin; do
    fetch "$base/$f" "$DATA/yfcc10m/$f"
  done
  ls -la "$DATA/yfcc10m"
}

case "${1:-sift}" in
  sift) sift ;;
  yfcc) yfcc ;;
  all) sift; yfcc ;;
  *) echo "usage: $0 [sift|yfcc|all]"; exit 2 ;;
esac
