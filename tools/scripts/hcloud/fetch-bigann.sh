#!/usr/bin/env bash
# First 50M rows of BigANN base.1B.u8bin plus the public queries (resumable, IPv4: the CDN
# stalled over IPv6 from the development machine).
set -u
B=https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann
TOTAL=$((8 + 50000000*128))
curl -4 -sL --fail -o query.public.10K.u8bin "$B/query.public.10K.u8bin"
for try in $(seq 1 20); do
  have=$(stat -c %s base.50M.u8bin.part 2>/dev/null || echo 0)
  [ "$have" -ge "$TOTAL" ] && break
  curl -4 -sL --fail -r $have-$((TOTAL-1)) "$B/base.1B.u8bin" >> base.50M.u8bin.part
done
[ "$(stat -c %s base.50M.u8bin.part)" -eq "$TOTAL" ] || { echo SIZE_MISMATCH; exit 1; }
mv base.50M.u8bin.part base.50M.u8bin; echo DOWNLOAD_DONE
