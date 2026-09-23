# Hetzner Cloud plan for the 50M run

Status: scripts ready in `tools/scripts/hcloud/`. **Nothing has been created.** Creating the
servers bills the owner's account, so it waits for the owner's go.

## What 50M needs

These figures were measured on the development machine (`docs/reports/phase-4-scale.md`),
with SQ8-only residency (ADR 0013):

| per node, per row stored | memory | disk |
|---|---|---|
| BigANN 128-d, settled | 0.43 KB | 0.85 KB |
| ingest transients (8 shards, 64 MB memtables) | about 3 GB per node, flat | |

A node that stores R rows needs about 0.43 KB × R + 3 GB of RAM, and 0.85 KB × R of disk, plus
room for one compaction. The ground-truth computation and the bench client need about 7 GB for
50M × 128 bytes, plus CPU.

## Options (prices from `hcloud server-type describe`, 2026-09-23, gross, hel1)

| option | servers | rows per node | RAM needed per node | cost per hour |
|---|---|---|---|---|
| A. one big host, 3 processes (same topology as local) | 1 × ccx53 (32 dedicated vCPU, 128 GB) | 50M × 3 on one host | 65 GB + 9 GB for the 3 processes | €1.03 |
| **B. real 3-node cluster, every node holds every shard** | 3 × ccx43 (16 vCPU, 64 GB) + 1 × ccx33 bench | 50M | 25 GB | €1.87 |
| C. 5 nodes, replication 3 (placement, new) | 5 × ccx33 (8 vCPU, 32 GB) + 1 × ccx33 bench | 30M | 16 GB | €1.68 |

**Recommendation: B.** It is the first run with a real network between the nodes, which is what
SPEC Phase 4 means by a 3-node cluster. It has enough headroom to also try f32-resident
reranking at 50M: 50M × 0.94 KB is about 47 GB, tight but inside 64 GB. C exercises the new
placement at a lower cost, and can follow on the same fleet by adding two nodes.

The time budget for B is about 30 minutes of setup and build, 20 minutes of dataset download
in the datacenter, about 2 hours of ingest at the measured rate (to be re-measured on 16 dedicated
cores), plus compaction and queries. That comes to about 4 hours, **roughly €8**. The per-hour
price is the only commitment. `teardown.sh` stops billing.

## Safety

- Every resource is labelled `project=cairn`. `teardown.sh` deletes by that label only and asks
  before deleting. The project may hold other servers.
  None of them carries the label.
- The firewall admits SSH only from the operator's public IPv4. Cluster traffic stays on a
  private network, `10.77.0.0/16`.
- A dedicated SSH key (`~/.ssh/cairn_hcloud`) is used, so no existing key is reused.

## Steps

```
tools/scripts/hcloud/provision.sh              # network, firewall, key, 3 nodes + bench VM
tools/scripts/hcloud/deploy.sh                 # rust, rsync repo, release build, dataset download
CAIRN_SERVER_FLAGS="--sq8-only --target-segment-rows 2000000 --compaction-slots 3" \
  tools/scripts/hcloud/run-cluster.sh start 16 14
# on the bench VM: cairn-bench cluster-scale --dataset bigann --n 50000000 --node 1=10.77.0.11:7100 ...
tools/scripts/hcloud/teardown.sh               # deletes everything labelled project=cairn
```
