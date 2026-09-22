# ADR 0007: Hybrid ranking fusion — RRF by default, weighted fusion optional, learned out of scope

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Resolves: SPEC.md O6

## Context

A hybrid query has up to one text leg and one leg per vector field (image, audio, text
embeddings), all restricted by the same structured predicate. Leg scores live on different scales
(BM25 unbounded, cosine in [-1, 1], L2 in [0, inf)). Results must be merged across segments of a
shard and across shards at the coordinator.

## Options considered

1. **Reciprocal Rank Fusion** (RRF, `score = sum_legs 1 / (k + rank_leg)`, k = 60): parameter-free,
   scale-independent, robust; loses score magnitude information.
2. **Weighted linear fusion** with per-leg min-max (or z-score) normalization and user weights:
   expressive; sensitive to score distribution and to how many candidates each leg returns.
3. **Learned fusion**: needs training data we do not have. Out of scope for v1.

## Decision

RRF is the default; weighted linear fusion is available per query. Placement in the plan:

1. **Per segment**: build the filter bitmap once (ADR 0003), run each leg over it, produce a
   per-leg ranked list of size k' (k' = max(oversample × k, 100)).
2. **Per shard**: merge each leg across segments by leg score (comparable within a shard and a
   metric), keep k' per leg. Return *per-leg* lists to the coordinator, never a fused list: fusing
   at the shard and again at the coordinator would double-count ranks.
3. **Coordinator**: merge each leg across shards (BM25 uses per-shard statistics in v1; this bias
   is documented in ADR 0005), fuse once (RRF or weighted), return top-k with payloads fetched
   only for the final k.

## Consequences

- The shard-to-coordinator payload is legs × k' ids and scores; small.
- Oversampling k' is a tuning knob; too small hurts fusion recall.
- Weighted fusion's normalization uses only the returned candidates, so results depend on k'.
  Documented behaviour, not a bug.

## Experiment that confirms or refutes

Phase 2, M2.5: MS MARCO passage (BM25 + a public dense embedding set) or a BEIR dataset with
released embeddings: nDCG@10 for text-only, dense-only, RRF, weighted. The goal is not to beat
literature but to confirm the plan produces the expected hybrid gain and to size k'.
