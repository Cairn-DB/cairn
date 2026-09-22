# MS MARCO passage dev (small): BM25 with the Cairn text index

- Date: 2026-09-22
- Passages: 8841823; segments of 1000000 rows (9), per-segment BM25 statistics merged by score
- Tokenizer: Unicode alphanumeric runs, lowercased, no stemming, no stopwords; k1 = 0.9, b = 0.4
- Queries evaluated: 6980 (dev small with qrels)
- Build: 10.4 s wall (9 threads)

| metric | value |
|---|---|
| MRR@10 | 0.2037 |
| Recall@100 | 0.6905 |
| queries/s (1 thread, 9 segments) | 24 |

## Reading (2026-09-22)

- Corpus: the Tevatron mirror `msmarco-passage-corpus/corpus.jsonl.gz`, which carries a title per
  passage; titles were prepended to the text. The commonly quoted Anserini BM25 baseline
  (MRR@10 ≈ 0.184 with default parameters, ≈ 0.187 tuned, on the title-less collection) is
  therefore not an apples-to-apples comparison; titles are known to help BM25. Treat 0.204 as
  "in the expected range for BM25", not as a win. Figures for the baseline are quoted from memory
  and unverified.
- Nine segments with per-segment IDF (ADR 0005's documented bias) did not visibly hurt.
- 24 queries/s is slow: every query allocates and zeroes a score array per segment (1M floats × 9).
  A term-at-a-time accumulator keyed by touched rows, or block-max WAND, is the fix; noted for
  Phase 4 tuning. Correctness is what Phase 2 needed here.
