# Longbow Roadmap (Remaining Work)

This document tracks the remaining work to scale Longbow past 1,000,000
vectors per dataset across all 21 search modes and 15 data types. Completed
items (P0 performance, OIDC federation, DevSecOps, geo/temporal/text
candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered
storage) have been removed.

See `docs/performance.md` for benchmark results and baselines.

---

## Summary of Remaining Work

| ID | Area | Priority | Status |
|---|---|---|---|
| P1-4 | Geo: spatial hash grid for radius candidates | CRITICAL | Implemented |
| P1-5 | Geo: spatial cell cache (LRU) | CRITICAL | Candidate cap covers hot path |
| P1-6b | Geo: benchmark + scalability test (≥500 QPS at 1M) | CRITICAL | Implemented |
| P2-4b | Temporal: benchmark + scalability test (≥1000 QPS at 1M) | CRITICAL | Implemented |
| P3-1 | Text: trigram inverted index for fuzzy/contains | HIGH | Implemented |
| P3-2 | Text: SIMD-vectorized BM25 scoring | HIGH | Implemented |
| P3-3 | Text: top-K heap with early termination | HIGH | Implemented |
| P3-4 | Text: shard corpus cache by partition | HIGH | Implemented |
| P3-5b | Text: benchmark + scalability test (≥500 QPS at 1M) | HIGH | Implemented |
| P4-1 | Filtered: bitmap intersection for multi-predicate | HIGH | Implemented (filter_mask.go) |
| P4-3 | Filtered: predicate-aware entry point selection | HIGH | Implemented (hnsw_predicate.go) |
| P4-4 | Filtered: shard filter evaluation | HIGH | Implemented (parallel_filter.go) |
| P4-5 | Filtered: P99 < 50ms at 1M | HIGH | Open |
| P4-6 | Filtered: scalability test | HIGH | Implemented (filtered_scale_test.go, 100k) |
| P5-1 | Complex: tiled batch kernels | HIGH | Implemented (P0-2.2) |
| P5-2 | Complex: zero-copy Float32→complex128 (API change) | HIGH | Open |
| P5-3 | Complex: verify TurboQuant auto-standardization at ≥500k | HIGH | Implemented (ShouldAutoQuantize verified) |
| P5-4 | Complex: chunked loading (disk-backed) | HIGH | Implemented (mmap/disk store) |
| P5-5 | Complex: RSS < 20 GB at 500k | HIGH | Open |
| P5-6 | Complex: scalability test | HIGH | Implemented (complex_scale_test.go, 100k) |
| P6-5 | TurboQuant: benchmark 5× speedup | HIGH | Implemented (kernels/LUT) |
| P6-6 | TurboQuant: scalability test | HIGH | Open |
| P7-1 | uint16/uint32: profile slow kernel | MEDIUM | Kernels exist |
| P7-2 | uint16/uint32: dedicated AVX2 kernels | MEDIUM | Implemented |
| P7-3 | uint16/uint32: parity benchmark | MEDIUM | Open |
| P7-4 | uint16/uint32: scalability test | MEDIUM | Open |
| P8-3 | Memory: cap enforcement + graceful degradation | MEDIUM | Open |
| P8-4 | Memory: checkMemoryBeforeWrite on all ingest paths | MEDIUM | Open |
| P8-5 | Memory: 1M complex128 RSS < 20 GB with disk spill | MEDIUM | Open |
| P8-6 | Memory: scalability test | MEDIUM | Open |
| P9-1 | HNSW quality: reachability ≥95% at 500k/1M | MEDIUM | Open |
| P9-2 | HNSW quality: tune addbatch_inflight + BulkChainLinks | MEDIUM | Open |
| P9-3 | HNSW quality: graph repair agent after bulk insert | MEDIUM | Open |
| P9-4 | HNSW quality: recall @10 at 1M vs brute force | MEDIUM | Open |
| P9-5 | HNSW quality: scalability test | MEDIUM | Open |
| P10-1 | Filtered tail: bitmap intersection | MEDIUM | Open |
| P10-2 | Filtered tail: push filters into traversal | MEDIUM | Implemented (hnsw_predicate.go) |
| P10-3 | Filtered tail: predicate-aware entry point | MEDIUM | Implemented (entry_points.go) |
| P10-4 | Filtered tail: shard filter evaluation | MEDIUM | Implemented (dataset.go worker pool) |
| P10-5 | Filtered tail: P99 < 50ms at 1M | MEDIUM | Open |
| P10-6 | Filtered tail: scalability test | MEDIUM | Implemented (filtered_scale_test.go) |
| P11-1 | Concurrency: dynamic worker scaling | LOW | Open |
| P11-2 | Concurrency: NUMA-aware allocation | LOW | Open |
| P11-3 | Concurrency: request batching | LOW | Open |
| P11-4 | Concurrency: 8-worker AVX-512 benchmark | LOW | Open |
| P0-2.5 | (Optional) Zero-copy Float32→complex128 extraction | LOW | Open |

---

## Remaining Optional Items

- [ ] P0-2.5 (Optional) Zero-copy Float32→complex128 extraction (requires API change; currently uses zero-allocation worker-local scratch widening in `extractVectorComplex128Into`).

---

## Verification Artifacts

| Gate | Command | Status |
|---|---|---|
| Build | `make build` | pass |
| Vet | `make vet` | pass |
| Unit tests | `make test-short` | pass |
| Static check | `make staticcheck` | 0 issues |
| Dead code | `make deadcode` | pre-existing only |
| Observability | `make observability-check` | pass |
| Accuracy | `make accuracy-gate` | pass |
| Oracle | `make oracle-gate` | pass |

## Baselines

- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)

---

## Remaining Work (Detailed Subtasks)

### P1 (Geo) — Remaining

- [x] P1-2 Add annulus cell pruning to `Quadtree.QueryRadiusPruned` (skip cells whose bbox is entirely outside radius).
- [x] P1-3 Add radius filter to `MortonGrid.QueryRadiusPruned`.
- [x] P1-4b Build a finer-grained spatial hash grid for radius queries.
- [ ] P1-5 (Optional) LRU cell cache for repeated geo queries.
- [x] P1-6b Benchmark geo at 500k/1M; target ≥ 500 QPS. Measured with `BenchmarkGeoIndex_SearchRadiusScale`: 6,788 / 1,504 / 757 QPS at 100k / 500k / 1M (clustered, 50 km radius, k=10).
- [x] P1-7 Add `TestGeoSearchScalability` plus `TestGeoIndex_SearchRadiusParity` (exhaustive-scan comparison on clustered, high-latitude, polar, and Euclidean data).

### P2 (Temporal) — Complete

- [x] P2-1 Add sorted timestamp B-tree/skip-list index per dataset. `TemporalTree` already keeps timestamps in sorted 1024-node leaves and publishes a columnar `(ts, groupOff, ids, norms)` snapshot after batch ingest, so no second index was added. `TestTemporalTreeChunkBoundaries` pins ordering, duplicates, out-of-order inserts, and every chunk/group boundary.
- [x] P2-2 Implement range scan via B-tree leaf traversal. `getRangeChunked` and the columnar `groupRange`/`entryRange` path lower-bound both endpoints and read only the covering groups instead of scanning the corpus. `TestTemporalRangeParity` compares range results against an exhaustive active-version reference for distinct, duplicate-id, same-timestamp, shuffled-insertion, empty, boundary, and inverted-range workloads, on both the columnar and chunked paths.
- [x] P2-3 Sliding window as ring buffer over the sorted index (O(k) not O(N)). The sorted-index tail already provides this: `GetUniqueLatest` walks groups newest-first and stops once the requested number of distinct ids is collected, so the cost is O(window + duplicate runs). `TestTemporalSlidingWindowScansOnlyTheTail` asserts a 16-id window over 20k entries scans ≤64 nodes. A separate insertion-order ring buffer was deliberately not added: window selection is timestamp-ordered, and insertion order diverges from timestamp order once updates arrive out of order.
- [x] P2-4b Benchmark temporal range at 500k/1M; target ≥ 1000 QPS. Measured with `BenchmarkTemporalRangeScale` (in-process sorted index, k=10) at 100k / 500k / 1M: **20,461 / 19,984 / 20,514 QPS** for a 1k window and **1,696 / 1,649 / 1,585 QPS** for a 10k window. The 4096-candidate truncation was removed because results are ranked by norm, so truncating by timestamp silently changed the top-k; `TestTemporalRangeExactTopKBeyondOldCap` pins the exact top-100 over a 5000-id full range.
- [x] P2-5 Add `TestTemporalRangeScalability` (≥1000 QPS at 1M for a 1k `SearchRange` and `SearchSlidingWindow`, after warming the lazy version-history index) plus the exhaustive parity, chunk-boundary, degenerate-input, and tail-cost tests.

### P3 (Text) — Complete

- [x] P3-1 Add trigram inverted index for fuzzy/contains (reuse `string_contains_index.go`). `internal/store/index/string_contains_index.go` now exports a reusable `TrigramIndex` (byte trigrams, intersected postings). `cachedTextCorpus` builds one per shard over the lowered text and phrase candidates are intersected trigrams, so phrase/contains queries are exact instead of a full scan.
- [x] P3-2 SIMD-vectorize BM25 scoring. `scoreTextCandidates` batches candidates with `simd.BM25ScoreBatch` per canonical sub-term, with scalar fallback for small/huge candidate sets. `scoreCachedDocBM25` was canonicalised to the same sub-term set, and scalar/SIMD parity is covered by `TestAdvancedTextBM25BatchParity`.
- [x] P3-3 (Done) Top-K heap with early termination.
- [x] P3-4 Shard corpus cache by record batch. `cachedTextCorpus` is now a list of immutable `cachedTextShard` values; unchanged shards (including their trigram index and postings) are reused across appends, tombstone changes and rebuilds, and global `nDocs`/`avgDL`/`df` are merged incrementally.
- [x] P3-5b Benchmark advancedtext at 500k/1M; target ≥ 500 QPS. Measured with `BenchmarkAdvancedTextScale` (in-process sharded corpus, selective `sku42 AND electronics`, k=10): **83,745 / 14,376 / 7,691 QPS** at 100k / 500k / 1M.
- [x] P3-6 Add `TestAdvancedTextScalability`. Measured **7,780 QPS** at 1M. Candidate generation, phrase substring recall (`TestAdvancedTextPhraseDoesNotMissSubstrings`), fuzzy vocabulary expansion (`TestAdvancedTextCandidateParity`), the removed 50k evaluation cap (`TestAdvancedTextNoCandidateCap`), shard reuse (`TestAdvancedTextShardReuse`) and scalar/SIMD parity are covered.

### P4/P10 (Filtered) — Remaining

- [ ] P4-5 Benchmark filtered at 500k/1M; target P99 < 50ms.
- [ ] P4-6 Add `TestFilteredSearchScalability`.
- [ ] P10-3 Predicate-aware entry point selection.
- [ ] P10-4 Shard filter evaluation across worker pool.
- [ ] P10-5 P99 < 50ms at 1M.
- [ ] P10-6 Add `TestFilteredSearchTailLatency`.

### P5 (Complex) — Remaining

- [ ] P5-2b Optional API change for Float32→complex128 (P0-2.5).
- [ ] P5-3 Verify TurboQuant auto-standardization at ≥500k.
- [ ] P5-5 RSS < 20 GB at 500k.
- [ ] P5-6 Add `TestComplexScalability`.

### P6 (TurboQuant) — Remaining

- [ ] P6-5b Benchmark 5× speedup at 200k/500k.
- [ ] P6-6 Add `TestTurboQuantIndexScalability`.

### P7 (uint dispatch) — Remaining

- [ ] P7-1 Profile uint16/uint32 slow kernel.
- [ ] P7-2 Dedicated AVX2 kernels for uint16/uint32.
- [ ] P7-3 Parity benchmark.
- [ ] P7-4 Add `TestUintDispatchScalability`.

### P8 (Memory) — Remaining

- [ ] P8-3 Memory cap enforcement + graceful degradation.
- [ ] P8-4 `checkMemoryBeforeWrite` on all ingest paths.
- [ ] P8-5 1M complex128 RSS < 20 GB with disk spill.
- [ ] P8-6 Add `TestMemoryScaling`.

### P9 (HNSW quality) — Remaining

- [ ] P9-1 Reachability ≥95% at 500k/1M.
- [ ] P9-2 Tune `addbatch_inflight` + `BulkChainLinks`.
- [ ] P9-4 Recall @10 at 1M vs brute force.
- [ ] P9-5 Add `TestHNSWQualityAtScale`.

### P11 (Concurrency) — Remaining

- [ ] P11-1 Dynamic worker scaling.
- [ ] P11-2 NUMA-aware allocation.
- [ ] P11-3 Request batching.
- [ ] P11-4 8-worker AVX-512 benchmark.

### P0-2.5 (Optional) — Remaining

- [ ] P0-2.5 Zero-copy Float32→complex128 extraction (API change).
