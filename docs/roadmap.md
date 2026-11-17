# Longbow Roadmap (Scaling Past 1M Vectors)

This roadmap tracks the work required to scale Longbow beyond 1,000,000
vectors per dataset across all 21 search modes and 15 data types, derived
from the 2026-11-16 validation matrix (§0b–§0d of `docs/performance.md`)
and deep code analysis of the hot paths.

## Current State (2026-11-16)

- All 21 search modes functional and wired end-to-end (verified by
  `TestSearchModes_All21Modalities`).
- Validated scales: 50k / 100k / 200k (dim 128 & 384 & 768), 500k & 1M (dim 128).
- At 1M dim128: dense QPS 2.8k–3.3k (float32/int8/uint8); geo 8–18 QPS;
  advancedtext 45–120 QPS; temporalrange 6–14 QPS; filtered P99 200–600ms.
- Memory: float32 13.9 GB, int8/uint8 ~10.5 GB at 1M — near the 14 GiB ceiling.
- TurboQuant indexing: 282–1101s at 200k–768d; polar reconstruction is 47.8%
  of distance time.

## P1: Geo Search (8–18 QPS at 1M) — CRITICAL

### P1 Root Cause

`GeoIndex.SearchRadius/SearchBox/HybridSearch` (`internal/store/geo_search.go`,
`geo_quadtree.go`, `morton_grid.go`) perform an O(N) scan over all vectors
with haversine distance. The Quadtree/MortonGrid structures exist but pruning
is ineffective at high density (cells overlap the query radius).

### P1 Subtasks

- [ ] P1-1 Profile `SearchRadius` to confirm O(N) haversine dominates (pprof).
- [ ] P1-2 Add annulus cell pruning to `Quadtree.QueryRadiusPruned` (skip cells
      whose bounding box is entirely outside the radius).
- [ ] P1-3 Add radius filter to `MortonGrid.QueryRadiusPruned`.
- [ ] P1-4 Integrate geo with HNSW: build a secondary spatial HNSW or use the
      existing graph with a geo-aware entry point (k-NN seed from Quadtree).
- [ ] P1-5 Add a spatial cell cache for repeated queries (LRU over cell results).
- [x] P1-6 Candidate cap implemented in SearchRadius (max 4096 nearest by lat/lon).
- [ ] P1-6b Benchmark geo search before/after at 500k/1M; target ≥ 500 QPS.
- [ ] P1-7 Add `TestGeoSearchScalability` verifying QPS ≥ threshold at 500k.

## P2: Temporal Range Search (6–14 QPS at 1M) — CRITICAL

### P2 Root Cause

`TemporalSearch` with `search_type=range` scans all vectors and filters by
timestamp (`internal/store/temporal_search.go`, `temporal_columnar.go`).
No sorted temporal index exists; `TemporalTree` is a flat structure.

### P2 Subtasks

- [ ] P2-1 Add a sorted timestamp B-tree / skip-list index per dataset
      (`TemporalTree` extension) mapping timestamp → vector IDs.
- [ ] P2-2 Implement range scan via B-tree leaf traversal instead of full scan.
- [ ] P2-3 Add sliding-window as a ring buffer over the sorted index (O(k) not O(N)).
- [x] P2-4 Candidate cap implemented in SearchRange (keep most recent 4096).
- [ ] P2-4b Benchmark temporal range at 500k/1M; target ≥ 1000 QPS.
- [ ] P2-5 Add `TestTemporalRangeScalability`.

## P3: Advanced Text Search (45–120 QPS at 1M) — HIGH

### P3 Root Cause

Corpus cache + postings fast path added (§0b), but at 1M the corpus
(`cachedTextDoc` × 1M) and postings maps consume significant memory, and
`ast.evaluate` still runs per candidate. BM25 scoring is scalar.

### P3 Subtasks

- [ ] P3-1 Add trigram inverted index for fuzzy/contains queries (reuse
      `string_contains_index.go` trigramIndex).
- [ ] P3-2 Vectorize BM25 scoring with SIMD over term frequencies.
- [ ] P3-3 Add a top-K heap with early termination when score < current min.
- [ ] P3-4 Shard the corpus cache by dataset partition to reduce lock contention.
- [x] P3-5 Candidate cap implemented in fallback full-scan (50000 docs).
- [ ] P3-5b Benchmark advancedtext at 500k/1M; target ≥ 500 QPS.
- [ ] P3-6 Add `TestAdvancedTextScalability`.

## P4: Filtered Search P99 (200–600ms at 1M) — HIGH

### P4 Root Cause

Filtered search (numeric/bool/string) scans candidates and applies predicates
post-hoc; `filter_mask.go` and `parallel_filter.go` help but P99 spikes
indicate tail latency from large candidate sets and mutex contention.

### P4 Subtasks

- [ ] P4-1 Add bitmap intersection for multi-predicate filters (AND of bitsets).
- [ ] P4-2 Push filters into HNSW traversal (early-exit on predicate mismatch)
      instead of post-filtering.
- [ ] P4-3 Add a predicate-aware entry point selection (start from a node
      matching the filter).
- [ ] P4-4 Shard filter evaluation across the shared worker pool.
- [ ] P4-5 Benchmark filtered/filteredbool/filteredstring at 500k/1M; target
      P99 < 50ms.
- [ ] P4-6 Add `TestFilteredSearchScalability`.

## P5: Complex Type Memory & Index Time (12.3 GB RSS, 190–319s at 200k 768d) — HIGH

### P5 Root Cause

Complex64/complex128 extraction uses zero-copy `unsafe.Slice` (done), but
indexing is still CPU-bound on `selectNeighborsComplex128` and the float64
slab. At 1M complex128 would exceed 60 GB.

### P5 Subtasks

- [ ] P5-1 Add tiled complex128 batch kernels (parallel over dimension tiles).
- [ ] P5-2 Add zero-copy Float32→complex128 extraction (P0-2.5, requires API change).
- [ ] P5-3 Enable TurboQuant auto-standardization for complex types at ≥500k
      (already triggered in store_ingest.go; verify).
- [ ] P5-4 Add chunked loading to avoid full materialization (disk-backed).
- [ ] P5-5 Benchmark complex types at 500k; target RSS < 20 GB.
- [ ] P5-6 Add `TestComplexScalability`.

## P6: TurboQuant Index Time (1101s at 200k 768d) — HIGH

### P6 Root Cause

Polar reconstruction is 47.8% of distance time (`turboQuantDistanceAVX2Scratch`);
sincos LUT exists but batched reconstruction and AVX2 kernel are incomplete.

### P6 Subtasks

- [x] P6-1 Complete AVX2 reconstruction kernel (`TQPolarReconstructLevelAVX2`).
- [x] P6-2 Add batched reconstruction across `tqBatchWidth` candidates
      (`turboquant_batch.go`).
- [x] P6-3 Add Sincos LUT for polar transform (avoid runtime trig).
- [x] P6-4 Profile and validate numerical parity vs scalar reference.
- [ ] P6-5 Benchmark TurboQuant index time at 200k/500k; target 5× speedup.
- [ ] P6-6 Add `TestTurboQuantIndexScalability`.

## P7: uint16/uint32 Dispatch (elevated index times at 384d+) — MEDIUM

### P7 Root Cause

uint16/uint32 at 384d show 88–218s index time vs ~25–60s for int16/int32.
Dispatch may be falling back to a slower generic kernel.

### P7 Subtasks

- [ ] P7-1 Profile `uint16`/`uint32` indexing to identify the slow kernel.
- [ ] P7-2 Add dedicated high-dim uint16/uint32 AVX2 kernels (mirror int16/int32).
- [ ] P7-3 Benchmark before/after; target parity with int16/int32.
- [ ] P7-4 Add `TestUintDispatchScalability`.

## P8: Memory Scaling & Disk Spill (1M+ requires >14 GB) — MEDIUM

### P8 Root Cause

At 1M dim128 float32 RSS is 13.9 GB. Complex types at 1M would exceed
available RAM. Auto-spill exists but graph traversal requires in-memory data.

### P8 Subtasks

- [ ] P8-1 Implement disk-backed vector storage with memory-mapped reads
      (reuse `mmap_vector.go`, `disk_vector_store_block.go`).
- [ ] P8-2 Add a tiered storage policy (hot in RAM, cold on disk) with
      LRU eviction (`tiered_storage.go`).
- [ ] P8-3 Add memory cap enforcement with graceful degradation (evict layers,
      spill to disk) — extend `GraphLayerEvictionManager`.
- [ ] P8-4 Add `checkMemoryBeforeWrite` to all ingest paths (already partial).
- [ ] P8-5 Benchmark 1M complex128 with disk spill; target RSS < 20 GB.
- [ ] P8-6 Add `TestMemoryScaling`.

## P9: HNSW Graph Quality at Large N — MEDIUM

### P9 Root Cause

Bulk insertion uses adaptive M and chain links; at 500k+ the graph may
degrade (lower reachability, more fallback to sequential scan).

### P9 Subtasks

- [ ] P9-1 Add reachability regression test at 500k/1M (target ≥ 95%).
- [ ] P9-2 Tune `addbatch_inflight` and `BulkChainLinks` for large N.
- [ ] P9-3 Add a graph repair agent pass after bulk insert (`hnsw_repair.go`).
- [ ] P9-4 Benchmark recall @10 at 1M vs brute force.
- [ ] P9-5 Add `TestHNSWQualityAtScale`.

## P10: Filtered Search Tail Latency (P99 200–600ms at 1M) — MEDIUM

### P10 Root Cause

Filtered search applies predicates post-hoc over large candidate sets;
tail latency from GC pauses and mutex contention.

### P10 Subtasks

- [ ] P10-1 Add bitmap intersection for multi-predicate filters.
- [ ] P10-2 Push filters into HNSW traversal (early exit on mismatch).
- [ ] P10-3 Add predicate-aware entry point selection.
- [ ] P10-4 Shard filter evaluation across the worker pool.
- [ ] P10-5 Target P99 < 50ms at 1M.
- [ ] P10-6 Add `TestFilteredSearchTailLatency`.

## P11: Concurrency & Worker Scaling — LOW

### P11 Root Cause

4 pinned workers on AVX2; server architectures benefit from 8+ unpinned
workers (per testplan §4.1). At 1M, ingest and search both saturate.

### P11 Subtasks

- [ ] P11-1 Add dynamic worker scaling based on queue depth (already partial
      in `StartIngestionAutoscaler`).
- [ ] P11-2 Add NUMA-aware allocation for multi-socket servers.
- [ ] P11-3 Add request batching for small queries.
- [ ] P11-4 Benchmark with 8 workers on AVX-512; document gains.

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
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`,
  `baseline_25k.json` (legacy)
