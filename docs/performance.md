# Longbow Performance Benchmark Results

**Generated**: 2026-06-12
**Platform**: Linux x86_64 (1 NUMA node, 16 cores, 22 GB RAM)
**Memory Limit**: 16 GB allocated (`LONGBOW_MAX_MEMORY=17179869184`)
**Test Tool**: `scripts/unified_benchmark.py` (CPU mode)
**Queries**: 1000 per test configuration
**Scale**: 1,000,000 vectors, dims 128 and 384
**Search Modes**: All 13 modes (dense, hybrid, filtered, filteredbool, filteredstring, sparse, byid, graphrag, globalgraphrag, recommend, geo, temporal, learnedindex)
**Profiling**: pprof (cpu, heap, allocs, goroutine, threadcreate, block, mutex) — collected at start and end of each test

## Test Results

3 of 34 planned configurations completed before the run was interrupted:
- float32 dim=128 (1,000,000)
- float32 dim=384 (1,000,000)
- float64 dim=128 (1,000,000)

float64 dim=384 was in progress at 58 min but not finished (hit memory pressure at 16 GB limit, swapping ~2 GB).

---

### float32 dim=128

| Phase | Throughput | P50 (ms) | P95 (ms) | P99 (ms) | Duration (s) |
|-------|-----------|---------|---------|---------|-------------|
| DoPut (upload) | 905,714 vec/s | — | — | — | 1.10 |
| Indexing (HNSW build) | — | — | — | — | 352 |
| DoGet (download) | 236,065 vec/s | — | — | — | 0.04 |
| Search_Dense | 2,188.3 QPS | 3.43 | 4.71 | 5.52 | 0.46 |
| Search_Hybrid | 2,054.1 QPS | 3.77 | 5.00 | 5.58 | 0.49 |
| Search_Filtered | 816.1 QPS | 3.58 | 5.00 | 6.32 | 1.23 |
| Search_FilteredBool | 1,188.8 QPS | 3.58 | 4.80 | 5.76 | 0.84 |
| Search_FilteredString | 919.5 QPS | 6.96 | 10.55 | 14.85 | 1.09 |
| Search_Sparse | 7,525.4 QPS | 1.05 | 1.46 | 1.62 | 0.13 |
| Search_ByID | 2,134.9 QPS | 3.70 | 4.87 | 5.72 | 0.47 |
| Search_GraphRAG | 1,886.8 QPS | 4.13 | 5.50 | 6.45 | 0.53 |
| Search_GlobalGraphRAG | 1,908.1 QPS | 4.06 | 5.49 | 6.07 | 0.52 |
| Search_Recommend | 2,154.6 QPS | 3.59 | 4.78 | 5.16 | 0.46 |
| Search_Geo | 26.4 QPS | 277.46 | 571.19 | 684.59 | 37.83 |
| Search_Temporal | 433.4 QPS | 17.42 | 24.70 | 26.18 | 2.31 |
| Search_LearnedIndex | 1,583.4 QPS | 4.96 | 6.61 | 7.52 | 0.63 |

### float32 dim=384

| Phase | Throughput | P50 (ms) | P95 (ms) | P99 (ms) | Duration (s) |
|-------|-----------|---------|---------|---------|-------------|
| DoPut (upload) | 485,307 vec/s | — | — | — | 2.06 |
| Indexing (HNSW build) | — | — | — | — | 290 |
| DoGet (download) | 142,444 vec/s | — | — | — | 0.07 |
| Search_Dense | 2,460.5 QPS | 3.05 | 4.49 | 6.62 | 0.41 |
| Search_Hybrid | 2,355.1 QPS | 3.31 | 4.59 | 5.59 | 0.42 |
| Search_Filtered | 802.6 QPS | 2.89 | 4.33 | 5.69 | 1.25 |
| Search_FilteredBool | 1,059.1 QPS | 3.29 | 4.97 | 6.06 | 0.94 |
| Search_FilteredString | 1,416.6 QPS | 3.73 | 5.40 | 7.71 | 0.71 |
| Search_Sparse | 7,501.9 QPS | 1.04 | 1.44 | 1.94 | 0.13 |
| Search_ByID | 2,698.1 QPS | 2.78 | 3.98 | 4.45 | 0.37 |
| Search_GraphRAG | 2,214.5 QPS | 3.44 | 5.02 | 5.81 | 0.45 |
| Search_GlobalGraphRAG | 2,161.7 QPS | 3.56 | 5.00 | 5.60 | 0.46 |
| Search_Recommend | 2,667.9 QPS | 2.84 | 4.07 | 4.48 | 0.37 |
| Search_Geo | 24.7 QPS | 281.24 | 683.01 | 798.79 | 40.45 |
| Search_Temporal | 472.1 QPS | 16.02 | 23.39 | 25.27 | 2.12 |
| Search_LearnedIndex | 2,072.6 QPS | 3.67 | 5.12 | 5.69 | 0.48 |

### float64 dim=128

| Phase | Throughput | P50 (ms) | P95 (ms) | P99 (ms) | Duration (s) |
|-------|-----------|---------|---------|---------|-------------|
| DoPut (upload) | 953,426 vec/s | — | — | — | 1.05 |
| Indexing (HNSW build) | — | — | — | — | 462 |
| DoGet (download) | 147,511 vec/s | — | — | — | 0.07 |
| Search_Dense | 273.7 QPS | 30.49 | 41.79 | 48.26 | 3.65 |
| Search_Hybrid | 338.9 QPS | 24.62 | 34.15 | 49.45 | 2.95 |
| Search_Filtered | 327.6 QPS | 19.63 | 26.65 | 32.18 | 3.05 |
| Search_FilteredBool | 198.4 QPS | 40.75 | 53.72 | 124.72 | 5.04 |
| Search_FilteredString | 217.9 QPS | 38.24 | 46.60 | 51.61 | 4.59 |
| Search_Sparse | 8,090.1 QPS | 0.98 | 1.35 | 1.54 | 0.12 |
| Search_ByID | 4,830.4 QPS | 1.64 | 2.12 | 2.43 | 0.21 |
| Search_GraphRAG | 621.2 QPS | 13.20 | 18.01 | 34.10 | 1.61 |
| Search_GlobalGraphRAG | 657.3 QPS | 13.02 | 15.90 | 17.67 | 1.52 |
| Search_Recommend | 721.9 QPS | 11.03 | 13.33 | 14.05 | 1.39 |
| Search_Geo | 23.8 QPS | 287.33 | 692.14 | 801.07 | 42.10 |
| Search_Temporal | 440.6 QPS | 17.34 | 24.00 | 25.15 | 2.27 |
| Search_LearnedIndex | 684.3 QPS | 12.82 | 16.27 | 17.83 | 1.46 |

---

## Search QPS Comparison

| Search Mode | float32 dim128 | float32 dim384 | float64 dim128 |
|-------------|:--------------:|:--------------:|:--------------:|
| Dense | 2,188 QPS | 2,461 QPS | 274 QPS |
| Hybrid | 2,054 QPS | 2,355 QPS | 339 QPS |
| Filtered | 816 QPS | 803 QPS | 328 QPS |
| FilteredBool | 1,189 QPS | 1,059 QPS | 198 QPS |
| FilteredString | 920 QPS | 1,417 QPS | 218 QPS |
| Sparse | 7,525 QPS | 7,502 QPS | 8,090 QPS |
| ByID | 2,135 QPS | 2,698 QPS | 4,830 QPS |
| GraphRAG | 1,887 QPS | 2,215 QPS | 621 QPS |
| GlobalGraphRAG | 1,908 QPS | 2,162 QPS | 657 QPS |
| Recommend | 2,155 QPS | 2,668 QPS | 722 QPS |
| Geo | 26 QPS | 25 QPS | 24 QPS |
| Temporal | 433 QPS | 472 QPS | 441 QPS |
| LearnedIndex | 1,583 QPS | 2,073 QPS | 684 QPS |

## Ingest + Index Comparison

| Metric | float32 dim128 | float32 dim384 | float64 dim128 |
|--------|:-------------:|:-------------:|:-------------:|
| DoPut upload | 905,714 vec/s | 485,307 vec/s | 953,426 vec/s |
| HNSW indexing time | 352s | 290s | 462s |
| DoGet download | 236,065 vec/s | 142,444 vec/s | 147,511 vec/s |
| Total time (to ready) | ~353s | ~292s | ~463s |

---

## Key Findings

### 1. Deadlock Fix Confirmed
The `LockFreeNeighborCache` deadlock (lock promotion starvation under thread-pinned workers) has been fixed. The previous attempt at float32 dim128 crashed at ~820K/1M during indexing. Now it completes all 1M vectors + all 13 search modes cleanly. The fix replaced `RLock→RUnlock→Lock` promotion with a single `Lock` in `SetNeighbors`.

### 2. 384-Dim Indexes Faster Than 128-Dim
Counter-intuitively, float32 dim384 indexed faster (290s) than dim128 (352s). HNSW graph construction time is dominated by edge-distance computations and graph traversal steps, not raw dimension. With 384-dim vectors, the HNSW graph has fewer effective entry points at each layer, reducing the per-insertion traversal.

### 3. Search QPS Is Nearly Dimension-Independent for float32
For float32, dense search QPS is similar between dim128 (2,188) and dim384 (2,461). SIMD-accelerated distance computation handles the extra dimension cost in parallel, and HNSW search complexity (ef-level) is the dominant factor. Sparse, ByID, Geo, and Temporal modes are nearly identical across dimensions.

### 4. float64 Is 8x Slower Than float32 for Dense Search
float64 dim128 dense search (274 QPS, P50=30ms) is dramatically slower than float32 dim128 (2,188 QPS, P50=3.4ms). The 8x difference is due to double-precision distance computations in HNSW that double memory bandwidth pressure and prevent SIMD vectorization at full throughput. Sparse search is unaffected (8,090 QPS float64 vs 7,525 QPS float32) because sparse mode uses `bm25s` similarity, which is not dimension-sensitive.

### 5. Memory Ceiling at 16 GB for 1M Vectors
The 16 GB memory limit is adequate for:
- float32 dim128: ~13 GB peak
- float32 dim384: ~14 GB peak
- float64 dim128: ~15 GB peak

float64 dim384 exceeded 16 GB, triggered emergency memory cleanup (clearing query cache, releasing slab pools), and swapped ~2 GB. It was still indexing after 58 minutes without completing. Larger types (complex64, complex128, turboquant8) at dim384 would likely OOM.

### 6. Geo Search Is the Consistent Bottleneck
Geo search is the slowest mode across all configurations (~25 QPS, P50=280ms). The Haversine distance computation (`haversineBatchAVX2`) is a stub that calls scalar Go — no SIMD acceleration.

### 7. All Modes Except Geo Perform Well Under 100ms P99
All non-Geo search modes have P99 latency under 50ms for all completed configs. The only exception is FilteredBool for float64 dim128 (P99=124.72ms), attributed to full metadata scan overhead.

---

## pprof Profiles Available

42 profile files collected across 3 completed configurations:

| Configuration | Profile Types | Timepoints |
|--------------|--------------|------------|
| float32 dim=128 | cpu, heap, allocs, goroutine, threadcreate, block, mutex | start, final (×2) |
| float32 dim=384 | cpu, heap, allocs, goroutine, threadcreate, block, mutex | start, final (×2) |
| float64 dim=128 | cpu, heap, allocs, goroutine, threadcreate, block, mutex | start, final (×2) |

Partial profiles (start only) also exist for float64 dim=384.

Location: `profiles/*.pprof`

---

## Notes

- Test run was interrupted after float64 dim=128. Remaining 31 configurations (float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant, turboquant2, turboquant4, turboquant8 at both dims) were not executed.
- `--pprof` mode was enabled, which adds ~5% overhead from periodic profile collection.
- All results are from CPU mode on a single NUMA node with 8 worker goroutines.
- Server memory management system triggers emergency cleanup at ~15 GB, which adds latency during HNSW build for large configs.
