# Observations & Next Steps

Based on the 2026-06-11 400k-scale benchmark matrix (dim=384, 4 dtypes, 13 search modes, 500 queries per mode, `M=32`, `MMax0=16`, `efConstruction=200`). Full results in `performance.md`.

## Key Milestone — All 4 dtypes pass at 400k (500 queries, MMax0=16)

The self-deadlock fix in `ensureChunksLocked`, MMax0=16 correction, and async temporal ingestion combine for significantly improved build times and search QPS. All 13 search modes pass with 500 queries each, zero errors, zero OOMs.

| dtype       | HNSW Build | Was       | Dense QPS | Sparse QPS |
|-------------|-----------|-----------|-----------|------------|
| float32     | **~86s**  | >3600s    | **2,012.5** | **6,537.1** |
| int8        | **~164s** | 737s      | **718.8**   | **6,910.9** |
| complex128  | **~418s** | 135s      | **153.2**   | **6,396.9** |
| turboquant  | **~115s** | >3600s    | **1,772.6** | **6,668.3** |

**Key improvements since 2026-06-10 baseline**:
- **MMax0=16 fix (P6)**: Benchmark script now sets `LONGBOW_HNSW_MMAX0=16`
- **Async temporal ingestion (#11)**: Offloads tree updates from `AddBatch`
- **FilteredBool optimization (P9)**: 5.8–20× improvement via raw Arrow buffer access
- **500 queries per mode**: Eliminates outlier-dominated results from previous 10-query smoke test

## Observations

1. **float32 is the fastest dense-search dtype at 400k**: At 2,012 QPS (3.78ms p50), float32 dense search leads all dtypes. It benefits from direct SIMD L2 distance computation without conversion overhead. Build time at ~86s (MMax0=16) is a dramatic improvement from the 714s MMax0=64 run.

2. **turboquant is the most versatile dtype**: Second-fastest dense search (1,773 QPS), best FilteredBool (1,064 QPS), all vector modes exceed 1,500 QPS. 4-bit compression (192 bytes/vector). Build time at ~115s. The optimal choice for most workloads.

3. **complex128 is viable but memory-bound**: ~14 GB RSS at 400k (87% of 16 GB limit) with MMax0=16. Build at ~418s. **ByID remains fast at 3,536 QPS** (P3 fix). For 1M+ scales, M=8 is recommended.

4. **Sparse search dominates across all dtypes**: 6,397–6,911 QPS — 3–9× faster than dense. Uses the inverted index and does not traverse the HNSW graph.

5. **FilteredBool is no longer the slowest path**: The P9 fix (raw Arrow packed-bitset buffer) eliminated per-element `Value(i)` calls, delivering 5.8–20× improvements. turboquant FilteredBool now runs at 1,064 QPS (3.81ms p50). **FilteredString and Geo are now the slowest modes** across most dtypes.

## Issues Found

### P0 — Migration self-deadlock in ensureChunksLocked (FIXED)

**File**: `internal/store/index/arrow_hnsw_memory.go:186`

**Root cause**: `ensureChunksLocked` acquired a reader on GraphData, then called `growInternal` → `oldData.Release()`, which spins forever on `readerCount > 0` — but the same goroutine holds the reader. The `defer data.ReleaseReader()` can't run until `ensureChunksLocked` returns, which can't return until `Release` completes.

**Why it only triggered at 100k+**: Sharded migration gives each shard ~100 vectors/batch (below `BulkInsertThreshold=256`), so the sequential path is used. Each shard's second `AddBatch` triggers growth while holding the reader.

**Fix**: Release reader before `growInternal` (`arrow_hnsw_memory.go:207-212`). Also added migration pause channel (`hnsw_autoshard.go`) and `atomic.Pointer` for `tqDecodeCache`.

**Verification**: Full 400k benchmark for all 4 dtypes completes successfully. No more infinite spin/deadlock.

### P1 — TurboQuantAVX2 "simd: length mismatch" at non-pow2 dims (FIXED)

**File**: `internal/simd/turboquant.go:206`

**Impact**: All turboquant searches at non-power-of-2 dimensions returned 0 results. Fixed by truncating query slice to `[:dim]`.

### P2 — Metadata NodeCount not synced after indexing timeout (FIXED)

**File**: `internal/store/index/arrow_hnsw_insert.go:584`

**Impact**: When HNSW indexing timed out, metadata registry's `NodeCount` stayed at 0 while `h.nodeCount` was updated, producing silent 0-result searches. Fixed with `updateMetadata` call in deferred function.

### P3 — complex128 ByID anomalously slow at 400k (FIXED & CONFIRMED)

**File**: `internal/store/index/navigation_search.go`

**Impact**: `Search_ByID` on complex128 previously returned only 3.8 QPS at p50=2,141ms because `[]float64` query vectors (physically extracted by Arrow) on `complex128` datasets were not mapped to the optimized `complex128Computer`. This caused a fallback to slow, non-cached disk lookups for every visited node.

**Fix**: Modified `resolveHNSWComputer` to intercept `[]float64` query vectors for `types.VectorTypeComplex128` indexes, converting them to `[]complex128` and returning `complex128Computer`. This restores the high-performance SIMD search path.

**Verification**: 2026-06-11 benchmark confirms ByID at 1,680 QPS (P50=3.12ms).

### P4 — complex128 memory pressure at 400k

**Impact**: 14 GB RSS (88% of 16 GB limit) with M=32, MMax0=16. At MMax0=64, RSS hit 16 GB (100% of 16 GB limit), triggering GC tuning and ingestion throttling. Higher memory limit or M=8 may be needed for 1M+ scales.

### P5 — Temporal index ingestion bottleneck at high scale

**File**: `internal/store/temporal_search.go`

**Impact**: Ingestion of large batches (e.g. 500k vectors) triggers a slow step warning for temporal indexing (`applyBatchToMemory trace: slow step`), taking ~5.2s.

**Root cause**: `TemporalIndex.AddBatch` sequentially processes vectors and calls `Insert` one by one on `TemporalTree` and `SegmentTree`, resulting in 500k sequential lock acquisitions/releases and repeated slice growth/array copying.

**Mitigation/Suggestions**:
1. **Bulk Insertion API**: Implement `InsertBatch` in `TemporalTree` and `SegmentTree` that locks once, pre-sorts elements by timestamp, pre-sizes internal slices, and performs batch insertions to avoid lock overhead and repeated allocations.
2. **Parallelized Norm/Metadata Processing**: Parallelize vector norm calculations inside `AddBatch` before acquiring any locks.
3. **Asynchronous Temporal Ingestion**: Offload the temporal index updates to a background worker queue, similar to HNSW graph updates, so it does not block the primary ingestion worker thread.

## Recommendations (in order) — Updated 2026-06-10

1. **✅ FIXED — Migration self-deadlock in ensureChunksLocked**. Root cause of all 400k build timeouts. All 4 dtypes now complete within 400–670s.

2. **✅ RESOLVED — complex128 ByID slowdown at 400k**. Fixed routing of `[]float64` queries on `complex128` dataset to return optimized `complex128Computer` instead of falling back to slow disk/fallback path. Restores QPS from 3.8 to high-performance SIMD search.

3. **✅ RESOLVED — float32 HNSW build at 400k dim=384**. 342s with MMax0=16, efConstruction=200. For even faster builds: `LONGBOW_HNSW_EF_CONSTRUCTION=100` + `LONGBOW_HNSW_M=16` yields 51.5s (at potential recall cost).

4. **✅ RESOLVED — turboquant 400k build**. 400.5s with MMax0=16. All 13 search modes functional. No TQ-specific optimization needed beyond the self-deadlock fix.

5. **🟡 NICE TO HAVE — efConstruction auto-tuning**. The scale-adaptive script picks efConstruction based on count tier (100/200/400). Could be improved with automatic latency-budget-based selection.

6. **⏳ TARGET — Disk-backed validation at 1M+ vectors**. Current in-memory benchmark validates correctness up to 400k. 1M+ requires:
   - Disk-backed storage to stay within memory limits
   - Larger `bench-tool` timeout
   - Potential CUDA acceleration for distance computation

7. **⏳ TARGET — CUDA execution on RTX 4060**. Pending hardware setup.

8. **⏳ UPDATE — complex128 memory analysis**. If targeting 1M+ scales with complex128, M=8 and/or 32 GB+ memory limit is needed.

9. **✅ RESOLVED — Temporal indexing bottlenecks (batch insertion APIs)**. Implemented `InsertBatch` on `TemporalTree` (sort + single lock + cursor-based) and `SegmentTree` (single lock). Also parallelized norm computation in `AddBatch` and added async ingestion worker (`SetAsyncIngestion`). Slow step warnings reduced from 500k individual lock operations to 1 batch per call.

10. **✅ FIXED — Benchmark script MMax0 mismatch (`scripts/unified_benchmark.py`)**. Now sets `LONGBOW_HNSW_MMAX0=16` alongside `LONGBOW_MAX_M0=16`. Future benchmark runs will match the MMax0=16 baseline.

11. **✅ IMPLEMENTED — Async temporal ingestion by default**. `NewTemporalIndex` now starts the ingest worker and sets `asyncIngest=true` in the constructor. `AddBatch` offloads tree updates to a background goroutine, returning immediately. Call `SetAsyncIngestion(false)` to restore synchronous behavior.

12. **✅ FIXED — FilteredBool performance on float32**. `boolFilterOp.MatchBitmap` and `Match` now access the raw Arrow packed-bitset buffer directly instead of calling `Value(i)` per element. Processes 8 bits per source byte with bitwise extraction, eliminating the per-element Arrow access overhead.

## Scale Sweep Results (all dtypes, dim=384)

| Count | dtype | MMax0 | HNSW Build | Dense QPS | Notes |
|-------|-------|-------|-----------|-----------|-------|
| 400k  | float32     | 64 (default) | **714s** (ef=200, M=32) | 601 | ⚠️ 2× baseline build time |
| 400k  | float32     | 16†     | **342s** (ef=200, M=32) | 1,451 | ✅ Baseline |
| 400k  | int8        | 64      | **349s** (ef=200, M=32) | 162 | |
| 400k  | complex128  | 64      | **709.5s** (ef=200, M=32) | 57 | ⚠️ 16GB RSS, ByID 1,680 QPS ✅ |
| 400k  | complex128  | 16†     | **670s** (ef=200, M=32) | 88 | 14GB RSS |
| 400k  | turboquant  | 64      | **571.5s** (ef=200, M=32) | 475 | ✅ Best hybrid/GraphRAG |
| 400k  | turboquant  | 16†     | **400.5s** (ef=200, M=32) | 498 | ✅ Baseline |
| 100k  | turboquant  | 64      | **43.5s** (ef=400) | 143 | ✅ All 13 modes |
| 50k   | turboquant  | 64      | **27.5s** (ef=400) | 818 | All 13 modes |

† Baseline results from 2026-06-10. Current run uses default MMax0=64.

**Key finding**: MMax0 has a significant impact on build time and search QPS. The benchmark script's `LONGBOW_MAX_M0=16` only caps grown MMax0 but does not set the initial value. Adding `LONGBOW_HNSW_MMAX0=16` is needed to match the baseline configuration.

### P6 — float32 HNSW build 2× slower with default MMax0 (FIXED)

**File**: `scripts/unified_benchmark.py` (`start_server()` section)

**Impact**: The 2026-06-11 benchmark used default `MMax0=64` (instead of `MMax0=16` from the baseline) because the script sets `LONGBOW_MAX_M0=16` (which caps *grown* MMax0 at 16) but does **not** set `LONGBOW_HNSW_MMAX0=16` (which sets the initial value before growth). This caused:

| dtype       | Build (MMax0=16, baseline) | Build (MMax0=64, current) | Ratio |
|-------------|---------------------------|--------------------------|-------|
| float32     | 342s                      | 714s                     | 2.1×  |
| int8        | 331s                      | 349s                     | 1.1×  |
| complex128  | 670s                      | 709s                     | 1.1×  |
| turboquant  | 400s                      | 571s                     | 1.4×  |

Dense QPS also dropped proportionally (e.g., float32 from 1,451 to 601 QPS) because the denser graph traverses more edges per query.

**Fix**: Added `LONGBOW_HNSW_MMAX0=16` alongside `LONGBOW_MAX_M0=16` in the script's scale-adaptive configuration. Future benchmark runs will match the MMax0=16 baseline.

### P7 — Temporal index slow step at high batch sizes (UPDATE)

**Status**: Partially mitigated by the P5 batch insertion API.

The `applyBatchToMemory trace: slow step` warning for the "temporal index" step still fires at ~5–9s per large batch for complex128 and turboquant at 400k. The new `InsertBatch` APIs reduced the per-vector overhead (single lock vs N locks + sorted cursor traversal), but for 24k+ vector batches the total time is dominated by:
- Shard map insertions (128 shards, one lock per shard per vector)
- `TemporalTree.InsertBatch` sorting O(n log n) and arena allocation
- `SegmentTree.InsertBatch` O(n log R) bitmap insertions

**Suggestion**: The async ingestion worker (`SetAsyncIngestion(true)`) completely offloads tree updates from the critical path.

### P8 — complex128 memory at 100% with MMax0=64 (WARNING)

**Impact**: At 400k with MMax0=64, complex128 hit 16 GB RSS (100% of the 16 GB limit), triggering GOGC reduction to 40 and ingestion worker throttling. With MMax0=16, peak RSS was 14 GB (88%). Scale to 1M+ requires M=8 and/or 32 GB.

### P9 — FilteredBool on float32 is extremely slow at 400k (FIXED)

**File**: `internal/query/filter_evaluator.go` (`boolFilterOp.MatchBitmap` + `Match`)

**Impact**: FilteredBool on float32 dim=384 returned only 3.9 QPS with P50=1,640ms — the slowest of any mode across all dtypes. The bool filter evaluation involved per-node metadata lookups in Arrow arrays, which is O(N) over all 400k nodes even though the filter is highly selective.

**Root cause**: `MatchBitmap` called `o.col.Value(i)` and `o.col.IsNull(i)` for every row — each call incurred Arrow array bounds checking, function call dispatch, and per-element bit extraction overhead. No SIMD path existed for boolean columns (unlike int64/float32 which use `simd.MatchInt64`/`simd.MatchFloat32`).

**Fix**: Rewrote `MatchBitmap` and `Match` to access the raw Arrow boolean data buffer (`Buffers()[1]`) and validity buffer (`Buffers()[0]`) directly as packed bitsets. The new implementation processes 8 bits per source byte with bitwise extraction and branching eliminated from the inner loop. Null handling uses direct bitmap inspection instead of `IsNull()`.

### P10 — Disk-backed search results (NEW — 2026-06-11)

**Status**: Completed full 4-dtype disk-backed benchmark with `--use-disk --iouring`.

**Summary**:

| dtype       | Dense QPS Disk | vs In-Mem | Disk Usage | Best Mode |
|-------------|---------------|-----------|-----------|-----------|
| float32     | 20.7          | 29.0×     | 586 MB    | Sparse: 4,554 QPS |
| int8        | 89.0          | 1.8×      | 146 MB    | ByID: 2,828 QPS |
| complex128  | 59.9          | 1.0×      | 2,344 MB  | ByID: 1,463 QPS |
| turboquant  | 805.8         | **0.6× faster** | 586 MB | Recommend: 1,249 QPS |

**Key findings**:

1. **turboquant is the optimal dtype for disk** — dense/hybrid/graph all match or exceed in-memory QPS because 4-bit vectors fit in CPU cache regardless of backing store.

2. **int8 is excellent for disk** — only 1.8× slower than in-memory for dense, uses just 146 MB for 400k vectors. ByID at 2,828 QPS is nearly in-memory speed.

3. **complex128 is competitive on disk** — dense QPS is identical to in-memory because 6 KB/vector saturates memory bandwidth either way; page cache absorbs the reads.

4. **float32 suffers most on disk** — 29× slower dense search. Consider int8/turboquant for disk-backed float32 applications.

5. **io_uring enabled via custom backend** (`internal/storage/wal_backend_arrow_iouring.go`). The kernel 7.0.0's io_uring subsystem handled writes without visible stall. Direct I/O (`O_DIRECT`) not used — went through page cache.

**Recommendation**: TurboQuant is strongly recommended for disk-backed deployments. For float32 workloads, the disk-backed mode should use int8 or turboquant indexing to avoid the 29× dense QPS penalty.

### P11 — FilteredBool regression on turboquant disk-backed (RESOLVED — measurement noise)

**Status**: **RESOLVED** — the 13.9× regression was **measurement noise from 10-query samples**.

**Definitive results with 200 queries** (2026-06-11):

| Config | QPS | P50 | P95 | P99 |
|--------|:---:|:---:|:---:|:---:|
| In-Memory (200q) | 26.6 | 278 ms | 446 ms | 633 ms |
| Disk (200q) | **438.0** | **10.6 ms** | **15.9 ms** | **182 ms** |

**Disk is 16.5× faster than in-memory** (438 vs 26.6 QPS) at 200 queries. The original 10-query sample (3.8 QPS disk, 52.8 QPS in-memory) was skewed by 1-2 outlier queries causing an apparent 13.9× regression in the wrong direction.

**Root cause of noise**: The 10-query benchmark's P50 of 1,693ms (disk) vs 178ms (in-memory) was dominated by a single slow query — likely a GC pause or page cache miss during a shared process lifecycle phase. With 200 queries, the distribution stabilizes tightly around 10-16ms P50/P95 for disk.

**Unexpected finding**: Disk-backed FilteredBool on turboquant significantly outperforms in-memory. This is likely because mmap-based `DiskGraph` neighbor lookups avoid arena allocation overhead and GC pressure, while the page cache keeps hot data resident.

**Perf matrix saved**: `perf_matrix_cpu_filteredbool_disk_200q_20260611_155649.json`
**In-memory baseline**: `perf_matrix_cpu_filteredbool_mem_200q_20260611_161103.json`

---

## New Env Vars Available

| Env Var | Default | Location |
|---------|---------|----------|
| `LONGBOW_HNSW_EF_CONSTRUCTION` | 400 | `index_types.go:182` |
| `LONGBOW_HNSW_M` | 32 | `index_types.go:188` |
| `LONGBOW_HNSW_MMAX` | 64 | `index_types.go:194` |
| `LONGBOW_HNSW_MMAX0` | 64 | `index_types.go:200` |
| `LONGBOW_HNSW_BULK_INSERT_THRESHOLD` | 256 | `arrow_hnsw_bulk.go:27` |
| `LONGBOW_BENCH_HNSW_TIMEOUT` | 3600 | `bench-tool/main.go:443` |
| `LONGBOW_MAX_M0` | (none) | `insertion_core.go:278` |

## Status of Previous Recommendations (from prior `nextsteps.md`)

- ✅ CLI flag consistency (`cmd/longbow/flags.go`)
- ✅ Automated continuous benchmarking (`.github/workflows/ci.yml`)
- ✅ P0 `arena is nil` fix (commit `a2f535ef`)
- ✅ 50k int8 concurrent stress test
- ✅ `longbow_arena_nil_error_total` Prometheus counter
- ✅ Document `inBulkInsert` + `readerCount` contract in `docs/hnsw.md`
- ✅ `LONGBOW_BENCH_FAST=1` env var as a synonym for `--ci`
- ✅ `GraphData.ShallowStructuralClone()` for per-batch private clones
- ✅ TurboQuantAVX2 "simd: length mismatch" fix
- ✅ Metadata NodeCount sync after indexing timeout
- ✅ HNSW config env var overrides
- ✅ `LONGBOW_BENCH_HNSW_TIMEOUT`
- ✅ Default turboquant bits 8→4
- ✅ Turboquant dim alignment docs (`docs/turboquant.md`)
- ✅ Turboquant 50k/100k verification
- ✅ Migration self-deadlock fix (ensureChunksLocked)
- ✅ TQ decode cache + scratch pool reuse (`sync.Pool`)
- ✅ Migration pause channel (`hnsw_autoshard.go`)
- ✅ `atomic.Pointer` for `tqDecodeCache` data race fix
- ✅ **ALL 4 DTYPES PASS AT 400k dim=384** — float32, int8, complex128, turboquant
- ✅ complex128 ByID investigation: resolved and confirmed (1,680 QPS)
- ✅ Temporal index batch insertion APIs (InsertBatch + parallel norms + async worker)
- ✅ **Disk-backed validation complete** (400k dim=384, 4 dtypes, `--use-disk --iouring`). Full comparison in `performance.md`. Turboquant matches/exceeds in-memory QPS. float32 dense drops 29× (expected).
- ⏳ CUDA execution on RTX 4060: pending
- ✅ temporal index bottleneck: mitigated — async ingestion enabled by default in `NewTemporalIndex`, slow step fully offloaded from `AddBatch` critical path
- ✅ `unified_benchmark.py` MMax0 mismatch: fixed — now sets `LONGBOW_HNSW_MMAX0=16` alongside `LONGBOW_MAX_M0=16`
- ✅ FilteredBool on float32 fixed: `boolFilterOp.MatchBitmap` uses raw Arrow packed-bitset buffer, eliminates per-element `Value(i)` calls

---

## New Recommendations (2026-06-11)

### P13 — FilteredString optimization (NEW)

**Impact**: FilteredString is now the slowest or second-slowest mode for all dtypes:
- float32: 54.8 QPS (135.88ms)
- int8: 130.9 QPS (54.37ms)
- complex128: 34.3 QPS (158.73ms)
- turboquant: 54.6 QPS (127.37ms)

**Root cause**: Similar to the old FilteredBool bottleneck — string comparison iterates all rows calling `col.Value(i)` for each. There is no SIMD string match path.

**Suggestion**: Apply the same raw-buffer approach used in P9. For string equality, access the Arrow string data buffer directly and use `bytes.Equal` or memcmp on raw offsets/lengths. Consider a prefix-filter or hash-based pre-filter for `eq`/`neq` operators on short strings.

### P14 — Geo spatial search optimization (NEW)

**Impact**: Geo is consistently one of the slowest modes (26–51 QPS) across all dtypes. The Haversine distance computation over 400k candidates dominates.

**Suggestion**: Implement a spatial index (e.g., geohash or quadtree) to pre-filter candidates within a bounding box before running Haversine. This would reduce the O(N) scan to O(log N) lookup for typical geo queries with radius constraints.

### P15 — int8 dense search SIMD gap (NEW)

**Impact**: int8 dense at 719 QPS (10.42ms p50) is ~2.8× slower than float32 dense (2,012 QPS). The integer distance computation at 384-dim is bandwidth-bound, but lacks the SIMD-optimized L2 distance that float32 has.

**Suggestion**: Implement an AVX2/AVX-512 int8 dot-product/L2 distance kernel. 384-dim int8 vectors (384 bytes) fit in two 256-bit AVX2 loads — a properly vectorized implementation should approach float32 throughput.

### P16 — complex128 mode optimization (NEW)

**Impact**: Several complex128 modes cluster at 100–315 QPS:
- dense: 153 QPS, hybrid: 99 QPS, graphrag: 315 QPS, recommend: 201 QPS, learnedindex: 203 QPS

These are memory-bandwidth-bound at 6,144 bytes/vector. The 16-byte complex type doubles the memory traffic vs float64 and quadruples it vs float32.

**Suggestion**: Pre-compute complex magnitudes during ingestion and store them alongside the HNSW graph. Reference: `complex128Computer` in `navigation_search.go` already optimizes distance computation — but graph traversal still reads full vectors. Consider using the magnitude as a bound for pruning (similar to VP-tree).

### P17 — 1M+ scale validation with disk-backed storage (ONGOING)

**Impact**: Current in-memory benchmark validates up to 400k. Scaling to 1M+ exceeds the 16 GB memory limit for float32 (1,536 B × 1M = 1.5 GB vectors + 1.5× graph overhead ≈ 3.75 GB — fits, but HNSW temporarily doubles during migration). For complex128 (6,144 B × 1M = 6 GB vectors + graph), 16 GB is tight.

**Suggestion**: Run disk-backed benchmarks at 1M+ with turboquant as the primary dtype (192 bytes/vector after quantization). Use `--use-disk --iouring` to keep peak RSS under 10 GB. Also validate int8 at 1M+ (384 MB vectors, fits comfortably).
