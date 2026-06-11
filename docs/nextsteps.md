# Observations & Next Steps

Based on the 2026-06-10 400k-scale benchmark matrix (dim=384, 4 dtypes, 13 search modes, 10 queries per mode, `M=32`, `MMax0=16`, `efConstruction=200`). Full results in `performance.md`.

## Key Milestone — All 4 dtypes pass at 400k

The self-deadlock fix in `ensureChunksLocked` was the enabler. Previously float32 and turboquant both hit the 3600s bench-tool timeout. Now:
| dtype       | HNSW Build | Was       | Dense QPS | Sparse QPS |
|-------------|-----------|-----------|-----------|------------|
| float32     | **342s**  | >3600s    | 1,451.3   | 5,372.5    |
| int8        | **331s**  | 737s      | 160.5     | 4,309.7    |
| complex128  | **670s**  | 135s      | 87.5      | 4,226.7    |
| turboquant  | **400.5s**| >3600s    | 498.2     | 4,194.3    |

## Observations

1. **float32 is now the fastest dense-search dtype at 400k**: 1,451 QPS at 4.40ms p50 — 9× faster than int8 dense (161 QPS). The 342s build time with MMax0=16 is acceptable. float32 benefits from direct SIMD L2 distance without conversion overhead.

2. **turboquant is the most versatile dtype**: Second-fastest dense search (498 QPS), best hybrid/GraphRAG performance (>750 QPS), 4-bit compression (192 bytes/vector). The 400.5s build is a dramatic improvement from >3600s.

3. **complex128 is viable but memory-bound**: 14 GB RSS at 400k (88% of 16 GB limit). The 670s build time (M=32) is slower than the previous 135s (M=16). ByID is anomalously slow at 3.8 QPS.

4. **Sparse search dominates across all dtypes**: 4,194–5,373 QPS — an order of magnitude faster than dense. Sparse uses the inverted index and does not traverse the HNSW graph.

5. **Filtered search is the slowest path**: 15–115 QPS depending on filter type, across all dtypes. Filter evaluation overhead dominates.

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

### P3 — complex128 ByID anomalously slow at 400k

**File**: N/A (needs investigation)

**Impact**: `Search_ByID` on complex128 returns only 3.8 QPS at p50=2,141ms. For comparison, int8 ByID hits 2,100 QPS at 3.27ms. The 4× element size (128-bit complex vs 8-bit int8) combined with Arrow columnar scan overhead creates a bottleneck.

**Mitigation**: Likely needs an index or bloom filter for 128-bit key lookups instead of full column scan.

### P4 — complex128 memory pressure at 400k

**Impact**: 14 GB RSS (88% of 16 GB limit) with M=32. Higher memory limit or M=8 may be needed for 1M+ scales.

## Recommendations (in order) — Updated 2026-06-10

1. **✅ FIXED — Migration self-deadlock in ensureChunksLocked**. Root cause of all 400k build timeouts. All 4 dtypes now complete within 400–670s.

2. **🟡 INVESTIGATE — complex128 ByID slowdown at 400k**. 3.8 QPS vs 2,100 QPS for int8. Likely Arrow column scan overhead with 128-bit wide elements. Consider indexing.

3. **✅ RESOLVED — float32 HNSW build at 400k dim=384**. 342s with MMax0=16, efConstruction=200. For even faster builds: `LONGBOW_HNSW_EF_CONSTRUCTION=100` + `LONGBOW_HNSW_M=16` yields 51.5s (at potential recall cost).

4. **✅ RESOLVED — turboquant 400k build**. 400.5s with MMax0=16. All 13 search modes functional. No TQ-specific optimization needed beyond the self-deadlock fix.

5. **🟡 NICE TO HAVE — efConstruction auto-tuning**. The scale-adaptive script picks efConstruction based on count tier (100/200/400). Could be improved with automatic latency-budget-based selection.

6. **⏳ TARGET — Disk-backed validation at 1M+ vectors**. Current in-memory benchmark validates correctness up to 400k. 1M+ requires:
   - Disk-backed storage to stay within memory limits
   - Larger `bench-tool` timeout
   - Potential CUDA acceleration for distance computation

7. **⏳ TARGET — CUDA execution on RTX 4060**. Pending hardware setup.

8. **⏳ UPDATE — complex128 memory analysis**. If targeting 1M+ scales with complex128, M=8 and/or 32 GB+ memory limit is needed.

## Scale Sweep Results (all dtypes, dim=384, MMax0=16)

| Count | dtype | HNSW Build | Dense QPS | Notes |
|-------|-------|-----------|-----------|-------|
| 400k  | float32     | **342s** (ef=200, M=32) | 1,451 | ✅ ef=100/M=16 → 51.5s |
| 400k  | int8        | **331s** (ef=200, M=32) | 161 | |
| 400k  | complex128  | **670s** (ef=200, M=32) | 88 | ⚠️ 14GB RSS, ByID 3.8 QPS |
| 400k  | turboquant  | **400.5s** (ef=200, M=32) | 498 | ✅ Best hybrid/GraphRAG |
| 100k  | turboquant  | **43.5s** (ef=400) | 143 | ✅ All 13 modes |
| 50k   | turboquant  | **27.5s** (ef=400) | 818 | All 13 modes |

**Key finding**: The self-deadlock in `ensureChunksLocked` was the root cause of ALL build timeouts at 400k. With MMax0=16 and efConstruction=200, all 4 dtypes build within 400–670s.

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
- ✅ **ALL 4 DTYPES PASS AT 400k DIM=384** — float32, int8, complex128, turboquant
- 🟡 complex128 ByID investigation: pending
- ⏳ Disk-backed validation at 1M+ vectors: pending
- ⏳ CUDA execution on RTX 4060: pending
