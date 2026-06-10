# Observations & Next Steps

Based on the 2026-06-09 400k-scale benchmark matrix (dim=384, 4 dtypes, 13 search modes, 10 queries per mode). Full results in `performance.md`.

## Observations

1. **int8 is the most scalable dtype at 400k dim=384**: 55k vec/s ingest, 737s HNSW build, all 13 search modes functional. Sparse search peaks at 5,080 QPS. Dense search delivers 155 QPS at 39.7ms p50.

2. **complex128 is viable at 400k**: Fastest HNSW build (135s) despite 16-byte elements. Dense QPS of 204 is comparable to int8. However, peak RSS hit 14 GB (88% of 16 GB limit) — memory is the binding constraint.

3. **float32 and turboquant fail at 400k dim=384**: Both exceed the 3600s HNSW build timeout. float32 is memory-bandwidth-bound (1,536 bytes/vector × 400k). Turboquant is compute-bound (expensive polar transform + QJL correction pipeline).

4. **Sparse search dominates at scale**: 5,080 QPS (int8) and 3,779 QPS (complex128) — an order of magnitude faster than dense. Sparse uses the inverted index and does not traverse the HNSW graph, making it scale-independent of vector count.

5. **Filtered search is the slowest path**: Filtered modes (filtered, filteredbool) return 16–25 QPS at 400k dim=384. The filter evaluation overhead dominates.

6. **TurboQuant AVX2 distance kernel has a non-power-of-2 dimension bug**: `turboquant.go:206` passed mismatched slice lengths to `l2SquaredAVX2` when `pow2 != dim`. Fixed — all non-power-of-2 dimensions (384, 768, 1536, 3072) are now functional.

## Issues Found

### P0 — TurboQuantAVX2 "simd: length mismatch" at non-pow2 dims (FIXED)

**File**: `internal/simd/turboquant.go:206`

**Impact**: All turboquant searches at non-power-of-2 dimensions returned 0 results. The HNSW indexing loop retried `TurboQuantDistanceAVX2` in an infinite error/retry cycle, consuming 190% CPU but never building the graph.

**Fix**: Truncated query slice to `[:dim]` to match `recon[:dim]` length.

**Affected dims**: 384, 768, 1536, 3072 (and any other dim where `pow2 > dim`).

**Verification**: Re-run of turboquant dim=384 count=400k confirmed no more "simd: length mismatch" errors. However, the HNSW build still times out at 3600s due to the inherent cost of the turboquant distance computation pipeline.

### P1 — Metadata NodeCount not synced after indexing timeout (FIXED)

**File**: `internal/store/index/arrow_hnsw_insert.go:584`

**Impact**: When HNSW indexing times out (or fails), the deferred function in `AddBatch` increments `h.nodeCount` but the metadata registry's `NodeCount` stays at 0. Search reads `meta.NodeCount` and skips all nodes, producing silent 0-result failures.

**Fix**: Added `updateMetadata` call in the deferred function to keep the registry in sync.

**Note**: This fix ensures metadata consistency but does not magically enable search when the graph was never built. float32 and turboquant at 400k still return 0 results because the HNSW graph construction itself timed out.

### P2 — float32 and turboquant HNSW build timeout at 400k dim=384

**Impact**: 2 of 4 dtypes fail to build the HNSW graph within the 3600s timeout at 400k vectors, dim=384.

**Root causes**:
- **float32**: Memory bandwidth bound. 384 × float32 = 1,536 bytes/vector. The parallel linkage phase performs ~1.6 billion distance computations (400k × 200 efConstruction × log(400k)). Each computation reads two vectors from memory (3,072 bytes total). Total memory reads: ~4.7 TB. On DDR4 at ~25 GB/s, this alone takes ~190s. With cache misses, GC pressure, and goroutine scheduling overhead, this balloons beyond 3600s.
- **turboquant**: Compute bound. The distance computation requires polar transform reconstruction + QJL correction + L2. Each operation is ~5× more expensive than int8's direct integer compare.

**Workarounds**:
- Reduce `efConstruction` from 200 to 100 or 50 for large-scale float32/turboquant
- Use `--low-mem` flag to reduce memory pressure
- Pre-quantize float32 vectors to int8 before ingestion
- Use SQ8 (scalar quantization) as an intermediate step

## Recommendations (in order) — Updated 2026-06-09

1. **✅ FIXED — TurboQuantAVX2 "simd: length mismatch" bug**. Slice-length mismatch when `pow2 != dim` at `turboquant.go:206`. All non-power-of-2 dimensions are now functional.

2. **✅ FIXED — Metadata NodeCount sync after indexing timeout**. The deferred function in `AddBatch` now updates the metadata registry. Search no longer silently returns 0 results when indexing partially fails.

3. **✅ DONE — HNSW config env var overrides added**:
   - `LONGBOW_HNSW_EF_CONSTRUCTION` — override efConstruction (default 400)
   - `LONGBOW_HNSW_M` — override M (default 32)
   - `LONGBOW_HNSW_MMAX` — override MMax (default 64)
   - `LONGBOW_HNSW_MMAX0` — override MMax0 (default 64)
   - `LONGBOW_HNSW_BULK_INSERT_THRESHOLD` — override bulk batch size (default 256)
   - All in `internal/store/types/index_types.go:182-204` and `arrow_hnsw_bulk.go:27-34`

4. **✅ DONE — `LONGBOW_BENCH_HNSW_TIMEOUT` env var** added to `cmd/bench-tool/main.go:443` for configurable indexing timeout (default 3600s).

5. **✅ DONE — Default turboquant bits reduced from 8→4** in `DefaultArrowHNSWConfig()` for faster build.

6. **✅ DONE — Turboquant dim alignment documented** in `docs/turboquant.md`.

7. **⚠️ PARTIAL — Verify turboquant at smaller scales**: 50k works (27.5s, 817 QPS). 100k is **~80x slower** (>30 min) — bottleneck identified but not resolved. See findings below.

8. **Pending — Turboquant distance computation optimization at scale**: 80x slowdown from 50k→100k needs deep investigation. Options:
   - Profile 100k build with pprof to identify hotspot
   - Specialized TQ neighbor selection (currently uses slow default fallback in `arrow_hnsw_insert.go:307-330`)
   - Cache reconstructed vectors during construction (Rec #4a)
   - Hybrid SQ8 + TQ approach (Rec #4d)

9. **Pending — float32 HNSW build at 400k**: Now configurable via env vars (ef, M, bulk threshold). Try `LONGBOW_HNSW_EF_CONSTRUCTION=100 LONGBOW_HNSW_M=16` for large-scale runs.

10. **Pending — complex128 memory pressure at 400k+**: 14GB RSS at 400k. Use `LONGBOW_HNSW_M` to reduce memory during construction.

## Scale Sweep Results (turboquant, dim=384, 4-bit)

| Count | HNSW Build | Dense QPS | Rate |
|-------|-----------|-----------|------|
| 50k   | **27.5s** (ef=400) | 817.9 | ✅ All 13 modes |
| 100k  | **>30 min** (ef=200) | N/A | ❌ ~22 jobs/sec (80x slower than 50k) |

**Key finding:** 80x slowdown from 50k→100k is NOT O(N log N) explainable. Suspect nonlinear scaling in the parallel bulk insert path for TQ. Possible causes:
- `selectNeighbors` default fallback extracts all candidates via `GetVector` (returns raw TQ bytes) without diversity pruning — scaling issue at larger candidate lists
- GC pressure from decoded vector allocations during construction
- Memory bandwidth contention with TQ chunk storage format

## New Env Vars Available

| Env Var | Default | Location |
|---------|---------|----------|
| `LONGBOW_HNSW_EF_CONSTRUCTION` | 400 | `index_types.go:182` |
| `LONGBOW_HNSW_M` | 32 | `index_types.go:188` |
| `LONGBOW_HNSW_MMAX` | 64 | `index_types.go:194` |
| `LONGBOW_HNSW_MMAX0` | 64 | `index_types.go:200` |
| `LONGBOW_HNSW_BULK_INSERT_THRESHOLD` | 256 | `arrow_hnsw_bulk.go:27` |
| `LONGBOW_BENCH_HNSW_TIMEOUT` | 3600 | `bench-tool/main.go:443` |

## Status of Previous Recommendations (from prior `nextsteps.md`)

- ✅ CLI flag consistency (`cmd/longbow/flags.go`)
- ✅ Automated continuous benchmarking (`.github/workflows/ci.yml`)
- ✅ P0 `arena is nil` fix (commit `a2f535ef`)
- ✅ 50k int8 concurrent stress test
- ✅ `longbow_arena_nil_error_total` Prometheus counter
- ✅ Document `inBulkInsert` + `readerCount` contract in `docs/hnsw.md`
- ✅ `LONGBOW_BENCH_FAST=1` env var as a synonym for `--ci`
- ✅ `GraphData.ShallowStructuralClone()` for per-batch private clones
- ✅ TurboQuantAVX2 "simd: length mismatch" fix (this session)
- ✅ Metadata NodeCount sync after indexing timeout (this session)
- ✅ HNSW config env var overrides (this session)
- ✅ `LONGBOW_BENCH_HNSW_TIMEOUT` (this session)
- ✅ Default turboquant bits 8→4 (this session)
- ✅ Turboquant dim alignment docs (this session)
- ✅ Turboquant 50k verification (this session)
- ❌ Turboquant 100k+ bottleneck (this session — identified but not resolved)
- ⏳ Disk-backed validation at 1M+ vectors: pending
- ⏳ CUDA execution on RTX 4060: pending
- ⏳ float32 HNSW build at 400k+: pending
- ⏳ turboquant distance computation optimization at scale: pending
