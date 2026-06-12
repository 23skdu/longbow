# Recommended Next Steps (Updated 2026-06-12)

Based on the full benchmark matrix + targeted optimization run (6 integer dtypes at dim128 50K), these are the updated priorities.

## ✅ Completed — Integer Distance Optimization (40 min)

**Changes applied** in `internal/simd/simd_baseline.go` and `internal/simd/dispatch.go`:

| Dtype | Optimization | Impact |
|-------|-------------|--------|
| uint32 | dot/Euclidean: float64 accumulator → **uint64** | **+80% dense QPS** |
| int32 | dot/Euclidean: float64 accumulator → **int64** | **+31% dense QPS** |
| uint64 | Euclidean: 4x → **8x unrolled**; cosine: scalar → **4x unrolled** | **+410% dense QPS** |
| int64 | Euclidean: 4x → **8x unrolled**; cosine: scalar → **4x unrolled** | **+19% dense QPS** |
| uint8 | dot product: AVX2 dispatch was routing to unrolled fallback → **fixed** | ~neutral (AVX2 was already on other paths) |
| uint16 | dot product: AVX2 dispatch was routing to unrolled fallback → **fixed** | **+17% dense QPS** |

## ✅ Completed — Geo Search (Quadtree Optimization)

**Changes** in `internal/store/geo_search.go`:
- Removed double Haversine: quadtree `QueryRadius` now returns bounding-box candidates only (no per-point Haversine filter)
- Exact Haversine distance is computed once in `SearchRadius` via batch path
- Reduced per-query allocation overhead

## ✅ Completed — turboquant Consistency

**Changes** in `cmd/bench-tool/main.go`:
- Added `TqBits int \`json:"tq_bits,omitempty"\`` to `BenchmarkResult`
- Populated in all result types (DoPut, DoGet, Indexing, Search)

**Changes** in `scripts/unified_benchmark.py`:
- Fixed default `tq_bits` from 4 → 0 for non-turboquant types
- Added `tq_bits` to result JSON and both summary/table markdown outputs

## ✅ Completed — Benchmark Script Improvements

**Changes** in `scripts/unified_benchmark.py`:
- Platform title now shows actual OS (Linux) instead of "Apple M3 Pro"
- Summary table includes ALL search modes (not just dense)
- Full results table now dynamically includes all present search modes

## 1. Filtered string auto-indexing

**Problem**: `filteredstring` mode is consistently 10-20x slower than `filteredbool`. At 500K vectors (float32, dim128): 55 QPS vs 601 QPS.

**Fix**: Implement string attribute indexing (inverted index or hash-based bloom filter) rather than scanning all metadata entries at query time.

**Expected gain**: 5-10x improvement.

## 2. HNSW build profiling

**Problem**: Ingest drops from 1.2M vec/s (50K count) to 53K vec/s (500K count). At 500K, the server is spending ~95% of time on HNSW construction.

**Suggestion**: Analyze pprof profiles in `profiles/` to identify exact HNSW bottleneck (distance computation vs graph traversal vs memory allocation).

## 3. Geo search — SIMD Haversine

**Problem**: `haversineBatchAVX2` is a stub that falls through to scalar Go. True SIMD Haversine would require breaking the computation into vectorized `sin`/`cos`/`sqrt`/`atan2` passes (all have real AVX2/AVX-512/Neon implementations already in the SIMD package).

**Expected gain**: 2-4x for geo search batch distance pass.

## 4. Full benchmark re-run

Run all 136 configurations to get a complete before/after comparison. Currently only 6 integer dtypes at dim128 50K have been validated.

## 5. Memory profiling

37MB of pprof data collected across all configurations. Key profiles to analyze:
- `profiles/*_500000_profile_*.pprof` — CPU profile at scale
- `profiles/*_500000_heap_*.pprof` — heap at max scale
