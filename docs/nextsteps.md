# Recommended Next Steps (Updated 2026-06-12)

Based on the 1M-vector benchmark run (dim128+384, float32/float64, 1000 queries, all 13 search modes) and the deadlock fix in `LockFreeNeighborCache`.

---

## Priority 1: Geo Search SIMD Acceleration

**Problem**: Geo search is the slowest mode at ~25 QPS (P50=280ms, P99=700-800ms) across all configurations. It's the only mode that fails to meet interactive latency targets.

**Root cause**: `haversineBatchAVX2` in `internal/vector/geo_search.go` is a stub that falls back to scalar Go. The Haversine distance requires sin, cos, sqrt, and atan2 — all of which have existing SIMD implementations (`SinFloat32`, `CosFloat32`, `Atan2Float32` in `internal/simd/`).

**Fix**: Implement batched Haversine that decomposes into 4 passes through the SIMD primitives:
1. Batch sin(lat1) + sin(lat2) + cos(lat1) + cos(lat2)
2. Batch cos(delta_lon)
3. Batch central angle computation (acos or atan2 of intermediate)
4. Batch multiply by Earth radius

**Expected gain**: 2-4x (to ~75-100 QPS)

**Files to change**: `internal/vector/geo_search.go` (vectorized kernel), `internal/simd/dispatch.go` (add `HaversineBatch` entry point)

---

## Priority 2: Memory Budget for 1M-Scale Indexing

**Problem**: 16 GB is insufficient for larger dtypes at dim384 with 1M vectors. float64 dim384 hit the limit and swapped (2 GB). complex128 at dim384 would need ~24 GB+.

**Root causes**:
- HNSW graph construction allocates edge lists per node proportional to M × layers
- Multi-type vectors (float64, complex128) double/quadruple the memory per edge
- Emergency memory cleanup at ~15 GB adds latency but doesn't free enough to continue efficiently

**Options**:
- Document minimum memory requirements per (dtype, dim, count) combination
- Implement progressive HNSW construction that batches edge computations to reduce peak memory
- Add memory-budget-aware efConstruction tuning (reduce M at scale to fit memory)
- Add pre-flight memory estimation in the server that rejects requests exceeding budget

**Files to change**: `internal/store/index/arrow_hnsw_bulk.go` (memory-aware builder), `internal/server/server.go` (pre-flight check)

---

## Priority 3: float64/Integer Distance Optimization

**Problem**: float64 dense search (274 QPS) is 8x slower than float32 (2,188 QPS). Integer types (int64, uint64) also underperform due to 8-byte memory bandwidth pressure.

**Root cause**: Distance computation for float64 uses double-precision arithmetic that saturates memory bandwidth. The SIMD dispatch prefers AVX2 which operates on 256-bit registers — for float64 this means only 4 elements per instruction vs 8 for float32.

**Options**:
- Map float64 → float32 with controlled precision loss at query time (optional, off by default)
- Implement AVX-512 path for float64 (512-bit = 8 float64 elements per instruction) — this requires AVX-512 hardware
- Pre-encode float64 vectors as float32 at ingest (user opt-in, trades precision for 2x throughput)

**Files to change**: `internal/simd/simd_avx2.go`, `internal/simd/simd_avx512.go` (if available)

---

## Priority 4: Complete Benchmark at 1M Scale

**Problem**: Only 3 of 34 planned configurations completed. The remaining 31 (float16, int8-64, uint8-64, complex64/128, turboquant2/4/8 at dim128+384) have no data at 1M scale.

**Strategy**: Rather than running all 34 sequentially, run in 3 batches by memory profile:

| Batch | Configs | Est. Peak Memory | Est. Time |
|-------|---------|-----------------|-----------|
| A (small) | float32, float16, int8, uint8, turboquant2 dim128+384 | ~8-14 GB | ~2 h |
| B (medium) | int16, uint16, int32, uint32, turboquant4 dim128+384 | ~10-16 GB | ~3 h |
| C (large) | float64, int64, uint64, complex64, complex128, turboquant8 dim128+384 | ~14-24 GB | ~4 h |

**Batch C at dim384 may need >16 GB.** Consider testing float64 dim384 first (estimated 18+ GB) and if it OOMs, split further or reduce to 500K count.

**Total est. time**: ~9-10 hours uninterrupted.

---

## Priority 5: LockFreeNeighborCache Hardening

**Problem**: The deadlock in `LockFreeNeighborCache.SetNeighbors` was fixed, but the cache still uses `sync.RWMutex` which can be a contention point under heavy parallel insert traffic.

**Status**: Fixed in commit `27a4df8b` — replaced `RLock→RUnlock→Lock` promotion with direct `Lock`.

**Future**: Consider migrating to a lock-free data structure (e.g., `sync.Map` with CAS) for `SetNeighbors` to eliminate mutex contention entirely at scale. Profile first to determine if it's a bottleneck.

---

## Priority 6: Filter Performance at Scale

**Problem**: Filtered search modes (filtered, filteredbool, filteredstring) show elevated P99 tail latency across all configs. For float64 dim128, FilteredBool reaches P99=124ms.

**Root cause**: All filter modes do a full metadata scan per query. At 1M vectors with string/bool attributes, this dominates query time.

**Options**:
- Implement bloom filters for string columns
- Add bitmap indexes for bool columns
- Add query-time filter pushdown to the HNSW graph traversal (prune edges based on filter)

**Files to change**: `internal/store/query/filter.go`, `internal/store/metadata/`

---

## Priority 7: Benchmark Infrastructure Improvements

- **Auto-resume**: Add a `--resume` flag to skip completed configs (check for existing result JSON)
- **Memory profiling**: Add peak RSS tracking per test phase to `perf_matrix*.json`
- **Multi-run statistics**: Run each config 3x and report mean/stddev for QPS and latency
- **Heatmap generation**: Automatically generate a heatmap from the perf matrix showing QPS by dtype × dim
- **Swap detection**: Alert when the system starts swapping during a benchmark run

---

## Data Available

- 42 pprof profiles in `profiles/` (7 types × start + final × 3 configs)
- 3 result JSON files in `data/perf_logs/result_*.json`
- 1 partial perf matrix: `data/perf_logs/perf_matrix_cpu_full_bench_20260612_220550.json`
- 12 GB of server logs in `data/perf_logs/longbow_*.log`
