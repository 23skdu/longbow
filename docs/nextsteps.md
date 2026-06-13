# Recommended Next Steps (Updated 2026-06-13)

Based on the 1M-vector benchmark run (dim128+384, float32/float64, 1000 queries, all 13 search modes), deadlock fix in `LockFreeNeighborCache`, Geo SIMD Haversine, float64 cosine dispatch fix, sync.Map migration, string trigram index, and Emerald/Granite Rapids build tag infrastructure.

---

## Priority 0: Intel Emerald Rapids & Granite Rapids SIMD Support

**Problem**: Longbow has no AMX (Advanced Matrix Extensions) support for Intel Sapphire Rapids / Emerald Rapids / Granite Rapids CPUs. AMX provides tile-based matrix multiply instructions (`TDPBSSD`/`TDPBF16PS` for INT8/BF16 on Emerald Rapids, `TDPFP16PS` for FP16 on Granite Rapids) that can deliver 5-10x higher throughput than AVX-512 for matrix-heavy workloads like HNSW distance computation and matmul.

**Status**: Build tag infrastructure is implemented but AMX assembly kernels are not yet written.

### What's Built

| Component | File | Description |
|-----------|------|-------------|
| `emerald` build tag | `emerald_amd64.go`, `emerald_stubs_amd64.go` | Compile-time gate for Emerald Rapids AMX kernels |
| `granite` build tag | `granite_amd64.go`, `granite_stubs_amd64.go` | Compile-time gate for Granite Rapids AMX-FP16 kernels |
| CPU detection | `cpu_detection.go` | `HasAMX`, `HasAMXINT8`, `HasAMXBF16`, `HasAMXFP16`, `HasAMXCOMPLEX` |
| Runtime dispatch selection | `cpu_detection.go` | Auto-selects `"emerald"` or `"granite"` when AMX hardware detected |
| Dispatch entries | `dispatch.go` | `dispatchTable["emerald"]` and `dispatchTable["granite"]` |
| Metrics | `dispatch.go` | `SimdStaticDispatchType.Set(4/5)` for emerald/granite |

### Build Usage

```bash
go build -tags emerald ./cmd/longbow        # Emerald Rapids
go build -tags granite ./cmd/longbow         # Granite Rapids
go build ./cmd/longbow                       # Falls back to runtime detection (avx2/avx512)
```

### Required AMX Assembly Kernels

| Kernel | AMX Instruction | File to Create | Priority | HW Required |
|--------|----------------|---------------|----------|-------------|
| `euclideanAMX` (float32 INT8) | `TDPBSSD` | `emerald_amd64.s` | High | Emerald Rapids |
| `dotAMX` (float32 INT8) | `TDPBSSD` | `emerald_amd64.s` | High | Emerald Rapids |
| `l2SquaredAMX` (float32 INT8) | `TDPBSSD` | `emerald_amd64.s` | High | Emerald Rapids |
| `matMulAMX` (float32) | `TDPBF16PS` | `emerald_amd64.s` | High | Emerald Rapids |
| `euclideanF16AMX` (float16) | `TDPFP16PS` | `granite_amd64.s` | Medium | Granite Rapids |
| `dotF16AMX` (float16) | `TDPFP16PS` | `granite_amd64.s` | Medium | Granite Rapids |
| `matMulF16AMX` (float16) | `TDPFP16PS` | `granite_amd64.s` | Medium | Granite Rapids |

### AMX Programming Model

AMX uses 8 tile registers (TMM0-TMM7), each configured via `TCONFIG` with row/col dimensions. Key operations:
1. **TDPBSSD**: Signed INT8 tile dot product → INT32 accumulator (HNSW integer distance)
2. **TDPBF16PS**: BF16 tile multiply → float32 accumulator (HNSW float distance)
3. **TDPFP16PS**: FP16 tile multiply → float32 accumulator (Granite Rapids, HNSW float16)
4. **TILECONFIG**: Configures tile dimensions before use
5. **TILELOADD/TILESTORED**: Load/store tile data

### Calling Convention

Each AMX kernel should:
1. Accept `uintptr` (not Go slices) to match the existing assembly convention
2. Configure tiles with `TILECONFIG` on entry
3. Process vectors in tile-sized batches (typically 16×16 or 16×32 for INT8)
4. Accumulate results in INT32 (TDPBSSD) or float32 (TDPBF16PS)
5. Issue `TILERELEASE` on exit

**Expected gain**: 2-5x over AVX-512 for INT8/BF16 dot products, 5-10x over AVX2.

---

## Priority 1: Geo Search SIMD Acceleration

**Problem**: Geo search is the slowest mode at ~25 QPS (P50=280ms, P99=700-800ms) across all configurations. It's the only mode that fails to meet interactive latency targets.

**Root cause**: Haversine distance in `internal/vector/geo_search.go` requires sin, cos, sqrt, and atan2 — all of which have existing SIMD primitives.

**Status**: `haversineBatchAVX2` implemented with inline float32 polynomial approximations (sin/cos/atan2), fused 13→4 passes, wired into AVX2 and AVX-512 dispatch. Still ~25 QPS; further optimization needs assembly-level transcendental kernels.

**Next**: Write AVX2 assembly kernels for `SinFloat32`, `CosFloat32`, `Atan2Float32` using `VEXTRACTPS`/`VCVTPS2PD`/`VSQRTPS` to avoid float32→float64 conversion overhead completely.

**Expected gain**: 2-4x (to ~50-100 QPS)

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

**Status**: `cosineFloat64AVX2` and `cosineFloat64AVX512Kernel` now wired into dispatch table (previously all float64 cosine used scalar unrolled4x).

**Options**:
- Map float64 → float32 with controlled precision loss at query time (optional, off by default)
- Implement AVX-512 path for float64 (512-bit = 8 float64 elements per instruction) — this requires AVX-512 hardware
- Pre-encode float64 vectors as float32 at ingest (user opt-in, trades precision for 2x throughput)

---

## Priority 4: Complete Benchmark at 1M Scale

**Problem**: Only 11 of 17 dim128 configs completed cleanly before complex64 hit `ResourceExhausted` at 16 GB. complex128 also OOM'd. Turboquant types not yet tested.

**Strategy**: Run in batches by memory profile with `--iouring --use-disk` flags for disk-backed vector storage:

| Batch | Configs | Est. Time | Notes |
|-------|---------|-----------|-------|
| A (small) | float32, float16, int8, uint8, turboquant2 | ~30 min | Fits in 16 GB |
| B (medium) | float64, int16, uint16, int32, uint32, turboquant4 | ~1 h | Fits in 16 GB |
| C (disk) | int64, uint64, complex64, complex128, turboquant, turboquant8 | ~2 h | Needs `--use-disk` for large types |

**Key findings from partial run**:
- float64 dim128: 8x slower than float32 for dense search (274 vs 2,188 QPS)
- int8 dim128: 1,126 QPS dense (comparable to float32)
- complex64/complex128: OOM at 16 GB, need disk-backed storage
- All non-Geo modes: P99 < 50ms for small types
- Geo: consistently ~25-32 QPS across all types

---

## Priority 5: LockFreeNeighborCache Hardening

**Problem**: The deadlock in `LockFreeNeighborCache.SetNeighbors` was fixed, but the cache still uses `sync.RWMutex` which can be a contention point under heavy parallel insert traffic.

**Status**: Fixed in commit `27a4df8b` — replaced `RLock→RUnlock→Lock` promotion with direct `Lock`. Further hardened in `020e8f57` — replaced `map[uint32]*LockFreeNeighborList` + `sync.RWMutex` with `sync.Map`, fixed `Clear()` data race.

---

## Priority 6: Filter Performance at Scale

**Problem**: Filtered search modes (filtered, filteredbool, filteredstring) show elevated P99 tail latency across all configs. For float64 dim128, FilteredBool reaches P99=124ms.

**Root cause**: All filter modes do a full metadata scan per query. At 1M vectors with string/bool attributes, this dominates query time.

**Status**: `StringContainsIndex` implemented (`string_contains_index.go`) with trigram inverted index for O(1) contains/prefix candidate generation. 9 tests pass with race detector.

**Next**: Wire `StringContainsIndex` into filtered search path, add bitmap indexes for bool columns.

---

## Priority 7: Benchmark Infrastructure Improvements

- **Auto-resume**: Add a `--resume` flag to skip completed configs (check for existing result JSON)
- **Memory profiling**: Add peak RSS tracking per test phase to `perf_matrix*.json`
- **Multi-run statistics**: Run each config 3x and report mean/stddev for QPS and latency
- **Heatmap generation**: Automatically generate a heatmap from the perf matrix showing QPS by dtype × dim
- **Swap detection**: Alert when the system starts swapping during a benchmark run
- **OOM resilience**: If a config hits `ResourceExhausted`, skip remaining large dtypes and move to next batch

---

## Priority 8: Fix `avx512` Build Tag

**Problem**: The `avx512` compile-time build tag (`go build -tags avx512`) is broken due to duplicate function declarations between `simd_amd64.go` (always compiled on amd64) and `avx512.go`/`turboquant_avx512_amd64.go` (compiled only with avx512 tag). The avx512 *runtime* path works (via `cpu_detection.go`), but the build tag path has been broken since the avo-generated assembly refactor.

**Fix**: Remove kernel function declarations from `simd_amd64.go` that duplicate those in `avx512.go`; either move them to `avx512.go` or conditionally compile with `!avx512` constraints.

**Impact**: Unblocks `-tags=avx512` and enables `-tags=emerald`/`-tags=granite` to correctly use AVX512 assembly kernels + AMX.

---

## Data Available

- 11 complete result JSON files (dim128, dtypes float32 through uint64) in `data/perf_logs/`
- Partial results for complex64 (OOM) and complex128 (OOM)
- 3 server binaries: `bin/longbow` (vanilla), `bin/longbow-emerald` (with emerald tag), `bin/longbow-granite` (with granite tag)
- Perf matrix: `data/perf_logs/perf_matrix_cpu_dim128_all_20260613_054412.json`
- 3 bench configs' server logs in `data/perf_logs/longbow_cpu_*.log`
