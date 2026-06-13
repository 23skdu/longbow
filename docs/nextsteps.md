# Recommended Next Steps (Updated 2026-06-13)

Based on the 1M-vector benchmark run (dim128+384, float32/float64, 1000 queries, all 13 search modes), deadlock fix in `LockFreeNeighborCache`, Geo SIMD Haversine, float64 cosine dispatch fix, sync.Map migration, string trigram index, and Emerald/Granite Rapids build tag infrastructure.

---

## Priority 0: Intel Emerald Rapids & Granite Rapids SIMD Support

**Problem**: Longbow has no AMX (Advanced Matrix Extensions) support for Intel Sapphire Rapids / Emerald Rapids / Granite Rapids CPUs. AMX provides tile-based matrix multiply instructions (`TDPBSSD`/`TDPBF16PS` for INT8/BF16 on Emerald Rapids, `TDPFP16PS` for FP16 on Granite Rapids) that can deliver 5-10x higher throughput than AVX-512 for matrix-heavy workloads like HNSW distance computation and matmul.

### ✅ Completed

| Component | Files | Description |
|-----------|-------|-------------|
| `emerald`/`granite` build tags | `emerald_amd64.go`, `emerald_stubs_amd64.go`, `granite_amd64.go`, `granite_stubs_amd64.go` | Compile-time gates for AMX kernels |
| CPU detection | `cpu_detection.go` | `HasAMX`, `HasAMXINT8`, `HasAMXBF16`, `HasAMXFP16`, `HasAMXCOMPLEX`; auto-selects `"emerald"` or `"granite"` |
| Dispatch tables | `dispatch.go` | `dispatchTable["emerald"]`, `dispatchTable["granite"]`, metrics `Set(4/5)` |
| Build verified | — | `go build -tags emerald ./...` and `-tags granite` pass on vhagar (Emerald Rapids HW) |
| Pushed to `main` | — | Commit `49aad114` and ancestors |

#### AVX-512 Assembly Bug Fixes (`transcendental_amd64.s`, `softmax_avx512_amd64.s`)

| Bug | Fix | Impact |
|-----|-----|--------|
| **Segfault in exp/log tail** | Save `CX` (dst ptr) to `R10` before mask shift count clobbered it; restore after | `TestExpSIMD`, `TestLogSIMD` now pass |
| **4-byte constants → 64-byte ZMM loads** | All scalar constants expanded to 64 bytes (16 copies) | No garbage bytes read; correct polynomial evaluation |
| **Reversed Horner order (exp)** | FMA chain now starts from `c5` (highest deg) → `c0`, not `c0` → `c5` | `exp(0)=1.0`, not `exp(0)=c5=0.001342` |
| **Same bugs in softmax** | Applied same constant + Horner fixes to `softmax_avx512_amd64.s` | Softmax assembly kernel structurally correct |

#### Function Redirects (Dispatch Fixes)

| Function | Old Behavior | New Behavior |
|----------|-------------|-------------|
| `sigmoidAVX512` | Called no-op `sigmoidAVX512Kernel` (empty `RET`) | Delegates to `sigmoidGeneric` |
| `softmaxAVX512` | Called broken `softmaxAVX512Kernel` (no horizontal max) | Delegates to `softmaxGeneric` |
| `DotProductFMA` | Called stub `dot32FMA` returning 0 | Routes to `dotAVX512` |
| `EuclideanDistanceFMA` | Called stub `l2Squared32FMA` returning 0 | Routes to `l2SquaredAVX512` |
| `CosineDistanceFMA` | Called stub `cosine32FMA` returning 0 | Routes to `cosineAVX512` |

### ☐ Remaining

| Task | Details |
|------|---------|
| Write AMX assembly kernels | `emerald_amd64.s`: `TDPBSSD` (INT8 dot/L2/cosine), `TDPBF16PS` (BF16 matmul). `granite_amd64.s`: `TDPFP16PS` (FP16 dot/L2/matmul) |
| Fix `TestSoftmaxSIMD` on vhagar | `softmaxAVX512 → softmaxGeneric` redirect works locally but test fails on vhagar — likely dispatch init ordering issue |
| Fix `avx512` build tag | Pre-existing breakage: duplicate declarations between `simd_amd64.go` (always amd64) and `avx512.go`/`turboquant_avx512_amd64.go` (only with `avx512` tag). Move decls behind `!avx512` constraint or into `avx512.go`. |

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

## Data Available

- 11 complete result JSON files (dim128, dtypes float32 through uint64) in `data/perf_logs/`
- Partial results for complex64 (OOM) and complex128 (OOM)
- 3 server binaries: `bin/longbow` (vanilla), `bin/longbow-emerald` (with emerald tag), `bin/longbow-granite` (with granite tag)
- Perf matrix: `data/perf_logs/perf_matrix_cpu_dim128_all_20260613_054412.json`
- 3 bench configs' server logs in `data/perf_logs/longbow_cpu_*.log`
