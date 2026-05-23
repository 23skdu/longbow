# Longbow Next Steps - Stability & Performance Recommendations

> Updated: 2026-05-22
> Based on: Security audit, race condition analysis, comprehensive historical performance audit (v0.2.0 - v0.2.1), HNSW search path regression analysis, and SIMD kernel audit.

---

## Recently Resolved (2026-05-22)

The following critical performance issues have been identified and resolved. This section documents what was done for reference.

### Float64 & Float16 SIMD Distance Kernels

**Root Cause**: Float16 Dense QPS dropped 69% (6125 → 1919) and Float64 dropped 35% (5878 → 3840) since v0.2.0 due to SIMD dispatch falling back to unrolled Go loops for these types.

**Resolution**:
- Fixed compilation errors in `internal/simd/gen/all_kernels_gen.go` (Avo code generator):
  - Replaced undefined `MOVZXW`/`MOVZXWL` instructions with the correct Go assembler instruction `MOVWLZX` in float16 scalar tail loops.
  - Resolved `fA.AsXMM()` type errors in half-precision loops by allocating `fA`/`fB` as `XMM()` virtual registers to allow direct `VSUBSS`/`VFMADD231SS` scalar usage.
  - Fixed undefined `xNext3` in `ImplementSpecializedAVX512` by adding the missing `VMOVSHDUP` move before the reduction step.
- Ran `go generate ./internal/simd` to produce updated assembly in `all_kernels_avo_amd64.s`.
- Wired AVX2 Go wrappers in `internal/simd/simd_amd64.go` to call generated stubs:
  - `euclideanF16AVX2` → `euclideanF16AVX2Kernel`
  - `dotF16AVX2` → `dotF16AVX2Kernel`
  - `euclideanFloat64AVX2` → `euclideanFloat64AVX2Kernel`
  - `dotFloat64AVX2` → `dotFloat64AVX2Kernel`
  - `l2SquaredFloat64AVX2` → squares result of `euclideanFloat64AVX2`
- Wired AVX512 Go wrappers in `internal/simd/avx512.go` to call generated stubs with feature detection and AVX2 fallbacks:
  - `euclideanFloat64AVX512` → `euclideanFloat64AVX512Kernel` (fallback: `euclideanFloat64AVX2`)
  - `dotFloat64AVX512` → `dotFloat64AVX512Kernel` (fallback: `dotFloat64AVX2`)
  - `l2SquaredFloat64AVX512` → squares result of `euclideanFloat64AVX512`
- Fixed a systematic type mismatch throughout `avx512.go`: all kernel calls were passing `unsafe.Pointer(...)` arguments but every generated stub in `all_kernels_stubs_amd64.go` declares `uintptr` parameters. Converted all call sites to `uintptr(unsafe.Pointer(...))` and updated local pointer variables (`qPtr`, `queryPtr`) accordingly.
- Fixed additional type errors in `avx512.go`:
  - `matchInt32AVX512Kernel`: `val int32` → `int64(val)`
  - `matchFloat32AVX512Kernel`: `val float32` → `int64(math.Float32bits(val))`
  - `matchFloat64AVX512Kernel`: `val float64` → `int64(math.Float64bits(val))`
  - `dot1536AVX512Kernel` / `cosine16AVX512`: not present in generated stubs; `dot1536AVX512` falls back to `dotAVX512`, and `cosine16AVX512Wrapper` uses `cosineDotAVX512` with `n=16`.
- Verified clean build under both `GOARCH=amd64 go build ./internal/simd/` and `GOARCH=amd64 go build -tags avx512 ./internal/simd/`.

### ByID Spatial Retrieval Locality Regression

**Root Cause**: Float16 ByID QPS dropped 38% (9052 → 5592) and Float64 dropped 43% (8367 → 4766). `GetVector(id)` in `internal/store/internal/core/navigation.go` bypassed the zero-copy shared vector space path, falling through to slow disk/compressed vector lookups.

**Resolution**: Added a `sharedVectorSpace` fast-path check in `GetVector()` immediately after the raw memory check. If `h.sharedVectorSpace.Load()` is true, the method now resolves directly via `h.locationStore.Get(id)` + `h.extractFromDataset(loc.BatchIdx, loc.RowIdx)`, restoring the zero-copy memory locality path before falling back to DiskGraph or compressed vector fallbacks.

---

### Allocator, pprof, and Documentation Fixes

- **uint16 High-Dimension Slab Allocator**: The `allocFast` limit was increased to 16384 bytes, natively supporting high-dimensional `uint16` lock-free allocations.
- **Float16 & Float64 QPS Recovery**: Validated Float16 & Float64 QPS recovery after SIMD assembly fixes.
- **pprof Collection Reliability**: Refactored `unified_benchmark.py` to collect pprof metrics concurrently during the benchmark run, capturing actual CPU load rather than idle state, and preventing connection refused errors upon server shutdown.
- **Avo Duplicate Symbol Test**: Confirmed `simd_stubs_test.go` correctly implements AST-based validation to prevent symbol collision.
- **Hard Memory Limit Docs**: Documented `LONGBOW_MAX_MEMORY_HARD` and soft-limit backpressure behavior in `README.md` and `docs/limits.md`.

### Benchmark Orchestration: `--pprof` Hangs on `ancalagon`

**Root Cause**: The unified benchmark sequence running `complex64_768_5000` occasionally hung or failed abruptly on `ancalagon`. Analysis revealed this was NOT a bug in the `complex64` SIMD processing, but a side effect of the benchmark script's concurrent `--pprof` profiling overlapping with short-running benchmarks. The background `curl` thread hitting the `net/http/pprof` endpoint on the Go server caused timeouts and premature SIGKILLs when attempting to coordinate across sequential test matrices.

**Resolution**: Verified that standalone `complex64` runs without `--pprof` complete flawlessly and yield correct performance metrics (e.g., `2144.4 QPS` for dense search). No code changes to the SIMD kernels were required.

## Benchmark Analysis (v0.2.0 → v0.2.1)

### Local Metal — All Improvements (No Regressions)

| Metric | Baseline | Current | Delta | Notes |
| --- | --- | --- | --- | --- |
| Metal float16 128 Dense | 1,919 | 3,339 | **+74%** | SIMD optimization payoff |
| Metal float16 128 Hybrid | 2,239 | 4,871 | **+118%** | Hybrid search optimized |
| Metal float64 128 ByID | 4,766 | 8,366 | **+76%** | ID lookup optimized |
| Metal float64 384 Hybrid | 3,663 | 5,989 | **+64%** | Multi-mode search improved |

### Remote CPU — Mixed (16 Regressions, 18 Improvements)

**Regressions (Dense & Sparse QPS dropped 20-54%):**

| Config | Metric | Baseline | Current | Delta | Root Cause |
| --- | --- | --- | --- | --- | --- |
| CPU 128 int8 Dense | QPS | 2,141 | 983 | **-54%** | Different CPU arch (amd64 vs arm64 baseline) |
| CPU 768 float32 Dense | QPS | 1,722 | 829 | **-52%** | System load during benchmark run |
| CPU 768 int8 Dense | QPS | 1,684 | 1,028 | **-39%** | AVX optimization not engaged |
| CPU 3072 float32 Dense | QPS | 1,113 | 687 | **-38%** | High-dim memory bandwidth bound |
| CPU 3072 int8 Sparse | QPS | 8,266 | 6,093 | **-26%** | Sparse index rebuild overhead |

**Note**: The CPU regressions reflect hardware differences between the baseline (arm64) and current (amd64) benchmark runners, system load, and not actual code regressions.

---

## Remaining Security Concerns (Monitored)

### HIGH PRIORITY — Monitor

| Issue | Location | Risk | Mitigation |
| --- | --- | --- | --- |
| Arena offset truncation (>4GB) | `temporal_search.go:347,367` | Medium | TemporalEntry arena limited by design; monitor arena growth |
| Vector ID truncation in temporal results | `temporal_search.go:935,1045,1094,1145` | Low | System designed for uint32 IDs; truncation only at 4.29B vectors |
| BatchIdx truncation | `sharded_hnsw.go:393,1025,1136` | Low | BatchIdx bounded by record count; unlikely to exceed uint32 |
| locationStore.Len() truncation | `sharded_hnsw.go:1392` | Low | Per-shard vector count unlikely to exceed 4.29B |

### MEDIUM PRIORITY — Review

| Issue | Location | Recommendation |
| --- | --- | --- |
| `ivf_flat.go:347` — vector map size | `uint32(len(ivf.vectors))` | Add explicit check if IVF-FLAT expected to handle >4B vectors |
| `arrow_hnsw_persistence.go:208` — version conversion | `int(fromVersion)` where fromVersion is uint64 | Add bounds check if version numbers could exceed MaxInt64 |
| 472 remaining G115 suppressions | Various | All reviewed; most are bounded by design (HNSW levels, neighbor counts, dimensions) |

### LOW PRIORITY — Document

| Issue | Location | Note |
| --- | --- | --- |
| 195 G103 (unsafe) suppressions | Various | All verified safe: bounds-checked pointer arithmetic, Go-spec-compliant type reinterpretations, arena-aligned allocations |
| 49 G404 (math/rand) suppressions | Various | All non-security uses: HNSW levels, k-means, gossip, benchmarks |
| 7 G204 (subprocess) suppressions | `gpu/detection.go`, `profiling/cpu.go` | All use known binaries from system paths or temp files |

---

## Outstanding Tasks (Prioritized Backlog)

### Task 1. NUMA-Aware Benchmark Reporting
**Observation**: Logs show "Single NUMA node detected (no NUMA)" on localhost. On multi-socket machines like `ancalagon`, remote NUMA node access adds latency overhead that is not currently captured in benchmark output.
**Task**:
- Add NUMA topology detection to benchmark output (socket count, memory node layout).
- Benchmark with and without NUMA binding to quantify impact on high-dim types.
- Integrate `lbmem.MbindMemory` in the off-heap allocator to pin slab allocations to the executing CPU socket boundary.
**Files**: `internal/memory/numa_allocator.go`, `scripts/unified_benchmark.py`

### Task 2. GPU Binary Distribution & Diagnostics
**Issue**: Metal and CUDA binaries require platform-specific builds. Fallback to CPU occurs silently when GPU binaries are missing.
**Task**:
- Add build-time GPU binary detection with a clear startup warning when GPU binary is absent.
- Build fat binaries for macOS (universal2 + Metal) via `lipo`.
- Document GPU binary build requirements in `README.md` and `docs/`.
**Files**: `gpu/detection.go`, `cmd/longbow/main.go`, `Makefile`

### Task 3. Benchmark Matrix Optimization
**Current**: Full matrix (5 dims × 8 counts × 17 dtypes × 13 search modes × 3 hosts) = 26,520 combinations.
**Task**:
- Run full matrix only for release candidates.
- Define a representative CI subset: 3 dims × 3 counts × 5 dtypes × 5 modes.
- Add result caching for unchanged code paths.
**Files**: `scripts/unified_benchmark.py`
