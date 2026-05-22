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

### P1 — High Impact

#### 1. Profile `uint16` High-Dimension Slab Allocator Slow Path
**Context**: Mid-run benchmarks showed `uint16 dim=1024 count=5000` ingestion dropped from 412K to 342K vec/s on Metal. This likely reflects `allocFast` failing for allocations above 4096 bytes (uint16 at dim=1024 requires 2048 bytes, but larger dims may still miss the fast path under contention).
**Task**: Profile the slab allocator specifically for `uint16` sizes above 2KB. Verify whether the lock-free `allocFast` CAS path is being hit or falling back to `allocCommon` mutex. Extend the lock-free threshold if needed.
**Files**: `internal/memory/arena.go`, `internal/memory/slab_pool.go`

#### 2. Validate Float16 & Float64 QPS Recovery After SIMD Fix
**Context**: The SIMD kernels for Float16 and Float64 are now fully wired. A targeted benchmark run is required to confirm the expected QPS recovery (Float16 Dense back toward 6000+, Float64 ByID back toward 8000+).
**Task**: Run the full benchmark matrix for `float16` and `float64` types at dim=128 on both local Metal and ancalagon. Compare against the v0.2.1 baseline in `docs/performance.md`.
**Files**: `scripts/unified_benchmark.py`, `docs/performance.md`

#### 3. pprof Collection Reliability
**Issue**: Benchmark script fails to collect pprof profiles (connection refused on metrics port 9470). Server shuts down before profile collection completes.
**Task**: Add a configurable delay between benchmark completion and server shutdown, or collect profiles mid-run via the HTTP endpoint during active benchmarking. Update `scripts/unified_benchmark.py`.
**Files**: `scripts/unified_benchmark.py`

### P2 — Medium Impact

#### 4. NUMA-Aware Benchmark Reporting
**Observation**: Logs show "Single NUMA node detected (no NUMA)" on localhost. On multi-socket machines like `ancalagon`, remote NUMA node access adds latency overhead that is not currently captured in benchmark output.
**Task**:
- Add NUMA topology detection to benchmark output (socket count, memory node layout).
- Benchmark with and without NUMA binding to quantify impact on high-dim types.
- Integrate `lbmem.MbindMemory` in the off-heap allocator to pin slab allocations to the executing CPU socket boundary.
**Files**: `internal/memory/numa_allocator.go`, `scripts/unified_benchmark.py`

#### 5. GPU Binary Distribution & Diagnostics
**Issue**: Metal and CUDA binaries require platform-specific builds. Fallback to CPU occurs silently when GPU binaries are missing.
**Task**:
- Add build-time GPU binary detection with a clear startup warning when GPU binary is absent.
- Build fat binaries for macOS (universal2 + Metal) via `lipo`.
- Document GPU binary build requirements in `README.md` and `docs/`.
**Files**: `gpu/detection.go`, `cmd/longbow/main.go`, `Makefile`

#### 6. Benchmark Matrix Optimization
**Current**: Full matrix (5 dims × 8 counts × 17 dtypes × 13 search modes × 3 hosts) = 26,520 combinations.
**Task**:
- Run full matrix only for release candidates.
- Define a representative CI subset: 3 dims × 3 counts × 5 dtypes × 5 modes.
- Add result caching for unchanged code paths.
**Files**: `scripts/unified_benchmark.py`

### P3 — Low Impact / Documentation

#### 7. Avo Generator Duplicate Symbol Detection Test
**Context**: Avo-generated stubs in `all_kernels_stubs_amd64.go` can duplicate manually declared stubs in other files, causing redeclaration errors on cross-compilation.
**Task**: Update `internal/simd/simd_stubs_test.go` to use `go/parser` and `go/ast` to detect duplicate function declarations across all files in `internal/simd`. Add this test to the CI gate.
**Files**: `internal/simd/simd_stubs_test.go`

#### 8. Memory Cap Hard Limit Documentation
**Current**: `LONGBOW_MAX_MEMORY` environment variable sets a soft limit with exponential backpressure.
**Task**: Add `LONGBOW_MAX_MEMORY_HARD` documentation to `README.md` and `docs/configuration.md`, clarifying the difference between soft and hard limits, the backpressure scaling behavior (5ms–100ms), and the `ResourceExhausted` gRPC response behavior.
**Files**: `docs/configuration.md`, `README.md`
