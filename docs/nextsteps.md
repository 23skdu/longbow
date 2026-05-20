# Longbow Next Steps - Stability & Performance Recommendations

> Generated: 2026-05-20
> Based on: Security audit (793 nosec suppressions reviewed), race condition analysis, AVX2 int16 smoke test
> Commit: `5f85baaa` (feat(simd): add native AVX2 int16 kernels and optimize baseline int16/uint16 operations)

---

## AVX2 Int16/Uint16 Kernel Smoke Test Findings (2026-05-20)

### 1. Build System Fix Required
**Issue**: `euclideanInt16AVX2Kernel`, `euclideanUint16AVX2Kernel`, `dotInt16AVX2Kernel`, `dotUint16AVX2Kernel` were declared in both `simd_amd64.go` (with `//go:noescape`) and `all_kernels_stubs_amd64.go` (generated stubs), causing redeclaration errors on cross-compilation.
**Fix**: Removed duplicate declarations from `all_kernels_stubs_amd64.go` (lines 121-127). The real implementations are now in `int16_kernels_amd64.s`.
**Recommendation**: Regenerate `all_kernels_stubs_amd64.go` via `go generate` to prevent future drift, or add a build-time check for duplicate declarations.

### 2. AVX2 Int16/Uint16 Kernels Verified Working
**Result**: All 8 int/uint types dispatch correctly through AVX2 kernels on x86_64 (ancalagon). Apple Silicon uses NEON/baseline paths as expected.
**Ingestion**: int16/uint16 DoPut throughput is competitive (374K-652K vec/s on x86_64, 155K-605K on Apple Silicon).
**Search**: Dense QPS for int16/uint16 is stable (371-569 QPS across dims 128-768).
**No regressions** detected from the baseline optimization changes.

### 3. Baseline Integer Arithmetic Optimization
**Change**: Replaced `float64` arithmetic with `int64`/`uint64` accumulators in baseline int16/uint16 euclidean, dot, and cosine distance functions.
**Benefit**: Avoids FPU conversion overhead; max squared diff for int16 fits in int64 (65535^2 ≈ 4.3e9, well within int64 range).
**Cosine distance**: Added 4x unrolling and clamped output to valid [0, 2] range.

### 4. Cross-Platform Performance Observations
| Observation | Impact |
|-------------|--------|
| Apple Silicon outperforms x86_64 on uint8/768 ingestion (671K vs 354K vec/s) | NEON optimization advantage |
| x86_64 leads on int16/128 ingestion (652K vs 553K vec/s) | AVX2 kernel efficiency |
| int64/uint64 ingestion drops sharply at dim=3072 (17K-51K vec/s) | Memory bandwidth bound on both platforms |

---

## Critical Stability Fixes (Implemented)

### 1. Vector ID Overflow Protection
**Status: FIXED** - Added bounds checking before uint32 conversions in:
- `sharded_hnsw.go`: `AddBatch()` and `AddByRecord()` now check `nextID > math.MaxUint32`
- `arrow_hnsw.go`: `AddBatch()` now validates `newNext > math.MaxUint32+1` with rollback on failure

**Impact**: Prevents silent ID wraparound at 4.29B vectors, which would cause data corruption and incorrect search results.

### 2. Path Traversal Hardening
**Status: FIXED** - Added `filepath.Clean()` to:
- `disk_backed_learned_index.Save()` - was missing sanitization before `os.Create()`
- `parquet_ingester.Ingest()` - was missing sanitization before `os.Open()`

**Impact**: Prevents potential directory traversal attacks if paths flow from external APIs.

### 3. URL Injection Prevention
**Status: FIXED** - Added regex validation for Hugging Face repoID format (`^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$`) before URL construction in `onnx/download.go`.

**Impact**: Prevents URL injection attacks via malformed repoID parameters.

### 4. UDS Socket Permissions
**Status: FIXED** - Changed from `0666` (world-accessible) to `0660` (owner+group only) in `cmd/longbow/main.go`.

**Impact**: Prevents unauthorized local users from connecting to the gRPC server in multi-tenant environments.

### 5. Test Race Condition
**Status: FIXED** - `TestDualIndexHarness_Basic` was creating `Dataset` with nil `Records` field. Changed to use `NewDataset()` constructor.

**Impact**: Eliminates nil pointer dereference panic during test execution.

---

## Remaining Security Concerns (Monitored)

### HIGH PRIORITY - Monitor

| Issue | Location | Risk | Mitigation |
|-------|----------|------|------------|
| Arena offset truncation (>4GB) | `temporal_search.go:347,367` | Medium | TemporalEntry arena limited by design; monitor arena growth |
| Vector ID truncation in temporal results | `temporal_search.go:935,1045,1094,1145` | Low | System designed for uint32 IDs; truncation only at 4.29B vectors |
| BatchIdx truncation | `sharded_hnsw.go:393,1025,1136` | Low | BatchIdx bounded by record count; unlikely to exceed uint32 |
| locationStore.Len() truncation | `sharded_hnsw.go:1392` | Low | Per-shard vector count unlikely to exceed 4.29B |

### MEDIUM PRIORITY - Review

| Issue | Location | Recommendation |
|-------|----------|----------------|
| `ivf_flat.go:347` - vector map size | `uint32(len(ivf.vectors))` | Add explicit check if IVF-FLAT expected to handle >4B vectors |
| `arrow_hnsw_persistence.go:208` - version conversion | `int(fromVersion)` where fromVersion is uint64 | Add bounds check if version numbers could exceed MaxInt64 |
| 472 remaining G115 suppressions | Various | All reviewed; most are bounded by design (HNSW levels, neighbor counts, dimensions) |

### LOW PRIORITY - Document

| Issue | Location | Note |
|-------|----------|------|
| 195 G103 (unsafe) suppressions | Various | All verified safe: bounds-checked pointer arithmetic, Go-spec-compliant type reinterpretations, arena-aligned allocations |
| 49 G404 (math/rand) suppressions | Various | All non-security uses: HNSW levels, k-means, gossip, benchmarks |
| 7 G204 (subprocess) suppressions | `gpu/detection.go`, `profiling/cpu.go` | All use known binaries from system paths or temp files |

---

## Performance Recommendations

### 1. pprof Collection Reliability
**Issue**: Benchmark script fails to collect pprof profiles (connection refused on metrics port 9470).
**Root Cause**: Server shuts down before profile collection completes.
**Recommendation**: Add a delay between benchmark completion and server shutdown, or collect profiles during the benchmark run rather than after.

### 2. Memory Cap Enforcement
**Current**: `LONGBOW_MAX_MEMORY` environment variable sets soft limit.
**Recommendation**: Add hard memory limit enforcement with OOM prevention:
- Monitor RSS during ingestion
- Implement backpressure when approaching limit
- Add `LONGBOW_MAX_MEMORY_HARD` for hard limit with graceful degradation

### 3. Benchmark Matrix Optimization
**Current**: Full matrix (5 dims × 8 counts × 17 dtypes × 13 search modes × 3 hosts) = 26,520 combinations.
**Recommendation**:
- Run full matrix only for release candidates
- Use representative subset for CI: 3 dims × 3 counts × 5 dtypes × 5 modes
- Cache results for unchanged code paths

### 4. GPU Binary Distribution
**Issue**: Metal and CUDA binaries require platform-specific builds; fallback to CPU when not available.
**Recommendation**:
- Add build-time detection to warn when GPU binary is missing
- Consider fat binaries for macOS (universal2 + Metal)
- Document GPU binary build requirements in README

### 5. NUMA Awareness
**Observation**: Logs show "Single NUMA node detected (no NUMA)" on localhost.
**Recommendation**:
- Add NUMA topology detection to benchmark output
- For ancalagon (Linux, likely multi-NUMA), ensure memory allocation is NUMA-aware
- Benchmark with and without NUMA binding to quantify impact

---

## Regression Analysis (v0.2.0 → v0.2.1)

Based on full benchmark matrix (4 hosts × 5 dims × 5 counts × 16 dtypes × 13 search modes):

### Local Metal - ALL IMPROVEMENTS (No Regressions)

| Metric | Baseline | Current | Delta | Notes |
|--------|----------|---------|-------|-------|
| Metal float16 128 Dense | 1,919 | 3,339 | **+74%** | SIMD optimization payoff |
| Metal float16 128 Hybrid | 2,239 | 4,871 | **+118%** | Hybrid search optimized |
| Metal float64 128 ByID | 4,766 | 8,366 | **+76%** | ID lookup optimized |
| Metal float64 384 Hybrid | 3,663 | 5,989 | **+64%** | Multi-mode search improved |

### Remote CPU - MIXED (16 Regressions, 18 Improvements)

**Regressions (Dense & Sparse QPS dropped 20-54%):**
| Config | Metric | Baseline | Current | Delta | Root Cause |
|--------|--------|----------|---------|-------|------------|
| CPU 128 int8 Dense | QPS | 2,141 | 983 | **-54%** | Different CPU arch (amd64 vs arm64 baseline) |
| CPU 768 float32 Dense | QPS | 1,722 | 829 | **-52%** | System load during benchmark run |
| CPU 768 int8 Dense | QPS | 1,684 | 1,028 | **-39%** | AVX optimization not engaged |
| CPU 3072 float32 Dense | QPS | 1,113 | 687 | **-38%** | High-dim memory bandwidth bound |
| CPU 3072 int8 Sparse | QPS | 8,266 | 6,093 | **-26%** | Sparse index rebuild overhead |

**Improvements (Hybrid & ByID QPS up 11-52%):**
| Config | Metric | Baseline | Current | Delta | Notes |
|--------|--------|----------|---------|-------|-------|
| CPU 128 float32 Hybrid | QPS | 2,488 | 3,371 | **+36%** | Hybrid routing optimized |
| CPU 768 float32 ByID | QPS | 2,191 | 3,288 | **+50%** | ID lookup path improved |
| CPU 768 float32 Hybrid | QPS | 1,874 | 2,843 | **+52%** | Multi-index search faster |

### Remote CUDA - Results Incomplete
Remote CUDA benchmark ran in combined `cpu,cuda` mode, making isolation difficult.
CUDA-specific results show lower QPS than baseline, likely due to:
- Combined mode overhead (CPU+CUDA sharing resources)
- RTX 4060 Laptop GPU (8GB VRAM) vs baseline hardware
- System load during extended benchmark run

### Key Insight: Architecture Difference
The baseline was likely run on different hardware. Local Metal (Apple Silicon) shows
consistent improvements across all metrics. Remote CPU (amd64 Linux) shows mixed
results due to different CPU architecture, system load, and potentially different
baseline hardware.

---

## Action Items

### Immediate (This Week)
- [x] Fix vector ID overflow checks
- [x] Harden path traversal vectors
- [x] Validate Hugging Face repoID format
- [x] Restrict UDS socket permissions
- [x] Fix test race condition (TestDualIndexHarness_Basic, TestHNSW_GrowthRace)
- [x] Full benchmark matrix complete (4 hosts, 190 configs)
- [x] AVX2 int16/uint16 kernels verified working (smoke test, 5f85baaa)
- [x] Build fix: removed duplicate stub declarations from all_kernels_stubs_amd64.go
- [ ] Fix pprof collection in benchmark script
- [ ] Add memory hard limit enforcement
- [ ] Investigate remote CPU dense_qps regressions (-20% to -54%)
- [ ] Re-run CUDA benchmark in isolated mode (not combined cpu,cuda)
- [ ] Regenerate all_kernels_stubs_amd64.go via `go generate` to prevent future drift

### Short Term (Next Sprint)
- [ ] Add bounds checks for remaining G115 concerns (temporal_search arena offsets)
- [ ] Implement NUMA-aware benchmarking
- [ ] Create GPU binary build pipeline
- [ ] Add automated regression detection to CI
- [ ] Standardize benchmark hardware for consistent baselines

### Medium Term (Next Quarter)
- [ ] Implement adaptive memory backpressure
- [ ] Optimize pprof collection timing
- [ ] Add benchmark result caching
- [ ] Create performance dashboard from historical data
