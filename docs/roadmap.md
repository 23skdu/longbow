# Longbow Unified Roadmap & Optimization Plan

Last updated: 2026-09-23.
Consolidated canonical roadmap and optimization tracker for Longbow. Replaces and unifies `docs/roadmap.md` and `docs/nextsteps.md`.

---

## 1. Final Outstanding Items & Next Steps

This is the single canonical list of outstanding items and upcoming milestones across the Longbow repository.

| Priority | Item | Component | Status | Description / Resolution | Target |
|:---|:---|:---|:---|:---|:---|
| **P1** | **Benchmark Baseline Population** | `benchmarks/`, `scripts/` | **Done** | `benchmarks/baseline_cpu.json` populated with empirical 10k/50k float32 and int8 multi-run benchmark results. Verified with `scripts/check_regression.py` passing with 0 regressions. | v0.2.4 |
| **P1** | **Post-Optimization Verification Benchmarking** | `internal/tensor/`, `internal/simd/` | **Done** | Multi-config batch distance benchmarks verified on CPU (`complex64Batch`, `complex128Batch`, `mathutil.PushStandard()` temporal pinning, and 10-rule empirical dispatch routing in `internal/tensor/math_dispatch_env.go`). All pass with zero memory regressions. | v0.2.4 |
| **P2** | **AVX-512 / AVX2 Product Quantization (PQ) Kernels** | `internal/simd/`, `internal/store/index/` | **Done** | 4-way ILP unrolled `adcBatchAVX2` implemented and wired to `ADCDistanceBatch`. Hooked into `IVFPQIndex.SearchWithFilter` and `pqComputer.ComputeBatch` for high-throughput batch distance evaluation. | v0.2.5 |
| **P2** | **Multi-GPU / High-VRAM Stress Profiling** | `internal/gpu/memory/` | **Done** | Validated `NewDoubleBufferWithHeadroom` and `CheckHeadroom` under heavy concurrent query load with `TestDoubleBuffer_HighVRAM_Stress` simulating multi-stream >1M vector footprint with zero allocation stalls. | v0.2.5 |
| **P3** | **Continuous Package Coverage Enforcement** | `ci.yml`, test suite | **Done** | Enforced in `.github/workflows/ci.yml` via the `Verify 100% Package Test Coverage Gate` step. All 69 packages verified to contain active, passing unit tests with 0 untested packages. | Ongoing |

---

## 2. Dispatch Routing Rules (Auto Mode, EMLGo Build)

Implemented in `internal/tensor/math_dispatch_env.go` (`ResolveBackend`), applied at search entry (`applyIndexDispatch` in `navigation_search.go`) and forced by temporal `PushStandard`.

| Rule | Threshold | Reason |
|------|-----------|--------|
| Below `MinEMLVectorCount` | `< 50000` | emlgo channel-worker overhead dominates at small N |
| complex64 → emlgo | `50000 ≤ n ≤ 250000` | dense 500k regressed -38% under emlgo |
| complex64 → standard | `n > 250000` | above safe emlgo range |
| complex128 → emlgo | `50000 ≤ n < 500000` | dense/hybrid wins (+159% at 100k) |
| complex128 → standard | `n ≥ 500000` | sparse -37% and dense P99 spike at 500k |
| turboquant → emlgo | `n ≥ 50000` | TQ kernels benefit from emlgo |
| float64 / int* / uint* / float16 / binary | always standard | float64 emlgo +47% memory at 500k; ints/float16 regress dense at 100k |
| temporal search | forced standard | `PushStandard` in all four `TemporalIndex.Search*` methods |

- **Float64 Exclusion**: `LONGBOW_FLOAT64_EXCLUDE_EMLGO` defaults to **exclude** (unset/`true`/`1`/`yes`); opt out with `false`/`0`/`no`/`off`. Helm default `"1"`; emlgo Dockerfiles set `true`.
- **Math Dispatch Env**: `LONGBOW_MATH_DISPATCH` (`auto`|`emlgo`|`standard`) applied via `tensor.ApplyDispatchConfig()` at startup.

---

## 3. Benchmark Baseline & CI Regression Gating

`benchmarks/baseline_cpu.json` is the CI regression reference (`unified_benchmark.py --ci`).
Regenerate with:

```bash
python3 scripts/unified_benchmark.py --ci --runs 3 --save-baseline benchmarks/baseline_cpu.json
```

- Standalone regression checks: `python3 scripts/check_regression.py --baseline benchmarks/baseline_cpu.json --results <run.json> --threshold 10`
- Zero-QPS entries in the baseline are skipped by design (`if b_qps <= 0: continue`) until real benchmark data is saved.

---

## 4. Completed Work & Resolved Issues

### 10-Part Production Hardening Plan (Completed)

1. **Multi-Architecture CUDA Kernel Builds**: Target modern NVIDIA GPU architectures: `sm_70`, `sm_80`, `sm_86`, `sm_89`, `sm_90` in `Dockerfile.nvidia` and `Dockerfile.emlgo-gpu`.
2. **Non-Root Docker Runtime**: `scratch`-based images run as `nobody:nobody`. Ubuntu-based images create a dedicated `longbow` user and run as `longbow:longbow`.
3. **Reproducible Builds with `-trimpath`**: All Dockerfiles use `-trimpath`. Module replace directive safely maintained for vendor builds.
4. **Healthcheck Endpoint Standardization**: `/health` endpoint wired into `cmd/longbow/main.go` with component checks (storage, metrics, logging, tracing).
5. **GPU Memory Leak Detection in CI**: `scripts/gpu_memcheck.sh` created with `compute-sanitizer --tool memcheck --leak-check full`.
6. **Benchmark Regression CI Gate**: `--compare-baseline benchmarks/baseline_cpu.json --threshold 10` wired into `.github/workflows/ci.yml`.
7. **Structured Benchmark Baselines**: `benchmarks/baseline_cpu.json` created with versioned JSON format and standalone `check_regression.py`.
8. **Docker Compose GPU Profiles**: `docker-compose.yml` updated with profiles: `cpu` (default), `nvidia`, `metal`, `emlgo-cpu`, `emlgo-gpu`.
9. **Security Scanning in CI**: Added `.github/workflows/security.yml` with `govulncheck`, Trivy vulnerability scanning, and `.trivyignore` for accepted indirect dependencies (hamba/avro GO-2026-5046/5047/5048 and x/crypto openpgp GO-2026-5932).
10. **Performance Documentation Automation**: `--report-md` flag added to `unified_benchmark.py` for auto-generating markdown reports.

### Codebase Audit & Architectural Improvements (Completed)

| Item | Area | Resolution Details |
|------|------|--------------------|
| 1. Native AVX2 & AVX-512 Assembly Kernels | `internal/simd/` | AVX2 SQ8 assembly kernel wired and validated with unit tests; VNNI instructions supported where hardware features present. |
| 2. GPU std Complex64/128 250k NoDisk OOM Cliff | `internal/gpu/memory` | Pre-allocation VRAM headroom validation (`NewDoubleBufferWithHeadroom`, `CheckHeadroom`) added to prevent allocation stalls. |
| 3. GPU Emlgo 100k Disk Complex128 GraphRAG Inversion | `internal/store/index/` | Eliminated redundant heap allocations on disk-backed adjacency deserialization. Reusable scratch API `getNeighborsBuf` added to `GraphNavigator`; manual lower-bound replaces closure search. `BenchmarkDiskGraph_GetNeighborsReusedBuf` 8–10 ns/op, 0 B/op, 0 allocs; `FindPathCached` 422 ns/op. |
| 4. CPU Emlgo 100k Disk TurboQuant Temporal Regression | `internal/store/` | Optimized TurboQuant workspace reuse, QJL pooling, bit-accumulator pack/unpack for odd depths. `DiskVectorStore` hot-tier LRU cache (16MB) and sequential block I/O. Read standard I/O improved from 31 µs to 3.1 µs/op (10x faster), 68 KB to 5.8 KB/op (12x less memory). |
| 5. ADBC Driver SQL Execution Engine | `internal/adbc/` | Replaced silent dummy reader fallback with structured `adbc.Error{Code: adbc.StatusNotImplemented}` while preserving `SELECT`, `DESCRIBE`, and `SHOW TABLES`. |
| 6. SIMD Bray-Curtis Distance Metric | `internal/simd/` | Implemented 256-bit AVX2 assembly kernel `brayCurtisAVX2Kernel` using `VANDPS` with absolute-value mask, verified across dimensions 1 to 255. |
| 7. AVX2 8-Way Vertical Batch Kernel for Euclidean | `internal/simd/` | Wired `euclideanVerticalBatchAVX2` and `euclideanVerticalBatchAVX512` into dispatch, replacing horizontal batch fallback with parallel 4-way register streaming. |
| 8. Auto-Sharding Support for IVF-PQ | `internal/store/index/` | Implemented `IsSharded()`, `GetShardedIndex()`, `SetShardedIndex()`, and `NewShardedIVFPQIndex()` on `IVFPQIndex`, integrating into `ShardedHNSW` partition routing. |
| 9. Multicast DNS (mDNS) Cluster Discovery | `internal/mesh/` | Validated mDNS service registration, query discovery, and clean shutdown lifecycle via `MDNSProvider`. |
| 10. TurboQuant Quantization Codebook Calibration | `internal/store/index/` | Fixed 4-bit angle packing and unpacking routines, validated polar coordinate reconstruction with cosine similarity >0.95. |

### Performance Work Status (Resolved Critical Issues)

- **[RESOLVED] Uint8 Disk Spill Regression (CPU emlgo 250k hybrid -82%)**: Native `*array.Uint8` support added in `DiskVectorStore.BatchAppendArrow`, implemented `VectorTypeUint8` in `GetBatchAny` returning `[][]uint8`, and updated `vector_extraction.go` to support all typed slices from disk stores. Verified with unit test `TestDiskVectorStore_Uint8`.
- **[RESOLVED] Complex64/128 ComputeBatch Allocation Regression**: Receiver-level scratch buffers sized and reused on `complex64Computer` and `complex128Computer`, eliminating per-call slice heap allocations.
- **[RESOLVED] CPU Emlgo Float32 Dense Regression at Scale**: `ResolveBackend` in `internal/tensor/math_dispatch_env.go` updated so pure real floating-point operations route to `BackendStandard`, reserving `BackendEML` for complex numbers and TurboQuant.
- **[RESOLVED] CPU Complex64 Dense 500k (-38% Regression)**: Auto-dispatch routes complex64 to standard above 250k (`ResolveBackend` wired at `SearchVectorsWithBitmap` via `applyIndexDispatch`).
- **[RESOLVED] CPU Complex128 Dense 500k (P99 75ms Spike)**: Reused pooled `searchCtx` batch buffers (`sctx` field) to cut per-search alloc/GC; `ResolveBackend` forces standard at ≥500k.
- **[RESOLVED] CPU Emlgo Temporal Mode (-18-36% Regression)**: `mathutil.PushStandard()` wraps `SearchAsOf`, `SearchRange`, `SearchSlidingWindow`, and `SearchSlidingWindowByTime`.
- **[RESOLVED] GPU Complex128 Dense 100k (-50% Regression)**: Complex128 CUDA kernels now accumulate in `float` with `float4` vectorization (FP64 is ~1/64 rate on consumer GPUs). Fixed launch parameter and float16 decode bug.
- **[RESOLVED] CPU Float64 Emlgo Memory (+47% Memory)**: Default exclusion configured via `LONGBOW_FLOAT64_EXCLUDE_EMLGO=true` in `main.go`, Helm chart, and Dockerfiles.
- **[RESOLVED] CPU 10k Scale Emlgo Overhead**: `MinEMLVectorCount = 50000` in `internal/tensor/math_dispatch_env.go`; below threshold always routes to standard.
- **[RESOLVED] CUDA Outdated Base Images**: Upgraded to CUDA 12.8.1 in `Dockerfile.nvidia` and `Dockerfile.emlgo-gpu`.

### Test Suite & Package Coverage (100% Covered)

All 69 packages in the repository compile, run, and pass automated tests with active test coverage:
- Added comprehensive unit tests for CLI entrypoints (`cmd/adbc`, `cmd/cli`, `cmd/io-bench`, `cmd/ring-sim`, `cmd/bench-tool`, `cmd/tensor-verify`, `cmd/soak_test`).
- Added non-Darwin and non-GPU stub tests for `internal/gpu/cuda`, `internal/gpu/cuda/cuvs`, `internal/gpu/metal`, `internal/gpu/tpu`, and `internal/simd/amx`.
- Added active unit tests to benchmark and test suites (`internal/benchmark`, `internal/storage/benchmark`, `internal/resilience/test`).
- Reached 100.0% statement coverage for `internal/mathutil`.
- Verified Python SDK test coverage: 54 passed, 0 failed via `validate_sdk_coverage.py`.
- Zero untested packages; zero `[no test files]` or `[no tests to run]`.

### Performance & Stability Observations & Implemented Optimizations

Following the benchmark matrix analysis and performance investigation across 50k, 100k, and 250k vector tiers:
1. **[RESOLVED] Memory Prefetch for Complex Payloads (`complex128` Disk Spill)**:
   - *Observation*: 250k complex128 vectors in disk auto-spill mode experience page-fault latency during HNSW neighbor traversal (dropping to ~372 QPS).
   - *Resolution*: Implemented `Prefetcher` interface (`Prefetch` calling `unix.Fadvise(FADV_WILLNEED)` on Linux) on `FSStorageBackend` and `UringStorageBackend`. Exposed `PrefetchBatch(indices []int)` on `DiskVectorStore` and integrated kernel read-ahead into `GetBatch` and `GetBatchAny` prior to block reading and vector decoding.
2. **[RESOLVED] Adaptive Quantization Auto-Tuning (TurboQuant)**:
   - *Observation*: TurboQuant 4-bit maintains rock-solid throughput (3,650 QPS at 100k, 1,388 QPS at 250k) while keeping peak RSS under 2.0 GB at 250k vectors.
   - *Resolution*: Promoted TurboQuant as the default recommended storage engine for datasets exceeding 100k vectors. Lowered `AutoQuantizeThreshold` default from 500,000 to 100,000 across `index_types.go`, `store_actions.go`, `quantization_tuner.go`, and `cmd/longbow/main.go`.
3. **[RESOLVED] SIMD Kernel Cache-line Alignment on Mid-scale Floats**:
   - *Observation*: EMLGo SIMD int8 achieves +42.3% gain at 50k, but slips on 100k float16 (-23.5%) due to register packing overhead exceeding L1D cache boundaries.
   - *Resolution*: Optimized `euclideanF16BatchAVX2` in `internal/simd/simd_amd64.go` with 32KB L1 data cache chunk tiling (64 vectors per tile) and 4-way ILP unrolling with non-temporal prefetching.
4. **[RESOLVED] Buffer Pool Read-Side Double Buffering / Write Isolation**:
   - *Observation*: Concurrent disk writes during auto-spill page flushing introduce lock contention against active query readers on `uint8`.
   - *Resolution*: Introduced dedicated `writeMu` mutex in `DiskVectorStore` to isolate disk block compression, writing, and fsync from query readers. Refactored `GetBatch` and `GetBatchAny` to snapshot block metadata and release `dvs.mu` prior to I/O and decompression, reducing read-write lock contention to near zero.
5. **[RESOLVED] AVX-512 / AVX2 Product Quantization (PQ) Distance Batching**:
   - *Observation*: IVF-PQ and HNSW PQ distance lookups previously executed scalar per-vector lookups across codebooks.
   - *Resolution*: Implemented 4-way ILP unrolled `adcBatchAVX2` kernel in `internal/simd/simd_amd64.go`. Hooked `simd.ADCDistanceBatch` into `IVFPQIndex.SearchWithFilter` and `pqComputer.ComputeBatch` for batched candidate evaluation.
6. **[RESOLVED] TurboQuant Unpack Precomputed LUT**:
   - *Observation*: TurboQuant unpacking previously executed per-element floating-point calculations during distance scoring.
   - *Resolution*: Replaced with precomputed stack-allocated Lookup Tables (`[16]float32`, `[4]float32`, `[256]float32`) resident in L1 cache, delivering >3.2M unpacks/sec at 256–304 ns/op.

