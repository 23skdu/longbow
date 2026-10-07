# Longbow Unified Roadmap & Optimization Plan

Last updated: 2026-10-05 (TurboQuant follow-up in §9).
Consolidated canonical roadmap and optimization tracker for Longbow. This document absorbed `docs/nextsteps.md`, which was removed in its entirety.

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

Merged from `docs/nextsteps.md`: that file's status summary listed the AVX-512/AVX2 PQ kernels and the Multi-GPU/High-VRAM stress profiling as **[Open]**, which was stale. Both are recorded as **Done** above and their resolutions are detailed in §4; the roadmap is the canonical source and the two entries are reconciled here rather than tracked in two places.

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
| float64 / int\* / uint\* / float16 / binary | always standard | float64 emlgo +47% memory at 500k; ints/float16 regress dense at 100k |
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
7. **[VALIDATED] Full 8-Variant Baseline Matrix (2,304 Metric Points)**:
   - *Observation*: Full 8-variant matrix executed across CPU and GPU builds (Standard & EMLGo), pure memory (`nodisk`) and auto-spill (`disk`) across all 16 data types, 100k & 250k vector counts, and all 9 search modalities (`dense`, `hybrid`, `sparse`, `filtered`, `byid`, `graphrag`, `geo`, `temporal`, `learned_index`). Peak throughput reached 3,644 QPS on GPU (`uint16` 100k sparse) and 3,611 QPS on CPU (`complex64` 250k sparse). Ingestion throughput achieved 385,000 to 603,742 vec/s.
   - *Status*: Baseline recorded in `benchmarks/baseline_matrix.json` and documented in `docs/performance.md`.
8. **[CONFIRMED] Auto-Spill Isolation & Throughput Gains**:
   - *Observation*: Auto-spill disk mode averaged +21.2% QPS improvement over in-memory mode in standard builds across 576 measurements, while bounding server RSS within the 60% memory threshold. The `writeMu` reader-writer separation and asynchronous flushing eliminate lock contention during background page writes.
9. **[CONFIRMED] Integer EMLGo SIMD Acceleration**:
   - *Observation*: EMLGo SIMD builds demonstrated massive throughput gains on integer vectors: `uint8` 100k reached 1,577 QPS (+95.5%), `uint16` 100k disk reached 1,515 QPS (+209.1%), and `uint64` 100k disk reached 2,067 QPS (+352.8%).
10. **[RESOLVED] Intel P-Core BD PROCHOT Hardware Throttling & Core Affinity**:
    - *Observation*: On hybrid architectures (such as Intel i7-12650H), Embedded Controller BD PROCHOT clamped P-cores (CPUs 0–11) to 485 MHz while E-cores (CPUs 12–15) ran unthrottled at 2.50 GHz (5.15x higher frequency). Default `GOMAXPROCS=16` scheduled 75% of Go worker threads onto the throttled cores, introducing severe barrier stalls across parallel SIMD loops and causing an apparent ~3x–8x benchmark throughput drop.
    - *Resolution*: Added CPU affinity management via `LONGBOW_CPU_AFFINITY` environment variable and `--cpu-affinity` CLI flags in `scripts/unified_benchmark.py` and `scripts/run_benchmark_full.sh`, pinning the Longbow server and `bench-tool` to unthrottled high-frequency cores (CPUs 12–15).
11. **[RESOLVED] Arrow Vector Type Metadata Preservation & Inadvertent TurboQuant Demotion**:
    - *Observation*: `cmd/bench-tool` previously created Arrow record batches for `float32` vectors without attaching schema metadata `longbow.vector_type`. An automatic promotion rule in `store_actions.go` promoted batches with missing metadata (`!hasMetadataType`) to `VectorTypeTQ` (4-bit TurboQuant), inadvertently subjecting float32 vectors to lossy 4-bit quantization and decompression on query hotpaths.
    - *Resolution*: Updated `cmd/bench-tool/main.go` to explicitly populate `longbow.vector_type` across all 16 supported data types in `generateRecord`.
12. **[RESOLVED] In-Memory Auto-Spill Threshold Boundary**:
    - *Observation*: `scripts/unified_benchmark.py` previously forced `LONGBOW_AUTO_SPILL_DISK="true"` whenever the dataset vector count reached 100k, forcing pure in-memory (`nodisk`) benchmarks to invoke disk auto-spill paging logic.
    - *Resolution*: Restored auto-spill threshold in `scripts/unified_benchmark.py` to 500,000 vectors, ensuring 100k and 250k pure memory benchmarks stay resident in RAM.
13. **[RESOLVED] Unconditional OTLP gRPC Exporter Retry Storms**:
    - *Observation*: `initTracer()` in `cmd/longbow/main.go` unconditionally initialized an active OpenTelemetry gRPC exporter targeting `localhost:4317`. When no OTLP collector was running, gRPC background connection retries failed every 5 seconds, contending for runtime threads and logging to stderr during benchmark runs.
    - *Resolution*: Made OTLP trace exporter initialization conditional on `OTEL_EXPORTER_OTLP_ENDPOINT` or `LONGBOW_TRACING_ENABLED=true`.
14. **[RESOLVED] Query Hotpath Logging Mutex Contention**:
    - *Observation*: Per-query `Info()` logging on `DoGet`, `SearchHybrid`, and `LearnedIndex` serialized concurrent query workers on Zerolog's standard output write lock.
    - *Resolution*: Demoted high-frequency per-query log events from `Info()` to `Debug()`, eliminating stdout mutex serialization across concurrent query workers.

### Roadmap Section 7 Steps Since Completed

1. **Index `VersionHistory` for batch temporal reads** — `VersionHistory` now publishes an immutable columnar snapshot (prefix-summed `entOff`, a per-version `ts` column, and a dense id->slot column) mirroring the layout from item 10, and looks up "active at t" with an upper-bound binary search instead of a reverse linear scan. Semantics are unchanged and pinned: active-at-t is the greatest `Timestamp <= t` (a lower bound would be wrong), ties resolve to the last-inserted version, and out-of-order timestamp inserts fall back to the literal reverse scan because for a non-monotonic group the old answer is not monotone in t. Error strings are preserved verbatim. **Measured**: 1.2-1.4x at 1 version/id, 2.4-7.7x at 10-100 versions/id; the binary search breaks even with the linear scan at ~2-4 versions/id and wins from 6-8. Decomposed at 100k ids: the per-id map lookup was ~16 ns/id and the linear scan 40-300 ns/id, so the scan was the real cost. End-to-end `SearchAsOf` on a fully live 100k corpus: **-13.8%** (12.2ms -> 10.5ms), with the history batch itself -33.0% and its share of the query falling 44% -> 34%. The snapshot rebuild is O(versions) and amortised over a 1/8 growth window under the read lock; it is the thing to revisit if `maxVersions` is ever raised by an order of magnitude.

2. **Close the markdown-lint and docs-drift gap** — `docs/**` now passes `markdownlint-cli2` with 0 issues under the workflow's own glob. A `.markdownlint-cli2.jsonc` relaxes only `MD013` (line-length: tables, ASCII diagrams and shell transcripts run to ~735 columns) and `MD060` (table-column-style: the docs mix `|---|---|` and `| --- | --- |` to match column widths), each with the reason recorded in the file; every other default rule stays enabled and no per-file suppressions were added. The 248 genuine defects were fixed in the markup. Separately, `scripts/generate_performance_and_roadmap.py` was emitting headings glued to lists in `docs/performance.md` and would have deleted roadmap sections 6 and 7 wholesale on any run; both are fixed at the source, and generation is now idempotent.

3. **Fix the negative-index guard on the roaring-backed `Bitset`** — `Set`, `Clear`, `Contains` and `Slice` all converted an `int` index to a roaring `uint32` without checking the sign, so `Set(-1)` set bit `4294967295` and `Contains(-1)` answered for it. All four now reject negatives, matching the convention `ArrowBitset` already used. The guards were confirmed by a regression test that fails without them, and the three `#nosec G115` annotations now carry the guard as their justification.

4. **Fix the AVX2 TurboQuant pack kernels (2-bit and 4-bit)** — `packTQ2AVX2Kernel` and `packTQ4AVX2Kernel` in `internal/simd/turboquant_amd64.s` carried the same three assembly defects as the 8-bit kernel fixed earlier: the constant-broadcast chain clobbering upper YMM lanes, a missing floor before integer conversion, and a narrowing stage that permuted packed codes out of element order. Both kernels now broadcast constants straight from memory, floor before converting (`VROUNDPS $1`), and assemble packed codes with `VPMADDUBSW`/`VPMADDWD`/`VPSHUFB` in element order. Scalar tails were rewritten with per-byte field counters. Pinned bit-exactly against `PackTQ2Generic`/`PackTQ4Generic` across 19 sizes and 7 input distributions in `turboquant_pack_amd64_test.go` (38 subtests fail without the fix).

5. **Make the SIMD generation reproducible and reviewable** — `go generate ./...` in `internal/simd` was previously destructive, dropping 28 hand-written kernels and colliding on 7 FMA stubs. Hand-written kernels moved to `internal/simd/kernels_manual_amd64.s`, duplicate stubs removed from `gen/all_kernels_gen.go`, and stale `gen/softmax_gen.go` unwired. `go generate ./...` is now a byte-for-byte no-op enforced in CI by `scripts/check_simd_generation.sh`. ARM64 compilation was also unblocked by moving AMD64 GEMM tests to `internal/tensor/coverage_amd64_test.go`.

## 5. Ten Concrete Steps to Improve Performance Across Data and Search Types

Based on empirical CPU, Heap, and Mutex pprof profile data collected during multi-scale benchmarking across all data types and search modalities, the following 10 optimization initiatives are prioritized. Each entry records the implemented outcome and the **measured** result on an Intel i7-12650H (16 vCPU, AVX2, no AVX-512); the original target is retained so the gap stays visible.

1. **4-Ary Flat SIMD Heap for HNSW Priority Queue** — **Done (mixed)**
   - **Empirical Finding**: `pprof` shows `MaxCandidateHeapAdapter.down`, `MinCandidateHeapAdapter.down`, and `Less/Swap` account for **15.2% of total search time** in `searchLayer`.
   - **Optimization**: `MinCandidateHeapAdapter`/`MaxCandidateHeapAdapter` (`internal/store/index/candidate_heap.go`) converted from binary to 4-ary heaps (parent `(j-1)/4`, children `4i+1..4i+4`), still flat, allocation-free, API-compatible.
   - **Measured**: end-to-end `BenchmarkInt8Search_50k` is neutral (113.7µs vs 115.2µs). The up-heavy result-set trim path improves 8-25% (ef=64: 34.2 vs 43.3 ns/elem); the pop-all drain regresses 5-15% because `down` trades height for width. No SIMD selection kernel was added — the ≤4-element `float32` scan is not worth vectorizing.
   - **Target Impact**: +12% to +18% QPS — **not demonstrated**; the 4-ary trade is a wash end-to-end.

2. **Lock-Free Striped Adjacency Updates for Parallel Ingestion** — **Already Done (variant)**
   - **Empirical Finding**: Mutex profiling reveals that `ArrowHNSW.AddConnectionsBatch` accounts for **56.9%** and `AddConnection` accounts for **25.1%** of lock delay during concurrent index ingestion.
   - **Status**: superseded. `AddConnection` (`internal/store/index/neighbor_ops.go:14`) tries the lock-free `PackedNeighbors` CAS path first and only falls back to a per-node CAS spinlock; `internal/store/index/lockfree_neighbors.go:120` provides copy-on-write neighbor lists; `packed_adjacency.go:54,86` uses 65,536 striped mutexes (not 64). Benchmarked by `BenchmarkHNSW_LockContention`, `BenchmarkNeighborAccess_LockFree*`, `BenchmarkLayer0Contention`.
   - **Target Impact**: 2.5x to 3.2x faster HNSW construction — met by the shipped superset.

3. **AVX-512 & 8-Way Unrolled ILP for Complex128 / Float64 Kernels** — **Done (AVX2 measured; AVX-512 unmeasurable here)**
   - **Empirical Finding**: `euclideanFloat64AVX2Kernel` consumes **42.6% of search time** for complex128 vectors.
   - **Optimization**: `euclideanFloat64AVX2Kernel` and `dotFloat64AVX2Kernel` (`internal/simd/gen/all_kernels_gen.go`) rewritten with 8 independent `VFMADD231PD` accumulators plus a scalar tail and a pairwise reduction tree. The AVX-512 float64 wrappers are now compiled on every amd64 build with a runtime `hasAVX512` guard instead of requiring `-tags avx512`, which was never set anywhere — previously the AVX-512 kernels were dead code.
   - **Measured**: 8-way ILP vs the old single-accumulator kernel is **2.6-6.0x** (`BenchmarkEuclideanFloat64_AVX2_8Way`); vs the Go scalar `Unrolled4x` reference, 3.7x at dim 384 and 3.9x at dim 768. AVX-512 speedup **not measured** — the host lacks AVX-512; correctness is pinned by `TestAVX512Float64RuntimeGuard`.
   - **Target Impact**: +85% to +120% — AVX2 path met; AVX-512 requires AVX-512 hardware to quantify.

4. **Thread-Local Metric Accumulators on Query Hotpaths** — **Done**
   - **Empirical Finding**: `prometheus.(*counter).Inc` and `prometheus.hashAdd` consume **3.43% of total CPU time** on search hotpaths due to atomic contention.
   - **Optimization**: `internal/metrics/sharded_counter.go` adds a 64-byte-padded, per-P `ShardedCounter` (drain via `Swap(0)`, so a concurrent `Add` is either drained or deferred — never lost, never double counted) with a 100ms async flusher. `internal/metrics/hotpath_counters.go` + `internal/store/index/hotpath_metrics.go` convert 14 hotpath counters, including the per-candidate `nodes_skipped` and `branch_prediction` increments. Label lookups are hoisted to package init / lazily-resolved per-dataset handles; a single refcounted flusher goroutine is started in `NewVectorStore` and stopped in `stopWorkers`.
   - **Measured**: inline `WithLabelValues().Inc()` 37-46ns → hoisted 6.3ns → hoisted+sharded 1.2ns at 16 goroutines (**~20x under contention**). `CounterVec.WithLabelValues` alone measured 100.1 ns/op. 5,524 increments per sparse-filtered search at 10k vectors. End-to-end the win is ~0.07% single-threaded (below noise on this box); the 16-way parallel A/B leans ~3% faster.
   - **Target Impact**: +3.5% QPS — per-increment cost removed; end-to-end effect is smaller than profiled.

5. **Direct Zero-Copy Arena Pointers in `GetWithGeneration`** — **Done**
   - **Empirical Finding**: `memory.(*SlabArena).GetWithGeneration` and `TypedArena.GetWithGeneration` consume **15.38% cumulative CPU time** during vector distance evaluations.
   - **Optimization**: `internal/memory/arena_batch.go` adds `SlabBatch`/`TypedBatch[T]`, resolving the slab table and generation policy once per batch and serving per-vector slices from a hot-slab cache. `internal/store/types/graph_data.go` exposes `VectorChunkBatch[T]`; `float32Computer`, `float64Computer` and `int8Computer` `ComputeBatch` now resolve per batch instead of per vector.
   - **Measured**: arena cost 4.12 → **3.16 ns/vector** at 1024 (−23%). `ComputeBatch` float32 −25%, float64 −30%, int8 −22%, allocs unchanged at 0. Parity fuzzing (`FuzzSlabBatch_Parity`, 604k execs) and generation-bump tests pin the visibility semantics.
   - **Target Impact**: +10% to +15% distance throughput — met on the batch loops; the per-vector `ComputeSingle` path (SIMD-dominated) is unaffected.

6. **SIMD Vectorized Bitmask Filtering for Int8 and Structured Predicates** — **Done**
   - **Empirical Finding**: `RoaringBitmap.Contains` dominates filtered searches at high predicate selectivity.
   - **Optimization**: `internal/store/index/filter_mask.go` converts the roaring filter to a dense `types.BitVector` **once per search** via roaring's own `WriteDenseTo` (a bulk `memmove` for bitmap containers, ~1900x faster than the BM25-style per-id iterator), and the 9 traversal probes in `search_float32.go`, `search_float64.go` and `distance_dispatch.go` use it. Falls back to roaring when `denseBytes > 256 KiB` or array+run container values exceed 65536.
   - **Measured**: per-candidate probe 1.4-25x faster (2.5-3 ns flat vs 8-74 ns). Interleaved A/B end-to-end: **1.05x at 90% selectivity to 1.42x at 10-50%** on 10k vectors; no configuration regresses. Conversion repays within ~200-3000 probes.
   - **Target Impact**: +25% to +40% QPS — partially met; the win is bounded by the ~200ns distance computation per candidate.

7. **Linear Spatial Morton Hash Grid to Replace Recursive Quadtree in Geo Search** — **Implemented, opt-in (not default)**
   - **Empirical Finding**: `store.(*Quadtree).subdivide` causes 2.83% of allocations; Geo search throughput is 368-1,223 QPS.
   - **Optimization**: `internal/store/morton_grid.go` implements a contiguous Z-order grid (64-bit Morton codes, 12-bit default resolution, open-addressed cell directory, no per-insert node allocation) behind the `GeoPointIndex` interface, selectable via `GeoIndexTypeMorton`. Parity with `Quadtree` is asserted over 4 resolutions x 3000 points x 400 queries.
   - **Measured** (pinned, min of 5, load 1.63): insert 150.5 vs 349.4 ns (**2.3x faster, 0 allocs**); `QueryBox/selective` 19,118 vs 16,399 ns (**17% slower**); `QueryBox/global` 365,997 vs 262,113 ns (**40% slower**); `SearchRadius` 650,498 vs 647,507 ns (**tied**). Cause: a fixed uniform grid has non-adaptive selectivity, whereas the quadtree subdivides to <=64 points/node. Resolutions 8-20 were swept; none flips the ordering.
   - **Decision**: the quadtree **remains the default**; the grid is opt-in for write-heavy datasets. A previous revision of this work had shipped the grid as the default, which would have been a silent query regression.
   - **Target Impact**: 3x to 5x higher Geo search QPS — **not met**; only the write path improved.

8. **Pre-Sized Zero-Allocation Buffer Pooling for Arrow IPC Responses** — **Done**
   - **Empirical Finding**: `bytes.growSlice` is 14.85% of memory profiling during DoGet streaming.
   - **Optimization**: arrow-go's `flight.NewRecordWriter` writes into an unpooled internal `bytes.Buffer`, so `internal/store/record_writer_pool.go` adds a `pooledFlightPayloadWriter` implementing `ipc.PayloadWriter` over `IPCBufferPool`, with `estimateIPCResponseBytes` pre-sizing from top-k, projection schema and measured row width. Wired into all 5 DoGet sites in `store_query.go` plus `vector_search_exchange.go`.
   - **Measured**: `BenchmarkDoGetResponseBuffer_Pooled` 13,152 ns/op, **0 B/op, 0 allocs** vs `Unpooled` 165,524 ns/op, 548,880 B/op. Full `flight` writer: 9,000 ns/op, 5,770 B/op, 47 allocs vs 47,899 ns/op, 159,368 B/op, 51 allocs (**~5x faster, 27x less memory**). Byte-identical output is asserted by `TestPooledFlightWriter_ByteIdenticalToStock`.
   - **Target Impact**: eliminates GC pressure on DoGet — met.

9. **Precomputed Polar Angle Look-Up Tables (LUT) for TurboQuant4** — **Done (+2 bugs fixed)**
   - **Optimization**: a single fixed-size `tqPolarLUT [1020]float32` covering bit depths 1-8 (`init()`-built, no lazy race) replaces per-element `math.Sincos` in `TurboQuantDistanceGeneric` (`internal/simd/turboquant.go`) and in the decoder's `polarReconstruct` (`internal/store/index/turboquant.go`). The decode table is built by feeding codes through the platform's own unpacker so the AVX2 FMA-vs-split rounding is preserved bit-exactly.
   - **Measured**: generic distance 2.1-3.0x; polar reconstruct 7.0-11.9x; end-to-end `Decode` 4.1-5.5x, allocs unchanged. `polarReconstructRecursive` (encode path) deliberately stays scalar — it consumes continuous `atan2` output that a code-indexed LUT cannot represent.
   - **Two pre-existing defects found and fixed**: `packTQ8AVX2Kernel` had three bugs (Go's assembler emitting `VMOVSS m32,Xn` with `VEX.L=1` zeroing the broadcast; a `VPERMPD 0xD8` lane-order error in the int32→uint8 narrowing; and round-half-up vs `VCVTPS2DQ`'s round-half-even) — 8-bit round-trip cosine went from **-0.1 to 0.995**. The 2-bit scratch fast path never wrote the 1-3 leftover codes of the always-odd `angleCount`, reading stale pooled memory (a cross-request leak).
   - **Supported range**: 4-8 bits meet the cosine > 0.90 contract; 1-3 bits are below the codec's accuracy floor by design.
   - **Target Impact**: +30% to +50% — exceeded on the decode path.

10. **Columnar Column-Oriented Skip-Lists for Temporal Search Modes** — **Done**
    - **Empirical Finding**: temporal search traverses interval trees with per-node branching latency.
    - **Optimization**: `internal/store/temporal_columnar.go` publishes an immutable columnar snapshot (`ts []int64`, `groupOff []uint32`, `ids []uint64`, `norms []float32`) via `atomic.Pointer`, with amortized rebuild on insert and an eager rebuild per `InsertBatch`. A hand-written AVX2 lower bound (`internal/store/temporal_colsort_amd64.s`, `VPCMPGTQ`/`VPTEST`) ships alongside scalar and unrolled kernels, but is **not** on the default path: it measured slower than the scalar kernel because a lower bound is bound by its dependent load chain, not comparison throughput.
    - **Measured**: `GetRange` **8.8-10.4x**, `GetUniqueIDsInRange` 2.2x, `GetUniqueLatest` 1.3x; end-to-end `SearchAsOf`/`SearchRange`/`SearchSlidingWindow` +10% to +110% with 60-80% fewer allocations. Binary search crosses over linear scan at n≈128-256. The remaining hot cost is `VersionHistory.GetVersionsAtBatch` (~96 ns/id at 100k), not the temporal filter.
    - **Target Impact**: +50% to +75% — met on the structure, partially at the search level.

11. **Adaptive `ef` Scaling for Filtered Graph Traversal** — **Done**
    - **Empirical Finding**: In `internal/store/index/navigation_search.go:358-444`, `efSearch` defaulted to `config.EfSearch` regardless of predicate selectivity. For selective predicates (e.g. 1-10% match rate), the first layer-0 walk returned $< k$ matching candidates, triggering up to 3 sequential retry loops and PID tuning updates. Moreover, for non-`[]float32` vector types (`[]float64`, `[]int8`, etc.), queries bypassed the retry loop, failed to find $k$ results under selective filters, and ignored `filterBitmap` when `filterMask` was nil.
    - **Optimization**: Selectivity is estimated at search entry via `filter.GetCardinality() / totalNodes` (or by sampling up to 64 nodes when an `HNSWPredicate` is supplied without a bitmap). Initial `efSearch` is scaled upfront via `clamp(int(math.Ceil(float64(k) / selectivity * 1.25)), baseEf, maxEf)` before entering layer 0, with `searchCtx.visitedNodesBudget` scaled proportionally. Unified candidate extraction (`extractSearchResults`) and retry loop across all vector types (`float32`, `float64`, `int8`, etc.), with idempotent metric flushing via `ctx.distComputeCount = 0` and PID tuner max limit querying (`PIDTuner.GetMaxEf`).
    - **Measured**: `BenchmarkAdaptiveEfScaling_SelectiveFilter` achieves **585 µs/query (~1,710 QPS)** on 5% selectivity across 2,000 vectors with 100% $k$-recall on attempt 0 without retries. Zero regressions across the full `internal/store/...` test suite under `-race`.
    - **Target Impact**: 2-3x lower search latency at $\le 10\%$ selectivity by eliminating retry traversals; 100% recall parity and consistent metric reporting for non-float32 filtered searches — met.

### Cross-Cutting Fixes Found Along The Way

- **Bitmap pool aliasing** (`internal/store/types/bitmap.go`, `internal/pool/bitmap_pool.go`): `Release()` returned bitmaps the `Bitset` did not own, so the pool could hand the same `*roaring.Bitmap` to two owners — two writers mutating one bitmap. Fixed with explicit ownership tracking; this was the true cause of the flaky `TestBitset_Slice` (3 failures in 10 under `-race`).
- **TurboQuant 8-bit pack kernel and 2-bit scratch tail**: see item 9.

### Known Open Issues

- **Resolved 2026-09-29:** `TestAddBatch_Bulk_Typed` recall flake. Root cause was three separate defects, none of them in the test:
  1. **Bulk insert left nodes unreachable.** Every node in a sub-batch searches the same frozen graph, so a fresh node's only inbound edges are reverse links handed to its pre-batch neighbours, and those are pruned the moment such a neighbour sits at its connection limit. On degenerate geometry (collinear points) every node picks the same neighbours, so whole sub-batches ended with in-degree zero. Measured: **99 of 512 nodes unreachable** from the entry point. The fix chain-links each node to its insertion-order predecessor at layer 0, an edge that both exists and survives pruning.
  2. **Neighbour pruning ignored distance order.** `computePrunedNeighbors` fed "existing links, then new ones" into a diversity heuristic defined over ascending distance, so the first entry won unconditionally and every later candidate was compared against it instead of against the query.
  3. **Type-blind neighbour selection.** The float32 kernel reads the float32 vector arena, which is empty for every other element type, so passing a non-float32 pool to it rejected every candidate and left a node with its single oldest link. Selection is now type-aware.

  A fourth, latent hazard is now guarded: `resolveDistanceKernel` validates a resolved SIMD kernel against the scalar reference once at index construction and falls back if they disagree, because a kernel that differences before widening (unsigned types) returns plausible distances that are off by orders of magnitude. `euclideanDistanceUint8` was added because the unsigned element types cannot be differenced in their own width — it wraps instead of going negative. Regression tests: `arrow_hnsw_bulk_connectivity_test.go` (fails without the fix, asserting both reachability and an exhaustive `ef = k = n` search), plus `TestAddBatch_Bulk_Typed` at 20/20.
- **Resolved 2026-09-29:** predicate-pruned HNSW traversal returning zero results. `search_float32.go`, `search_float64.go` and `distance_dispatch.go` applied the predicate to the traversal frontier as well as the result set, so the graph walk could not pass *through* non-matching nodes; a match reachable only via rejected nodes was unreachable, and a rejected entry point could return nothing. The fix gates only the result set. Measured filtered recall against an exact scan: **0.06 → 0.38**; the per-search short-result rate at 2/3 rejection went from 3/3 failing builds to 0/60. Regression tests in `internal/store/index/predicate_traversal_test.go` (deterministic connectivity, statistical result count, and brute-force recall).
- **Resolved 2026-09-29:** the arm64 TurboQuant 8-bit pack bug. The reported NEON narrowing defect was re-derived and found to be **correct** (`XTN2` writes the upper half of the destination, not a second register), so that claim was withdrawn. A different, real bug was found instead: the `VFMIN_V` macro encoded to the same word as `VFMAX_V` for the operands actually used, so the `norm > 1` clamp never ran and out-of-range angles wrapped. Fixed to the true FMIN base, verified against `llvm-mc`. `GOARCH=arm64` now also builds, which it did not before (AMX entry points had no non-amd64 definitions).

---

## 6. Security Scanning & Accepted Risks (merged from `docs/nextsteps.md`)

`.github/workflows/security.yml` runs `govulncheck` via `scripts/check_govuln.sh`, a Trivy filesystem scan, and a Trivy IaC scan. Both `check_govuln.sh` (exit 0 clean/allowlisted, 1 unexpected vuln, 2 tool missing) and `.trivyignore` fail on anything outside this allowlist.

Accepted indirect dependencies with no available upstream patch, tracked as accepted risk:

| Advisory | Package | Reason | Fixed in |
|:---|:---|:---|:---|
| `GO-2026-5046` / `CVE-2026-46385` | `hamba/avro` (via `pulsar-client-go`, temporal `avro.Freeze`) | CPU exhaustion in the Avro decoder | N/A |
| `GO-2026-5047` / `CVE-2026-46384` | `hamba/avro` | Integer overflow in the Avro decoder | N/A |
| `GO-2026-5048` / `GHSA-mx64-mj3q-7prj` | `hamba/avro` | Unbounded map allocation DoS | N/A |
| `GO-2026-5932` | `golang.org/x/crypto/openpgp` | Unmaintained / unsafe by design; module is required but never called | N/A |

These IDs are duplicated in `.trivyignore` and the `ALLOWLIST` array in `scripts/check_govuln.sh`; both now reference this section.

---

## 7. Next Ten Steps (Performance & Features)

Derived from the measurements in §5, the deep code analysis of the storage, indexing, SIMD, and clustering subsystems, and the open verification gaps. Each states the evidence it rests on, so the ordering can be re-checked when the numbers change.

1. **SIMD Vectorized Dequantization & Fallback Distance Loops**
   - **Empirical Finding**: In `internal/store/index/distance_dispatch.go:142-156` and `230-236`, when SQ8 quantization is active or when comparing unaligned int8/uint8 vectors against float query vectors, distance calculation executes scalar dequantization (`deq := minV + float32(v8[i])*scale; diff := val - deq; sum += diff*diff`) in element-by-element Go loops.
   - **Optimization**: Implement AVX2/NEON fused dequantize-and-L2 kernels: unpack uint8 to int16, widen to float32 using `VPMOVZXBD`, scale with `VFMADD213PS` / `VFMADD231PS`, accumulating into 4 parallel vector registers.
   - **Target Impact**: 4x-6x throughput increase on SQ8 and mixed-type candidate distance evaluation; eliminate scalar fallback bottlenecks in `searchLayer`.

2. **Zero-Copy Native Batch Decoding in `DiskVectorStore`**
   - **Empirical Finding**: Mutex and CPU profiling during auto-spill disk reads in `internal/store/disk_vector_store.go:530-534, 635-638, 685-687, 710-714` reveals that decompressed block vectors are parsed float-by-float and double-by-double using `binary.LittleEndian.Uint32` / `binary.LittleEndian.Uint64` / `float16.FromLEBytes` in nested loops over `dim`. For float32/float64/int8 on little-endian hardware (x86_64, ARM64), memory representation in the uncompressed buffer is already IEEE 754 contiguous.
   - **Optimization**: Replace scalar loops with zero-allocation pointer casts (`unsafe.Slice((*float32)(unsafe.Pointer(&raw[offset])), dim)`) or fast vectorized chunk copy (`copy(results[i], rawSlice)`). Pre-size and reuse worker output buffers from an internal pool to eliminate `make([]float32, dim)` allocations per vector.
   - **Target Impact**: 5x-8x faster vector extraction from decompressed disk blocks; reduce allocation overhead from $O(N \cdot \text{dim})$ to zero.

3. **Missing Type Support in `DiskVectorStore.GetBatchAny` and Bound Validation**
   - **Empirical Finding**: In `internal/store/disk_vector_store.go:614-720`, `GetBatchAny` only handles `float64`, `int8`, `uint8`, and `float16`. For `int16`, `uint16`, `int32`, `uint32`, `int64`, `uint64`, `complex64`, and `complex128`, it hits `default:`, which uses `elemSize = 4` and decodes as `[][]float32`, corrupting vector strides and returning invalid types. Additionally, `findBlock(idx)` in `disk_vector_store.go:395-408` does not validate `idx < block.StartIdx + block.NumVectors`, causing out-of-bounds indices beyond `totalCount` to alias the last block and trigger slicing panics.
   - **Optimization**: Implement typed branches for all 16 supported data types in `GetBatchAny` (matching `VectorType` enum), fix vector stride calculations, and guard `findBlock` against indices $\ge$ `totalCount`.
   - **Target Impact**: Prevent silent data corruption on disk reads for 11 data types; eliminate out-of-bounds index panics.

4. **Streaming Heap-Merge for Distributed Flight Scatter-Gather**
   - **Empirical Finding**: In `internal/sharding/stream_aggregator.go:124-200`, `StreamAggregator.Aggregate` receives $M$ pre-sorted streams from cluster shards, bundles all incoming batches into a global Arrow table, allocates an `indexItem` struct per row (`indices := make([]indexItem, 0, numRows)`), executes a full $O(N \log N)$ `sort.Slice` over the entire combined set, and reconstructs brand-new Arrow RecordBatches via dynamic reflection builders.
   - **Optimization**: Since each shard stream is already sorted by score/distance, implement an $M$-way $K$-sized min/max streaming tournament heap (Priority Queue) over incoming Arrow batch row readers. Emit the top-$K$ directly into pre-allocated Arrow arrays without flattening the entire multi-shard result set into memory.
   - **Target Impact**: Reduce scatter-gather memory consumption from $O(M \cdot K)$ to $O(K)$; speed up multi-shard query merge by 3x-5x on large clusters.

5. **Full-Spectrum Runtime AVX-512 CPUID Dispatch Elimination of Build Tags**
   - **Empirical Finding**: In `internal/simd/avx512.go` and `internal/simd/avx512_stubs_amd64.go`, all float32, float16, int8, int16, uint16, SQ8, and TurboQuant AVX-512 kernels are guarded by `//go:build amd64 && avx512`. Because `-tags avx512` is never passed in standard Go builds or releases, all non-float64 AVX-512 kernels remain completely dead code on every binary shipped, falling back to AVX2 even on AVX-512 capable processors.
   - **Optimization**: Follow the model established in `internal/simd/avx512_float64_amd64.go`: remove `!avx512` build constraints, compile AVX-512 assembly wrappers into all AMD64 builds, and guard execution at runtime with `features.HasAVX512` (and `features.HasAVX512VBMI` for TQ/VBMI).
   - **Target Impact**: Activate dormant AVX-512 kernels across float32, float16, and integer vector types on modern CPUs (+30% to +80% SIMD throughput without custom build tags).

6. **Fix and Test AVX-512 TurboQuant Pack Kernels**
   - **Empirical Finding**: `packTQ8AVX512Kernel`, `packTQ4AVX512Kernel`, and `packTQ2AVX512Kernel` in `internal/simd/turboquant_amd64.s` carry the same three assembly bugs previously fixed in AVX2: constant broadcasts clobbering upper ZMM lanes, missing `VROUNDPS $1` (floor) prior to `VCVTPS2DQ`, and lane-order permutations during int32->uint8/uint4/uint2 narrowing.
   - **Optimization**: Port the verified memory-direct broadcasting, pre-floor rounding, and element-order packing logic from the AVX2 kernels to AVX-512 (512-bit ZMM registers with AVX-512F / AVX-512BW / VBMI). Pin correctness with bit-exact unit tests mirroring `turboquant_pack_amd64_test.go`.
   - **Target Impact**: Bit-exact encoding parity between AVX-512 TurboQuant pack kernels and generic references across 2-bit, 4-bit, and 8-bit depths.

7. **Establish Emulated ARM64 and AVX-512 CI Validation Lanes**
   - **Empirical Finding**: ARM64 and AVX-512 tests currently cannot execute natively on standard GitHub Actions x86_64 runners without specialized tooling. While cross-compilation passes, assembly kernels in `turboquant_arm64.s`, `simd_arm64.s`, and AVX-512 assembly files remain unexecuted in automated testing.
   - **Optimization**: Add a GitHub Actions CI matrix job utilizing `docker/setup-qemu-action` or `qemu-user-static` for ARM64 test execution, and Intel SDE (Software Development Emulator) for AVX-512 / VBMI / AMX test validation.
   - **Target Impact**: 100% test execution coverage of non-AMD64 and AVX-512 assembly kernels in CI, preventing latent regressions.

8. **Resolve `LookupNeighbors` Type Incompleteness and ID Mapping**
   - **Empirical Finding**: In `internal/store/index/get_neighbors.go:96-115`, `arrowHNSWLookupNeighbors` computes neighbor distance only if the stored vector is `[]float32` (line 101); for all other vector types (`float64`, `int8`, `complex64`, etc.), distance is silently returned as `0.0`. Furthermore, line 110 populates `NeighborResult.ID` with the internal uint32 graph node index (`nbrID`) rather than translating it back to the external client `uint64` ID via `GetLocation` / batch records.
   - **Optimization**: Use the index's resolved distance computer (`h.distFuncAny` or `DistanceComputer`) to compute neighbor distances across all vector types. Add internal-to-external ID translation using the index location metadata so clients receive correct external IDs.
   - **Target Impact**: Correct distances and true external IDs for `LookupNeighbors` across all 16 supported data types.

9. **Adaptive Cell Subdivision for Morton Grid Spatial Index (or Deprecation)**
   - **Empirical Finding**: `MortonGrid` (`internal/store/morton_grid.go`) improves spatial insert latency (150.5 ns vs 349.4 ns, 2.3x faster, 0 allocs) but regresses query latency by 17% to 40% against `Quadtree` because its fixed uniform resolution causes excessive cell scanning and collision chain traversal in non-uniform geographic distributions.
   - **Optimization**: Either implement two-tier adaptive cell subdivision (fine Z-order buckets only when points per cell $> 64$, keeping flat slice storage) to match Quadtree search pruning, or formalize deprecation of `GeoIndexTypeMorton` as a query engine, restricting it to append-heavy staging workloads.
   - **Target Impact**: Bring Morton spatial query throughput within $\pm 5\%$ of Quadtree while retaining the 2.3x allocation-free insert throughput; or formalize documented deprecation to prevent query regressions.

10. **Lock-Free Atomic Generation Pointers for EntryPoint and Level Updates**
    - **Empirical Finding**: In `internal/store/index/arrow_hnsw_insert.go:412-430`, updating `entryPoint` and `maxLevel` during concurrent insertions requires taking `h.growMu.Lock()`, serializing concurrent batch writers even when inserting disjoint subgraphs. Mutex contention profiling during concurrent 50k batch ingestion shows up to 14% lock wait time on `growMu`.
    - **Optimization**: Convert `entryPoint` and `maxLevel` to atomic 64-bit combined CAS (`(maxLevel << 32) | entryPoint`) or lock-free atomic generational snapshots, allowing concurrent insertions to update higher-level entry points without acquiring the exclusive `growMu` write lock.
    - **Target Impact**: Eliminate writer lock contention on `growMu` during high-throughput parallel ingestion; +15% to +25% concurrent `AddBatch` ingestion throughput.

---

## 8. AVX2 Non-EMLGo Matrix Validation — Findings and Recommendations

Produced 2026-10-05 from a fresh `unified_benchmark.py` matrix: 50k / 250k / 500k vectors, 15 data types, all 13 search modes, `dim=128`, on both the CPU and CUDA **AVX2** builds with no `-tags emlgo`. Binaries were `bin/longbow_main` (`go build ./cmd/longbow`) and `bin/longbow-cuda_main` (`go build -tags gpu ./cmd/longbow`); `AVX2` was confirmed at runtime (CPUs 0-11 sit at 1.4-3.3 GHz under BD PROCHOT while CPUs 12-15 run unthrottled, so the harness was pinned with `--cpu-affinity 12-15`, matching the settings this document records in §4 item 10). Harness flags were `--queries 500 --workers 4` to match the `docs/performance.md` header.

Raw artefacts are under `data/perf_logs/` (`perf_matrix_{cpu,cuda}_avx2_*`), and the comparison tooling used to produce the numbers below is in `scratch/bench-runs/` (`parse_docs_baseline.py`, `compare_matrix.py`, `ab_server.sh`, `bisect_server.sh`, `ab_bulkpath.sh`).

### 8.1 Coverage

| Scale | Engine | Configs | Search points | Not completed, and why |
|---|---|---|---|---|
| 50k | CPU | 15 / 15 | 195 | — |
| 250k | CPU | 13 / 15 | 169 | `turboquant4`, `turboquant8` — see 8.5. (10 narrow dtypes were lost in the first pass to H1 and refilled.) |
| 500k | CPU | 13 / 15 | 169 | `turboquant4`, `turboquant8` — see 8.5. Needs a 14 GiB ceiling; see 8.6/H2. |
| 50k | GPU | 15 / 15 | 195 | — (4 narrow dtypes were lost in the first pass to H1 and refilled.) |
| 250k | GPU | 13 / 15 | 169 | `turboquant4`, `turboquant8` — see 8.5 |
| 500k | GPU | 3 / 15 | 39 | `float64`, `complex64`, `complex128` only; the 12 narrow dtypes were lost to H1 and the run was stopped. Same 14 GiB requirement as CPU. |

`docs/performance.md` contains **no 500k rows at all**, so the 500k tier is new data with no baseline and is reported as absolute numbers only. Note that 15 configurations surface as only 14 distinct `dtype` labels because of H5 (`turboquant4` and `turboquant8` both record as `turboquant`).

### 500k CPU, 14 GiB ceiling, `dim=128`, 500 queries x 4 workers (new data, no baseline)

| dtype | dense | hybrid | sparse | byid | geo | temporal | learnedindex | ingest (vec/s) |
|---|---|---|---|---|---|---|---|---|
| int8 | 3,933 | 3,742 | 6,428 | 4,387 | 32 | 15 | 3,496 | 352,665 |
| uint8 | 3,717 | 3,651 | 6,572 | 3,378 | 31 | 15 | 3,326 | 411,101 |
| int32 | 3,614 | 3,197 | 4,511 | 3,501 | 31 | 15 | 3,146 | 166,808 |
| uint64 | 3,097 | 2,999 | 6,223 | 3,389 | 30 | 15 | 2,844 | 86,025 |
| uint16 | 658 | 630 | 5,823 | 3,393 | 30 | 15 | 608 | 222,731 |
| int16 | 653 | 622 | 6,271 | 4,152 | 31 | 15 | 621 | 207,423 |
| float32 | 506 | 386 | 4,937 | 349 | 31 | 41 | 559 | 254,156 |
| float16 | 447 | 440 | 6,271 | 4,026 | 31 | 15 | 494 | 222,591 |
| complex64 | 371 | 366 | 6,500 | 298 | 29 | 41 | 394 | 121,408 |
| float64 | 345 | 301 | 6,538 | 215 | 32 | 40 | 365 | 105,265 |
| uint32 | 339 | 307 | 6,299 | 3,965 | 32 | 15 | 302 | 144,889 |
| int64 | 284 | 258 | 6,330 | 3,949 | 32 | 15 | 255 | 101,912 |
| complex128 | 242 | 271 | 6,326 | 215 | 27 | 38 | 201 | 58,774 |

Two things stand out and are worth a follow-up. First, `int16` and `uint16` sit 6x below `int8`/`uint8` at identical element counts and corpus sizes, which is not a property HNSW should have; `uint64` recovers to 3,097 QPS while `int64` drops to 284 QPS, so the spread is not monotonic in element width and is more likely a distance-kernel selection artefact than a memory-bandwidth one. Second, `geo` and `temporal` collapse to 27-41 QPS and 15 QPS respectively at 500k, three orders of magnitude below `sparse` — these two modes are the ones §8.4 and the §7 Morton-grid entry already flag, and 500k is where they stop being usable at all.

### 8.2 The `docs/performance.md` baseline cannot gate a regression

Diffing the fresh matrix against `docs/performance.md` at the ±10% threshold gives:

| Tier | Compared points | Regressions | Improvements |
|---|---|---|---|
| 50k CPU | 65 | 32 | 30 |
| 250k CPU | 129 | 37 | 77 |
| 250k GPU | 117 | 24 | 80 |

**None of those counts should be acted on as they stand**, because the baseline is not internally self-consistent: it is a merge of several benchmark invocations whose parameters were never recorded, and its own QPS and P50 columns contradict each other. The regression counts do cluster exactly where §8.3 predicts they should once the harness noise is removed, which is the encouraging part; the point is that the baseline cannot be trusted to *clear* a change either, because a recorded improvement may simply be a different worker count.

The check is arithmetic. With `--workers W`, the reported QPS cannot exceed `W / P50`. Taking `W = 4` as the document claims, `QPS x P50` must not exceed 4000:

| Baseline row | QPS | P50 (ms) | Implied concurrency |
|---|---|---|---|
| `250000 128 uint16 cuda byid` | 1,863.0 | 4.158 | **7.75** |
| `100000 128 int32 cpu dense` | 2,181.8 | 3.458 | **7.54** |
| `100000 128 int8 cpu sparse` | 2,742.5 | 2.817 | **7.73** |
| `250000 128 float64 cpu hybrid` | 281.3 | 26.980 | **7.59** |
| `50000 128 float32 cpu dense` | 2,935.0 | 1.241 | 3.64 (consistent with 4) |

408 of 806 rows (**50.6%**) imply more than 4.2 concurrent workers at their own stated P50 — that is, more than the documented 4. The implied values top out at 7.75, with a median of 4.38 and a 95th percentile of 7.09, and **none exceeds 8.5**, so the high group is consistent with `unified_benchmark.py`'s default `--workers 8` while the 50k tier is consistent with 4. The remaining 398 rows imply *fewer* than 4, which means their QPS and P50 columns disagree in the other direction and are equally unusable. `docs/testplan.md` §4 says "8 concurrency workers", the `docs/performance.md` header says "4 Workers (`--workers 4`)", and the script default is 8. A reader cannot tell which tier used which.

### Recommendations for the baseline

- **R1. Stop diffing against `docs/performance.md`.** Treat it as an informational record only. The authoritative regression signal is a revision-to-revision A/B on identical hardware, cores, harness flags and client binary — the method used for everything in 8.3.
- **R2. Regenerate the baseline as machine-readable, per-run artefacts** (`benchmarks/baseline_matrix.json` already has this shape) with the full parameter set recorded next to every number: binary revision, build tags, core affinity, worker count, query count, mode list, mode order, spill setting, and memory ceiling. One row per (revision, scale, dim, dtype, engine, mode).
- **R3. Make the baseline self-validating.** Add the `QPS x P50 <= workers x 1000` invariant as an assertion in the report writer, and refuse to emit a report that violates it. A mis-recorded `--workers` then fails the report at generation time instead of being discovered by a later audit.
- **R4. Reconcile `docs/testplan.md` §4 with the `docs/performance.md` header**, and state one worker count per tier. `docs/testplan.md` §3.2 also lists only 50k/100k/250k while §4, this document and the roadmap all target 500k; `docs/performance.md` has 1M rows and no 500k rows. Pick the tier list once.

### 8.3 [REGRESSION — CONFIRMED] The bulk-insert chain link costs up to 4.4x on dense search

This is the one regression that survived every attempt to explain it away, and it is code-caused, not environmental. Three independent methods agree on it: an end-to-end server A/B against the baseline revision, a server-level bisection across seven revisions, and a deterministic in-process micro-benchmark.

**Step 1 — the regression is real.** Same harness, same `bench-tool`, same cores (0-3), same flags, four interleaved runs per variant, HEAD server binary vs the `docs/performance.md`-baseline commit `2f4dc1c4` server binary, `float32` `dim=128` `n=50000`, medians:

| Mode | base `2f4dc1c4` | HEAD | Delta |
|---|---|---|---|
| filteredstring | 1,227.6 | 233.7 | **-81.0%** |
| filteredbool | 1,431.2 | 330.4 | **-76.9%** |
| learnedindex | 1,723.7 | 447.6 | **-74.0%** |
| filtered | 1,431.5 | 514.7 | **-64.0%** |
| dense | 1,802.2 | 704.4 | **-60.9%** |
| hybrid | 607.4 | 306.0 | -49.6% |
| graphrag | 1,147.6 | 596.3 | -48.0% |
| globalgraphrag | 1,040.6 | 561.6 | -46.0% |
| byid | 2,062.6 | 1,517.8 | -26.4% |
| temporal | 352.4 | 329.0 | -6.6% |
| geo | 245.5 | 296.6 | +20.8% |
| sparse | 3,828.3 | 4,810.9 | +25.7% |
| recommend | 327.5 | 448.6 | +37.0% |

Only the HNSW-traversal family regresses. `sparse`, `geo` and `recommend` — the modes that do not walk the graph — all improved.

**Step 2 — bisect.** One server binary per revision, three interleaved rounds, `--search-modes dense --queries 300`, medians:

| Revision | Median dense QPS | vs base | What it changed |
|---|---|---|---|
| `2f4dc1c4` (baseline) | 1,801.1 | — | — |
| `e145eb5c` | 1,680.5 | -6.7% | roadmap §5 items (4-ary heap, sharded counters, arena batching) |
| `7f872022` | 2,548.2 | +41.5% | gate filtered traversal on the result set, not the frontier |
| `a955a0c1` | 453.8 | **-74.8%** | **keep bulk-inserted nodes reachable** |
| `ee17b3b9` | 578.6 | -67.9% | adaptive `ef` scaling |
| `6a53fd7c` | 852.1 | -52.7% | AVX-512 kernels + temporal parser |
| HEAD | 783.2 | -56.5% | — |

The cliff is `a955a0c1`, and it is the only revision in the range that changes how the layer-0 graph is shaped.

**Step 3 — mechanism.** `a955a0c1` fixes a real bug: bulk insert used to leave nodes with in-degree zero, so `TestAddBatch_Bulk_Typed` failed. Its fix, in `internal/store/index/arrow_hnsw_bulk.go:addBatchBulkInternal`, unconditionally chain-links every layer-0 bulk-inserted node to its insertion-order predecessor in both directions, and reserves two of the node's degree slots for it:

```go
if lc == 0 && node.id > 0 {
    h.computeDistances(ctxLink, data, node.id-1, []uint32{node.id}, chainDist[:])
    _ = h.AddConnectionsBatch(ctxLink, data, node.id-1, []uint32{node.id}, chainDist[:], lc, int(h.mMax0.Load()))
    _ = h.AddConnectionsBatch(ctxLink, data, node.id, []uint32{node.id - 1}, chainDist[:], lc, int(h.mMax0.Load()))
}
...
const chainLinksPerNode = 2   // m = mMax0 - 2 for every layer-0 node
```

The commit's own comment justifies it as "always linked and adjacent in distance for sorted-ish data", and its regression test (`arrow_hnsw_bulk_connectivity_test.go`) uses **collinear** data, where node `i` and node `i-1` genuinely are nearest neighbours. That reasoning does not transfer:

- For unsorted input — which is what the benchmark generates and what any real embedding load looks like once rows are shuffled — the chain edge is a **long-range random shortcut**, not a proximity edge. Every node in the corpus gets one, so the layer-0 graph acquires 250k-500k arbitrary edges and loses the small-world structure that greedy HNSW descent depends on. More nodes are visited per query, which is exactly what the latency shows.
- `chainLinksPerNode = 2` is additionally charged against every node unconditionally, which at the harness's `MMax0 = 16` (`unified_benchmark.py` sets `LONGBOW_HNSW_MMAX0=16` for `count >= 50000`) is 12.5% of the layer-0 degree budget spent on edges that were not selected by distance. The regression test uses `MMax0 = 64`, where the same reservation is 3% and invisible. Step 5 shows this is *not* the dominant term, so it should not be mistaken for the fix.
- The cost is in the *index*, so it is paid by every query forever. It is not a one-off ingest cost.

**Step 4 — confirmation.** Same HEAD binary, only `LONGBOW_HNSW_BULK_INSERT_THRESHOLD` changed (so the bulk path is bypassed), six interleaved runs each, `float32 dim=128 n=50000 dense`:

| Insert path | Median dense QPS |
|---|---|
| bulk (`AddBatchBulk`, chain links active) | 626.7 |
| sequential (`AddBatch`) | **2,764.3 (+341.1%)** |

The non-bulk path at HEAD is also 53% *faster* than the base revision's bulk path (2,764 vs 1,801 QPS), so the fix can be had without giving anything up.

**Step 5 — it is the arbitrary edge, not the degree reservation.** `BenchmarkDenseSearch_Float32_50k` and `..._M32` in `internal/store/index/bench_float32_dense_test.go` pin the corpus, the HNSW parameters and the query set and force a single ingest worker, so the graph is deterministic and ns/op differences are attributable to the search path alone. On an otherwise idle host, min of 4 x 400 iterations:

| Revision | ns/op | vs base |
|---|---|---|
| `2f4dc1c4` (baseline) | 26,037 | — |
| `7f872022` | 29,613 | +13.7% |
| `a955a0c1` | 63,223 | **+142.9%** |
| HEAD | 57,216 | +119.7% |

That independently reproduces the server-level bisect through a completely different code path. Doubling the layer-0 degree budget at HEAD (`MMax0` 16 -> 32, min of 4 x 400) makes it **worse**, not better: 59,437 -> 64,205 ns/op. So the 2-slot reservation is not the cost. The entire regression is the navigation damage from an arbitrary long-range edge injected into every node.

### 8.3.1 R5 was attempted, reverted, and cannot land before R26

R5 - replace the unconditional chain link with a proximity-gated one - was
implemented, measured, and **reverted**. Recording this because the revert is the
finding, and because R5 as written is not implementable.

**What was tried.** Add the predecessor edge only when the predecessor is within the
node's own candidate median, and charge the two-slot `chainLinksPerNode` reservation
only on nodes that actually received an edge:

```go
chainDist <= candidates[len(candidates)/2].Dist   // candidates sorted ascending
```

This is O(1), adds no locking, and does exactly what §8.3 asked: on shuffled input it
stops injecting an arbitrary long-range edge into every layer-0 node. The collinear
test still passed, because there the predecessor genuinely is the nearest neighbour
and the gate admits it.

**Why it was reverted.** It broke graph connectivity, badly, and the graph-quality
gate added in §9.2 caught it:

| Reachable at layer 0, n=20,000 | Unconditional | Proximity-gated |
|---|---|---|
| turboquant 4-bit | 98.2% | **81.8%** |
| turboquant 8-bit | 97.9% | **86.8%** |

That refutes the premise behind R5. The roadmap assumed the chain edge was "the one
neighbour guaranteed to be linked", a fallback used only in the degenerate case. On
unordered input the predecessor is usually far, so the gate rejects nearly every
chain edge - and 18% of the TurboQuant corpus immediately became unreachable. The
edge is not a rare fallback; on unordered input it is where most inbound edges come
from, because a fresh node's only other inbound edges are reverse links that get
pruned whenever the target is already at capacity.

**Ordering consequence.** R5 cannot land before R26. The correct sequence is:

1. **R26** - guarantee inbound edges properly: when every reverse link for a fresh
   node was pruned, force an edge from the node's nearest pre-batch neighbour,
   evicting that neighbour's farthest edge. Until this exists there is nothing safe
   to replace the chain with.
2. **R5** - *then* gate the chain edge on proximity, which by then is the redundant
   safety net rather than the load-bearing structure.

**The -60.9% dense QPS figure in §8.3 could not be reproduced.** Interleaved A/B of
the gate against the unconditional edge, min of 3 x 300 iterations on
`BenchmarkDenseSearch_Float32_50k`:

| Round | Unconditional | Gated |
|---|---|---|
| 1 | 86,453 ns/op | 84,770 ns/op |
| 2 | 79,426 ns/op | 91,280 ns/op |
| 3 | 73,863 ns/op | 74,256 ns/op |

No difference beyond run-to-run noise, and neither arm reproduces the 57,216 ns/op
recorded for HEAD in Step 5. Recall was tried as the measurement over five corpus
seeds:

| Seed | 1 | 2 | 3 | 4 | 5 | mean |
|---|---|---|---|---|---|---|
| unconditional | 0.110 | 0.175 | 0.120 | 0.175 | 0.105 | 0.137 |
| gated | 0.165 | 0.160 | 0.100 | 0.170 | 0.115 | 0.142 |

The sign flips between seeds; a single-seed run read as a 50% improvement, which was
an artifact. So while the *connectivity* regression above is unambiguous and large,
a *query-throughput* benefit of gating is not demonstrated, and the harness cannot
currently resolve one (§9.3). §8.3's QPS table should be treated as unverified.

- **R5. NOT DONE - blocked on R26.** Implemented and reverted; the measured
  connectivity regression is in the table above.
- **R6. NOT DONE.** `chainLinksPerNode` is still charged unconditionally against
  every layer-0 node. It was coupled to the gate, so it reverted with it. Note this
  is the part that is genuinely safe to drop on its own, since §8.3 Step 5 showed the
  reservation is not the regression - but it currently protects the edge that R26
  still depends on, so dropping it alone is not obviously safe either.
- **R7. DONE.** `arrow_hnsw_bulk_chainlink_test.go` covers unordered geometry -
  `TestBulkInsert_UnorderedCorpusStaysReachable` on both shuffled and natural-order
  corpora (99.9% reachable on both, asserted at a 99% floor) and
  `TestBulkInsert_UnorderedCorpusRecallFloored`. The file documents the reverted gate
  and its measured effect, so a future attempt fails here instead of in production.
- **R26. Force an inbound edge when reverse links are all pruned - ATTEMPTED AND
  REVERTED, see 8.3.2.**
- **R27. Re-verify §8.3's server-level QPS table before acting on it.** The Step 2
  bisection put the cliff at `a955a0c1` and three methods agreed, but the in-process
  benchmark cannot reproduce the magnitude today. Either it was partly a property of
  the machine state when measured, or the benchmark does not exercise the same ingest
  shape as the server. That distinction matters: if it is the latter, then the fix's
  benefit is invisible to the harness meant to gate it.

### 8.3.2 R26 was attempted, and it is not achievable the way it was specified

R26 - "force an inbound edge from the node's nearest pre-batch neighbour when all
its reverse links were pruned" - was implemented two ways and both were reverted. It
is the prerequisite for R5, so this closes off the obvious approach and says what
the actual fix has to look like.

**Attempt 1 - inline, where the reverse links are written.** Instrumented: of
19,488 checks, **18,652 found the edge already present**, only 11 appended and 825
evicted - and the graph still finished **22% unreachable**. The edges are real when
they are written and gone by the end.

**Attempt 2 - a repair pass at the end of the bulk insert, where the graph is
quiescent.** Correct in principle, and it does repair nodes: with the chain link
disabled it forced 814 and 936 edges across sub-batches. Reachability with the chain
off went from **78.1% to 78.7%** - no better, because:

- the pass runs per `AddBatch`, and **the next `AddBatch` prunes the forced edges
  away**. Only a few hundred nodes are stranded per pass, all of them get fixed, and
  the fix does not survive to the end of ingest;
- the pass costs an O(nodes x probe) list read on every `AddBatch` regardless.

**Why the chain link survives and a forced edge does not.** This is the useful part.
A forced edge is hosted on the node's *nearest* neighbour, which is a long-lived,
heavily-connected node that receives many competing insertions and is pruned
constantly. The chain edge is hosted between two nodes inside the *same sub-batch*,
and a fresh node sees far less contention, so the edge it holds is not immediately
displaced. Durability here is a function of how contested the host node is, not of
whether the edge is distance-selected.

**What this rules out.** Any fix that repairs reachability after the fact, during a
continuing bulk insert, is not going to hold. A candidate that lands must be one of:

- **A pruning rule that never drops a node's last inbound edge.** Requires tracking
  in-degree globally, and a read of it inside the pruning path. This is the
  principled fix and it is what R26 should have said.
- **DiskANN's `keep_pruned_connections`.** Pruned edges are not discarded but demoted
  to a secondary list, so no edge is ever actually lost and a later pass can
  re-promote them. Larger change; adds a second traversal structure per node.
- **Host repair edges on nodes inside the current sub-batch** rather than on popular
  old neighbours. Cheapest, and closest to what the chain link already does, but it
  reintroduces the arbitrary edge that R5 exists to remove, so it only helps if the
  host is chosen by distance among the sub-batch.

- **R26. REFORMULATED - "never drop a node's last inbound edge".** The attempts above
  are evidence that the post-hoc framing does not work, not that the guarantee is
  unwanted. Kept open with the three candidate mechanisms above.
- **R28. Measure how much contention each candidate host sees.** The difference
  between a repair that holds and one that does not was node age and sub-batch
  membership, neither of which is visible in the code today. Before choosing a
  mechanism, count insertions per host node over a bulk insert, because that number
  predicts durability and would tell us whether R5 is achievable at all.

### Recommendations for the bulk-insert chain link

- **R5. Replace the unconditional chain link with a proximity-gated one. [ATTEMPTED AND REVERTED - blocked on R26, see 8.3.1]** Only add the `i -> i-1` edge when the two nodes are actually near each other (for example when the chain distance is within the candidate pool's median distance), and otherwise guarantee reachability the way HNSW normally does: by relaxing pruning for reverse links, or by re-checking that the node has at least one inbound edge after selection and retrying with a relaxed heuristic. The invariant the test needs is "no node is stranded", not "every node has a predecessor edge".
- **R6. Drop `chainLinksPerNode` once R5 lands. [NOT DONE - reverted with R5, see 8.3.1]** The degree reservation is currently charged unconditionally, but it is *not* the regression (Step 5: doubling `MMax0` makes the regression slightly worse, not better), so it should not be treated as the fix. Remove it together with the unconditional edge rather than tuning it, and re-measure.
- **R7. Make the connectivity regression test cover unsorted data. [DONE, see 8.3.1]** `TestBulkInsert_CollinearGraphStaysConnected` only exercises the geometry where the chain edge is a good edge. Add a shuffled-corpus variant that asserts (a) zero unreachable nodes and (b) recall and node-visit count within a stated factor of the sequential-insert graph. Without (b) the test cannot catch this class of regression, because reachability alone is satisfied by the chain.
- **R8. Guard `AddBatchBulk` behind a recall-and-visit-count budget** at dataset build time, the way `resolveDistanceKernel` already validates a SIMD kernel once at construction: build the bulk graph, compare recall and mean nodes-visited against the sequential path on a sample, and fall back if the bulk graph is materially worse. This makes the choice data-driven instead of a constant threshold.
- **R9. Re-baseline TurboQuant's "recommended at scale" claim.** §4 item 2 of this document promotes TurboQuant as the default engine above 100k vectors on the strength of 100k/250k QPS. In this matrix TurboQuant has the worst dense-search numbers of any dtype at 50k on CPU — `turboquant4` at `dense` 278 / `hybrid` 258 / `recommend` 261 QPS and `turboquant8` at 502 / 488 / 499 QPS, against `float32` at 702 / 645 / 859 and `complex128` at 2,720 / 2,066 / 3,424 — because it is the most sensitive to the chain link — a 4-bit codebook discards the distance information that would otherwise make a wrong edge recoverable. Its ingest advantage is real; its query advantage at scale is not established and should be re-measured after R5.

### 8.4 [NOT A REGRESSION] `temporal` needs a harness fix, not a code fix

`temporal` is the worst-looking row in the whole matrix — 23 regressions, worst `-93.5%` (`250000 128 float16 cpu temporal`, 390.5 -> 25.3 QPS) and a uniform `-71%` to `-92%` across every dtype at 250k on both engines. It is also **not** a code regression: the base-vs-HEAD A/B in 8.3 puts it at **-6.6%**, i.e. within noise. What moved is the harness and the mode ordering.

- `cmd/bench-tool/main.go:BuildSpecialTicket` stamps `"timestamp": time.Now().UnixNano()` into every `as_of` ticket. `TemporalIndex.SearchAsOf` keys its result cache on `fmt.Sprintf("asof:%d:%d", timestamp, k)`, so **every query is a cache miss and every query inserts a new cache entry** — 500 uncached searches plus 500 LRU inserts per config, and `TemporalResultCache.Get`/`Set` share one mutex across all workers.
- The 13-mode order is `Dense, Hybrid, Filtered, FilteredBool, FilteredString, Sparse, ByID, GraphRAG, GlobalGraphRAG, Recommend, Geo, Temporal, LearnedIndex`. The `docs/performance.md` 250k rows were collected from a 9-mode run in which `Temporal` ran 8th. `Temporal` now runs 12th, immediately after `Geo` — the slowest mode in the set — so it inherits `Geo`'s cache and GC state.
- The 5-minute `searchCtx` in `cmd/bench-tool/main.go:539` is shared by all 13 modes. It did not bite at these scales (the worst config summed to 44 s of search time), but at 500k and above `Temporal` alone is projected at 60 s+, so the later modes will start silently truncating. This is a latent failure that will be mistaken for a regression.

### Recommendations for the temporal harness

- **R10. Give each search mode its own context budget** in `bench-tool`, derived per mode, instead of one 5-minute budget for all 13. Record a per-mode `context_deadline_exceeded` flag in the JSON so a truncated mode can never be reported as a low QPS.
- **R11. Make the temporal ticket deterministic.** Use a fixed timestamp (or a small set drawn from the corpus) instead of `time.Now().UnixNano()`, and add an explicit cache-hit/miss count to the temporal result JSON. Without that, the mode measures the cache-miss path even when the cache exists to be hit.
- **R12. Freeze the mode list and its order in the baseline**, and record it per row. A baseline collected from 9 modes cannot be compared against a 13-mode run: the modes before the one under test change its cache state.

### 8.5 [OBSERVATION] TurboQuant at 250k/500k is not viable on this engine

TurboQuant is the one dtype that failed to index at every tier above 50k:

| Tier / engine | `turboquant4` | `turboquant8` | Reference: same tier, other dtypes |
|---|---|---|---|
| 250k CPU | did not index inside 45 min | not attempted | 60.5 s (`int8`), 105.0 s (`float32`), 144.0 s (`complex128`) |
| 250k GPU | failed | failed | 13 / 15 dtypes indexed normally |
| 500k CPU | did not index inside 96 min | not attempted | 129.5 s (`int8`) to 266.5 s (`uint64`) at a 14 GiB ceiling |

The bulk path charges one chain-link distance computation plus two `AddConnectionsBatch` calls per node on top of TurboQuant's own quantization work, and a 4- or 8-bit polar codebook gives the neighbour selector the least information with which to recover from a wrong edge. That is a third independent reason to land R5 before promoting TurboQuant, and it means §4 item 2's recommendation needs re-measuring end to end — ingest throughput and query throughput point in opposite directions here.

### 8.6 Harness defects found while running the matrix

| # | Defect | Location | Effect |
|---|---|---|---|
| H1 | `pkill -9 -x bench-tool` (and `longbow`, `longbow-cli`, ...) runs on **every** `start_server`, killing processes belonging to other benchmark invocations | `scripts/unified_benchmark.py:504` | Two concurrent runs are mutually destructive. This is what lost 10/15 configs at 250k CPU, 4/15 at 50k GPU and 12/15 at 500k GPU in the first pass — the in-flight client was SIGKILLed mid-search and reported as `FAILED` with an empty error. Any parallelism in the matrix must go through one invocation. |
| H2 | The server memory ceiling is taken from `$LONGBOW_MAX_MEMORY` and defaults to 18 GiB; `--memory` is never read | `scripts/unified_benchmark.py:271` (`limit_gb = os.environ.get("LONGBOW_MAX_MEMORY", str(self.args.memory))`) vs the 18 GiB literal in `start_server` | On this 22 GiB host the documented `--memory 10GB` knob has no effect at all, and no setting of `--memory` would help because it is ignored. The probed envelope at 500k on CPU is narrow: **8 GiB is refused** by admission control with `ResourceExhausted` on all 13 non-TurboQuant dtypes; **12 GiB admits the narrow dtypes** (`int8` indexes in 138.5 s) **but refuses `complex128`**; **14 GiB admits everything** (`int8` 140.0 s, `complex128` 196.0 s); the harness default of **18 GiB is above the safe ceiling** and the client is SIGKILLed mid-indexing with no diagnostic. `docs/testplan.md` §4 asks for 16 GB, which is inside the danger zone on a 22 GiB host. |
| H9 | `_save_checkpoint` is a no-op when `self.results` is empty | `scripts/unified_benchmark.py:283` | A run in which every config is `ResourceExhausted` writes **no artefact at all** — not even a record of the exhaustion. The 8 GiB probe produced a 9,194-line log and zero result files. |
| H10 | Per-config results live only in memory until the checkpoint is written, and the checkpoint file name carries the run timestamp | `_save_checkpoint`, `output_file` | Resuming a tier requires `--resume` inside the *same* invocation; `--resume` on a fresh invocation finds no `output_file` and starts over. An interrupted multi-hour tier cannot be resumed. |
| H3 | `int(self.args.memory)` on the string default `"10GB"` | `_check_memory_limit` | `--estimate-memory` raises `ValueError` whenever `LONGBOW_MAX_MEMORY` is unset. |
| H4 | `self.args.memory // (1024**3)` on the same string | Markdown report writer | `--report-md` raises `TypeError`. |
| H5 | `turboquant4` and `turboquant8` are both rewritten to dtype `turboquant` before the result is recorded | `scripts/unified_benchmark.py:run_benchmark` | The two bit depths are indistinguishable in the results JSON, so half the TurboQuant matrix cannot be attributed. Every "turboquant" row in this matrix is two configurations. |
| H6 | `ByID` always builds `"id":"0"` | `cmd/bench-tool/main.go:BuildSpecialTicket` | Measures one permanently hot node instead of the mode. `byid` showed a 7x swing between runs (596 to 4,348 QPS) for this reason. |
| H7 | `GenerateRecord` seeds from `time.Now().UnixNano()` per chunk | `cmd/bench-tool/main.go` | No two runs see the same corpus, so ingestion and index shape are not comparable across runs. Combined with H6, this is why per-mode variance reaches 40%+. |
| H8 | Silent `FAILED` with no diagnostic on client death | harness failure path | A SIGKILLed client is indistinguishable from a timeout in the results. Record the child's exit signal. |

### Recommendations for the harness defects

- **R13. Scope the process cleanup to the run.** Pass `--server-pid`/an explicit process handle, or match on the server binary path rather than the bare process name, so a run only reaps what it started.
- **R14. Make `--memory` authoritative.** Parse the size string once, use it in `start_server`, and derive the spill threshold from it. Fix H3 and H4 in the same change; all three are the same missing parser.
- **R14a. Derive the default ceiling from the host, not from a literal.** `start_server` should default to a fraction of detected physical RAM (the auto-spill threshold then has something to work against) instead of the 18 GiB constant, and it should refuse to start a tier whose estimate cannot fit alongside the measured free memory rather than discovering it via an OOM kill.
- **R14b. Write the checkpoint even when there are no results** (H9), and name the output file from the label rather than the timestamp so a tier can be resumed across invocations (H10).
- **R15. Record `tq_bits` in the result config** so `turboquant4` and `turboquant8` are separate rows, and key the comparison on it.
- **R16. Make `ByID` and the corpus generator deterministic** — a seed flag, drawn from a fixed seed for the corpus and derived from the query index for `ByID`. Repeatability is a precondition for a 10% regression gate; today it is not met.
- **R17. Surface the child exit signal** in the failure record so a killed client is distinguishable from a timeout or a server crash.

### 8.7 Recommended order of work

1. **R5/R6/R7** — the bulk-insert chain link. This is the only confirmed product regression in the matrix, it is worth up to 4.4x on every HNSW-family query at every dtype and scale, and it is currently baked into every published baseline.
2. **R1/R2/R3** — make the baseline trustworthy. Until the baseline records its parameters and asserts `QPS x P50 <= workers x 1000`, no regression count in this document can be acted on.
3. **R13/R14** — remove the two harness defects that silently destroy matrix data (H1) and disable the documented memory knob (H2).
4. **R10/R11/R12** — fix the temporal harness so the mode's numbers mean something, then re-measure.
5. **R15/R16/R17** — remove the remaining sources of non-repeatability.
6. **R8/R9** — re-baseline TurboQuant at scale once the insert path is fixed.
7. **Make the `resolveDistanceKernel` fallback observable, then chase the integer-type spread** surfaced by the 500k table in §8.1: `int16`/`uint16` sit 6x below `int8`/`uint8` at identical element count and corpus size, and `uint64` sits 11x above `int64`. `resolveDistanceKernel` validates each resolved SIMD kernel against the scalar reference once at construction and silently falls back when they disagree, which would produce exactly this shape of spread — but that is a hypothesis, not a measurement, and it is testable in minutes: log or metric the fallback (`internal/store/index/distance_resolvers.go:33`) and re-run one 500k config. This is worth doing first because the gate is invisible today, and an invisible kernel fallback is precisely the failure mode it was added to prevent.

---

## 9. TurboQuant: Query Path Improved, Construction Cost Explained

Follow-up to §8, after the matrix showed TurboQuant as the only dtype that never
finished indexing above 50k (§8.5). §9.1 landed a query-path improvement; §9.2
explains the construction cost, which turns out to be correctness rather than a
regression - and which invalidates the TurboQuant numbers published before
`a955a0c1`.

### 9.1 What was fixed, and measured

The TurboQuant search path resolved a TurboQuant code slice from scratch **per
candidate, three times over**: once in the inline prefetch-touch loop in
`distance_dispatch.go`, once in `tqComputer.Prefetch`, and once again in
`tqComputer.ComputeBatch`, which looped over `ComputeSingle`. Each resolution is
an atomic slab-table load, a division, a slab pointer chase and a generation
comparison. Profiling a 250k TurboQuant build (`longbow_main`, pprof over the
metrics port) showed the distance computation was only ~3% of CPU while the
lookups around it were ~43%.

| Change | File |
|---|---|
| `GraphData.BeginTQChunkBatch` opens a batch-scoped view of the TurboQuant chunk table, mirroring `BeginFloat32ChunkBatch` | `internal/store/types/graph_data.go` |
| `VectorChunkBatch.Width()` exposes the packed byte stride | `internal/store/types/graph_data.go` |
| `TurboQuantCompute.DistanceDirectCodes` scores an already-resolved code slice | `internal/store/index/arrow_hnsw_compute_tq.go` |
| `tqComputer.ComputeBatch` resolves the chunk once per search instead of once per candidate, falling back to `ComputeSingle` for any id the batch will not serve | `internal/store/index/distance_computer.go` |
| `tqComputer.Prefetch` uses a 16-entry chunk cache instead of a lookup per call | `internal/store/index/distance_computer.go` |
| Deleted a dead per-candidate float32 chunk lookup whose result was discarded | `internal/store/index/distance_dispatch.go` |
| Hoisted `GraphData.PackedSize()` out of the per-candidate loop | `internal/store/index/distance_dispatch.go` |
| The TQ batch view is owned by the computer (built per search) instead of being reopened per candidate block and per graph hop | `internal/store/index/distance_dispatch.go` |
| QJL sign selection is arithmetic instead of a data-dependent branch, bit-exact because `correction * -1` is an exact IEEE negation (2,905 -> 2,721 ns/op at dim=768, bits=4, -6%) | `internal/simd/turboquant.go` |

Effect on the 250k TurboQuant build profile, before → after:

| Symbol | Before | After |
|---|---|---|
| `SlabArena.GetWithGeneration` | 13.2% | **1.3%** |
| `tqComputer.Prefetch` | 26.0% | **12.9%** |
| `BeginTQChunkBatch` + `newVectorChunkBatch` + `BeginBatch` | 12.1% (introduced, then hoisted away) | gone |
| `GraphData.PackedSize` | 5.2% | gone |
| `turboQuantDistanceAVX2Scratch` | 2.5% | 2.2% |

**What the evidence is, and is not.** The profile deltas above are the evidence
for the batching: they come from the 250k server build, where the arena is large
enough for the slab-table load and pointer chase to miss cache. The in-process
`BenchmarkTQComputeBatch` does *not* show the win — at 20k vectors the whole
TurboQuant arena is one slab, so the lookup is an L1 hit either way and the
batched and per-candidate variants land within noise (33.2-36.1 us vs 34.0 us for
64 candidates). Anyone reading that benchmark as confirmation of the batching would
be wrong; it is there to pin the distance-function floor (about 470 ns per
candidate) and the prefetch cost (about 5 ns per call with the cache), both of
which are chunk-local.

**Correctness**: `TestTQComputeBatch_MatchesPerCandidate` asserts the batched
loop is bit-identical to the per-candidate reference for 4-bit and 8-bit, across
chunk boundaries and past the resident range. `./internal/simd`,
`./internal/store/index`, `./internal/store/types`, `./internal/memory` and
`./internal/store` all pass; `golangci-lint` reports 0 issues.

**What is left in the distance function.** After the above, the remaining
TurboQuant distance cost is the scalar polar reconstruction - `pow2-1` pairs of
two table lookups, two multiplies and two interleaved stores - at 47% of
`turboQuantDistanceAVX2Scratch`, plus the angle unpack at 14%. The AVX2 path only
vectorises the final 128-float L2. Closing the reconstruction needs an AVX2 kernel
with a shuffle-based gather from the 16-entry (4-bit) lookup table, which is real
assembly work and is **not** done here. It is the next lever on TurboQuant query
throughput, and it is independent of both the bulk-insert stall below and the
chain-link regression in §8.3.

### 9.2 The TurboQuant construction cost is CORRECTNESS, not a regression

**This supersedes the earlier claim in this section that the TurboQuant stall was
a ~9x regression.** That claim was wrong. The cost is real and large, but it is
`a955a0c1` building TurboQuant graphs correctly for the first time, and the
baseline it was measured against was building broken ones.

**Bisection.** `TestBisectTQBuild` (in `internal/store/index/zz_bisect_tq_test.go`)
grows one index through the store's real ingest shape - 25 record batches of
10,000 into 250,000 - and reports the `turboquant/float32` ratio. float32 and
turboquant run in the same process, so the ratio is drift-free even though the
absolute times move between runs.

| Revision | float32 | turboquant4 | ratio | f32 last batch | tq last batch |
|---|---|---|---|---|---|
| `2f4dc1c4` (docs baseline) | 30.0 s | **15.8 s** | 0.53 | 1.370 s | 0.651 s |
| `e145eb5c` | 33.1 s | 15.6 s | 0.47 | 1.508 s | 0.636 s |
| `7f872022` | 33.7 s | 15.9 s | 0.47 | 1.513 s | 0.663 s |
| **`a955a0c1`** | 34.2 s | **129.1 s** | **3.77** | 1.706 s | **9.981 s** |
| `ee17b3b9` | 36.0 s | 140.4 s | 3.90 | 1.694 s | 11.178 s |
| `6a53fd7c` | 36.3 s | 143.7 s | 3.96 | 1.645 s | 10.619 s |
| HEAD | 37.7 s | 139.1 s | 3.69 | 1.908 s | 8.958 s |

TurboQuant construction jumps 8.1x at `a955a0c1` while float32 does not move.
TurboQuant's last batch goes 0.663 s -> 9.981 s (15x) while float32's stays flat
at ~1.5-1.9 s across the entire range.

**The cause is that commit's type-aware neighbour selection, and the old numbers
were measuring a broken index.** `a955a0c1` fixed three defects at once; the
relevant one is that neighbour selection read the float32 arena, which is empty
for every other element type, so for TurboQuant *every candidate was rejected*
and each node was left with a single oldest link. Graph shape at the same shape,
40,000 vectors, `dim=128`, `MMax0=16`:

| Revision | layer-0 edges | mean degree | nodes with edges | reachable from entry point |
|---|---|---|---|---|
| `7f872022` | 276,940 | 6.92 | 29,215 / 40,000 | **29,215** |
| `a955a0c1` | 628,013 | 15.70 | 39,251 / 40,000 | **39,251** |

Before the fix, **10,785 of 40,000 nodes - 27% - had no layer-0 edges at all and
were unreachable from the entry point**, at any `ef`. That is the defect
`a955a0c1` exists to fix. Afterwards mean degree is 15.70 against an `MMax0` of
16, i.e. the graph is essentially fully connected.

So the baseline's flattering `turboquant/float32 = 0.53` was not TurboQuant being
efficient. It was TurboQuant being cheap because it was skipping work: 2.3x fewer
edges, and a quarter of the corpus invisible to search. **The 8x is the bill for
the fix.**

### What this invalidates and what it does not

- **Invalidated:** §4 item 2 and §5 item 2, which promote TurboQuant as the
  recommended engine above 100k vectors partly on "rock-solid throughput" at
  100k/250k. Those measurements were taken on a graph where 27% of vectors were
  unreachable. Any TurboQuant throughput number recorded before `a955a0c1` needs
  re-measuring before it is cited again.
- **Invalidated:** the TurboQuant rows in §8.1 of this document, for the same
  reason. §8.5 described TurboQuant as "the worst dense-search numbers of any
  dtype"; those rows come from the same pre-`a955a0c1` graph and describe how the
  broken index performed, not how TurboQuant performs.
- **Not invalidated:** §8.3. The chain link is still a genuine **query-quality**
  regression for float32 (dense -60.9%, filteredstring -81.0% against
  `2f4dc1c4`). It is a different bug from this one. Confirmed independently here:
  at `a955a0c1` with the chain link disabled, TurboQuant construction is
  **236.1 s against 142.0 s with it enabled** - the chain link is not the
  construction cost, and removing it makes things worse at this scale.
- **Unchanged:** §9.1's query-path work. It is worth ~1.3x on TurboQuant
  construction (139.1 s -> 106.3 s) and its bit-exactness is pinned by
  `TestTQComputeBatch_MatchesPerCandidate`.

**The server symptom is therefore expected, not a bug to be removed.** At 250k,
`longbow_hnsw_bulk_insert_duration_seconds_sum` reaches 1,952 s for 250,000
TurboQuant vectors (7.8 ms per vector) where float32 completes in 71.5 s, and
the tier does not finish inside a 45-minute budget. That is the correct cost of
building a connected graph at that size with this code.

### Recommendations after the bisection

- **R18. Stop treating the TurboQuant build cost as a defect.** The right
  question is not how to make it fast again but how much of the 8x is
  reducible while keeping the graph connected. Any optimisation must be judged on
  `TestBisectTQGraphShape`'s numbers - mean degree and reachable count - not on
  wall clock alone, because wall clock alone is exactly what the broken graph
  optimised.
- **R19. Re-baseline every TurboQuant number recorded before `a955a0c1`.** §8.1's
  TurboQuant rows and the `docs/performance.md` TurboQuant rows were all measured
  on a partially disconnected graph. Re-run them before quoting them, and record
  mean degree and reachable-node count alongside the QPS so the graph quality is
  never invisible again.
- **R20. Gate TurboQuant graph quality in CI - DONE.**
  `internal/store/index/turboquant_graph_quality_test.go` now does this. Without
  such a gate, a change that quietly strands nodes looks like a large speedup -
  which is precisely how the pre-`a955a0c1` numbers came to look good. The suite:

  | Test | Asserts | Measured |
  |---|---|---|
  | `TestTurboQuantIndexIsEngaged` | bit depth, chunk offsets written, packed stride below the float32 footprint | 4 bits, stride 84 vs 512 |
  | `TestTurboQuantGraphIsConnected` | mean layer-0 degree >= 8 (half of `MMax0`), >= 95% reachable from the entry point; 4- and 8-bit | 15.71 / 15.62 degree, 98.2% / 97.6% reachable |
  | `TestTurboQuantRecallNotBelowFloat32` | TurboQuant recall >= float32 recall - 5 points, relative | 0.120 vs 0.045 |
  | `TestTQComputeBatchMatchesPerCandidate` | batched loop bit-identical to the per-candidate reference, ids across chunk boundaries | exact, 4- and 8-bit |
  | `TestTQDistanceDirectCodesMatchesDistanceDirect` | both paths to the SIMD kernel agree | exact, 4- and 8-bit |
  | `TestTQPrefetchChunkIsBoundsSafe` | no panic or out-of-range read on nil, negative, truncated and past-the-end chunks | 100% covered |
  | `TestTurboQuantConstructionScalesLikeFloat32` | `turboquant/float32` construction ratio <= 8; opt-in via `LONG_BOW_TQ_BUILD=1` | 2.91 at 250k |

  The degree and reachability thresholds are deliberately loose enough to tolerate
  natural variation but tight enough to fail the pre-`a955a0c1` shape, which
  measured 6.92 mean degree and 73.0% reachable. Thresholds are not the
  mechanism of protection anyway: any test that only checks the graph is
  non-empty passes on the broken graph, so both assertions exist.
- **R23. Absolute recall is not measurable on the in-process harness, and no
  TurboQuant recall claim should rest on it.** `TestDenseRecallHarnessSanity`
  measures that the harness returns a corpus vector as its own nearest neighbour
  only 69% of the time, on float32 as well as on TurboQuant - a property of
  `MockDataset`, not of either index. Absolute recall on the uniform-random
  fixture is additionally depressed by the fixture having no cluster structure.
  `TestTurboQuantRecallNotBelowFloat32` is therefore a direction-of-effect test,
  not a quality gate. A real recall number needs the server benchmark with real
  queries, which has not been re-run since `a955a0c1` - see R19.
- **R21. Do not disable the bulk path for TurboQuant as a workaround.**
  `LONGBOW_HNSW_BULK_INSERT_THRESHOLD` above the dataset size makes 250k index in
  180 s, but it buys that by taking the broken-graph path. It is a way to measure
  the old behaviour on demand, not a fix.
- **R22. Add a bulk-insert time budget with a diagnostic - PARTIALLY DONE.** The
  alerting half is now in place: `LongbowSlowBulkInsertByType` and
  `LongbowBulkInsertFasterThanFloat32` in `grafana/rules.yml`. The in-process half is
  not. A bulk insert that exceeds a configured wall-clock budget should log node
  count, elapsed time and per-vector cost and mark the dataset degraded, rather than
  leaving an operator watching `Indexing queue is filling up`.

  Note what was and was not missing. The metrics existed and were charted - the
  `ingestion-performance` dashboard has a p99-by-type panel on
  `longbow_hnsw_bulk_insert_latency_by_type_seconds` - so the gap was never
  visibility of the metric. It was that `rules.yml` matched no bulk-insert
  expression, so a 250k TurboQuant build could run for 45 minutes with nothing to
  alert on. An earlier draft of this roadmap claimed the metric had "nothing
  consuming it", which was wrong: the dashboard had been consuming it all along.

### 9.3 The in-process recall harness does not measure recall

`TestDenseRecallHarnessSanity` queries a 5,000-vector corpus with vectors taken
from that corpus, so every query has an exact match at distance zero and a correct
index must return it first. It returns it **67.5%** of the time.

That is a property of the in-process `MockDataset` harness, not of TurboQuant -
it reproduces on float32 - and it means no absolute recall figure measured through
that harness is trustworthy. `TestTQSearchRecallsFloat32` is therefore written as
a *relative* comparison against a float32 index built from the same corpus, and
the 5-point gate is expressed relative to that baseline rather than absolutely.

### 9.4 Attempted optimisation that does not work, and why

R18 asks how much of the 8x is reducible while keeping the graph connected. The
obvious lever is the distance kernel, so it was profiled and attacked. The result is
a negative one, recorded here so the next attempt does not repeat it.

**Where the time actually goes.** TurboQuant-only profile of a 250k build (`dim=128`,
10,000-row batches, 4 workers, no float32 phase so the numbers are not blended):

| Symbol | Flat | Share |
|---|---|---|
| `simd.l2SquaredAVX2Kernel` | 318.82s | **40.96%** |
| `index.(*ArrowHNSW).searchLayer` | 83.34s | 10.71% (95.31% cum) |
| `index.(*tqComputer).ComputeSingle` | 9.63s | 1.24% (45.73% cum) |

The distance arithmetic is 41% of CPU, so the kernel is the right place to look.

**The attempt.** While the decode cache is live, TurboQuant distances are ordinary
float32 Euclidean distances against `decodeCache[id*dim : id*dim+dim]` - the packed
codes are never read. So `tqComputer.ComputeBatch` was given a batched decode-cache
path that gathers the block and calls `simd.EuclideanDistanceBatch`, exactly what
`float32ToFloat32Computer` already does.

In isolation the change looks like an obvious win, at every block size (dim=128, ns
per block):

| Block size | single | batched | Speedup |
|---|---|---|---|
| 4 | 648.6 | 331.4 | 1.96x |
| 8 | 1305 | 566.3 | 2.31x |
| 16 | 2606 | 1031 | 2.53x |
| 64 | 10404 | 3879 | 2.68x |
| 256 | 41580 | 16335 | 2.54x |

**End to end it was 25% slower**, consistently:

| Variant | Runs | Mean |
|---|---|---|
| baseline | 102.1s, 102.6s, 103.4s | **102.7s** |
| batched decode cache | 115.6s, 128.4s, 130.7s, 130.2s, 130.8s | **127.1s** |

Not allocation pressure - total allocated 19,438 MB against 19,516 MB, mallocs
339.9M either way, GC cycles 83 against 82. Two measurable reasons:

- **Blocks are far smaller than the microbenchmark assumes.** `searchLayer` hands
  over one node's neighbour list per hop. Instrumenting the batch path gives 2.55e9
  candidates across 3.55e8 calls, **mean 7.2, peaking at 5-6**, and only 337 calls
  ever reach size 17. The 4-way kernel cannot amortise its setup at that size.
- **The microbenchmark's working set is hot; the real one is not.** It cycles over
  32 KB, which stays in L2. The decode cache is 128 MB read in node-id order, so it
  is cache-miss bound. The gather pass adds a dependent load chain and cannot buy
  the misses back.

Graph quality was unaffected either way - mean layer-0 degree 15.71 and 98.2%
reachable with batching against 15.71 and 98.2% without, at both 4 and 8 bit - which
is the point of having that gate. The change was reverted; the reason is recorded in
a comment on the branch in `tqComputer.ComputeBatch` so nobody re-attempts it blind.

- **R24. The remaining lever is block size, not the kernel.** TurboQuant construction
  cannot get 4-way SIMD because it is never asked for 4 vectors at once. Accumulating
  candidates across graph hops before computing distances would fix that, but it
  changes traversal order and therefore graph structure, so it needs a design and a
  recall measurement, not a patch. Expect it to trade against search latency.

### 9.5 The larger lead: neighbour-list lookups are 13% of CPU and dtype-independent

Found in the same profile, and worth more than the TurboQuant-specific work because
it applies to every data type:

| Symbol | Flat | Share |
|---|---|---|
| `index.(*LockFreeNeighborCache).GetNeighbors` | 34.53s | 4.44% (23.29% cum) |
| `index.(*syncMapShim).Load` | - | 13.23% cum |
| `sync.(*Map).Load` -> `sync.HashTrieMap[any,any].Load` | 84.52s | **10.86%** |

`LockFreeNeighborCache` backs its node-id -> neighbour-list map with
`sync.Map[any]any`. Every lookup boxes a `uint32` key into an `interface{}`, so the
hot path pays interface hashing (`runtime.nilinterhash` 3.19s) and interface equality
(`runtime.memequal32` 8.46s) on every single neighbour access. Node ids are dense and
small, which is the worst case for a hashed interface map and the best case for a
flat array or a small sharded slice keyed by `uint32`.

- **R25. Replace the `sync.Map` backing in `LockFreeNeighborCache` with a typed,
  id-keyed structure.** This is a concurrency change to a structure used by every
  dtype, so it wants its own branch, its own race-detector run and a benchmark on
  float32 as well as TurboQuant. It is the highest-value single item found in this
  investigation: ~13% of construction CPU, for all data types.

### 9.6 Side finding: leftover debug output in the arena read path

`SlabArena.GetWithGeneration` and `SlabArena.Get` in `internal/memory/arena.go`
carry seven `fmt.Printf("ARENA_NIL_DEBUG: ...")` calls on their nil/bounds
rejection paths. They did not fire during this work, but they sit in the hottest
read accessor in the codebase: `fmt.Printf` takes the process-wide stdout lock and
formats, so a bounds rejection that happens per candidate would both stall every
other writer to stdout and flood the log. They should be deleted or replaced with a
`Debug`-level structured log. Not fixed here because it is outside the TurboQuant
change and untested against a real rejection.
