# Longbow Unified Roadmap & Optimization Plan

Last updated: 2026-09-29.
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

### Cross-Cutting Fixes Found Along The Way

- **Bitmap pool aliasing** (`internal/store/types/bitmap.go`, `internal/pool/bitmap_pool.go`): `Release()` returned bitmaps the `Bitset` did not own, so the pool could hand the same `*roaring.Bitmap` to two owners — two writers mutating one bitmap. Fixed with explicit ownership tracking; this was the true cause of the flaky `TestBitset_Slice` (3 failures in 10 under `-race`).
- **TurboQuant 8-bit pack kernel and 2-bit scratch tail**: see item 9.

### Known Open Issues

- `TestAddBatch_Bulk_Typed` (`internal/store/index/arrow_hnsw_bulk_typed_test.go:277`) is a **pre-existing** recall flake — reproduced on a pristine checkout of `cfa20cb9` with none of these changes, so it is not caused by this work.
- Predicate-pruned HNSW traversal can return zero results with a moderately selective predicate: `search_float32.go` and `distance_dispatch.go` prune the frontier on the predicate, and if the (randomly chosen) entry point is rejected and no level-0 neighbour is admitted, the frontier closes. ~20% of builds returned <10 results at 2/3 rejection. Not addressed here.
- An unverified arm64 twin of the TurboQuant 8-bit pack bug remains in `internal/simd/turboquant_arm64.s` (`packTQ8NEONKernel` narrowing). It was left unpatched deliberately: there is no ARM hardware or emulator on this host and `GOARCH=arm64` does not currently build, so the fix could not be validated. The same defect class is latent in the AVX2 2/4-bit pack kernels, which are not on the AVX2 dispatch path.

