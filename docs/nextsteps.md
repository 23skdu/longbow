# Longbow Storage Engine - Future Roadmap

## Production Stability & Performance Hardening (v0.2.1 Blockers)

The following items are identified as critical blockers for v0.2.1 to ensure scalability beyond 1M vectors and 100k+ search QPS on high-dimensional data. Each task **must** include comprehensive unit/fuzz tests and corresponding Prometheus metrics for observability.

> ✅ All v0.2.1 P0 production-stability blockers have been resolved. See **COMPLETED MILESTONES** for details.

## P0 Blockers (Remaining)

- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.

## Future Performance Optimizations (v0.2.1+)

## COMPLETED MILESTONES

- [x] **Distributed Result Fusion (v0.2.1-rc3)**: Optimized the RRF (Reciprocal Rank Fusion) pipeline for multi-node cluster configurations. Extended `GlobalSearchCoordinator` to gather top-K raw Dense and Sparse lists globally before applying `ReciprocalRankFusion` to ensure mathematical correctness of rank denominators. Added unit tests for multi-node RRF equality against a single-node mega-index, and added Prometheus metrics (`longbow_global_rrf_latency_seconds`, `longbow_global_rrf_payload_bytes`).
- [x] **Remote gRPC Loopback Tuning (v0.2.1-rc3)**: Identified and remediated a ~60% search throughput gap on Linux (ancalagon) caused by TCP loopback overheads. Implemented `ListenUDS` socket listener and Unix Domain Socket (UDS) fallback via `unix://` scheme. Integrated `UDSConnectionsTotal` into metrics observability. Performance comparisons on Linux demonstrate a ~32% increase in DoGet throughput and a ~95% increase in Search QPS when utilizing UDS.
- [x] **Sparse Search SIMD Kernels (v0.2.1-rc3)**: Implemented highly optimized NEON, AVX2, and AVX-512 assembly kernels for BM25 score calculation. These kernels directly read 64-bit integer arrays and utilize unrolled loops (16x for AMD64, 8x for ARM64) and efficient downconversion to perform 32-bit floating point math, maximizing throughput for sparse retrieval.
- [x] **AVX-512 VBMI Bitpacking (v0.2.1-rc3)**: Implemented 2-bit (TQ2) and 4-bit (TQ4) packing kernels using `VPMULTISHIFTQB` and `VPERMB` for single-cycle bit gathering on Ice Lake+ hardware. Optimized NEON packing using vectorized `VUZP` and `VSHL` patterns.
- [x] **Off-heap Vector Storage (v0.2.1-rc3)**: Transitioned large vector buffers in `MemVectorStore` to `mmap`-backed `SlabArena` storage. This bypasses the Go GC for the majority of the index memory, eliminating the `runtime.scanObject` bottleneck for high-dimensional datasets.
- [x] **SlabPool & RefCount Prometheus Metrics** (v0.2.1-rc3): Exposed `longbow_slab_active_arenas` (GaugeVec), `longbow_slab_refcount_distribution` (HistogramVec), `longbow_slab_leak_probability` (GaugeVec), and `longbow_slab_hugepage_count` (Counter) into `internal/metrics`. Wired into `SlabPool.Get/Put/updateMetrics` via a peak-tracking field that drives the leak-probability heuristic. Upgraded `PackedAdjacency` `Retain/Release` call sites to use the new Vec API. Added 5 targeted unit tests in `internal/memory/slab_metrics_test.go`.
- [x] **Benchmark Health Check Loop** (v0.2.1-rc3): Refactored `unified_benchmark.py` `start_server` to record a `startup_start` timestamp and emit `[readiness] server ready in Xs` and `[readiness] server ready after Xs (N transient port-collision retries)` lines to benchmark summaries. Timeout messages now include elapsed time and retry count for macOS race postmortem.
- [x] **Wait-Free Graph Updates (CoW Adjacency)** (v0.2.1-rc3): Added `longbow_hnsw_cow_copy_count` (CounterVec, labels: `dataset`, `shard`) and `longbow_hnsw_update_contention_seconds` (HistogramVec, label: `dataset`) to `internal/metrics/hnsw_metrics.go`. Instrumented `ShardedHNSW.AddBatch` to record the RLock acquisition window as the contention proxy and to count per-shard CoW adjacency copies on every successful batch insert. Declaration tests added in `internal/store/slab_cow_metrics_test.go`.
- [x] **Transparent Hugepages (THP) for SlabPool** (v0.2.1-rc3): The `AdviseHugePage` call was already implemented in `slab_release_unix.go`. This milestone wired the `longbow_slab_hugepage_count` Counter into the `SlabPool.New` closure so successful `madvise(MADV_HUGEPAGE)` calls are now tracked. On Darwin the call is a graceful no-op. Integration test `TestTHP_HugePageAlignment` verifies all four standard pool sizes are correctly aligned.
- [x] **TurboQuant Metal GPU Parity**: Achieved functional parity between CPU and GPU distance calculations by standardizing on square root L2, synchronizing trigonometric math via lookup tables, and fixing memory alignment strides. (v0.2.1-rc1)
- [x] **Fused Dequantize-Distance (TurboQuant)**: Moved TurboQuant decoding directly into the GPU registers/SIMD distance kernels to eliminate intermediate memory traffic. (v0.2.1-rc1)
- [x] **GPU-Based Neighbor Pruning Kernel (Metal/CUDA)**: Offloaded the entire `UpdateNeighbors` logic (including heuristic pruning) to the GPU. (v0.2.1-rc2)
- [x] **Chunked Flat-Tree for Temporal Data**: Replaced pointer-based `TemporalTree` nodes with contiguous memory blocks (Arenas) representing tree levels. (v0.2.1-rc2)
- [x] **TPU Index Compliance**: Implemented `SearchComplex64`, `SearchComplex128`, `HaversineSearch`, `NormBatch`, and `PruneNeighbors` for `TPUIndex`.
- [x] **Metal Search Unification**: Transitioned standard Metal index calls to use optimized kernels for TurboQuant search and graph traversal.
- [x] **Real DiskWriterUring Bindings**: Replaced goroutine-based simulation with actual platform-specific bindings.
- [x] **Production gosec Remediation**: Resolved all G301, G304, G115 (Integer Overflow), and G104 security warnings.
- [x] **Metrics Documentation Parity**: Synchronized `docs/metrics.md` with 100% of internal Prometheus signals.
- [x] **CPU Graph Navigation**: Ensured full feature parity for non-GPU environments.
- [x] **TurboQuant CPU SIMD**: Optimized `SearchTurboQuant` with high-performance SIMD distance kernels.
- [x] **Async I/O Parity**: Refactored `DiskWriterUring` stubs to simulate non-blocking behavior.
- [x] **Strict Embedding Loading**: Hardened `EmbeddingGenerator` to enforce model loading.
- [x] **Location Store Stability**: Resolved critical race conditions in `ChunkedLocationStore`.
- [x] **Admission Hardening**: Lowered thresholds and implemented structured logging for rejection.
- [x] **Ready Handshake**: Enhanced `check_readiness` to prevent races during ingestion.
- [x] **Livelock Mitigation**: Integrated emergency memory cleanup and GC triggers.
- [x] **gRPC Resilience**: Tuned keepalive settings and enabled without-stream pings.
- [x] **Hugging Face Model Downloader**: Added ONNX model download functionality to `longbow-cli`.
- [x] **v0.2.1-rc3 Performance Audit (Local/Remote Matrix)**: Executed comprehensive 16-type, 5-dimension, 5-count performance matrix on Local (Metal) and Remote (CUDA) hosts. Resolved interface implementation gaps in `CUDAIndex` (`Clear`, `Reset`, `Sync`, `SearchGreedy`) to achieve 100% cross-backend compatibility. Validated system stability under high-throughput ingestion and search cycles with 24GB (local) and 16GB (remote) memory budgets. Collected pprof data and Prometheus metrics to verify zero-regression baseline for production release.
- [x] **SIMD-Accelerated TurboQuant Packing (v0.2.1-rc3)**: Finalized and integrated assembly kernels for TQ2, TQ4, and TQ8 across NEON (ARM64), AVX2, and AVX-512 (AMD64) architectures. Achieved high-performance bit-packing using vector narrowing and shift-OR patterns, eliminating the ingestion CPU bottleneck.
- [x] **Graceful Server Lifecycle (v0.2.1-rc3)**: Implemented robust shutdown logic in `cmd/longbow` to ensure metrics flush and pprof profiles are persisted before exit. Replaced `SIGKILL` dependency with a 2-second flush window and 15-second total timeout.
- [x] **Off-heap Graph Migration (v0.2.1-rc3)**: Transitioned HNSW nodes and edges to off-heap arenas during autoshard migration to eliminate `runtime.scanObject` overhead. Implemented `RelocateToOffHeap` across the storage stack to enable `mmap`-backed shadow indices.
- [x] **Streaming Shard Rebalancing (v0.2.1-rc3)**: Implemented a memory-efficient migration path using shared vector storage, fragmented handover, and priority-aware admission control to bypass the GC bottleneck during large-scale index transitions.
- [x] **Cross-Node WAL Replication (v0.2.1-rc3)**: Implemented synchronous, quorum-based WAL replication across cluster nodes using Arrow Flight. This ensures high availability and zero data loss by requiring an $N/2+1$ acknowledgment before a write is committed. Integrated with `WALBatcher` and instrumented with `longbow_wal_replication_latency_seconds` metrics. Verified with comprehensive unit tests and stabilized integration tests.
- [x] **Incremental In-Place Sharding & Memory Reclaim (v0.2.1-rc3)**: Transitioned index sharding from full in-memory rebuild to an incremental in-place sharding pipeline. Upgraded `ReleaseChunk` to release and nullify legacy heap vector slices (`Vectors`, `VectorsFloat64`, `VectorsComplex64`, `VectorsComplex128`) on-the-fly, allowing Go GC to collect them immediately. Added aggressive `runtime.GC` and `debug.FreeOSMemory` triggers under elevated memory pressure, bounding total memory ceiling strictly to 1.2x of the monolithic index size.
- [x] **Priority-Queue & Search Throttling in AdmissionController (v0.2.1-rc3)**: Added search query throttling queue (`querySem` semaphore capped at concurrency of 2) active during WAL replay or active sharding phases to prioritize ingestion safety. Handled automatic deferred release hooks in gRPC interceptors. Fully verified by a dedicated unit test suite.
- [x] **Block-Max WAND Sparse Search (v0.2.1-rc3)**: Optimized the sparse retrieval engine by implementing the Block-Max WAND (Weak AND) algorithm. Posting lists are divided into blocks of size `64`, allowing the search engine to skip scoring document blocks that cannot mathematically exceed the current top-K threshold. Fully verified to be mathematically equivalent and highly performant.
- [x] **Self-Healing GPU Watchdog (v0.2.1-rc3)**: Implemented an out-of-process GPU watchdog and context recovery handler inside the Metal device bridge. Metal indexes now automatically reset the global command queue and device context upon detecting command buffer hangs or driver panics, retrying the failed operation seamlessly without dropping active client connections.
- [x] **Dynamic Slab-Capacity Aware Migration Batching (v0.2.1-rc3)**: Replaced hardcoded HNSW migration batch sizes with a dynamic, byte-size aware calculator: `currentBatchSize = min(currentBatchSize, safeSlabLimit / (dim * bytesPerElement))`. This dynamically guarantees that no contiguous batch insertion will ever trigger a slab allocator limit breach, preventing autosharding migration failures.

## v0.2.1 Initial Performance Audit Observations

- **macOS (M3 Pro) Improvements**: Initial tests (`float32/128d/5k`) show a **~40% increase in ingestion throughput** (786k vs 550k vec/s) and a **~20% increase in search QPS** compared to v0.2.1 baselines.
- **Linux (ancalagon) Loopback Remediated**: Significant performance degradation previously observed on Linux loopback was successfully remediated via UDS sockets. Implementing UDS connectivity led to a **~95% Search QPS** and **~32% Streaming DoGet throughput** increase over the legacy TCP loopback baseline, closing the performance gap with macOS.

## v0.2.1 Performance Regression Observations (2026-05-17, Commit 7090beb5)

### Critical Regressions Resolved (2026-05-17)

- [x] **P0: Search_Dense returns 0 QPS at count >= 10,000**: Dense search failed at 10k+ vectors across all dimensions post-migration to sharding. **Resolution**: Corrected the global location mapping immediately following parallel `AddBatch` ingestion in `migrateToSharded`. This ensures searches using location lookups always receive the correct dataset `BatchIdx` and `RowIdx`.
- [x] **P0: Most search modes return 0 QPS at count >= 25,000**: Hybrid, ByID, GraphRAG, Recommend, LearnedIndex, Geo, and Temporal failed at 25k vectors. **Resolution**: Resolved via the location store correction in `migrateToSharded`, which restored 100% integrity of candidate filtering and metadata evaluation.
- [x] **P0: TurboQuant indexing error `tq vector N not found`**: Async batched index additions failed for TQ types during resize capacity scaling. **Resolution**: Correctly initialized `h.config.TurboQuantEnabled` and `h.config.TurboQuantBits` during constructor execution, preventing resizes from wiping the encoder state.

### Performance/Stability Improvements

- **Ingestion throughput scales well with dimension**: At count=5,000, ingestion ranges from 360 MB/s (dim=128) to 1,112 MB/s (dim=3072). This is a positive trend showing the ingestion pipeline handles higher dimensions efficiently. **Recommendation**: Maintain this trajectory; investigate if the same scaling holds at count=10,000+ once the Search_Dense regression is fixed.

- **Sparse search performance is exceptional**: Sparse search achieves 11,551-12,266 QPS across all dimensions at count=5,000, and continues to function at count=10,000 and 25,000. This validates the Block-Max WAND optimization. **Recommendation**: Use sparse search as a baseline for comparing other search mode fixes; ensure sparse search performance is maintained as other modes are fixed.

- **Search latency is excellent at count=5,000**: P95 latencies are sub-6ms for most search modes at count=5,000, with Sparse at 1.08ms and Temporal at 2.56ms. **Recommendation**: Set SLO targets based on these latencies; monitor latency degradation as count increases.- **Benchmark tooling needs improvement [COMPLETED]**: The `bench_tool_runner.sh` script lacks timeout handling (macOS doesn't have `timeout` command), and the nohup log output is buffered, making real-time monitoring difficult. **Resolution**: Implemented a highly resilient POSIX-compatible `run_with_timeout` wrapper (supporting `timeout`, `gtimeout`, and native POSIX process watchers) inside the benchmark scripts; enabled unbuffered execution with the `python3 -u` flag and dynamic `stdbuf`/`unbuffer` redirection; added a JSON-formatted `/progress` endpoint (`:6000/progress`) to the metrics server that serves live uptime, memory utilization ratio, backpressure status, active ingestion queue statistics, and individual dataset HNSW node configurations.

- **Server restart per test is slow**: The benchmark restarts the longbow server for each test configuration, adding ~5-10 seconds of overhead per test. With 425 tests, this adds ~35-70 minutes of overhead. **Recommendation**: Add a `-reset` flag to bench-tool that drops and recreates the dataset without restarting the server; or add a gRPC endpoint for dataset reset.

- **Memory allocation at 18GB is sufficient for count=5,000 but may be tight at count=250,000**: The benchmarks stalled at count=25,000, suggesting memory pressure may be a factor. **Recommendation**: Add memory usage metrics to the benchmark output; test with 24GB and 32GB allocations to identify the memory ceiling; implement graceful degradation when memory pressure is high.

- **Autosharding Slab capacity breach during dynamic allocation**: At large scale and specific datatypes (`uint32 dim=128 count=10,000` boundaries), graph migration can attempt to allocate a contiguous block larger than the default 1MB pool capacity (e.g. `alloc request 2097152 exceeds slab capacity 1048576`), gracefully aborting but blocking completion of the sharding boundary. **Recommendation**: Implement dynamic slab capacity expansion or a self-resizing `SlabPool` during migrations.

- **Livelock and CPU starvation due to memory-pressure log flooding [COMPLETED]**: When memory pressure approaches the strict 18GB ceiling, the `GCTuner` triggers worker throttling. However, the ingestion workers (12 threads running concurrently) log `"High memory pressure detected, throttling ingestion worker"` at `warn` level inside their tight 200ms loop. This writes millions of log lines (~18MB per test), causing heavy disk I/O, 600%+ CPU usage, and eventual client timeouts. **Resolution**: Designed and implemented an atomic CAS lock-free rate limiter utilizing `s.lastThrottlingLogTime` to restrict this warning to once every 5 seconds, reclaiming CPU cycles, saving disk I/O, and eliminating client-side timeouts.

## v0.2.1-rc3 Deep pprof Profiling Insights & System Bottlenecks (2026-05-18)

### Heap Allocation Analysis (`GoAllocator` Under Slab Abstractions) [RESOLVED]

- **Finding**: The heap profile shows **92.83% (`8.31 GB` out of `8.95 GB` total in-use heap)** is allocated via `github.com/apache/arrow-go/v18/arrow/memory.(*GoAllocator).Allocate`, specifically driven by HNSW adjacency list updates.
- **Impact**: Although the engine utilizes custom `SlabArena` and `TypedArena` managers, the underlying allocator relies on `GoAllocator` which uses standard Go heap allocations (`make([]byte)`). This subjects these giant slab arrays to full Go garbage collector scanning cycles, contributing to high GC CPU cycles and triggering premature dynamic memory pressure tuner actions under an 18GB ceiling.
- **Resolution**: Successfully refactored `NUMAAllocator` (which drives the underlying memory pools for HNSW graphs) from using `memory.NewGoAllocator()` to a **true off-heap allocator** `NewOffHeapAllocator()`. This routes HNSW node slabs directly to system-level `mmap`/`munmap` syscalls, bypassing GC scanning, reclaiming 92.83% of Go heap allocations, and eliminating dynamic page reclamation loop CPU contention.

### CPU Execution Analysis (`runtime.madvise` Contention) [RESOLVED]

- **Finding**: The CPU profile reveals that **54.01% of all system CPU time** is consumed by `runtime.madvise`, while only **29.58%** is spent on the actual distance calculations (`simd.euclideanNEONKernel`).
- **Impact**: When the system experiences memory pressure, the Go GC scavenger frequently invokes `madvise(MADV_DONTNEED)` to release physical pages back to the OS. Because HNSW indexing runs with 12 parallel threads, this triggers extreme kernel lock contention on the memory mapping lock (`mmap_lock`) and continuous TLB shootdowns across all CPU cores.
- **Resolution**: Reused pre-allocated memory pools and gated `debug.FreeOSMemory()` calls so that they are strictly avoided during the hot-paths and active migration/indexing stages, executing only as an absolute last-resort safety valve at `>0.97` memory ratio. This keeps virtual pages mapped within the pre-allocated pools, completely eliminating kernel `madvise` contention, avoiding TLB shootdowns, and boosting index construction speed by over **2x**!
