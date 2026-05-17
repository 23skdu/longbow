# Longbow Storage Engine - Future Roadmap

## Production Stability & Performance Hardening (v0.2.3 Blockers)

The following items are identified as critical blockers for v0.2.3 to ensure scalability beyond 1M vectors and 100k+ search QPS on high-dimensional data. Each task **must** include comprehensive unit/fuzz tests and corresponding Prometheus metrics for observability.

> ✅ All v0.2.3 P0 production-stability blockers have been resolved. See **COMPLETED MILESTONES** for details.

## P0 Blockers (Remaining)

- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.

## Performance Optimizations (v0.2.5+)

## COMPLETED MILESTONES

- [x] **Distributed Result Fusion (v0.2.5+)**: Optimized the RRF (Reciprocal Rank Fusion) pipeline for multi-node cluster configurations. Extended `GlobalSearchCoordinator` to gather top-K raw Dense and Sparse lists globally before applying `ReciprocalRankFusion` to ensure mathematical correctness of rank denominators. Added unit tests for multi-node RRF equality against a single-node mega-index, and added Prometheus metrics (`longbow_global_rrf_latency_seconds`, `longbow_global_rrf_payload_bytes`).

- [x] **Remote gRPC Loopback Tuning (v0.2.5+)**: Identified and remediated a ~60% search throughput gap on Linux (ancalagon) caused by TCP loopback overheads. Implemented `ListenUDS` socket listener and Unix Domain Socket (UDS) fallback via `unix://` scheme. Integrated `UDSConnectionsTotal` into metrics observability. Performance comparisons on Linux demonstrate a ~32% increase in DoGet throughput and a ~95% increase in Search QPS when utilizing UDS.
- [x] **Sparse Search SIMD Kernels (v0.2.5+)**: Implemented highly optimized NEON, AVX2, and AVX-512 assembly kernels for BM25 score calculation. These kernels directly read 64-bit integer arrays and utilize unrolled loops (16x for AMD64, 8x for ARM64) and efficient downconversion to perform 32-bit floating point math, maximizing throughput for sparse retrieval.
- [x] **AVX-512 VBMI Bitpacking (v0.2.5+)**: Implemented 2-bit (TQ2) and 4-bit (TQ4) packing kernels using `VPMULTISHIFTQB` and `VPERMB` for single-cycle bit gathering on Ice Lake+ hardware. Optimized NEON packing using vectorized `VUZP` and `VSHL` patterns.
- [x] **Off-heap Vector Storage (v0.2.1-rc3)**: Transitioned large vector buffers in `MemVectorStore` to `mmap`-backed `SlabArena` storage. This bypasses the Go GC for the majority of the index memory, eliminating the `runtime.scanObject` bottleneck for high-dimensional datasets.
- [x] **SlabPool & RefCount Prometheus Metrics** (v0.2.3): Exposed `longbow_slab_active_arenas` (GaugeVec), `longbow_slab_refcount_distribution` (HistogramVec), `longbow_slab_leak_probability` (GaugeVec), and `longbow_slab_hugepage_count` (Counter) into `internal/metrics`. Wired into `SlabPool.Get/Put/updateMetrics` via a peak-tracking field that drives the leak-probability heuristic. Upgraded `PackedAdjacency` `Retain/Release` call sites to use the new Vec API. Added 5 targeted unit tests in `internal/memory/slab_metrics_test.go`.
- [x] **Benchmark Health Check Loop** (v0.2.3): Refactored `unified_benchmark.py` `start_server` to record a `startup_start` timestamp and emit `[readiness] server ready in Xs` and `[readiness] server ready after Xs (N transient port-collision retries)` lines to benchmark summaries. Timeout messages now include elapsed time and retry count for macOS race postmortem.
- [x] **Wait-Free Graph Updates (CoW Adjacency)** (v0.2.3): Added `longbow_hnsw_cow_copy_count` (CounterVec, labels: `dataset`, `shard`) and `longbow_hnsw_update_contention_seconds` (HistogramVec, label: `dataset`) to `internal/metrics/hnsw_metrics.go`. Instrumented `ShardedHNSW.AddBatch` to record the RLock acquisition window as the contention proxy and to count per-shard CoW adjacency copies on every successful batch insert. Declaration tests added in `internal/store/slab_cow_metrics_test.go`.
- [x] **Transparent Hugepages (THP) for SlabPool** (v0.2.3): The `AdviseHugePage` call was already implemented in `slab_release_unix.go`. This milestone wired the `longbow_slab_hugepage_count` Counter into the `SlabPool.New` closure so successful `madvise(MADV_HUGEPAGE)` calls are now tracked. On Darwin the call is a graceful no-op. Integration test `TestTHP_HugePageAlignment` verifies all four standard pool sizes are correctly aligned.
- [x] **TurboQuant Metal GPU Parity**: Achieved functional parity between CPU and GPU distance calculations by standardizing on square root L2, synchronizing trigonometric math via lookup tables, and fixing memory alignment strides. (v0.2.3-rc1)
- [x] **Fused Dequantize-Distance (TurboQuant)**: Moved TurboQuant decoding directly into the GPU registers/SIMD distance kernels to eliminate intermediate memory traffic. (v0.2.3-rc1)
- [x] **GPU-Based Neighbor Pruning Kernel (Metal/CUDA)**: Offloaded the entire `UpdateNeighbors` logic (including heuristic pruning) to the GPU. (v0.2.3-rc2)
- [x] **Chunked Flat-Tree for Temporal Data**: Replaced pointer-based `TemporalTree` nodes with contiguous memory blocks (Arenas) representing tree levels. (v0.2.3-rc2)
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
- [x] **Cross-Node WAL Replication (v0.2.5+)**: Implemented synchronous, quorum-based WAL replication across cluster nodes using Arrow Flight. This ensures high availability and zero data loss by requiring an $N/2+1$ acknowledgment before a write is committed. Integrated with `WALBatcher` and instrumented with `longbow_wal_replication_latency_seconds` metrics. Verified with comprehensive unit tests and stabilized integration tests.

## v0.2.5 Initial Performance Audit Observations

- **macOS (M3 Pro) Improvements**: Initial tests (`float32/128d/5k`) show a **~40% increase in ingestion throughput** (786k vs 550k vec/s) and a **~20% increase in search QPS** compared to v0.2.1 baselines.
- **Linux (ancalagon) Loopback Remediated**: Significant performance degradation previously observed on Linux loopback was successfully remediated via UDS sockets. Implementing UDS connectivity led to a **~95% Search QPS** and **~32% Streaming DoGet throughput** increase over the legacy TCP loopback baseline, closing the performance gap with macOS.

## Performance & Stability Recommendations (v0.2.5+ Observations)

Based on recent comprehensive high-scale performance audits (100k/250k scale) under strict 18GB memory budgets across local (macOS Metal) and remote (Linux CUDA) environments, we recommend the following optimizations for future releases:

- **In-Place Shard Relocation during Index Migration**:
  - *Observation*: High memory footprint spikes (~8.6 GB) in `AutoShardingIndex.migrateToSharded` are caused by duplicating index structures during monolithic-to-sharded conversion.
  - *Recommendation*: Transition from full in-memory rebuild to an incremental in-place sharding pipeline. Vectors should be split and transferred to new shards progressively, releasing vector allocations on-the-fly to ensure the memory ceiling never exceeds 1.2x of the monolithic index size.

- **Concurrency Throttling under Ingest Migration Pressure**:
  - *Observation*: Concurrent `DoGet` searches during index sharding build-up result in memory allocations that trigger the `GCTuner` backpressure and lead to latency livelocks.
  - *Recommendation*: Add a priority queue or search throttling mechanism inside `AdmissionController` specifically active during hot WAL replay or index sharding phases, prioritizing ingestion safety over raw query throughput.

- **Inverted Index Block-Max WAND for Sparse Search**:
  - *Observation*: Sparse Search (BM25) latency regresses substantially when handling high-dimensional queries at high scales (100k+).
  - *Recommendation*: Optimize the sparse retrieval engine by implementing the Block-Max WAND (Weak AND) algorithm. This will allow the search engine to skip scoring document blocks that cannot mathematically exceed the current top-K threshold.

- **Platform-Specific GPU Watchdog & Recovery**:
  - *Observation*: The Metal driver thread occasionally experiences resource starvation/driver panics following consecutive CPU crash signals, leading to hung GPU buffers.
  - *Recommendation*: Implement an out-of-process GPU watchdog or a self-healing host-driver bridge in `metal_gpu.go` that can fully reset Metal device context and flush command queues without dropping active client connections.
