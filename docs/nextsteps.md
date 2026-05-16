# Longbow Storage Engine - Future Roadmap

## Production Stability & Performance Hardening (v0.2.3 Blockers)

The following items are identified as critical blockers for v0.2.3 to ensure scalability beyond 1M vectors and 100k+ search QPS on high-dimensional data. Each task **must** include comprehensive unit/fuzz tests and corresponding Prometheus metrics for observability.

> ✅ All v0.2.3 P0 production-stability blockers have been resolved. See **COMPLETED MILESTONES** for details.

## P0 Blockers (Remaining)

- **Off-heap Graph Nodes**: Transition HNSW nodes and edges to off-heap arenas to eliminate `runtime.scanObject` overhead, which currently consumes >60% CPU during high-load search.
- **Streaming Shard Rebalancing (v0.2.5)**: Implement a more memory-efficient migration path that avoids doubling the graph memory footprint during the monolithic-to-sharded transition. This is critical for 3072d+ vectors at 100k scale.
  - **Strategy**:
    - **Shared Vector Storage**: Refactor `ShardedHNSW` to use the primary `Dataset` Arrow records for vector lookups, eliminating shard-local vector copies and reducing memory footprint by 40-60%.
    - **Mmap-backed Shadow Index**: Transition the monolithic index to a read-only, `mmap`-backed snapshot during migration to free up Go heap for the new sharded index.
    - **Fragmented Handover**: Migrate data in shard-aligned blocks and call `ReleaseMonolithicChunk` immediately after each block is successfully replicated.
    - **Priority-Aware Admission**: Implement a "Migration Lane" in the `AdmissionController` to throttle migration background tasks when Search QPS or real-time Ingestion pressure exceeds 80% capacity.
- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.
- **Sparse Search ARM64 Assembly**: While functional via generic SIMD, Sparse Search (BM25) requires dedicated NEON assembly kernels to match AVX-512 throughput.

## Performance Optimizations (v0.2.5+)

- **Off-heap Vector Storage**: Transition large vector buffers (especially for high-dimensional 3072d sets) to `mmap` or `C.malloc` to reduce Go heap size and scan duration. This is now a high priority to bypass the 18GB GC bottleneck.
- **AVX-512 VBMI Bitpacking**: Implement 2-bit packing using `VPMULTISHIFTQB` for further throughput gains on modern CPUs.
- **Distributed Result Fusion**: Optimize the RRF (Reciprocal Rank Fusion) pipeline for multi-node cluster configurations.
- **Cross-Node WAL Replication**: Implement synchronous WAL replication for high-availability deployments.
- [x] **TurboQuant Packing Kernels**: Implemented SIMD-accelerated packing kernels for NEON (ARM64), AVX2, and AVX-512 (AMD64) to resolve the ingestion throughput bottleneck.
- **Remote gRPC Loopback Tuning**: Search throughput on Linux (ancalagon) is ~50% lower than macOS for loopback requests. Profile Go's gRPC stack on amd64 to identify potential context switching or syscall bottlenecks.

## COMPLETED MILESTONES

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
- [x] **v0.2.3 Performance Audit (Local/Remote Matrix)**: Executed comprehensive 16-type, 5-dimension, 5-count performance matrix on Local (Metal) and Remote (CUDA) hosts. Resolved interface implementation gaps in `CUDAIndex` (`Clear`, `Reset`, `Sync`, `SearchGreedy`) to achieve 100% cross-backend compatibility. Validated system stability under high-throughput ingestion and search cycles with 24GB (local) and 16GB (remote) memory budgets. Collected pprof data and Prometheus metrics to verify zero-regression baseline for production release.
- [x] **SIMD-Accelerated TurboQuant Packing**: Finalized and integrated assembly kernels for TQ2, TQ4, and TQ8 across NEON (ARM64), AVX2, and AVX-512 (AMD64) architectures. Achieved high-performance bit-packing using vector narrowing and shift-OR patterns, eliminating the ingestion CPU bottleneck.
