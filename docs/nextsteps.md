# Longbow Storage Engine - Future Roadmap

## Production Stability & Performance Hardening (v0.2.3 Blockers)

The following items are identified as critical blockers for v0.2.3 to ensure scalability beyond 1M vectors and 100k+ search QPS on high-dimensional data. Each task **must** include comprehensive unit/fuzz tests and corresponding Prometheus metrics for observability.

- [ ] **GPU-Based Neighbor Pruning Kernel (Metal/CUDA)**: Offload the entire `UpdateNeighbors` logic (including heuristic pruning) to the GPU. 
  - **Goal**: Eliminate the CPU-GPU data "ping-pong" during ingestion.
  - **Observability**: Add `longbow_gpu_ingest_kernel_duration_seconds` and `longbow_gpu_neighbor_prune_ops_total`.
  - **Testing**: Fuzz test neighbor connectivity parity between CPU and GPU implementations.
- [ ] **Chunked Flat-Tree for Temporal Data**: Replace pointer-based `TemporalTree` nodes with contiguous memory blocks (Arenas) representing tree levels.
  - **Goal**: Enable hardware prefetching and reduce cache misses for range/window queries at 250k+ scale.
  - **Observability**: Add `longbow_temporal_tree_cache_hit_ratio` and `longbow_temporal_query_scanned_nodes_total`.
  - **Testing**: Benchmarks comparing cache-miss rates using `perf` or `instruments`.
- [ ] **Fused Dequantize-Distance (TurboQuant)**: Move TurboQuant decoding directly into the GPU registers/SIMD distance kernels.
  - **Goal**: Maintain reduced memory footprint without the "decoding tax" in a separate pass.
  - **Observability**: Add `longbow_search_dequantize_latency_seconds`.
  - **Testing**: Unit tests for bit-exact parity between fused and separate dequantization paths.
- [ ] **Transparent Hugepages (THP) for SlabPool**: Implement explicit support for `madvise(MADV_HUGEPAGE)` in the off-heap slab allocator.
  - **Goal**: Reduce TLB misses during high-concurrency searches on Linux (ancalagon).
  - **Observability**: Add `longbow_slab_hugepage_count`.
  - **Testing**: Integration tests validating hugepage alignment on supported systems.
- [ ] **Wait-Free Graph Updates (CoW Adjacency)**: Implement a Copy-on-Write strategy for HNSW adjacency lists at the shard level.
  - **Goal**: Eliminate lock contention during massive parallel ingestion bursts.
  - **Observability**: Add `longbow_hnsw_cow_copy_count` and `longbow_hnsw_update_contention_seconds`.
  - **Testing**: High-concurrency race-enabled fuzz tests for graph integrity during CoW.
- [ ] **Benchmark Health Check Loop**: Refactor `unified_benchmark.py` to use a dedicated gRPC `/ready` polling loop.
  - **Goal**: Eliminate "connection refused" races on macOS by waiting for full service readiness.
  - **Observability**: Log server readiness handshake duration in benchmark summaries.
  - **Testing**: Validate that benchmark scripts retry gracefully on transient port collisions.
- [ ] **SlabPool & RefCount Prometheus Metrics**: Expose internal slab utilization and `PackedAdjacency` reference counts.
  - **Goal**: Enable real-time detection of "dangling arenas" and memory leaks before they trigger OOMs.
  - **Observability**: Add `longbow_slab_active_arenas`, `longbow_slab_refcount_distribution`, and `longbow_slab_leak_probability`.
  - **Testing**: Unit tests that purposefully create and then reclaim arenas, verifying metric delta accuracy.

## P0 Blockers (Remaining)

- **Streaming Shard Rebalancing (v0.2.5)**: Implement a more memory-efficient migration path that avoids doubling the graph memory footprint during the monolithic-to-sharded transition. This is critical for 3072d+ vectors at 100k scale.
  - **Strategy**:
    - **Shared Vector Storage**: Refactor `ShardedHNSW` to use the primary `Dataset` Arrow records for vector lookups, eliminating shard-local vector copies and reducing memory footprint by 40-60%.
    - **Mmap-backed Shadow Index**: Transition the monolithic index to a read-only, `mmap`-backed snapshot during migration to free up Go heap for the new sharded index.
    - **Fragmented Handover**: Migrate data in shard-aligned blocks and call `ReleaseMonolithicChunk` immediately after each block is successfully replicated.
    - **Priority-Aware Admission**: Implement a "Migration Lane" in the `AdmissionController` to throttle migration background tasks when Search QPS or real-time Ingestion pressure exceeds 80% capacity.
- **Off-heap Graph Nodes**: Transition HNSW nodes and edges to off-heap arenas to eliminate `runtime.scanObject` overhead, which currently consumes >60% CPU during high-load search.
- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.
- **Sparse Search ARM64 Assembly**: While functional via generic SIMD, Sparse Search (BM25) requires dedicated NEON assembly kernels to match AVX-512 throughput.

- **Production gosec Remediation**: [COMPLETED] Resolved all G301, G304, and G104 security warnings; applied verified `#nosec` pragmas for non-sensitive utility contexts.
- **Metrics Documentation Parity**: [COMPLETED] Implemented `scripts/verify_metrics.py` and synchronized `docs/metrics.md` with 100% of internal Prometheus signals.
- **v0.2.2-rc2 Performance Audit**: [IN PROGRESS] Orchestrated full 400-test matrix across Local (Metal) and Remote (CUDA) hosts. Initial 5k-scale results confirm 100% stability under 18GB pressure.

- **Hugging Face Model Downloader**: [COMPLETED] Added `longbow-cli` functionality to download ONNX models directly from Hugging Face.
- **CPU Graph Navigation**: [COMPLETED] Implemented `UpdateGraph` and `GraphExpand` for `CPUIndex`, ensuring full feature parity for non-GPU environments.
- **TurboQuant CPU SIMD**: [COMPLETED] Optimized `SearchTurboQuant` with high-performance SIMD distance kernels, eliminating reconstruction overhead.
- **Async I/O Parity**: [COMPLETED] Refactored `DiskWriterUring` stubs to simulate non-blocking behavior via background goroutines.
- **Strict Embedding Loading**: [COMPLETED] Hardened `EmbeddingGenerator` to enforce model loading and prevent silent fallback to stubs.
- **Location Store Stability**: [COMPLETED] Resolved critical race conditions in `ChunkedLocationStore` maps during concurrent sharding transitions.
- **Admission Hardening**: [COMPLETED] Lowered `AdmissionController` thresholds to 92% and implemented structured logging for rejection observability.
- **Ready Handshake**: [COMPLETED] Added `ActiveIngestStreams` tracking and enhanced `check_readiness` to prevent `NotFound` races during ingestion/search transitions.
- **Livelock Mitigation**: [COMPLETED] Integrated emergency memory cleanup (Query Cache clearing) and aggressive GC triggers into `GCTuner` at 88%+ pressure.
- **gRPC Resilience**: [COMPLETED] Tuned keepalive settings (30s) and enabled without-stream pings to maintain connection stability during heavy GC cycles.

## Performance Optimizations (v0.2.5+)

- **Off-heap Vector Storage**: Transition large vector buffers (especially for high-dimensional 3072d sets) to `mmap` or `C.malloc` to reduce Go heap size and scan duration. This is now a high priority to bypass the 18GB GC bottleneck.
- **AVX-512 VBMI Bitpacking**: Implement 2-bit packing using `VPMULTISHIFTQB` for further throughput gains on modern CPUs.
- **Distributed Result Fusion**: Optimize the RRF (Reciprocal Rank Fusion) pipeline for multi-node cluster configurations.
- **Cross-Node WAL Replication**: Implement synchronous WAL replication for high-availability deployments.
- **TurboQuant Packing Kernels**: Current TurboQuant ingestion is CPU-bound due to vector packing. Implement SIMD-accelerated packing/unpacking in the `DoPut` path to match the throughput of raw data types.
- **Remote gRPC Loopback Tuning**: Search throughput on Linux (ancalagon) is ~50% lower than macOS for loopback requests. Profile Go's gRPC stack on amd64 to identify potential context switching or syscall bottlenecks.
- **GC Overhead Reduction**: pprof results show `runtime.scanObject` consuming >60% of CPU during high-load search.
  - **Arena Allocation**: Implement `arena`-backed storage for HNSW nodes and edges to move them off the GC-scanned heap.

### Cross-Platform Integrity Recommendations

- **Consolidate SIMD Stubs**: Future development should favor a centralized `internal/simd/stubs_generic.go` for all non-native architecture fallbacks to prevent symbol redeclaration conflicts as new kernels are added (e.g., AVX-512 VBMI).
- **TurboQuant Scaling Validation**: Monitor TQ2/TQ4 search latency at 250k+ scales to verify that the SIMD distance kernels scale linearly without cache-line contention under the 18GB memory budget.

### Stability & Performance Recommendations (Post-Validation)

- **Off-Heap Vector Storage**: Transition vector storage from the Go heap to `mmap`-backed off-heap memory. This will prevent the Go GC from scanning millions of vector elements, reducing GC latency and preventing OOMs caused by heap fragmentation.
- **Eager Graph Deallocation**: Implement a more aggressive "release-as-you-go" strategy during `migrateToSharded`. Instead of holding the entire monolithic graph until migration completes, deallocate segments of the monolithic index as soon as they are successfully replicated into shards.
- **NUMA-Aware Allocation**: On high-core count systems (like ancalagon), implement NUMA-aware shard placement to reduce cross-socket memory latency during parallel search.
- **Compressed Neighbors**: Investigate delta-encoding or bit-packing for HNSW neighbor lists to reduce the graph footprint by an additional 30-50%.
