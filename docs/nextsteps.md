# Longbow Storage Engine - Future Roadmap

## P0 Blockers (Remaining)

- **Optimizing Index Migration (AutoShardingIndex)**: Implement a more memory-efficient migration path that avoids doubling the graph memory footprint during the monolithic-to-sharded transition. This is critical for 3072d+ vectors at 100k scale.
  - **Subtasks**:
    - **Incremental Handover**: Implement a mechanism to release monolithic graph segments as they are successfully replicated into shards.
    - **Migration GC Tuning**: Integrate explicit `runtime.GC()` triggers and `debug.FreeOSMemory()` calls between migration batches (5k-10k vectors) to clear interim allocation buffers.
    - **Shadow Search Optimization**: Refactor `SearchVectors` to prioritize the growing sharded index and use a memory-mapped snapshot of the monolithic index if heap pressure exceeds 85%.
    - **Admission Control**: Throttling of concurrent search/ingest operations specifically during the `migrateToSharded` window to prevent OOM spikes.
  - **Validation**:
    - **Unit Test**: `internal/store/hnsw_autoshard_test.go:TestMigrationStability` - Verify zero-loss concurrent operations.
    - **Fuzz Test**: `internal/store/hnsw_migration_fuzz_test.go` - Fuzz ingestion scales and memory limits to identify edge-case OOMs.
    - **PProf Audit**: Confirm memory overhead remains below 1.2x (down from 2.0x) during peak migration.
- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.
- **Sparse Search ARM64 Assembly**: While functional via generic SIMD, Sparse Search (BM25) requires dedicated NEON assembly kernels to match AVX-512 throughput.

## Recently Completed (v0.2.2-rc2 Final)

- **CPU Graph Navigation**: Implemented `UpdateGraph` and `GraphExpand` for `CPUIndex`, ensuring full feature parity for non-GPU environments.
- **TurboQuant CPU SIMD**: Optimized `SearchTurboQuant` with high-performance SIMD distance kernels, eliminating reconstruction overhead.
- **Async I/O Parity**: Refactored `DiskWriterUring` stubs to simulate non-blocking behavior via background goroutines.
- **Strict Embedding Loading**: Hardened `EmbeddingGenerator` to enforce model loading and prevent silent fallback to stubs.
- **Location Store Stability**: Resolved critical race conditions in `ChunkedLocationStore` maps during concurrent sharding transitions.
- **Admission Hardening**: Lowered `AdmissionController` thresholds to 92% and implemented structured logging for rejection observability.
- **Ready Handshake**: Added `ActiveIngestStreams` tracking and enhanced `check_readiness` to prevent `NotFound` races during ingestion/search transitions.
- **Livelock Mitigation**: Integrated emergency memory cleanup (Query Cache clearing) and aggressive GC triggers into `GCTuner` at 88%+ pressure.
- **gRPC Resilience**: Tuned keepalive settings (30s) and enabled without-stream pings to maintain connection stability during heavy GC cycles.

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
