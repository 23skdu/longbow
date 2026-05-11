# Longbow Storage Engine - Future Roadmap

## Recently Completed (v0.2.2-rc2 Final)

- **CPU Graph Navigation**: Implemented `UpdateGraph` and `GraphExpand` for `CPUIndex`, ensuring full feature parity for non-GPU environments.
- **TurboQuant CPU SIMD**: Optimized `SearchTurboQuant` with high-performance SIMD distance kernels, eliminating reconstruction overhead.
- **Async I/O Parity**: Refactored `DiskWriterUring` stubs to simulate non-blocking behavior via background goroutines.
- **Strict Embedding Loading**: Hardened `EmbeddingGenerator` to enforce model loading and prevent silent fallback to stubs.
- **Location Store Stability**: Resolved critical race conditions in `ChunkedLocationStore` maps during concurrent sharding transitions.

## P0 Blockers (Remaining)

- **TPU Physical Driver Integration**: Replace CGO stubs in `internal/gpu/tpu/tpu_index.go` with actual `libtpu.so` bindings once hardware-linked libraries are provided.
- **Sparse Search ARM64 Assembly**: While functional via generic SIMD, Sparse Search (BM25) requires dedicated NEON assembly kernels to match AVX-512 throughput.

## Performance Optimizations (v0.2.5+)

- **AVX-512 VBMI Bitpacking**: Implement 2-bit packing using `VPMULTISHIFTQB` for further throughput gains on modern CPUs.
- **Distributed Result Fusion**: Optimize the RRF (Reciprocal Rank Fusion) pipeline for multi-node cluster configurations.
- **Memory-Mapped HNSW**: Explore `mmap`-backed vector storage to reduce heap pressure and allow datasets larger than physical RAM.

- Cross-Node WAL Replication: Implement synchronous WAL replication for high-availability deployments.

## Benchmark-Driven Recommendations (v0.2.2-rc2 Observations)

### Stability Improvements

- **Dataset Initialization Handshake**: Investigate intermittent `NotFound` errors during rapid ingestion/search transitions. Implement a more robust "Ready" handshake between the storage engine and the benchmarking tool.
- **Memory Pressure Livelock Mitigation**: Although 18GB is allocated, high-scale (100k+) tests suggest GCTuner contention. Evaluate more aggressive pre-emptive garbage collection or fine-grained memory sharding to prevent livelocks under extreme pressure.

### Performance Gains

- **TurboQuant Packing Kernels**: Current TurboQuant ingestion is CPU-bound due to vector packing. Implement SIMD-accelerated packing/unpacking in the `DoPut` path to match the throughput of raw data types.
- **Remote gRPC Loopback Tuning**: Search throughput on Linux (ancalagon) is ~50% lower than macOS for loopback requests. Profile Go's gRPC stack on amd64 to identify potential context switching or syscall bottlenecks.
