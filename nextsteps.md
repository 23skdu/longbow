# Longbow Optimization Roadmap

## Future Work

*No pending optimization tasks identified for immediate implementation.*

## Completed (v0.1.5 - Reliability & Tuning)

* [x] **IO_URING for WAL (Linux)**: Implemented `io_uring` backend with full metrics and lifecycle tests.
* [x] **Graph Layout Optimization (CSR-like)**: Implemented `PackedAdjacency` (contiguous arrays) for HNSW neighbors to improve cache locality.
* [x] **Slab Allocation / Custom Arena**: Implemented `internal/memory/arena.go` to reduce GC pressure for small vector allocations.
* [x] **GOGC Auto-Tuning**: Implemented `internal/memory/gc_tuner.go` for dynamic GOGC adjustment based on heap usage.

## Completed (v0.1.4 - Performance & Features)

**Critical Concurrency & FP16**

* [x] **SIMD-Native FP16 Distance**: Implemented AVX-512/NEON Assembly kernels (~3.3x speedup).
* [x] **Optimistic Locking for HNSW**: Implemented sparse clearing and lock-free traversal patterns.
* [x] **Ingestion Pipelining**: Decoupled WAL writing from graph insertion with async backpressure.
* [x] **Zero-Copy Ingestion (FP16)**: Direct Arrow `Float16` array support in Flight server.
* [x] **Remove Global Locks in Metrics**: Cached high-cardinality label handles (17x speedup).

**Search Throughput & Memory**

* [x] **Disk-Based Vector Storage (DiskANN-lite)**: DiskVectorStore with mmap, S3 snapshots, and `madvise`.
* [x] **Bitset-based Filtering w/ SIMD**: Roaring bitmaps for metadata pre-filtering.
* [x] **Early Termination Strategies**: Adaptive `efSearch` based on convergence.
* [x] **Query Caching (LRU)**: Thread-safe LRU cache for embedding results.
* [x] **JIT Compilation**: `wazero`-based JIT for runtime kernel selection (AVX-VNNI).

**Distributed & Network**

* [x] **gRPC/Flight Flow Control**: Dynamic window sizing for high-bandwidth networks.
* [x] **Compressed Vector Transport**: Support for F16/SQ8 vector return formats to save bandwidth.
* [x] **Metrics Optimization & Tuning**: Comprehensive Prometheus coverage and lock reduction.
