# Next Steps: Optimization Roadmap (v0.1.5+)

## Phase 4: Reliability & Advanced System Tuning (Current Priority)

1. **IO_URING for WAL (Linux)**:
    * **Problem**: Heavy `syscall` overhead during synchronous WAL writes, especially at high throughput.
    * **Solution**: Implement `io_uring` backend for batching I/O operations and reducing context switches on Linux kernels.

2. **Slab Allocation / Custom Arena**:
    * **Problem**: High GC pressure from billions of small vector allocations (slices).
    * **Solution**: Implement a custom `Arena` allocator that reserves large "Slabs" (e.g., 2MB) and slots vectors linearly.

3. **GOGC Auto-Tuning (Memory Ballast)**:
    * **Problem**: Static GOGC settings can lead to either OOMs or excessive CPU usage during garbage collection.
    * **Solution**: Implement a dynamic tuner that adjusts GOGC based on `GOMEMLIMIT` and current heap usage.

4. **Distributed Consensus (Raft)**:
    * **Problem**: Current coordinator logic relies on simple health checks; no strong consistency for cluster state changes.
    * **Solution**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster membership and metadata consistently.

5. **Graph Layout Optimization (CSR-like)**:
    * **Problem**: Pointer chasing in HNSW graph (linked lists) causes cache misses.
    * **Solution**: Store neighbor lists as contiguous arrays to improve CPU cache prefetching and reduce TLB misses.

---

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
