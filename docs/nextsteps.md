# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Top Priority Items (Performance & Scalability)

### 1. IO_URING for WAL (Linux)

- **Impact**: **High**. Current profiling shows heavy `syscall` usage during writes.
- **Plan**: Implement `io_uring` backend for the Write Ahead Log (WAL) to batch I/O operations and
  strictly reduce syscall overhead on Linux.

### 2. HNSW Search Context Pooling

- **Impact**: **High**. Profiling revealed ~400MB allocations in `InsertWithVector` due to `visited` sets and queues.
- **Plan**: Implement aggression `sync.Pool` usage for `searchCtx` (visited bitsets, candidate queues)
  to achieve near-zero allocation inserts.

### 3. GOGC Auto-Tuning (Memory Ballast)

- **Impact**: **High**. Prevents OOMs and optimizes GC CPU usage.
- **Plan**: Implement a memory ballast or dynamic GOGC tuner that adjusts based on `GOMEMLIMIT` and
  current heap usage.

### 4. Software Prefetching for Graph Search

- **Impact**: **Medium/High**. Search latency is dominated by random memory access.
- **Plan**: Use assembly or unsafe directives to prefetch HNSW neighbor nodes into CPU cache during
  graph traversal (pipelined lookup).

### 5. Zero-Copy Flight `DoGet`

- **Impact**: **Medium**.
- **Plan**: Optimize the `DoGet` read path to stream internal Arrow RecordBatches directly to the wire without re-serialization/copying.

### 6. Sharded Graph Locks

- **Impact**: **Medium**. Reduces contention during high concurrency.
- **Plan**: Replace global `GraphData` RWMutex with fine-grained sharded locks or atomic/RCU patterns
  for adjacency list updates.

### 7. Optimized Delete (Atomic Tombstones)

- **Impact**: **Medium**.
- **Plan**: Enhance the deletion mechanism to use a high-performance, lock-free global atomic bitset (RoaringBitmap)
  for tombstones, checked efficiently during traversal.

### 8. Vectorized Metadata Filtering

- **Impact**: **Medium**.
- **Plan**: Utilize Arrow Compute or custom SIMD kernels to evaluate complex metadata filters (AND/OR/NOT) over columns, rather than row-by-row checks.

### 9. Compaction Rate Limiting

- **Impact**: **Low/Medium**. Improves tail latency.
- **Plan**: Implement token-bucket rate limiting for background compaction and snapshotting to prevent I/O
  saturation from affecting foreground latency.

### 10. Slab Allocation for Fixed 384-Dim Vectors

- **Impact**: **High**. reduces GC pressure.
- **Plan**: Implement a custom `Arena` allocator that reserves massive "Slabs" (e.g., 2MB pages) and slots vectors linearly to eliminate billions of small allocations.

### 11. Native Float16 Read-Path Optimization (SIMD)

- **Impact**: **High**. Current FP16 storage is 20x slower in concurrent workloads due to CPU conversion costs.
- **Plan**: Implement SIMD kernels that operate directly on `float16` buffers for L2 and Cosine distance calculations, eliminating the need for f16->f32 conversion during graph traversal.

### 12. AVX-512 Optimized L2 Kernel

- **Impact**: **High**.
- **Plan**: Write a hand-tuned Assembly kernel that unrolls the loop exactly 24 times (384/16) for 384-dim vectors.

---

## Future Optimizations (Backlog)

### Sharded Ingestion Routing

- Client-side deterministic routing to split batches based on `Hash(ID) % ShardCount` to reduce lock contention.

### Tombstone-Aware HNSW Repair

- Background worker that scans tombstones and locally "wires around" deleted nodes to maintain graph quality without full rebuilds.

### Explicit "Drop Dataset" Fast Path

- Ensure `DeleteDataset` simply unlinks the pointer (RCU) and schedules async cleanup for instant resource release.

### Pre-Computed "Medoids" for Routing

- Maintain ~100 centroids to skip top HNSW layers and jump close to the target immediately.

### Zstd Dictionary Compression

- Train a Zstd dictionary on the first 10k vectors to compress subsequent vectors for efficient WAL storage.

### Memory-Mapped "Read-Only" Mode

- "Freeze" a dataset to a compact mmap-friendly file format for zero-heap usage on static datasets.

### Adaptive HNSW Construction

- Dynamically adjust `M` (max connections) based on intrinsice dimensionality of initial vectors.

## Technical Debt & Architecture

### Distributed Consensus (Raft)

- **Location**: `internal/consensus`
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state.

### Refactor `internal/store` Monolith

- **Location**: `internal/store`
- **Plan**: Split into `internal/index`, `internal/storage`, and `internal/query`.

### Cross-Encoder Re-ranking

- **Location**: `internal/store/hybrid_pipeline.go`
- **Plan**: Integrate a real model (e.g., via ONNX or external service).

### Distributed Tracing (OpenTelemetry)

- **Location**: `internal/store/global_search.go`
- **Plan**: Instrument critical paths with OpenTelemetry spans.

---

## Recently Completed

- **Zero-Copy Flight Ingestion (Allocators)**: Implemented `TrackingAllocator` and injected it into `DoPut` to enable zero-copy "network-to-memory" ingestion and verified leak-free buffer lifecycle.
- **12GB Scalability Benchmarks**: Verified performance and scaling behavior for up to 25k 384d vectors on a single node with strict memory limits.
- **HNSW Parallel Bulk Load**: Optimized ingestion for large batches (>1k) using parallel layer generation and graph updates.
- **Auto-Sharding Control**: Implemented `LONGBOW_AUTO_SHARDING_ENABLED`.
- **Hybrid Search Pipeline**: Added Reranking and Fusion stages.
- **Spatial Index**: VP-Tree for Mesh Routing.
- **WAL Buffer Recycling**: Optimized write path.
