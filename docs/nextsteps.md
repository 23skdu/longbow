# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Top 10 Priority Items (Performance & Scalability)

### 1. IO_URING for WAL (Linux)

- **Impact**: **High**. Current profiling shows heavy `syscall` usage during writes.
- **Plan**: Implement `io_uring` backend for the Write Ahead Log (WAL) to batch I/O operations and strictly reduce syscall overhead on Linux.

### 2. HNSW Search Context Pooling

- **Impact**: **High**. Profiling revealed ~400MB allocations in `InsertWithVector` due to `visited` sets and queues.
- **Plan**: Implement aggression `sync.Pool` usage for `searchCtx` (visited bitsets, candidate queues) to achieve near-zero allocation inserts.

### 3. Product Quantization (PQ)

- **Impact**: **High**. Reduces memory footprint and bandwidth by 4x-8x.
- **Plan**: Implement Product Quantization compression for the main vector index, allowing much larger datasets to fit in RAM.

### 4. Software Prefetching for Graph Search

- **Impact**: **Medium/High**. Search latency is dominated by random memory access.
- **Plan**: Use assembly or unsafe directives to prefetch HNSW neighbor nodes into CPU cache during graph traversal (pipelined lookup).

### 5. Zero-Copy Flight `DoGet`

- **Impact**: **Medium**.
- **Plan**: Optimize the `DoGet` read path to stream internal Arrow RecordBatches directly to the wire without re-serialization/copying.

### 6. GOGC Auto-Tuning (Memory Ballast)

- **Impact**: **High**. Prevents OOMs and optimizes GC CPU usage.
- **Plan**: Implement a memory ballast or dynamic GOGC tuner that adjusts based on `GOMEMLIMIT` and current heap usage.

### 7. Sharded Graph Locks

- **Impact**: **Medium**. Reduces contention during high concurrency.
- **Plan**: Replace global `GraphData` RWMutex with fine-grained sharded locks or atomic/RCU patterns for adjacency list updates.

### 8. Optimized Delete (Atomic Tombstones)

- **Impact**: **Medium**.
- **Plan**: Enhance the deletion mechanism to use a high-performance, lock-free global atomic bitset (RoaringBitmap) for tombstones, checked efficiently during traversal.

### 9. Vectorized Metadata Filtering

- **Impact**: **Medium**.
- **Plan**: Utilize Arrow Compute or custom SIMD kernels to evaluate complex metadata filters (AND/OR/NOT) over columns, rather than row-by-row checks.

### 10. Compaction Rate Limiting

- **Impact**: **Low/Medium**. Improves tail latency.
- **Plan**: Implement token-bucket rate limiting for background compaction and snapshotting to prevent I/O saturation from affecting foreground latency.

---

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

- **Auto-Sharding Control**: Implemented `LONGBOW_AUTO_SHARDING_ENABLED` to prevent memory instability during benchmarks.
- **Performance Benchmarking**: Comprehensive 5k-25k vector benchmarks with pprof analysis.
- **Hybrid Search Pipeline Enhancements**: Completed implementation of pipeline stages.
- **Spatial Index for Mesh Routing**: Replaced linear scan with VP-Tree.
- **WAL Buffer Recycling**: Implemented buffer reusing.
- **Async Fsync**: Integrated `AsyncFsyncer`.
