# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Top 10 Priority Items (Performance & Scalability)

### 1. "Network-to-Memory" Zero-Copy Integration (Allocators)

- **Impact**: **High**. Eliminates the initial copy from network buffer to application memory,
  critical for 10GbE+ throughput.
- **Plan**:
    1. **Reproduction & Benchmark**: Write unit tests (`TestFlight_ZeroCopyAllocator`) to establish baseline allocation/copy metrics for current Flight `DoPut`.
    2. **Custom Allocator Prototype**: Implement a custom `memory.Allocator` that tracks buffer sources and
       allows lifecycle hooking.
    3. **IPC Reader Injection**: Modify `DoPut`/`DoExchange` to inject this custom allocator into
       `flight.NewRecordReader` (requires `ipc.WithAllocator`).
    4. **Buffer Lifecycle validation**: Implement strict checks for `Retain`/`Release` cycles to ensure
       network buffers aren't prematurely reclaimed or leaked.
    5. **Integration**: Wire the zero-copy reader into the main `VectorStore` ingestion pipeline,
       ensuring safety with async indexing.
    6. **Validation**: Benchmark memory bandwidth utilization and GC pressure to confirm "true"
       zero-copy behavior.

### 2. IO_URING for WAL (Linux)

- **Impact**: **High**. Current profiling shows heavy `syscall` usage during writes.
- **Plan**: Implement `io_uring` backend for the Write Ahead Log (WAL) to batch I/O operations and
  strictly reduce syscall overhead on Linux.

### 3. HNSW Search Context Pooling

- **Impact**: **High**. Profiling revealed ~400MB allocations in `InsertWithVector` due to `visited` sets and queues.
- **Plan**: Implement aggression `sync.Pool` usage for `searchCtx` (visited bitsets, candidate queues)
  to achieve near-zero allocation inserts.

### 4. GOGC Auto-Tuning (Memory Ballast)

- **Impact**: **High**. Prevents OOMs and optimizes GC CPU usage.
- **Plan**: Implement a memory ballast or dynamic GOGC tuner that adjusts based on `GOMEMLIMIT` and
  current heap usage.

### 5. Software Prefetching for Graph Search

- **Impact**: **Medium/High**. Search latency is dominated by random memory access.
- **Plan**: Use assembly or unsafe directives to prefetch HNSW neighbor nodes into CPU cache during
  graph traversal (pipelined lookup).

### 6. Zero-Copy Flight `DoGet`

- **Impact**: **Medium**.
- **Plan**: Optimize the `DoGet` read path to stream internal Arrow RecordBatches directly to the wire without re-serialization/copying.

### 7. Sharded Graph Locks

- **Impact**: **Medium**. Reduces contention during high concurrency.
- **Plan**: Replace global `GraphData` RWMutex with fine-grained sharded locks or atomic/RCU patterns
  for adjacency list updates.

### 8. Optimized Delete (Atomic Tombstones)

- **Impact**: **Medium**.
- **Plan**: Enhance the deletion mechanism to use a high-performance, lock-free global atomic bitset (RoaringBitmap)
  for tombstones, checked efficiently during traversal.

### 9. Vectorized Metadata Filtering

- **Impact**: **Medium**.
- **Plan**: Utilize Arrow Compute or custom SIMD kernels to evaluate complex metadata filters (AND/OR/NOT) over columns, rather than row-by-row checks.

### 10. Compaction Rate Limiting

- **Impact**: **Low/Medium**. Improves tail latency.
- **Plan**: Implement token-bucket rate limiting for background compaction and snapshotting to prevent I/O
  saturation from affecting foreground latency.

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

- **HNSW Parallel Bulk Load**: Optimized ingestion for large batches (>1k) using parallel layer generation
  and graph updates.
- **Auto-Sharding Control**: Implemented `LONGBOW_AUTO_SHARDING_ENABLED`.
- **Performance Benchmarking**: Comprehensive 5k-25k vector benchmarks.
- **Hybrid Search Pipeline**: Added Reranking and Fusion stages.
- **Spatial Index**: VP-Tree for Mesh Routing.
- **WAL Buffer Recycling**: Optimized write path.
