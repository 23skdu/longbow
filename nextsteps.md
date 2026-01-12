# Next Steps: Performance Optimization & Reliability

Based on deep analysis of `bench_9k` and `final` pprof data, we have identified key bottlenecks in Memory Allocation (`bytes.growSlice` accounts for 88% of Heap) and CPU efficiency during ingestion.

## High-Priority Performance Optimizations

### 1. Zero-Copy Arrow IPC & Buffer Pooling

**Problem**: 88% of heap allocations are `bytes.growSlice` triggered by `arrow/ipc` and `grpc` serialization.
**Solution**: Implement a custom `memory.Allocator` for Arrow that utilizes a fixed-size, pre-allocated buffer pool. Use `Release()` callbacks to return buffers to the pool instead of relying on GC.
**Expected Impact**: Reduce GC pause times by 50%, increase DoGet/DoPut throughput.

### 2. Pre-Warm HNSW Index Structures

**Problem**: Initial ingestion (3k vectors) is 10x slower (11 MB/s) than peak (360 MB/s) due to lazy initialization of graph layers.
**Solution**: Implement `PreWarm()` method to allocate initial graph layers and `visited` bitsets at startup or upon first write intention.
**Expected Impact**: Eliminate "cold start" penalty; consistent 200MB/s+ ingestion from start.

### 3. Pipelined HNSW Insertion

**Problem**: `runIndexWorker` shows serial execution of "Search Layer 0" -> "Link Neighbors".
**Solution**: Decouple the search phase (CPU intensive) from the linking phase (Memory/Lock intensive). Use a ring buffer to pass search results to a dedicated linker goroutine.
**Expected Impact**: 20-30% higher ingestion throughput on multi-core systems.

### 4. Optimize NEON SIMD Distance Calculation

**Problem**: Generic SIMD dispatch overhead is visible in CPU profiles.
**Solution**: For ARM64 (M-series), hardcode the NEON path using Go assembly or intrinsics to skip the runtime dispatch check for `L2Distance` and `Cosine`.
**Expected Impact**: 10-15% reduction in search latency.

### 5. Bitmap-Based Filtering

**Problem**: Filtered search iterates over metadata rows.
**Solution**: Use `roaring bitmaps` or `simd-bitmaps` to pre-calculate filter masks for commonly queried categories. Apply bitwise AND with HNSW adjacency lists.
**Expected Impact**: O(1) filtering overhead instead of O(N).

### 6. Adaptive gRPC Buffer Tuning

**Problem**: `google.golang.org/grpc/mem.(*simpleBufferPool)` is a significant allocator.
**Solution**: Tune `grpc.ReadBufferSize` and `grpc.WriteBufferSize` based on observed payload sizes (typically 4KB-16KB for search, 1MB+ for bulk loads).
**Expected Impact**: Reduced memory churn.

### 7. Dictionary Encoding for Metadata

**Problem**: Repeating string values ("cat_1", "cat_2") consume bandwidth and memory.
**Solution**: Automatically dictionary-encode string columns with low cardinality (<1000 unique values).
**Expected Impact**: 50-70% reduction in metadata storage and bandwidth.

### 8. Query Result Caching

**Problem**: Identical queries (e.g., from dashboards) re-execute full search.
**Solution**: Implement an LRU cache keyed by `hash(query_vector + k + filter)`. Cache only the top-K IDs.
**Expected Impact**: Near-zero latency for repeated queries.

### 9. Batched Tombstone Compaction

**Problem**: Real-time compaction fights for lock contention.
**Solution**: Accumulate tombstones in a lock-free bitset and schedule "Stop-the-World" (milliseconds) compaction only when deleted ratio > 10% or system is idle.
**Expected Impact**: Smoother tail latency (P99) during heavy writes.

### 10. Request Hedging

**Problem**: Tail latency (P99) spikes to 5ms+ at load.
**Solution**: Send search requests to 2 replicas (if available) and take the first response. Cancel the second request.
**Expected Impact**: P99 latency approaches P50.

### 11. Tiered Storage (SSD offloading)

**Problem**: Memory limit (6GB) restricts dataset size.
**Solution**: Move older or less-accessed graph layers to MMap'd SSD storage (`DiskANN` style), keeping only Level 0 and Entry Point in RAM.
**Expected Impact**: Support 10x larger datasets on same hardware.

### 12. Topic-Sharding for Hybrid Search

**Problem**: Hybrid search broadcasts to all nodes.
**Solution**: Use a consistent hash ring on the "text query" terms to route specific topics to specific shards, reducing scatter-gather fanout.
**Expected Impact**: Linear scaling for hybrid search throughput.

### 13. Fast-Path SearchByID

**Problem**: `SearchByID` currently may trigger a scan or graph search if ID mapping is slow.
**Solution**: Maintain a dense `[]offset` lookup table for numeric IDs to jump directly to the vector location.
**Expected Impact**: O(1) ID lookups.

### 14. Dynamic Batch Sizing

**Problem**: Fixed batch size (1000) is suboptimal for varying network conditions.
**Solution**: Client-side adaptive batching that increases batch size as long as latency stays within an SLA (e.g., 50ms).
**Expected Impact**: Optimal throughput-latency trade-off without manual tuning.

## Completed

- **Zero-Copy Allocator for Arrow** (Implemented)
- **Pre-Warm HNSW Index** (Implemented)
- **Pipelined HNSW Insertion** (Implemented)
- **Simd Optimizations** (Implemented)
- **Index Pipeline Debugging** (Completed 2026-01-11)
  - Fixed negative caching of empty search results
  - Added "op" alias support for filter operator field
  - Implemented case-insensitive operator matching
