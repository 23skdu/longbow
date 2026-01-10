# Next Steps: Optimization Roadmap (v0.1.4+)

Based on deep analysis of `0.1.3` performance reports, specifically the concurrency bottleneck in FP16 storage and ingestion overhead, here is the prioritized 15-point optimization plan.

## Phase 1: Critical Concurrency & FP16 (v0.1.4 Goal)

1. **[CRITICAL] SIMD-Native FP16 Distance**:
    * **Status**: DONE (Implemented NEON Assembly ~3.3x speedup for Cosine).
    * **Problem**: Current FP16 search implementation converts `float16` to `float32` on-the-fly for every distance calculation, causing massive CPU overhead and 20x slowdown under concurrent load.
    * **Fix**: Implement `L2` and `Cosine` distance functions directly in Assembly (AVX-512/NEON) that operate on `float16` buffers without upcasting.

2. **[CRITICAL] Optimistic Locking for HNSW**:
    * **Status**: DONE (Implemented Sparse Clearing).
    * **Problem**: High lock contention on `Visited` bitsets and Node locks during concurrent searches.
    * **Fix**: Implemented sparse clearing for visited sets (`O(ef)` vs `O(N)`) and verified `ArrowHNSW` already uses optimistic node locking.

3. **[CRITICAL] Ingestion Pipelining**:
    * **Status**: DONE (Decoupled WAL from Memory, added Backpressure).
    * **Problem**: `DoPut` is blocked by synchronous WAL writes and graph updates.
    * **Fix**: Decouple WAL writing (batching) from graph insertion. Return success once durable in WAL, process graph update asynchronously with backpressure.

4. **Zero-Copy Ingestion (FP16)**:
    * **Problem**: Ingestion converts `float32` -> `float16`.
    * **Fix**: Allow clients to send Arrow `Float16` arrays directly. Implement `Cast` kernels in the Arrow Flight server to handle incoming types efficiently.

5. **Remove Global Locks in Metrics**:
    * **Problem**: Prometheus constructs `WithLabelValues` often acquire a global map lock.
    * **Fix**: Cache metrics handles in local structures or use `ConstMetric` where appropriate to avoid hot-path locking.

## Phase 2: Search Throughput & Memory

1. **Disk-Based Vector Storage (DiskANN-lite)**:
    * **Problem**: RAM limit caps dataset size.
    * **Fix**: Offload vector data to NVMe SSD (mmap) while keeping the HNSW graph in memory. Use `madvise` to hint OS about access patterns.

2. **Bitset-based Filtering w/ SIMD**:
    * **Problem**: Metadata filtering checks each ID individually.
    * **Fix**: Construct roaring bitmaps for low-cardinality metadata fields. Use SIMD `AND/OR` operations to pre-filter candidate lists before traversing the graph.

3. **Graph Layout Optimization**:
    * **Problem**: Pointer chasing in HNSW graph (linked list of neighbors).
    * **Fix**: Store neighbor lists as contiguous arrays (CSR-like) to improve CPU cache prefetching and reduce TLB misses.

4. **Early Termination Strategies**:
    * **Problem**: Search inspects too many candidates (`efSearch`).
    * **Fix**: Implement adaptive `efSearch` that stops early if distance distribution indicates convergence or sufficient recall.

5. **Query Caching (LRU)**:
    * **Problem**: Repeated queries for popular terms re-execute full search.
    * **Fix**: Implement a small LRU cache for query embeddings -> result lists key-value store, with TTL validity.

## Phase 3: Distributed & Network

1. **gRPC/Flight Flow Control Tuning**:
    * **Problem**: `DoGet` stream stalling or buffering.
    * **Fix**: Tune gRPC flow control windows (`InitialWindowSize`, `InitialConnWindowSize`) to match high-bandwidth scenarios (10GbE+).

2. **Scatter-Gather Optimization**:
    * **Problem**: Coordinator waits for the slowest node (tail latency).
    * **Fix**: Implement speculative execution (send to replica) if primary is slow, and dynamic timeout adjustments.

3. **Compressed Vector Transport**:
    * **Problem**: Network bandwidth bottleneck.
    * **Fix**: Return SQ8 (8-bit quantized) vectors in search results by default, only returning full float32 if explicitly requested.

4. **Batch Search API**:
    * **Problem**: Single-vector search overhead.
    * **Fix**: Expose a batched search API allowing clients to send N query vectors in one request, amortizing network and dispatch initialization costs.

5. **JIT Compilation for Distance Kernels**:
    * **Problem**: Static dispatch misses runtime-specific optimizations.
    * **Fix**: Use `wazero` or similar logic to select/generate kernels at runtime based on exact CPU flags (e.g., AVX-VNNI) detected at startup.
