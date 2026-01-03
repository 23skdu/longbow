# Longbow Optimization Roadmap

This document outlines the next 20 prioritized improvements for the Longbow vector database, based on a deep analysis of the current codebase state, existing TODOs, and architectural goals.

## Priority 1: Correctness & Data Safety (Critical)

### 1. Fix Buffered WAL Sync Semantics

- **Location**: `internal/store/wal_buffered.go`
- **Issue**: The current `Sync()` method waits for *any* flush to complete, not necessarily the one containing the specific write. This creates a small window where a client thinks data is durable but it isn't.
- **Plan**: Implement a mechanism (e.g., using a monotonic sequence number or a channel per write batch) to ensure `Sync()` blocks until the *specific* offsets are flushed to the backend.

### 2. Implement Proper Checksum Verification on Recovery

- **Location**: `internal/store/wal_buffered.go`, `internal/store/wal_backend.go`
- **Issue**: While CRC32 is calculated on write, the recovery path needs rigorous validation to handle partial writes or corruption during crashes.
- **Plan**: Add a `Verify()` pass during WAL replay that checks CRCs and truncates the log at the first valid corruption point, logging a warning.

### 3. Concurrency Safety in HNSW Graph Growth

- **Location**: `internal/store/arrow_hnsw_insert.go`
- **Issue**: The `Grow()` and dynamic resizing logic involves complex locking (`growMu`, `initMu`). There are potential race conditions when `dims` are initialized lazily.
- **Plan**: Audit and potentially simplify the locking model for resizing. Consider pre-allocating larger chunks or using a more standard RCU (Read-Copy-Update) pattern for the `GraphData` pointer.

### 4. Fix Vector/SQ8 State Consistency

- **Location**: `internal/store/arrow_hnsw_insert.go`
- **Issue**: The code contains checks like `if data.Vectors == nil` inside the hot insert path, with "recovery" logic. This indicates a fragile initialization state.
- **Plan**: Formalize the state machine for index initialization. Ensure that `Vectors` and `VectorsSQ8` arrays are always consistent with `Capacity` before any inserts are allowed.

---

## Priority 2: Performance & Efficiency (High)

### 5. [x] Implement Product Quantization (PQ) Training

- **Location**: `internal/store/arrow_hnsw_insert.go` -> `TrainPQ`
- **Issue**: The `TrainPQ` method is currently a stub/noop.
- **Plan**: Implement a proper PQ training phase that clusters sample vectors to create a codebook. This will significantly reduce memory usage (e.g., 4x-10x reduction) compared to raw float32 or SQ8.

### 6. [x] SIMD Usage for SQ8 Distance Calculations

- **Location**: `internal/store/arrow_hnsw_insert.go`
- **Issue**: The SQ8 distance calculation path in `searchLayerForInsert` currently iterates serially: `// TODO: Batch SIMD`.
- **Plan**: Implement `simd.EuclideanDistanceSQ8Batch` to process multiple distance calculations in parallel using AVX2/AVX-512, matching the optimizations present for float32.

### 7. Adaptive Pre-Filtering Factor

- **Location**: `internal/store/index_search.go`
- **Issue**: `SearchVectors` uses a hardcoded `k * 10` oversampling factor for filtered searches.
- **Plan**: Implement an adaptive estimator based on filter selectivity. If a filter matches 1% of data, we need to search much deeper than if it matches 50%. Use the Bitset count to dynamically adjust the expansion factor.

### 8. Optimize HNSW Lock Contention

- **Location**: `internal/store/arrow_hnsw.go` (and `insert.go`)
- **Issue**: `shardedLocks` are used to protect node updates. A fixed array of 1024 locks (`source % 1024`) is used.
- **Plan**: Profile lock contention under heavy write load. Consider increasing the shard count or moving to a fully lock-free atomic CAS approach for adjaency list updates if feasible.

### 9. Zero-Copy Record Batch Construction

- **Location**: `internal/store/wal_buffered.go`
- **Issue**: The WAL `Write` method serializes `RecordBatch` to a temporary buffer before writing to the main buffer.
- **Plan**: Refactor to write directly to the main ring/buffer to avoid the intermediate allocation and copy, taking advantage of Arrow's memory layout.

### 10. Parallelize Search Within Shards

- **Location**: `internal/store/index_search.go`
- **Issue**: `SearchVectors` processes the candidate list in batches, but arguably could parallelize the distance computations for the *initial* candidate finding if `k` is very large.
- **Plan**: For large `k` or expensive distance metrics, use `errgroup` or a worker pool to compute distances for the visited set in parallel during the graph traversal (Standard HNSW doesn't easily allow this, but the batch distance calc step can be threaded).

---

## Priority 3: Features & Architecture (Medium)

### 11. Implement "Vacuum" / Compaction

- **Location**: `internal/store`
- **Issue**: Deleted vectors (noted by tombstones) remain in the HNSW graph, degrading performance over time.
- **Plan**: Implement a background compaction process that rebuilds the graph or sub-graphs to physically remove deleted nodes and re-optimize connections.

### 12. Persistent Indexing (DiskANN-style)

- **Location**: `internal/store/hnsw_*`
- **Issue**: The current HNSW implementation is memory-resident.
- **Plan**: Research and prototype a disk-backed VQ (Vector Quantization) + Adjacency List approach (inspired by DiskANN) to support datasets larger than RAM.

### 13. Advanced Sharding Strategy

- **Location**: `internal/store/sharded_hnsw.go`
- **Issue**: Sharding is currently linear based on ID (`id / threshold`).
- **Plan**: Implement Consistent Hashing for sharding to allow easier addition/removal of shards without massive data movement. This prepares the system for distributed rebalancing.

### 14. Scalar Quantization (SQ8) Auto-Tuning

- **Location**: `internal/store/scalar_quantization.go`
- **Issue**: SQ8 min/max ranges are likely static or determined once.
- **Plan**: Implement dynamic range re-calibration. As the data distribution shifts, the quantizer should update its bounds (lazily or periodically) to maintain accuracy.

### 15. Pluggable Distance Metrics

- **Location**: `internal/store` & `internal/simd`
- **Issue**: Heavily tied to Euclidean distance.
- **Plan**: Refactor the distance function interface to easily swap in Inner Product (Dot Product) or Cosine Similarity, ensuring SIMD paths exist for all.

---

## Priority 4: Observability & Maintenance (Low)

### 16. Detailed Lock Contention Metrics

- **Location**: `internal/store`
- **Issue**: We have latency metrics, but not lock wait times.
- **Plan**: Add Prometheus metrics for `Lock()` wait times on the HNSW `shardedLocks` and `dataset.dataMu`. This is crucial for performance debugging.

### 17. WAL Compression

- **Location**: `internal/store/wal_buffered.go`
- **Issue**: WAL can grow large quickly.
- **Plan**: Implement block-level compression (Snappy or Zstd) for WAL segments. Since Arrow is already packed, compression might yield modest gains, but it's worth benchmarking.

### 18. Chaos Testing for split-brain

- **Location**: `internal/sharding`
- **Issue**: Distributed mesh robustness needs verification.
- **Plan**: Add a test suite that artificially partitions the network or kills nodes to verify the `SplitBrainDetector` and recovery logic.

### 19. Tool-Use Sandbox Hardening

- **Location**: `internal/server` (Archer interactions)
- **Issue**: As Archer uses Longbow, ensuring resource limits on queries is vital.
- **Plan**: Enforce strict separation of concerns—ensure a complex query cannot OOM the vector store. Add memory arenas per-request.

### 20. Documentation & Examples

- **Location**: `docs/`
- **Issue**: `SearchHybrid` and complex filtering usage isn't well documented.
- **Plan**: Create a "Cookbook" in `docs/` with examples of Hybrid Search, Filtering, and tuning `efConstruction`/`M` parameters.
