# Next Steps & Roadmap

This document outlines the prioritized 15-point roadmap for optimizing, stabilizing, and evolving the Longbow vector database. These items are derived from deep codebase analysis and performance profiling.

## Critical Reliability & Stability

### 1. Fix Silent WAL Flush Failures (Completed)

**Target:** `internal/storage/wal_buffered.go`

- **Problem:** `tryFlush` ignores errors returned by `flushBufferToBackend` (TODO L235). This can lead to silent data loss if the disk fills up or permissions fail.
- **Solution:** Implement an error channel or shutdown latch to halt the server/ingestor immediately upon persistent WAL failures.

### 2. Robust Schema Evolution (Completed)

**Target:** `internal/store/schema_evolution.go`

- **Problem:** Schema changes currently rely on implicit handling or potentially unsafe casting.
- **Solution:** Implement strict versioning in `ApplyDelta`. Add validation logic to ensure backward-compatible additive changes (e.g., adding columns) and reject breaking changes.

### 3. Replace `context.TODO` in Distributed Logic

**Target:** `internal/store/sharded_hnsw.go`

- **Problem:** Use of `context.TODO()` prevents proper timeout and cancellation propagation in distributed scatter/gather operations.
- **Solution:** Thread proper `context.Context` with timeouts from the API layer down to the sharded search execution.

## Ingestion Performance

### 4. WAL Buffer Pooling

**Target:** `internal/storage/wal_buffered.go`

- **Problem:** `swapBufferLocked` allocates a new buffer for every batch, causing significant GC pressure under high write load.
- **Solution:** Implement `sync.Pool` for WAL `PatchableBuffer`s to reuse memory and reduce GC pauses.

### 5. Parallel Column Compaction

**Target:** `internal/store/compaction_streaming.go`

- **Problem:** `CompactDataset` processes columns serially. For datasets with many columns (e.g., high-dim vector + metadata), this is CPU-bound.
- **Solution:** Use an `errgroup` to compact independent columns in parallel, bounded by `GOMAXPROCS`.

### 6. Vectorized Compaction Block Copy

**Target:** `internal/store/compaction_streaming.go`

- **Problem:** Current compaction copies row-by-row even when row mappings are contiguous (TODO L178).
- **Solution:** Detect contiguous ranges in the `mapping` slice and use `copy()` or specialized SIMD `memcpy` to move blocks of data, significantly increasing memory bandwidth utilization.

### 7. Zero-Copy Arrow Integration (Completed)

**Target:** `internal/store/flight_zero_copy_allocator_test.go`

- **Problem:** Integration tests for the zero-copy allocator are marked TODO.
- **Solution:** Finalize `TestFlight_ZeroCopyAllocator_Integration` to ensure the allocator correctly passes ownership to Arrow without copying, reducing memory usage during network transport.

## Search Performance

### 8. Allocation-Free Sentinel Vectors

**Target:** `internal/store/arrow_hnsw_index.go`

- **Problem:** `getSentinelVector` allocates a new slice on every call (TODO L655). While this is an error path, it can degrade performance during race conditions or data misses.
- **Solution:** Use a global pre-allocated zero-vector (read-only) or a `sync.Pool` to return these sentinels.

### 9. HNSW Refinement Optimizations

**Target:** `internal/store/arrow_hnsw_search.go`

- **Problem:** Refinement logic re-resolves the distance computer, potentially re-allocating headers (TODO L253).
- **Solution:** Refactor `resolveHNSWComputer` to accept a `forceFullPrecision` flag, allowing reuse of internal state and avoiding unnecessary checks.

### 10. SIMD-Accelerated Filter Operations

**Target:** `internal/query/bitset.go`

- **Problem:** Bitset intersection/union is scalar.
- **Solution:** Implement AVX2/NEON intrinsics for bitset logic to speed up filtered search over large datasets.

### 11. Optimized Complex Number Distance (Completed)

**Target:** `internal/store/arrow_hnsw_search.go`

- **Problem:** Complex number support currently relies on interface conversion or casting in some paths.
- **Solution:** Implement dedicated, monomorphized distance functions for `complex64` and `complex128` to bypass interface overhead.

## Architecture & Features

### 12. Cross-Encoder Re-ranking Stub

**Target:** `internal/store/hybrid_pipeline.go`

- **Problem:** Cross-encoder scoring is currently a stub (TODO L353).
- **Solution:** Integrate a real scoring interface (e.g., via ONNX runtime or external service) to enable high-quality hybrid search re-ranking.

### 13. Configurable Gossip Advertising

**Target:** `cmd/longbow/main.go`

- **Problem:** Gossip currently binds to the listen address, which breaks in NAT/Kubernetes environments where the external IP differs (TODO L392).
- **Solution:** Add `GOSSIP_ADVERTISE_ADDR` configuration to explicitly set the announced address.

### 14. Expand Integer Type Support in Accessors

**Target:** `internal/store/graph_data_access.go`

- **Problem:** Optimized accessors are missing for `Uint8`, `Int16`, and other integer types (TODO L100).
- **Solution:** Implement specific accessors for all Arrow integer types to avoid slow generic fallback paths.

### 15. Async Index Persistence (Completed)

**Target:** `internal/store/persistence_worker.go`

- **Problem:** Index saving can block the main loop or delay shutdown.
- **Solution:** Move index serialization to a dedicated background worker that creates snapshots periodically without blocking incoming writes.
