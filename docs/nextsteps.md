# Next Steps & Roadmap

This document tracks the remaining work for Performance Optimization and Polymorphic Vector Support.
Completed items have been verified and archived.

## Polymorphic & High-Dimensionality Support

Remaining tasks to support 12 data types and 3072 dimensions.

### 1. 128-3072 Dimension Layout Optimization [x]

**Goal**: Specialized optimizations for power-of-two dimension points.

- **Subtasks**:
  - [x] Implement cache-line padding for specific widths.
  - [x] Add block-based SIMD processing for vectors > 1024 dims.
- **Metrics**: `ahnsw_search_throughput_dims` (Gauge).

### 2. Refactor HNSW Search for Native Polymorphism (Completed)

**Goal**: Decouple graph traversal from float32 assumptions.

- **Subtasks**:
  - [x] Refactor `mustGetVectorFromData` to use generic views without casting.
  - [x] Update `Search` and `Insert` implementation to use the dynamic dispatcher directly for non-float types (e.g. Int8).
- **Metrics**: `ahnsw_polymorphic_search_count` (Counter).

### 3. Enhanced Observability

**Goal**: Expose matrix of performance across all types.

- **Subtasks**:
  - Add granular Prometheus metrics for per-type latency.
  - Implement dimension-bucketed performance tracking.

### 4. High-Dimension Growth Stability

**Goal**: Ensure stability for extremely large vectors (3072 dims).

- **Subtasks**:
  - Optimize `Grow` and `Clone` for large vector pre-allocation.
  - Implement slab-aware memory reclamation.

### 5. Final Integration & Regression

**Goal**: Address legacy debt and validate full matrix.

- **Subtasks**:
  - Fix dimension mismatches in legacy HNSW tests.
  - Final sharded/multi-node validation across all 12 types.

---

## Ingestion Performance Optimizations

Remaining high-priority optimizations for the ingestion pipeline.

### 6. Eliminate Double-Checked Locking in Text Indexing

**Problem**: `runIndexWorker()` uses double-checked locking for inverted index creation, causing contention.
**Solution**:

- Use `sync.Map` for `InvertedIndexes` to eliminate read locks.
- Pre-create inverted indexes during dataset initialization.
- Batch text indexing operations per column.
**Expected Impact**: 10-15% faster hybrid search ingestion.

### 7. Optimize Deferred Connection Updates

**Problem**: `AddBatchBulk()` collects reverse connections with 16-way sharding, but processes serially per shard.
**Solution**:

- Increase sharding from 16 to 256 for better parallelism.
- Use lock-free ring buffer for deferred updates.
- Batch multiple targets per `AddConnectionsBatch` call.
**Expected Impact**: 20-25% faster graph construction in bulk mode.

### 8. Vectorized String Column Indexing

**Problem**: Text indexing processes strings one-by-one with array bounds checks.
**Solution**:

- Batch extract strings.
- Parallelize tokenization across string chunks.
- Use SIMD for whitespace detection/splitting.
**Expected Impact**: 30-40% faster text indexing.

### 9. Lazy BM25 Index Initialization

**Problem**: BM25 index is created eagerly.
**Solution**:

- Defer creation until first hybrid search query.
- Use bloom filter to track presence of text columns.
**Expected Impact**: 20-30% lower memory for vector-only workloads.

### 10. Prefetch Neighbor Chunks During Search

**Problem**: `searchLayerForInsert()` causes cache misses.
**Solution**:

- Prefetch next 4 candidate vectors using `PREFETCHT0`.
- Group candidates by chunk ID.
**Expected Impact**: 10-15% faster search during insertion.

### 11. Batched Metric Updates

**Problem**: Metrics are updated per-job, causing atomic contention.
**Solution**:

- Accumulate metrics in thread-local counters.
- Flush to Prometheus every 100ms.
**Expected Impact**: 5-8% lower CPU overhead.

### 12. Optimistic Locking for Connection Pruning

**Problem**: `PruneConnections()` acquires exclusive lock unnecessarily.
**Solution**:

- Check connection count with atomic load before locking.
- Use RCU-style updates for neighbor lists.
**Expected Impact**: 15-20% lower lock contention.

### 13. Parallel Schema Validation

**Problem**: `ApplyDelta()` validates schema serially.
**Solution**:

- Cache validated schemas in `sync.Map`.
- Skip validation for known schema fingerprints.
- Parallelize field type checking.
**Expected Impact**: 5-10% faster ingestion.

### 14. Automated Schema Evolution & Validation

**Goal**: Fully implement robust, automated schema evolution with comprehensive unit tests as
documented in `schema_evolution.md`.

- **Subtasks**:
  - [ ] Implement automated `Version` incrementing in `ApplyDelta` upon detection of additive
        schema changes.
  - [ ] Add strict validation logic to ensure incoming `RecordBatch` schemas are backward-compatible (additive only).
  - [ ] Develop a suite of unit and integration tests covering various evolution scenarios
        (adding columns, reordering, type mismatches).
  - [ ] Implement schema fingerprinting to skip validation for identical schemas in the ingestion hot path.
**Expected Impact**: Improved data integrity and developer experience during application evolution.
