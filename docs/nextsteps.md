# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Top 10 Priority Items

### 1. Fix SQ8 SIMD Distance Calculation

- **Impact**: **Critical**. `TestEuclideanDistanceSQ8` is currently failing (returning 0). This indicates broken SIMD
  or fallback logic for quantized vectors.
- **Location**: `internal/simd/sq8_test.go`, `internal/simd`
- **Plan**: Debug and fix the SQ8 distance implementation immediately.

### 2. Multi-Batch HNSW Support

- **Impact**: **High**. `arrow_hnsw_index.go` contains a TODO: "Handle multiple batches. Currently binding to Batch 0."
  This prevents the index from scaling beyond a single Arrow RecordBatch.
- **Location**: `internal/store/arrow_hnsw_index.go`
- **Plan**: Update logic to iterate through multiple batches in `dataset` or flatten/virtualize the view.

### 3. Distributed Consensus (Raft)

- **Impact**: **High**. Critical for strong consistency in cluster metadata, shard mapping, and automated failover.
- **Location**: `internal/consensus` (New)
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state.

### 4. Refactor `internal/store` Monolith

- **Impact**: **High**. The `internal/store` package has grown excessively, hindering maintainability.
- **Location**: `internal/store`
- **Plan**: Split into `internal/index`, `internal/storage`, and `internal/query`.

### 5. Filter Evaluator Reuse

- **Impact**: **Medium/High**. `store_query.go` TODO notes that we create a new filter evaluator for every batch.
- **Location**: `internal/store/store_query.go`
- **Plan**: Implement object pooling for evaluators or make them stateless/reusable.

### 6. Cross-Encoder Re-ranking Implementation

- **Impact**: **Medium**. We currently have a stub `CrossEncoderReranker`.
- **Location**: `internal/store/hybrid_pipeline.go`
- **Plan**: Integrate a real model (e.g., via ONNX or external service) to provide high-quality re-ranking.

### 7. Reverse Location Index (Location -> VectorID)

- **Impact**: **Medium**. Exact filter mapping currently uses a heuristic or slow scan.
- **Location**: `internal/store/hybrid_pipeline.go`
- **Plan**: Add a reverse index map to `HNSWIndex` or `ChunkedLocationStore` for O(1) lookups.

---

## Additional Technical Debt & TODOs

### Sharded HNSW Index Management

- **Location**: `internal/store/sharded_hnsw.go`
- **Plan**: Address shard lifecycle TODOs, including dynamic shard resizing and background compaction.

### Distributed Tracing (OpenTelemetry)

- **Location**: `internal/store/global_search.go`
- **Plan**: Instrument critical paths with OpenTelemetry spans.

### Testing

- **Coverage**: Add tests for multi-batch modifications in HNSW.
- **Benchmarks**: Benchmark `ColumnInvertedIndex` for high-cardinality columns.

---

## Recently Completed

- **Hybrid Search Pipeline Enhancements**: Completed implementation of pipeline stages, including exact match filtering
  via `ColumnInvertedIndex` and a second-stage re-ranking interface with cross-encoder stubs.
- **Spatial Index for Mesh Routing**: Replaced linear scan in `Region` router with VP-Tree for scalable lookups.
- **WAL Buffer Recycling**: Implemented buffer reusing for WAL batch compression to reduce GC pressure.
- **Query Coordinator Path Optimization**: Implemented Streaming Merge (Min-Heap), Replica Hedging, and Arrow Serialization.
- **Adaptive Search Limits**: Implemented auto-expansion of search limits based on filter selectivity.
- **WAL Synchronization & Durability**: Integrated `AsyncFsyncer`.
- **Async S3 Checkpointing**: Implemented non-blocking, multipart uploads.
- **Arrow HNSW Resource Cleanup**: Fully implemented `Close()` methods.
- **HNSW Stability**: Resolved critical data races in `ensureChunk` and safeguards for SIMD.
- **ID Resolution Optimization**: Replaced linear scan with O(1) PrimaryIndex lookup for `SearchByID` operations.
