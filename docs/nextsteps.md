# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Top 10 Priority Items

### 3. Distributed Consensus (Raft)

- **Impact**: **High**. Critical for strong consistency in cluster metadata, shard mapping, and automated failover.
- **Location**: `internal/consensus` (New)
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state.

### 4. Refactor `internal/store` Monolith

- **Impact**: **High**. The `internal/store` package has grown excessively, hindering maintainability.
- **Location**: `internal/store`
- **Plan**: Split into `internal/index`, `internal/storage`, and `internal/query`.

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

### Distributed Tracing (OpenTelemetry)

- **Location**: `internal/store/global_search.go`
- **Plan**: Instrument critical paths with OpenTelemetry spans.

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
- **Multi-Batch HNSW Support**: Added support for sequential and concurrent batch insertions in `ArrowHNSW` with proper test coverage.
- **Filter Evaluator Reuse**: Implemented object pooling for `FilterEvaluator` and vectorized filter application in `DoGet`.
- **SQ8 SIMD Fix**: Fixed `euclideanSQ8NEONKernel` linking and verified correctness with `TestEuclideanDistanceSQ8`.
- **Testing & Benchmarks**: Added HNSW batch modification tests and `ColumnInvertedIndex` high-cardinality benchmarks.
