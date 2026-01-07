# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on stability,
code correctness, and performance.

## Priority 1: Performance & Scalability

### 1. Optimize Query Coordinator Path

- **Context**: Current scatter-gather (`GlobalSearch`) is naive: serial waiting for all peers, JSON serialization,
  and full sort-merge.
- **Location**: `internal/store/global_search.go`, `internal/store/sharded_hnsw.go`.
- **Plan**:
  - **Streaming Merge**: Implement a concurrent streaming merger using a Min-Heap to process peer results as they
    arrive, overlapping network I/O with sorting.
  - **Replica Hedging**: Send queries to multiple replicas (if available) and accept the fastest response to minimize
    tail latency (p99).
  - **Arrow Serialization**: Switch Flight payloads from JSON to native Arrow RecordBatches to eliminate serialization overhead.

### 2. Adaptive Search Limits

- **Context**: Search limit currently uses hardcoded oversampling (`k * 10`), which may be inefficient or insufficient.
- **Location**: `internal/store/index_search.go`.
- **Plan**: Implement adaptive oversampling based on filter selectivity to balance recall and latency dynamically.

## Priority 2: Stability & Core Architecture

### 3. Robust Vector Storage

- **Context**: Current insertion tests rely on temporary stubs.
- **Location**: `internal/store/arrow_insert_test.go`.
- **Plan**: Implement comprehensive verification to ensure data durability.

### 4. WAL Synchronization

- **Context**: `AsyncFsyncer` is not fully integrated.
- **Location**: `internal/store/batched_wal.go`.
- **Plan**: Hook up async fsyncs and verify crash recovery.

### 5. Distributed Consensus (Raft)

- **Impact**: **High**. Critical for strong consistency in cluster metadata.
- **Location**: `internal/consensus` (New)
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state.

### 6. Refactor `internal/store` Monolith

- **Impact**: **High**. The `internal/store` package contains 360+ files.
- **Location**: `internal/store`
- **Plan**: Split into dedicated packages: `internal/index`, `internal/storage`, `internal/query`.

## Priority 3: Features & Observability

### 7. Filter Optimization

- **Context**: Filtering lacks bitset support/vectorization.
- **Location**: `internal/store/arrow_hnsw_index.go`.
- **Plan**: Vectorize filter evaluation logic (SIMD) and introduce bitset-based filtering.

### 8. Async S3 Checkpointing

- **Impact**: **Medium**. Prevents I/O stalls during snapshot operations.
- **Location**: `internal/store/s3_backend.go`
- **Plan**: Implement non-blocking, multipart uploads for snapshots.

### 9. Distributed Tracing (OpenTelemetry)

- **Impact**: **Medium**. Provides visibility into request latency.
- **Location**: `internal/store/global_search.go`
- **Plan**: Instrument paths with OpenTelemetry spans.

### 10. Binary Quantization (BQ)

- **Impact**: **High**. Offers extreme compression (32x).
- **Location**: `internal/store/quantization.go`
- **Plan**: Implement Binary Quantization support.
