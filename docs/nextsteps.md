# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on distributed
reliability, architecture refactoring, and advanced indexing features.

## Priority 1: Distributed Reliability & Persistence

### 1. Distributed Consensus (Raft)

- **Impact**: **High**. Critical for strong consistency in cluster metadata, shard mapping, and automated failover.
- **Location**: `internal/consensus` (New)
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state and implement a
  leader-based coordinator for metadata updates.

### 2. Async S3 Checkpointing

- **Impact**: **Medium**. Prevents I/O stalls during snapshot operations in cloud environments.
- **Location**: `internal/store/s3_backend.go`
- **Plan**: Implement non-blocking, multipart uploads for snapshots to ensure background persistence
  doesn't impact write throughput.

## Priority 2: Core Architecture & Refactoring

### 3. Refactor `internal/store` Monolith

- **Impact**: **High**. The `internal/store` package has grown excessively (360+ files),
  hindering maintainability and increasing compile times.
- **Location**: `internal/store`
- **Plan**: Split into dedicated, cohesive packages:
  - `internal/index` (HNSW, Brute-Force, Quantization logic)
  - `internal/storage` (WAL, Snapshot, Data structures)
  - `internal/query` (Search coordinator, Aggregation, Filtering)

### 4. Arrow HNSW Resource Cleanup

- **Impact**: **Medium**. Essential for preventing memory leaks in long-running processes,
  especially with dynamic index creation/deletion.
- **Location**: `internal/store/arrow_hnsw_index.go`
- **Plan**: Complete the `Close()` method to properly release all Arrow memory pools and
  background worker resources.

## Priority 3: Advanced Query Features

### 5. Hybrid Search Pipeline Enhancements

- **Impact**: **Medium**. Enables richer search capabilities.
- **Location**: `internal/store/hybrid_pipeline.go`
- **Plan**: Complete implementation of pipeline stages marked as TODO, specifically cross-encoder re-ranking stubs.

### 6. Sharded HNSW Index Management

- **Impact**: **Low-Medium**. Necessary for horizontal scaling.
- **Location**: `internal/store/sharded_hnsw.go`
- **Plan**: Address shard lifecycle TODOs, including dynamic shard resizing and background
  compaction of sharded layers.

## Priority 4: Observability

### 7. Distributed Tracing (OpenTelemetry)

- **Impact**: **Medium**. Provides visibility into request latency across the scatter-gather path.
- **Location**: `internal/store/global_search.go`, `internal/sharding/scatter_gather.go`
- **Plan**: Instrument critical paths with OpenTelemetry spans to identify bottlenecks in the
  distributed query flow.

---

## Recently Completed

- **Query Coordinator Path Optimization**: Implemented Streaming Merge (Min-Heap), Replica Hedging, and
  Arrow Serialization (Flight `DoGet`).
- **Adaptive Search Limits**: Implemented auto-expansion of search limits based on filter selectivity.
- **WAL Synchronization & Durability**: Integrated `AsyncFsyncer` and fixed snapshot/WAL truncation bugs.
- **HNSW Stability**: Resolved critical data races in `ensureChunk` and implemented nil vector safeguards
  for SIMD operations.
