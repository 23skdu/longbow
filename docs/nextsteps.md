# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on stability, code correctness, and performance.

<<<<<<< HEAD
## Priority 1: Stability & Correctness (High Impact)

### 1. Robust Vector Storage

- **Context**: Current insertion tests rely on temporary stubs (`// TODO: Implement when vector storage is ready`).
- **Location**: `internal/store/arrow_insert_test.go`, `internal/store/arrow_insert_properties_test.go`.
- **Plan**: Implement comprehensive vector storage verification to ensure data durability and retrieval correctness.

### 2. WAL Synchronization

- **Context**: `AsyncFsyncer` is not fully integrated with the `WALBackend` interface (`// TODO: Integrate AsyncFsyncer`).
- **Location**: `internal/store/batched_wal.go`.
- **Plan**: Ensure the Write-Ahead Log primarily guarantees data safety by properly hooking up async fsyncs and verifying crash recovery.

### 3. Adaptive Search Limits

- **Context**: Search limit currently uses hardcoded oversampling (`k * 10`), which may be inefficient or insufficient (`// TODO: Adaptive or configurable factor`).
- **Location**: `internal/store/index_search.go`.
- **Plan**: Implement adaptive oversampling based on filter selectivity to balance recall and latency dynamically.

## Priority 2: Core Architecture & Features
=======
## Priority 1: Core Architecture & Reliability
>>>>>>> main

### 1. Refactor `internal/store` Monolith

<<<<<<< HEAD
- **Impact**: Strengthens Longbow's position as a multi-modal store.
- **Location**: `internal/store/graph_store.go`.
- **Plan**: Implement weighted traversal, bidirectional edge lookups, and integration with vector search.

### 2. Filter Optimization

- **Context**: Filtering currently lacks bitset support and vectorization (`// TODO: Convert general filters to bitset?`).
- **Location**: `internal/store/arrow_hnsw_index.go`, `internal/store/filter_evaluator.go`.
- **Plan**:
  - Vectorize filter evaluation logic (SIMD).
  - Introduce bitset-based filtering for high-selectivity predicates.

## Priority 3: Technical Debt & Cleanup

### 1. Resource Management

- **Context**: Explicit resource cleanup is missing in some index paths (`// TODO: Clean up resources`).
- **Location**: `internal/store/arrow_hnsw_index.go`.
- **Plan**: Audit `Close()` methods and ensure all memory/file handles (Arrow allocators, WAL files) are released.

### 2. Networking & Discovery

- **Impact**: Improves operational stability.
- **Location**: `internal/mesh`.
- **Plan**: Replace naive mDNS with a robust library (e.g., hashicorp/memberlist) and optimize scanning.
=======
- **Impact**: **High**. The `internal/store` package contains 360+ files, making maintenance and testing difficult.
- **Location**: `internal/store`
- **Plan**: Split into dedicated packages: `internal/index` (HNSW/Vector), `internal/storage` (Persistence/WAL/S3),
  `internal/query` (Filtering/Eval).

### 2. Distributed Consensus (Raft)

- **Impact**: **High**. Critical for strong consistency in cluster metadata (schema changes, shard rebalancing) where
  Gossip is insufficient.
- **Location**: `internal/consensus` (New)
- **Plan**: Integrate a Raft library (e.g., `hashicorp/raft`) to manage cluster state and topology configurations.

### 3. Robust Node Discovery

- **Impact**: **Medium**. Improves operational stability and cloud compatibility.
- **Location**: `internal/mesh`
- **Plan**: Replace basic mDNS with `hashicorp/memberlist` for robust, WAN-capable gossip and membership.

## Priority 2: Search Performance

### 4. Dynamic/Adaptive Oversampling

- **Impact**: **Medium**. Improves recall/precision balance for filtered HNSW searches.
- **Location**: `internal/store/index_search.go`
- **Plan**: Implement the TODO to dynamically adjust the search `ef` search factor based on filter selectivity estimates.

### 5. Async S3 Checkpointing

- **Impact**: **Medium**. Prevents I/O stalls during snapshot operations on large datasets.
- **Location**: `internal/store/s3_backend.go`
- **Plan**: Implement non-blocking, multipart uploads for snapshots to decouple persistence latency from query availability.

### 6. SIMD Tail Handling

- **Impact**: **Low/Correctness**. Ensures correctness for edge-case vector dimensions not divisible by SIMD register width.
- **Location**: `internal/simd/compare_amd64.s`
- **Plan**: Implement proper tail handling or enforce 8-byte alignment strictness.

### 7. Binary Quantization (BQ)

- **Impact**: **High**. Offers extreme compression (32x) and speed for suitable datasets (e.g., embeddings with large
  dimensions).
- **Location**: `internal/store/quantization.go`
- **Plan**: Implement Binary Quantization support alongside existing PQ, optimizing for Hamming distance.

## Priority 3: Features & Observability

### 8. Structured Query Parser

- **Impact**: **Medium**. Enables complex boolean logic (AND/OR/NOT/Nested) for metadata filters.
- **Location**: `internal/query`
- **Plan**: Implement a parser for a SQL-like subset or structured JSON DSL to replace ad-hoc filter chains.

### 9. Distributed Tracing (OpenTelemetry)

- **Impact**: **Medium**. Provides visibility into request latency across the distributed scatter-gather path.
- **Location**: `internal/store/global_search.go`
- **Plan**: Instrument the `GlobalSearch` and `ShardedHNSW` paths with OpenTelemetry spans.

### 10. HNSWv2 Migration & Stabilization

- **Impact**: **Medium**. Modernizes the core index structure.
- **Location**: `internal/store/hnsw.go`
- **Plan**: Finalize the HNSWv2 implementation, verify its performance benefits, and create a seamless migration path
  (deprecating v1).
>>>>>>> main
