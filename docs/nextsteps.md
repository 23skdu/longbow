# Longbow Optimization Roadmap

This document outlines the next prioritized improvements for the Longbow vector database.

## Priority 1: Core Architecture & Reliability

### 1. Refactor `internal/store` Monolith

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
