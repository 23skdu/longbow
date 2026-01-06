# Longbow Optimization & Stability Roadmap

This document outlines the prioritized improvements for the Longbow vector database, focusing on stability, code correctness, and performance.

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

### 1. GraphRAG Enhancements

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
