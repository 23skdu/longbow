# Longbow Optimization Roadmap

This document outlines the next prioritized improvements for the Longbow vector database.

## Priority 1: Core Architecture & Features

### 1. GraphRAG Enhancements

- **Impact**: **High**. Strengthens Longbow's position as a multi-modal store by improving graph traversal
  capabilities for complex retrieval augmented generation workflows.
- **Location**: `internal/store/graph_store.go`
- **Plan**: Implement weighted traversal, bidirectional edge lookups, and integration with vector search results for
  "Graph-biased" ranking.

## Priority 2: Performance Optimizations

### 1. Vectorized Filter Evaluation

- **Impact**: **Medium**. Significantly speeds up filtered searches (e.g., "vectors where category='news'") by
  processing predicates using SIMD instead of scalar comparison.
- **Location**: `internal/store/filter_evaluator.go`
- **Plan**: Vectorize the evaluation loop to process 16 bytes (or more) at a time.

### 2. Dynamic Oversampling for Filtering

- **Impact**: **Medium**. Improves recall/precision balance for filtered HNSW searches without manual tuning.
- **Location**: `internal/store/index_search.go`
- **Plan**: Implement adaptive oversampling factor based on filter selectivity estimates.

## Priority 3: Technical Debt & Cleanup

### 1. Remaining Linting & Code Quality

- **Impact**: **Low**. Improves maintainability and reduces subtle bugs.
- **Tasks**:
  - Address `gocritic` feedback for style and minor performance wins (40+ issues).
  - Resolve remaining `unparam` issues (5 left).

### 2. Networking & Discovery

- **Impact**: **Low**. Improves operational stability for distributed deployments.
- **Location**: `internal/mesh`
- **Plan**: Replace naive mDNS implementation with a robust library and optimize member list scanning.

### 3. SIMD Tail Handling

- **Impact**: **Low**. Ensures correctness for edge-case vector dimensions.
- **Location**: `internal/simd/compare_amd64.s`
- **Plan**: Implement proper tail handling or enforce 8-byte alignment strictness.
