# Longbow Optimization Roadmap

This document outlines the next prioritized improvements for the Longbow vector database.

## Priority 1: Features & Architecture

### 1. Persistent Indexing (DiskANN-style)

- **Location**: `internal/store/hnsw_*`
- **Issue**: Current HNSW is purely in-memory.
- **Plan**: Research and prototype a disk-backed compressed adjacency list (PQ + Disk) to support datasets larger than RAM.

### 2. Pluggable Distance Metrics

- **Location**: `internal/store` & `internal/simd`
- **Issue**: System is tied to Euclidean distance.
- **Plan**: Refactor the distance interface to easily swap Inner Product (Dot Product) or Cosine Similarity, ensuring SIMD paths exist for all.

### 3. Hybrid Search: Adaptive Alpha

- **Location**: `internal/store/hybrid_search.go`
- **Issue**: Users must manually specify `alpha`.
- **Plan**: Implement a "smart" mode that estimates `alpha` based on query characteristics (e.g., if query looks like a keyword, lean sparse; if sentence, lean dense).

---

## Codebase TODOs

This section tracks technical debt and minor improvements identified in code comments.

### Core Store & Indexing

- **`internal/store/index_search.go`**: Oversample for filtering - implement adaptive or configurable factor.
- **`internal/store/filter_evaluator.go`**: Vectorize filter evaluation loop (16 bytes at a time).
- **`internal/store/arrow_hnsw_index.go`**: Convert general filters to bitsets for performance.
- **`internal/store/arrow_hnsw_index.go`**: Clean up resources properly.
- **`internal/store/sharded_hnsw.go`**: Pass metric type to ArrowHNSW once supported.
- **`internal/store/wal_buffered.go`**: improved error logging.
- **`internal/store/batched_wal.go`**: Recycle buffers to reduce allocations.
- **`internal/store/batched_wal.go`**: Integrate `AsyncFsyncer` with `WALBackend` interface properly.

### SIMD & Optimization

- **`internal/simd/pq_amd64.s`**: Optimize code loading.
- **`internal/simd/compare_amd64.s`**: Implement tail properly or accept 8-alignment strictness.

### Networking & Mesh

- **`internal/mesh/discovery_mdns.go`**: Implement actual mDNS using a library or raw socket.
- **`internal/mesh/region.go`**: Replace linear scan with spatial index (VP-Tree or BK-Tree) for large routing tables.
- **`cmd/longbow/main.go`**: Refactor advertise address logic in Gossip constructor.

### Tests & Infrastructure

- **`scripts/partition_test.sh`**: Use CLI or API to check member count and list instead of grep/sleep.
- **`internal/store/arrow_insert_test.go`**: Implement skipped tests when full vector storage is ready.
