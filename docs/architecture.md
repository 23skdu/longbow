# Longbow Systems Architecture

## Overview

Longbow is a distributed, high-performance vector database designed for low-latency retrieval and high-throughput ingestion. It leverages a hybrid storage engine, modern hardware optimizations, and a resilient distributed mesh.

## Core Components

### 1. Vector Engine

- **Hybrid Indexing**:
  - **Dense**: HNSW (Hierarchical Navigable Small World) for approximate nearest neighbor search.
  - **Sparse**: Inverted Index (Roaring Bitmaps) for keyword matching.
  - **Auto-Sharding**: Transparently upgrades standard indices to **ShardedHNSW** (lock-striped) when thresholds are met.
- **Interim Sharding**: Uses a temporary sharded index during migration to eliminate double-indexing overhead, ensuring linear scalability during resizing.
- **Zero-Copy**: Utilizes Apache Arrow for zero-copy data representation, minimizing serialization overhead.
- **Lock-Free**: Experimental lock-free indexing structures (skip list) for high concurrency.
- **SIMD & NUMA**: optimized for modern CPU architectures with SIMD instructions (AVX2/AVX-512) and NUMA-aware memory allocation.

### 2. Storage Layer

- **LSM Tree**: Log-Structured Merge tree based engine for efficient write-heavy workloads.
- **WAL (Write Ahead Log)**:
  - **`io_uring` Backend**: Linux-specific optimization using efficient async I/O ring buffers for high-throughput writes.
  - **FS Backend**: Fallback standard file system logger for compatibility.
- **Persistence**: Periodic checkpoints and snapshots ensure data durability.

### 3. Distributed Mesh

- **Memberlist Gossip**: Uses SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) protocol for failure detection and cluster membership.
- **Consistent Hashing**: Distributes data across nodes using a consistent hashing ring with replication.
- **Smart Client**:
  - Thick client architecture.
  - Handles **`FORWARD_REQUIRED`** errors transparently.
  - Maintains connection pools to cluster nodes.

## Data Flow

### Ingestion (DoPut)

1. Client sends `Arrow RecordBatch` to any node.
2. **Partition Proxy** intercepts the request.
3. Determines the owner node via Consistent Hashing.
4. If local: Writes to WAL and MemTable.
5. If remote: Returns `FORWARD_REQUIRED` to the Smart Client (or Proxies if configured).

### Retrieval (DoGet)

1. Client sends Vector Search Query (serialized in Flight Ticket).
2. **Smart Client** routes to the likely owner or a coordinator.
3. Node performs:
    - **Filter Evaluation**: Bitset-based pre-filtering.
    - **Vector Search**: HNSW graph traversal.
    - **Re-ranking**: Exact distance calculation on candidates.
4. Results streamed back as `Arrow RecordBatch`.

## Configuration

See [Configuration Guide](configuration.md) for details on:

- `STORAGE_USE_IOURING`
- `LONGBOW_GOSSIP_ENABLED`
- `LONGBOW_NUMA_Enable`
