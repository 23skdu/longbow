# Longbow Performance Optimization Guide

This guide details the advanced performance features available in Longbow, including NUMA awareness, Zero-Copy data access, and batching optimizations.

## 1. NUMA Architecture Support

Longbow includes native support for Non-Uniform Memory Access (NUMA) architectures, commonly found in dual-socket servers (e.g., AWS metal instances, bare metal).

### How it Works

- **Topology Detection**: Automatically detects NUMA nodes at startup.
- **Worker Pinning**: Indexing workers are pinned to specific NUMA nodes to maximize cache locality.
- **Local Allocation**: Memory for Arrow buffers is allocated on the same node as the worker processing it.

### Configuration

NUMA support is enabled by default on Linux if multiple nodes are detected. You can verify it via logs:

```
INFO: Detected 2 NUMA nodes
INFO: Started NUMA indexing workers nodes=2 count=16
```

## 2. Zero-Copy Data Access

For `DoGet` retrieve operations, Longbow utilizes Apache Arrow's zero-copy capabilities to map data directly from memory to the network wire without intermediate allocations.

- **Retain vs Copy**: When no filters are applied, record batches are Retained (ref-counted) rather than copied.
- **Slicing**: When filtering with a tombstone bitmap, we use zero-copy slicing to create a view of the data.

**Impact**: Reduces memory bandwidth usage by ~60% during heavy read workloads.

## 3. Vector Search Optimization

### Batch Distance Calculations

Longbow groups vector distance calculations into batches (default 4096) to leverage SIMD instructions effectively. This benefits high-dimensional vectors (e.g., OpenAI 1536-dim) by keeping CPU pipelines full.

### Stripe Locking

To prevent global lock contention during concurrent writes, HNSW graphs use striped locks based on vector ID. This allows simultaneous updates to different parts of the graph.

## 4. Tuning Guide

### Gossip Protocol

- `LONGBOW_GOSSIP_BATCH_SIZE`: Controls maximal UDP packet size (default 1400 bytes).
- `LONGBOW_GOSSIP_INTERVAL`: Frequency of state sync (default 200ms).

### Storage

- `LONGBOW_STORAGE_ASYNC_FSYNC`: Enable background fsync for WAL (default: true).
- `LONGBOW_DOPUT_BATCH_SIZE`: Records to buffer before WAL write (default: 100).
- `LONGBOW_DOGET_PIPELINE_DEPTH`: Prefetch depth for read pipeline (default: 8).
