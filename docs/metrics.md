# Longbow Metrics Reference

Complete reference for all Prometheus metrics exported by Longbow.

**Metrics Endpoint**: `http://localhost:9090/metrics` (configurable via `LONGBOW_METRICS_ADDR`)

Last Updated: 2026-04-16

## Table of Contents

1. [Flight & RPC](#flight--rpc)
2. [Vector Search](#vector-search)
3. [HNSW Index & PQ](#hnsw-index--pq)
4. [Memory Management & NUMA](#memory-management--numa)
5. [io_uring I/O](#io_uring-io)
6. [Graph Navigation](#graph-navigation)
7. [Health & Logging](#health--logging)

---

## Flight & RPC

### longbow_flight_ops_total

**Type**: Counter  
**Labels**: `action`, `status`  
**Description**: Total number of processed Arrow Flight operations (DoPut, DoGet, DoAction, etc.)

### longbow_flight_duration_seconds

**Type**: Histogram  
**Labels**: `action`  
**Description**: Duration of Arrow Flight operations

### longbow_flight_bytes_read_total

**Type**: Counter  
**Description**: Total bytes read from Arrow Flight tickets

### longbow_flight_bytes_written_total

**Type**: Counter  
**Description**: Total bytes written to Arrow Flight streams

### longbow_flight_active_tickets

**Type**: Gauge  
**Description**: Number of currently active Flight tickets

---

## Vector Search

### longbow_vector_search_latency_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Latency of vector search operations

### longbow_active_search_contexts

**Type**: Gauge  
**Description**: Number of concurrent DoGet/search operations in progress

---

## HNSW Index & PQ

### longbow_hnsw_node_count

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Current number of nodes in the HNSW graph (memory + disk).

### longbow_hnsw_complex_ops_total

**Type**: Counter  
**Labels**: `type`  
**Description**: Total number of complex number distance calculations

### longbow_hnsw_simd_dispatch_latency_seconds

**Type**: Histogram  
**Labels**: `type`  
**Description**: Latency of the dynamic SIMD kernel dispatcher by data type.

---

## Memory Management & NUMA

### longbow_arena_memory_bytes

**Type**: Gauge  
**Labels**: `size`  
**Description**: Current bytes allocated in arena pools categorized by block size.

### longbow_slab_fragmentation_ratio

**Type**: Gauge  
**Labels**: `size`  
**Description**: Current fragmentation ratio for slab pools (allocated / active).

### longbow_allocator_bytes_allocated_total

**Type**: Counter  
**Description**: Total cumulative bytes allocated by the custom memory allocator.

---

## io_uring I/O

### longbow_iouring_submit_latency_seconds

**Type**: Histogram  
**Labels**: `operation`  
**Description**: Latency of io_uring submission operations.

### longbow_iouring_ops_submitted_total

**Type**: Counter  
**Labels**: `operation`  
**Description**: Total number of operations submitted to the ring.

---

## ML Inference & Reranking

### longbow_onnx_inference_duration_seconds
**Type**: Histogram  
**Labels**: `backend`, `operation`  
**Description**: Duration of ONNX inference operations (embedding, reranking).

### longbow_reranker_inference_duration_seconds
**Type**: Histogram  
**Description**: Latency of the cross-encoder reranking stage.

### longbow_reranker_scores_computed_total
**Type**: Counter  
**Description**: Total number of doc-query pairs re-scored by the cross-encoder.

### longbow_onnx_metal_memory_used_bytes
**Type**: Gauge  
**Description**: Current VRAM utilization for the Metal (Apple Silicon) inference backend.

---

## High-Throughput IO (Parquet V2)

### longbow_snapshot_write_duration_seconds
**Type**: Histogram  
**Description**: Latency of reflection-free Parquet snapshotting.

### longbow_snapshot_size_bytes
**Type**: Histogram  
**Description**: Distribution of Parquet snapshot file sizes.

---

## Adaptive Indexing

### longbow_hnsw_adaptive_m_value
**Type**: Gauge  
**Labels**: `index_name`  
**Description**: Current dynamically adjusted `M` parameter (connections per node).

### longbow_hnsw_adaptive_adjustments_total
**Type**: Counter  
**Labels**: `index_name`  
**Description**: Cumulative count of dynamic graph structure adjustments.

---

## Graph Navigation

### longbow_graph_rag_rerank_latency_seconds
**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Latency of the spreading activation re-ranking phase.

---

**Total Metrics Documented**: 140+  
**Last Updated**: 2026-04-20
