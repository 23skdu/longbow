
# Longbow Performance Benchmarks

This document details the performance characteristics of Longbow running on a 3-node cluster with 8GB RAM per node.

**Date:** 2026-01-13 (Optimized)
**Cluster Config:**

- 3 Nodes (Docker Compose)
- 8GB RAM limit per node (approx)
- Gossip Enabled
- Backend: Arrow HNSW (in-memory)

## Summary

Longbow demonstrates extreme throughput for data ingestion and retrieval following async indexing and logging path optimizations.

- **Ingestion (DoPut):** **>1,000 MB/sec** (Peak observed: 1,028 MB/s)
- **Retrieval (DoGet):** **>660 MB/sec** (Peak observed: 668 MB/s)
- **Search Throughput:** ~250 QPS (Mixed load p50 ~3.1ms)
- **Latency:** Sub-millisecond for small datasets.

## Latest Optimization Benchmark (10,000 Vectors)

**Scenario:** 10,000 vectors, 384 dimensions, Float32.

| Metric | Result | Notes |
| :--- | :--- | :--- |
| **DoPut Throughput** | **1,028.90 MB/s** | Saturates local I/O / Network |
| **DoGet Throughput** | **668.25 MB/s** | Zero-copy path verified |
| **Search Throughput** | **250.06 QPS** | 4 Concurrent Workers |
| **Search Latency (p50)** | **3.14 ms** | |
| **Search Latency (p99)** | **12.33 ms** | |

## Previous Baseline (Pre-Optimization)

*For reference only. These numbers reflect performance prior to async indexing and logging removal.*

### Throughput (MB/s)

| Dataset Size | DoPut (MB/s) | DoGet (MB/s) |
| :--- | :--- | :--- |
| 3,000 | 27.28 | 114.86 |
| 5,000 | 59.41 | 147.95 |
| 7,000 | 76.43 | 161.47 |
| 13,000 | 100.55 | 169.58 |
| 20,000 | 40.74 | 85.87 |
| 35,000 | 32.02 | 86.20 |

## Methodology

- **Client:** Python SDK (longbowclientsdk)
- **Vectors:** 384-dimensional float32 (randomly generated)
- **Top-K:** 10
- **Environment:** 3-Node Cluster on Docker, Mac Host.
