
# Longbow Performance Benchmarks

This document details the performance characteristics of Longbow running on a 3-node cluster with 8GB RAM per node.

**Date:** 2026-01-14 (Optimized)
**Cluster Config:**

- 3 Nodes (Docker Compose)
- 8GB RAM limit per node (approx)
- Gossip Enabled
- Backend: Arrow HNSW (in-memory)

## Summary

Longbow demonstrates extreme throughput for data ingestion and retrieval following async indexing and logging path optimizations.

- **Ingestion (DoPut):** **>90 MB/sec** (Peak observed: 93.90 MB/s)
- **Retrieval (DoGet):** **>750 MB/sec** (Peak observed: 750.31 MB/s)
- **Search Throughput:** ~940 QPS (Small dataset p50 ~0.97ms)
- **Latency:** Sub-millisecond for small datasets.

## 128-dim Optimization Verification - 2026-01-14

**Scenario:** 128 dimensions, Float32. Validating `AddBatchBulk` Allocation Optimization.

- **Optimization:** Refactored `ArrowHNSW.AddBatchBulk` to eliminate dynamic map allocations and `growslice` events during concurrent ingestion.
- **Impact:** Reduced GC overhead from ~30% of CPU time to negligible levels.
- **Result:** Consistent ingestion throughput without GC pauses.

## Polymorphic Refactor Verification (Single Node) - 2026-01-13 (Updated)

**Scenario:** Single Node, 384 dimensions, Float32. Validating HNSW Polymorphic Refactor.

| Dataset Size | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Dense p95 (ms) |
| :--- | :--- | :--- | :--- | :--- |
| **3,000** | 111.94 | 95.74 | 830.63 | 1.05 |
| **5,000** | 80.76 | 113.15 | 564.54 | 1.42 |
| **9,000** | 48.11 | 132.34 | 131.73 | 22.80 |
| **15,000** | 48.34 | 160.29 | 72.39 | 43.92 |
| **25,000** | 88.94 | 185.02 | 42.78 | 44.13 |

*Note: Hybrid search not configured for this run.*

## Previous Optimization Benchmark (10,000 Vectors)

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
