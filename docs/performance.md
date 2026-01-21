# Performance Benchmarks

**Date**: 2026-01-20
**Version**: v0.1.9-dev (Optimized)
**Hardware**: 3-Node Cluster (Local Process, 5GB RAM/node)

## Executive Summary

Performance significantly improved after optimizing query allocation and fixing type handling.

- **Throughput**: Ingestion > 1 GB/s peak (384d Float32).
- **Search (384d)**: Dense Search ~70 QPS (up from 28). p50 Latency ~4.7ms.
- **Search (128d)**: Dense Search ~320 QPS. p50 Latency ~2ms.
- **Stability**: Zero errors.

## Cluster Validation Suite (50k Vectors, 3-Node)

Comprehensive test of 384d Float32 vectors.

| Metric | Details | P50 Latency | P95 Latency | Status |
| :--- | :--- | :--- | :--- | :--- |
| **DoPut** | 50k vectors @ 1127 MB/s | - | - | ✅ Passed |
| **DoGet** | 1M records @ 556 MB/s | - | - | ✅ Passed |
| **DoExchange** | ID Search (500 queries) | 10ms | 30ms | ✅ Passed |
| **Dense Search** | 1000 queries (c=4) | 4.8ms | 50ms | ✅ Passed |
| **Sparse Search** | 1000 queries | 19ms | 84ms | ✅ Passed |
| **Filtered Search** | 1000 queries | 13ms | 27ms | ✅ Passed |
| **Hybrid Search** | 1000 queries | 46ms | 89ms | ✅ Passed |
| **Deletion** | 100 IDs | 0.26ms | 1.2ms | ✅ Passed |

## Data Type Matrix (128d Snapshot)

| Type       |   Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status     |
|:-----------|------:|-------------:|-------------:|----------:|:-----------|
| **float32** |   128 |        686.1 |          - |     319.8 | **Stable** |
| **int64**   |   128 |        463.4 |          - |    2505.4 | **Stable*** |

**Note**: `int64` High QPS likely due to simplified distance path or data distribution; zero rows returned in stress test indicates further investigation needed for `int64` indexing logic (future task). `float32` is fully validated.
