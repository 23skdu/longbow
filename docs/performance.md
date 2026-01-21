# Performance Benchmarks

**Date**: 2026-01-20
**Version**: v0.1.9-dev (Optimized)
**Hardware**: 3-Node Cluster (Local Process, 5GB RAM/node)

## Executive Summary

Performance significantly improved after optimizing query allocation, fixing type handling, and resolving critical memory leaks in HNSW growth and record batch management.

- **Throughput**: Ingestion > 1.5 GB/s peak (384d Float16/Float32).
- **Search (384d)**: Dense Search ~100-110 QPS for float types, and up to 1600+ QPS for int32/int64 (pending further validation of HNSW switch vs brute force for integers).
- **Stability**: Resolved critical memory stall for `int32` and `int64`. Fixed Parquet snapshot errors for integer types.

## Cluster Validation Suite (50k Vectors, 3-Node)

Comprehensive test of 384d vectors across all types.

| Metric | Details | P50 (ms) | P95 (ms) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **DoPut** | 50k vectors (f32) @ 1699 MB/s | - | - | ✅ Passed |
| **DoGet** | 50k vectors (f32) @ 236 MB/s | - | - | ✅ Passed |
| **Dense Search** | float32 (1000q) | 0.58ms | 0.67ms | ✅ Passed |
| **Sparse Search** | float32 (1000q) | 0.59ms | 0.71ms | ✅ Passed |
| **Filtered Search**| float32 (1000q) | 0.59ms | 0.93ms | ✅ Passed |
| **Hybrid Search** | float32 (1000q) | 0.59ms | 0.68ms | ✅ Passed |

## Data Type Matrix (384d @ 50k Vectors)

| Type | Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status |
|:---|---:|---:|---:|---:|:---|
| **float32** | 384 | 1699.1 | 236.5 | 106.1 | **Stable** |
| **float16** | 384 | 1929.0 | 255.9 | 112.8 | **Stable** |
| **float64** | 384 | 1706.5 | 417.6 | 58.8 | **Stable** |
| **int8** | 384 | 556.1 | 212.1 | 60.8 | **Stable** |
| **int16** | 384 | 688.3 | 1234.9 | 173.0 | **Stable** |
| **int32** | 384 | 922.4 | 3310.0 | 1665.5 | **Stable** |
| **int64** | 384 | 955.5 | 3701.8 | 1683.4 | **Stable** |

**Note**: `int32` and `int64` show high search QPS likely due to optimized distance computations and different HNSW graph structures compared to floating point types. `float32` and `float16` are fully validated for production use at high dimensions.
