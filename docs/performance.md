# Performance Benchmarks

**Date**: 2026-01-19
**Version**: v0.1.9-dev
**Hardware**: 3-Node Cluster (Docker, 6GB RAM/node)

## Executive Summary

Latest benchmark run covering all supported data types and search configurations up to 50k vectors.
The system is fully stable across all tests, confirming the resolution of previous Tombstone Deletion errors.

- **Throughput**: Ingestion consistently > 100k vectors/s (147 MB/s) for 384d Float32.
- **Search Latency**: Dense Search P50 < 6ms. Specialized searches (Sparse/Hybrid) < 12ms.
- **Stability**: Zero errors in DoGet, DoPut, Search, and Deletion phases for 50k vectors.

## Cluster Validation Suite (50k Vectors, 3-Node)

Comprehensive test of 384d Float32 vectors.

| Metric | Details | P50 Latency | P95 Latency | Status |
| :--- | :--- | :--- | :--- | :--- |
| **DoPut** | 50k vectors @ 147 MB/s | - | - | ✅ Passed |
| **DoGet** | 1.6M records @ 350 MB/s | - | - | ✅ Passed |
| **DoExchange** | Binary Search (500 queries) | 9ms | 42ms | ✅ Passed |
| **Dense Search** | 1000 queries (c=4) | 5ms | 64ms | ✅ Passed |
| **Sparse Search** | 500 queries | 8ms | 11ms | ✅ Passed |
| **Filtered Search** | 500 queries | 10ms | 11ms | ✅ Passed |
| **Hybrid Search** | 500 queries | 7ms | 7ms | ✅ Passed |
| **Deletion** | 1000 IDs | - | - | ✅ Passed |

## Data Type Matrix (Up to 50k Vectors)

Performance across all supported types (128d & 384d).

| Type       |   Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status     |
|:-----------|------:|-------------:|-------------:|----------:|:-----------|
| **int8**   |   128 |       237.35 |       130.92 |      134.9 | **Stable** |
| **int16**  |   128 |       339.29 |      2595.54 |     2610.0 | **Stable** |
| **int32**  |   128 |       537.02 |      3331.13 |     2368.2 | **Stable** |
| **int64**  |   128 |       418.91 |      3479.31 |     2335.5 | **Stable** |
| **float32** |   128 |       660.47 |       366.48 |      151.8 | **Stable** |
| **float16** |   128 |       436.98 |       129.82 |       80.9 | **Stable** |
| **float64** |   128 |       826.05 |       374.31 |      111.1 | **Stable** |
| **complex64** |   128 |       887.92 |       213.69 |      103.6 | **Stable** |
| **complex128** |   128 |       927.55 |      1130.74 |       50.5 | **Stable** |
| **int8**   |   384 |       551.97 |       208.05 |       43.3 | **Stable** |
| **int16**  |   384 |       387.60 |      2043.96 |      585.7 | **Stable** |
| **int32**  |   384 |       624.01 |      2749.68 |      796.6 | **Stable** |
| **int64**  |   384 |       737.19 |      2233.72 |      787.7 | **Stable** |
| **float32** |   384 |       608.91 |      1008.38 |       42.2 | **Stable** |
| **float16** |   384 |       483.44 |       246.93 |       50.9 | **Stable** |
| **float64** |   384 |       862.00 |       650.91 |       35.2 | **Stable** |
| **complex64** |   384 |       833.33 |       529.84 |       32.2 | **Stable** |
| **complex128** |   384 |       984.27 |       398.45 |       40.4 | **Stable** |

## High-Dimensional Matrix (OpenAI Models)

*Retained from previous stable run (1536d/3072d not re-run in this suite).*

| Type       |   Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status     |
|:-----------|------:|-------------:|-------------:|----------:|:-----------|
| **int8**   |  1536 |      1068.25 |       584.71 |       15.6 | **Stable** |
| **int16**  |  1536 |        27.07 |       471.46 |       79.3 | **Stable** |
| **float16** |  1536 |        65.24 |       465.11 |       71.2 | **Compute Bound** |
| **float32** |  1536 |       551.16 |       436.32 |       32.2 | **Stable** |
| **float32** |  3072 |        43.21 |       726.30 |        8.8 | **Stable** |
