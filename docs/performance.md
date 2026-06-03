# Longbow Performance Benchmark Results

**Date**: 2026-06-02
**Configuration**:
- Vectors: 500,000
- Dimension: 384
- Data Types: float32, turboquant8
- Storage: Disk-backed (`LONGBOW_USE_DISK=1`), `io_uring` enabled
- Queries: 500 per mode
- Modes: dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal

## Ingest Performance

| Mode | DataType | Ingest Rate (vec/s) | Disk Usage (MB) |
|---|---|---|---|
| CPU | float32 | 51,128 | 732.4 |
| CPU | turboquant8 | 51,314 | 732.4 |
| CUDA | float32 | 51,319 | 732.4 |
| CUDA | turboquant8 | 51,319 | 732.4 |

## Search Performance (QPS)

| Search Mode | CPU (float32) | CPU (turboquant8) | CUDA (float32) | CUDA (turboquant8) |
|---|---|---|---|---|
| Dense | 301.4 | 283.5 | 315.5 | 282.1 |
| Hybrid | 305.9 | 301.7 | 313.2 | 288.3 |
| Sparse | 7,334.9 | 6,885.9 | 7,631.5 | 7,116.5 |
| Filtered | 261.5 | 250.9 | 262.7 | 238.0 |
| By ID | 309.3 | 305.4 | 318.0 | 284.9 |
| GraphRAG | 301.9 | 291.0 | 304.8 | 272.3 |
| Geo | 67.6 | 63.0 | 62.3 | 58.7 |
| Temporal | 1,017.4 | 900.0 | 914.4 | 909.7 |

## Search Latency (P99 ms)

| Search Mode | CPU (float32) | CPU (turboquant8) | CUDA (float32) | CUDA (turboquant8) |
|---|---|---|---|---|
| Dense | 44.3 | 49.7 | 40.0 | 41.5 |
| Hybrid | 36.2 | 39.4 | 33.8 | 40.2 |
| Sparse | 2.0 | 1.8 | 3.6 | 1.9 |
| Filtered | 340.1 | 330.4 | 340.5 | 359.4 |
| By ID | 37.9 | 35.2 | 33.1 | 37.0 |
| GraphRAG | 35.9 | 39.3 | 36.7 | 48.6 |
| Geo | 301.2 | 295.1 | 300.7 | 379.4 |
| Temporal | 11.7 | 12.7 | 13.0 | 13.2 |

## 5M & 10M Scale Benchmarks (128-dim, CUDA)
Benchmarks were attempted at 5-million and 10-million vector scales under a strict 16GB memory limit using `float32` and `turboquant8`.
- **float32 (5M & 10M)**: Encountered `ResourceExhausted` (OOM) during the search phase. This is expected as 5M `float32` vectors of 128 dimensions consume ~2.5GB (and 10M consumes ~5.1GB), plus the HNSW graph overhead (~1.2-2.4GB). When search caches and result buffers are allocated during peak query loads, the system exceeds the safe 95% threshold of the 16GB limit, triggering the admission controller to reject queries.
- **turboquant8 (5M & 10M)**: Memory footprint was highly optimized (~640MB for 5M vectors). However, `turboquant8` still hit `ResourceExhausted` during the final query phases (e.g., Learned Index) because the `SlabArena` pre-allocations and query result buffering collectively pushed the heap past the strict 15.2GB admission ceiling.
- **Status**: The active memory footprint of the index itself is small enough, but the peak memory during active parallel querying exceeds 16GB. Future high-scale benchmarks (>5M) should be conducted on machines with 32GB+ RAM or with graph eviction heavily tuned. The background `SIGBUS` bug during snapshotting (`os.Create` truncation) was successfully diagnosed and fixed using atomic `.tmp` renames.
