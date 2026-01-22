# Performance Benchmarks

**Date**: 2026-01-21  
**Version**: v0.1.5-rc5 (Post-Hang & Stability Fixes)  
**Hardware**: 3-Node Cluster (Local Process, 5GB RAM/node)

## Executive Summary

Performance and stability validated after fixing critical server-side deadlocks, log spam GC thrashing, and client-side SDK gRPC stream hangs. The system now handles 50k+ vector datasets seamlessly across all data types, including `float16`.

- **Stability**: ✅ Zero hangs or crashes during 50k row ingestion/search
- **Ingestion**: DoPut **620-875 MB/s** (50k vectors, 384d)
- **Retrieval**: DoGet **260-370 MB/s** (50k vectors, 384d)
- **Search**: Dense Search **67-110 QPS** (50k vectors, 384d, HNSW)

## Benchmark Results (50,000 Vectors, 384d)

### Ingestion & Retrieval (Throughput)

| Vector Type | DoPut (MB/s) | DoGet (MB/s) | Status |
|:---|---:|---:|:---|
| **Float32** | 875.31 | 262.26 | ✅ Passed |
| **Float16** | 620.80 | 371.80 | ✅ Passed |

### Search Performance (HNSW, k=10)

| Vector Type | QPS | p50 (ms) | p95 (ms) | p99 (ms) | Status |
|:---|---:|---:|---:|---:|:---|
| **Float32** | 109.93 | 4.68 | 40.05 | 64.92 | ✅ Passed |
| **Float16** | 67.33 | 9.92 | 49.44 | 85.52 | ✅ Passed |

## Critical Fixes Applied

### 1. Client-Side SDK Hang

Fixed a condition where the Python SDK would block indefinitely at `writer.close()` during `do_put`.

- **Solution**: Shifted to `done_writing()` signal with a default 180s timeout and non-blocking metadata retrieval.
- **Impact**: Ingestion of large datasets no longer hangs the client.

### 2. Server-Side Stability

- **Log Spam**: Removed high-frequency `DEBUG PARSER` logs that caused GC thrashing and excessive memory pressure.
- **Backpressure**: Fixed a race condition in the ingestion ring buffer by using accurate capacity metrics for throttling.
- **Deadlock**: Added context-awareness and panic recovery to the HNSW bulk insert pipeline to prevent infinite spin loops.
- **GC Tuning**: Increased GCTuner interval from 100ms to 500ms to improve tuner stability.

## Notes

- Benchmarks run on a local 3-node cluster.
- All tests utilized 384-dimensional vectors.
- Ingestion throughput reflects network-level Arrow streaming performance.
- Search performance includes HNSW index build time (performed incrementally during ingestion).
