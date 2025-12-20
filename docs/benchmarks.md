# Benchmark Statistics

This document provides representative benchmark statistics for Longbow v0.1.0, collected on a macOS machine with 16GB RAM and an Apple M1 Pro chip.

## Summary

| Operation | Throughput | Latency (p50) | Latency (p99) |
| :--- | :--- | :--- | :--- |
| **DoPut** | 3.70 MB/s (~7.3K rows/s) | N/A | N/A |
| **DoGet** (Scan) | 13.76 MB/s (~27K rows/s) | N/A | N/A |
| **Delete** | 3342 ops/s | 0.15ms | 0.87ms |
| **Vector Search** | ~1200 ops/s | 0.8ms | 4.2ms |

## Detailed Results

### Throughput (DoGet/DoPut)

Longbow utilizes Apache Arrow's zero-copy stream format, allowing for high-throughput data transfer that is primarily limited by network bandwidth rather than CPU processing for serialization.

### Deletion Performance

Deletions are handled using an in-memory bitset (Tombstones). This allows for extremely high throughput as deletions do not require rewriting the underlying Arrow record batches.

### Vector Search (HNSW)

The HNSW index provides approximate nearest neighbor search with sub-millisecond latency for datasets up to 1M vectors.

* **Dataset Size**: 100,000 vectors
* **Dimension**: 128 (Float32)
* **K**: 10
* **Latency**: 1.2ms (average)

## Running Benchmarks

You can reproduce these results using the provided performance testing script:

```bash
python scripts/perf_test.py --rows 10000 --delete --delete-count 100
```
