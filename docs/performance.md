# Performance Benchmarks (Jan 2026)

## Environment

- **Machine**: MacBook Pro (Apple Silicon)
- **Memory Limit**: 1GB (Baseline), 4GB (Stress)
- **Transport**: gRPC (Insecure)
- **Vectors**: 128-dim Float32 (Random)

## Methodology

- **Tool**: `scripts/perf_test.py`
- **Data**: 10,000 vectors per dataset.
- **Operations**: `DoPut` (Upload), `DoGet` (Download), `Concurrent Mixed` (Put+Get).
- **Concurrency**: 4 Workers.

## Results: TCP Baseline (Loopback)

Standard `localhost` TCP configuration.

| Metric | Result |
|---|---|
| **DoGet Throughput** | **1,692 MB/s** |
| **DoPut Throughput** | 103 MB/s |
| **Concurrent Ops** | **673 ops/s** |
| **Latency (p50)** | 5.5 ms |
| **Latency (p99)** | 19.8 ms |

*Status*: Stable. Correctly handles backpressure.

## Vector Search Performance (100k Scale)

Benchmarked on a single node with 100,000 vectors (128-dim f32) using `scripts/perf_test.py`.

| Search Type | Throughput | p50 Latency | p99 Latency |
|---|---|---|---|
| **Base Search** | 62.1 queries/s | 14.0 ms | 45.4 ms |
| **Filtered Search** | **77.4 queries/s** | **12.3 ms** | **17.6 ms** |

### Analysis: Bitmap-Based Filtering

The introduction of Bitmap-Based Filtering provides significant latency improvements, particularly for tail latencies (P99):

- **Throughput**: ~25% increase at 100k scale.
- **P99 Latency**: ~61% reduction (from 45ms to 17ms).
- **Benefit**: Pre-calculating the filter bitset and using it during HNSW traversal minimizes evaluator overhead and prevents "post-filtering" stalls where HNSW visits many nodes that are eventually filtered out.

## Ingestion Performance (Jan 2026)

Optimizations implemented to resolve `DoPut` throughput regression:

1. **ArrowHNSW by Default**: Replaced legacy `HNSWIndex` with parallelized `ArrowHNSW` (HNSW2).
2. **Lock Optimization**: Moved ID extraction and DiskStore appends outside global lock.
3. **Bulk Updates**: Implemented bulk updates for PrimaryIndex.
4. **Async Hybrid Indexing**: Offloaded text indexing to background worker.

### 128-Dimension Vectors (Float32)

| Scale | Throughput (MB/s) | Throughput (Rows/s) | P99 Latency (Hybrid) |
|---|---|---|---|
| **3k Vectors** | 209.6 MB/s | 362k rows/s | 5.83 ms |
| **15k Vectors** | **541.5 MB/s** | **935k rows/s** | 19.69 ms |
| **50k Vectors** | 450.1 MB/s | 777k rows/s | 5.78 ms |

### 384-Dimension Vectors (Float32)

| Scale | Throughput (MB/s) | Throughput (Rows/s) | P99 Latency (Hybrid) |
|---|---|---|---|
| **3k Vectors** | 223.1 MB/s | 143k rows/s | 6.15 ms |
| **15k Vectors** | 107.0 MB/s | 69k rows/s | 6.12 ms |
| **50k Vectors** | **314.2 MB/s** | **202k rows/s** | 6.90 ms |

## Transport & Stability Analysis

- **TCP Loopback** on macOS is highly optimized and currently superior for high-throughput Arrow Flight transfer.
- **UDS** requires further investigation into buffer sizing and flow control to match TCP stability and performance.
- **Recommendation**: Default to TCP for local sharding until UDS implementation is optimized.
