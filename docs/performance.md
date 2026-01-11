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

## Analysis

- **TCP Loopback** on macOS is highly optimized and currently superior for high-throughput Arrow Flight transfer.
- **UDS** requires further investigation into buffer sizing and flow control to match TCP stability and performance.
- **Recommendation**: Default to TCP for local sharding until UDS implementation is optimized.
