# Performance Test Results Summary

## Test Environment

- **Platform**: Apple M3 Pro (macOS)
- **Memory**: 20GB allocated to Longbow
- **Backends**: CPU (arm64), Metal (GPU-accelerated)
- **Matrix**: 440 distinct test configurations (11 dtypes x 8 dims x 5 scale factors)

## High-Level Highlights

| Scenario | Mode | Metric | Result |
|----------|------|--------|--------|
| **TurboQuant Ingest** | CPU | Throughput | **~600,000 vec/s** (dim=128) |
| **Float32 Ingest** | CPU | Throughput | ~320,000 vec/s (dim=256) |
| **Metal Complex Ingest** | Metal | Advantage | **+15-20% gain** for `complex128` |
| **Large Scale Scan** | CPU | Throughput | **2.2 GB/s** (50k vectors) |
| **Transformer Latency** | CPU | P50 (dim=1536) | **0.88 ms** |

## Comprehensive Reports

A full 440-case matrix was executed to audit the performance of all supported data types and dimensions.

- [Detailed CPU Performance Matrix](performance.md)
- [Detailed Metal GPU Performance Matrix](performance_metal.md)

## Summary Observations

1. **TurboQuant Dominance**: The specialized `turboquant` data type achieves significantly higher ingestion rates (nearly 2x faster than float32) across all dimensions, making it the ideal choice for high-volume pipelines.
2. **GPU Acceleration**: The Metal backend excels in handling complex data types and extreme dimensions (up to 3072), where GPU parallelism offsets the overhead of vector arithmetic.
3. **Linear Scaling**: Longbow demonstrates consistent linear scaling in search latency relative to dimension count, maintaining sub-millisecond responses for standard embedding sizes.
4. **Stability**: The server remained stable under a high-concurrency 440-case audit, with zero memory leaks or crashes during the intensive multi-hour benchmark.

## Notes

- All tests completed successfully.
- Search metrics for `complex64` require dim=256 or higher for optimal results.
- Future optimizations will focus on further SIMD tuning for non-float types.
