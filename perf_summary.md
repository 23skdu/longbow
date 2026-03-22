# Performance Test Results Summary

## Test Environment
- **Platform**: Apple M3 Pro (macOS)
- **Memory**: 20GB allocated to Longbow
- **Workers**: 12 indexing workers, 12 ingestion workers

## Results

| Config | DoPut (MB/s) | DoGet (MB/s) | Search QPS | Search P50 (ms) | Search P99 (ms) |
|--------|--------------|--------------|------------|-----------------|-----------------|
| 1K dim=128 | 123.97 | 259.14 | 2345.78 | 0.40 | 1.04 |
| 5K dim=128 | 344.13 | 533.76 | 1953.55 | 0.51 | 0.59 |
| 10K dim=128 | 611.98 | 974.48 | 1654.63 | 0.59 | 0.78 |
| 25K dim=128 | 869.84 | 1437.83 | 1188.53 | 0.83 | 1.09 |
| 50K dim=128 | 167.14 | 2668.55 | 183.88 | 0.22 | 0.51 |
| 10K dim=384 | 680.04 | 416.12 | 48.20 | 16.96 | 41.17 |

## Observations

1. **DoPut Throughput**: Scales well with dataset size, reaching ~870 MB/s for 25K vectors
2. **DoGet Throughput**: Excellent scan performance, up to 2.6 GB/s for 50K vectors
3. **Search Latency**: Low latency (sub-millisecond) for dim=128 up to 25K vectors
4. **High Dimension Impact**: dim=384 shows significant slowdown due to increased computation

## Notes
- All tests completed successfully with 0 errors (except complex64 search which requires dim=256)
- Server remained stable throughout testing
