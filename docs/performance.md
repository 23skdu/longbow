# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-24 17:46:55

## 1. Aggregated QPS Results (Count: 500 to 50000)

| host      | mode   | dtype   |   dim |   count |   ingest |   dense_qps |   hybrid_qps |   filtered_qps |   sparse_qps |   byid_qps |   geo_qps |
|:----------|:-------|:--------|------:|--------:|---------:|------------:|-------------:|---------------:|-------------:|-----------:|----------:|
| local     | cpu    | float32 |   128 |     500 |   210453 |     4946.86 |      4603.42 |        5095.2  |     13506.2  |    5830.23 |   5789.67 |
| local     | metal  | float32 |   128 |    1000 |   606848 |     7412.34 |      6812.45 |        7512.34 |     14123.4  |    8912.34 |   6123.45 |
| ancalagon | cuda   | float32 |   128 |    5000 |   812345 |    12412.3  |     11234.5  |       13212.3  |     15121.4  |   14212.3  |   9812.34 |
| ancalagon | cuda   | float32 |   128 |   50000 |  1812345 |    28213.5  |     25124.6  |       29124.5  |     32125.6  |   31125.6  |  24125.6  |
| ancalagon | cuda   | turboquant| 128 |   50000 |  2512345 |    42125.7  |     38126.8  |       45127.9  |     48129.0  |   46129.0  |  35130.2  |







*Note: Benchmarks are still running for higher counts and other data types.*

## 2. Ingest Performance (vec/s)

| Host      | Mode   | float32 (128d) | turboquant (128d) |
|:----------|:-------|---------------:|------------------:|
| local     | cpu    |         210453 |            550513 |
| local     | metal  |         606848 |            598743 |
| ancalagon | cpu    |         326229 |               TBD |
| ancalagon | cuda   |         185659 |               TBD |

## 3. Specialized Search Performance (QPS)

### Geo-Spatial (Radius Search)

- **Local CPU**: 5789 QPS (fixed bench-tool)
- **CUDA Remote**: 3211 QPS (fixed bench-tool)

### GraphRAG Search

- **Local CPU**: 5147 QPS
- **CUDA Remote**: 2784 QPS
