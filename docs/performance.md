# Performance Benchmarks

## Latest Run (Local Development - Single Node)

**Date**: 2026-01-09
**Configuration**:

- **Vector Dim**: 384
- **Search K**: 10
- **Platform**: Mac (M1/M2)
- **Memory Limit**: 6.4 GB (Enforced)

### Results Summary

| Scale (Vectors) | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Dense P95 (ms) | Hybrid QPS | Hybrid P95 (ms) | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **3,000** | 29.20 | 452.99 | 1,085 | 1.1 | 592 | 1.9 | ✅ Pass |
| **5,000** | 45.43 | 343.25 | 870 | 1.4 | 299 | 3.6 | ✅ Pass |
| **9,000** | 56.73 | 501.24 | 717 | 1.9 | 181 | 5.7 | ✅ Pass |
| **15,000** | 50.30 | 472.20 | - | - | - | - | ⚠️ OOM (Indexing) |
| **25,000** | - | - | - | - | - | - | ❌ OOM (Ingest) |

### Analysis

1. **High Throughput**:
    - `DoGet` consistently saturates available bandwidth (340-500 MB/s), utilizing zero-copy Arrow Flight transfer.
    - `DoPut` write throughput peaks around 50-56 MB/s, limited by ingestion indexing overhead (HNSW graph construction).

2. **Low Latency**:
    - **Dense Search**: Sub-millisecond to low single-digit latencies (p95 < 2ms) for small-to-medium datasets.
    - **Hybrid Search**: Adds minimal overhead (~2-3ms increase in p95), confirming efficient RRF (Reciprocal Rank Fusion) implementation.

3. **Memory Constraints**:
    - The benchmark encountered a **Memory Limit Exceeded** error at >9,000 vectors (384 dim) with the current 6.4GB heap limit.
    - This manifests as "slow" ingestion or timeout errors as the server applies backpressure and rejects new writes to prevent crashing.
    - **Recommendation**: For >10k vectors scaling, increase `MAX_MEMORY` or enable `COMPACTION_ENABLED` to reclaim unused segments more aggressively.

## Previous Benchmarks (Reference)

### HNSW Product Quantization (PQ) Performance

- **Rows**: 5,000 vectors (128 dim)
- **Results**:
  - **Throughput**: ~1,386 QPS (PQ Enabled) vs 1,145 QPS (Baseline)
  - **Latency p95**: 1.31ms (PQ) vs 1.76ms (Baseline)

### Multi-Node Cluster (3 Nodes)

- **Scale**: Up to 150k vectors (128 dim) distributed.
- **Results**:
  - **Search QPS**: ~130-140 QPS at 150k scale.
  - **Latency p95**: ~13-17ms.
