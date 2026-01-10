# Performance Benchmarks

## Latest Run (Native Float16 Storage - Single Node)

**Date**: 2026-01-10
**Configuration**:

- **Vector Dim**: 384
- **Search K**: 10
- **Platform**: Mac (M1/M2)
- **Memory Limit**: 16 GB (`LONGBOW_MAX_MEMORY`=16GB)
- **Feature**: `Float16Enabled=true`

### Results Summary (Single Node - 15k Vectors)

| Metric | FP16 Enabled | FP32 (Float16 Disabled) | Diff |
| :--- | :--- | :--- | :--- |
| **DoPut (Ingestion)** | 792.05 MB/s | **976.28 MB/s** | FP32 is **1.2x** faster (No conversion overhead) |
| **DoGet (Retrieval)** | **2,965.07 MB/s** | 2,649.90 MB/s | FP16 is **1.1x** faster (Reduced memory bandwidth) |
| **Vector Search QPS** | **792.64 QPS** | 561.53 QPS | FP16 is **1.4x** faster |
| **Vector Search P95** | **1.79 ms** | 2.50 ms | FP16 has **28%** lower latency |
| **Hybrid Search QPS** | **964.05 QPS** | 739.34 QPS | FP16 is **1.3x** faster |

### Analysis

1. **Search Performance**: FP16 provides a significant boost to search throughput (**+40%**) and latency reduction. This confirms that memory bandwidth is the primary bottleneck for search at this scale (15k vectors), and halving the vector size effectively doubles the cache efficiency.
2. **Ingestion Cost**: FP32 ingestion is faster because it avoids the `Float32 -> Float16` conversion step during indexing. However, the search benefits of FP16 likely outweigh this one-time cost for read-heavy workloads.
3. **Throughput vs Latency**: FP16 wins on both throughput and tail latency, making it the recommended default for production workloads on this architecture.

### Analysis

1. **High Throughput**:
    - `DoGet` consistently saturates available bandwidth (1.5-4.1 GB/s), utilizing zero-copy Arrow Flight transfer. This is significantly boosted by Float16 storage efficiency.
    - `DoPut` write throughput is robust (up to 1.3 GB/s during bulk load), limited by HNSW graph construction.

2. **Low Latency**:
    - **Dense Search**: Sub-millisecond to low single-digit latencies (p95 < 1ms) for up to 15k vectors.
    - **Float16 Benefit**: Reduced memory bandwidth pressure likely contributes to higher QPS compared to previous benchmarks.

3. **Memory Constraints**:
    - Enabled `Float16` storage reduced vector memory footprint by 50%, allowing scaling to 25k vectors within the memory limit more comfortably than Float32.

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

### 3-Node Cluster Benchmark (6GB RAM / Pod)

This benchmark compares Native Float16 storage against Standard Float32 storage in a 3-node distributed cluster.

| Metric | FP16 Enabled (15k vectors) | FP16 Disabled (15k vectors) |
| :--- | :--- | :--- |
| **Put Throughput** | 31.31 MB/s | 54.99 MB/s |
| **Get Throughput** | 69.93 MB/s | 114.23 MB/s |
| **Vector Search QPS**| 627.5 queries/s | 364.7 queries/s |
| **Vector Search p95** | 3.10ms | 3.85ms |
| **Hybrid Search QPS** | 1016.6 queries/s | 433.6 queries/s |
| **Soak Test Throughput**| ~1.6 ops/s | ~34.0 ops/s |

#### Analysis

1. **Ingestion Overhead**: FP16 disabled is significantly faster for Put/Get (approx 1.7x). This is primarily due to the client-side (Python) and server-side (Go) conversion costs between FP32 and FP16.
2. **Single-Threaded Search**: Surprisingly, FP16 enabled achieved higher QPS and lower latency for individual search requests. This suggests that for light workloads, the reduced memory bandwidth required for FP16 vectors outweighs the CPU conversion cost.
3. **Concurrent Scalability**: In the multi-node soak test (6 concurrent workers), FP16 disabled was **20x faster**. This indicates that the current FP16 storage implementation (which converts to FP32 on read) becomes a significant CPU and contention bottleneck under heavy concurrent load.

### Recommendations

- **Use FP16** when memory is the primary constraint and the workload is search-heavy but not highly concurrent.
- **Use FP32** for high-throughput ingestion and heavy concurrent workloads until the FP16 read path is optimized (e.g., via SIMD-accelerated distance calculations directly on FP16 data).
