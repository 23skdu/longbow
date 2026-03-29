# Performance Documentation

Longbow delivers exceptional performance across all supported data types and dimensions. This section contains benchmark results and performance tuning guidance.

## Quick Reference

| Scenario | Mode | Metric | Result |
|----------|------|--------|--------|
| **TurboQuant Ingest** | CPU | Throughput | **~600,000 vec/s** (dim=128) |
| **Float32 Ingest** | CPU | Throughput | ~320,000 vec/s (dim=256) |
| **Metal Complex Ingest** | Metal | Advantage | **+15-20% gain** for `complex128` |
| **Large Scale Scan** | CPU | Throughput | **2.2 GB/s** (50k vectors) |
| **Transformer Latency** | CPU | P50 (dim=1536) | **0.88 ms** |

## Detailed Benchmarks

### CPU Performance Matrix
- [CPU Performance Data](performance.md) - Full benchmark matrix for CPU-only mode

### GPU Performance Matrix
- [Metal GPU Performance Data](performance_metal.md) - Apple Metal GPU benchmarks

### I/O Baseline
- [Throughput Baselines](throughput_baseline.md) - Raw I/O performance measurements

## Performance Highlights

### Key Findings

1. **TurboQuant Dominance**: The specialized `turboquant` data type achieves significantly higher ingestion rates (nearly 2x faster than float32) across all dimensions, making it the ideal choice for high-volume pipelines.

2. **GPU Acceleration**: The Metal backend excels in handling complex data types and extreme dimensions (up to 3072), where GPU parallelism offsets the overhead of vector arithmetic.

3. **Linear Scaling**: Longbow demonstrates consistent linear scaling in search latency relative to dimension count, maintaining sub-millisecond responses for standard embedding sizes.

4. **Stability**: The server remained stable under high-concurrency benchmarks, with zero memory leaks or crashes during intensive multi-hour benchmark runs.

## Data Type Optimization Status

| Dimension | float32 | float64 | int32 | int16 | int8 | complex64 | turboquant |
|-----------|---------|---------|-------|-------|------|------------|--------------|
| 128 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 256 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 384 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 768 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1024 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1536 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 2048 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 3072 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |

**Legend:**
- ✅ Blocked = Blocked SIMD implementation (256/512 byte blocks)
- ✅ Blocked+Prefetch = Blocked SIMD with prefetch hints (1536+ only)
- ✅ Optimized = Direct SIMD kernels (128-384)

## Optimization History

### Completed Optimizations

| Optimization | Impact |
|-------------|--------|
| Blocked SIMD for float/int/uint (768+) | +30-50% QPS |
| Complex64/128 blocked via cast | +20-30% QPS |
| TurboQuant NEON Kernels (FWHT) | +3.7x Core / +40% QPS |
| HNSW M=32 for 768+ dims | +15-20% QPS |
| Prefetch for 1536+ dims | +10-15% QPS |

## Tuning Tips

### For Maximum Throughput
- Use TurboQuant for 4-8x memory reduction with minimal recall loss
- Enable Metal GPU on Apple Silicon for complex types
- Use batched operations for ingestion

### For Low Latency
- Use lower dimensions (128-384) when possible
- Enable CPU affinity (see [NUMA configuration](numa.md))
- Pre-warm caches with initial queries

### For Large Datasets
- Enable [Eviction](eviction.md) policies
- Use [Persistence](persistence.md) for checkpointing
- Consider [Distributed Architecture](distributed_architecture.md) for horizontal scaling
