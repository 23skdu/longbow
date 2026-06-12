# Longbow Performance Benchmark Results

**Generated**: 2026-06-12
**Platform**: Linux x86_64 (1 NUMA node, 16 cores)
**Memory**: 16 GB allocated
**Test Tool**: `scripts/unified_benchmark.py` (CPU mode)
**Queries**: 500 per test configuration
**Range**: Dims 128,384 | 17 datatypes | Counts 10K, 50K, 100K, 500K | All search modes

## Summary

All 136 configurations completed with **zero errors** across all search modes (dense, hybrid, filtered, filteredbool, filteredstring, sparse, byid, graphrag, globalgraphrag, recommend, geo, temporal, learnedindex).

| Metric | Best | Worst |
|--------|------|-------|
| Ingest throughput | 3,346,718 vec/s (int8, dim128, 50K) | 39,328 vec/s (complex128, dim384, 500K) |
| Dense search QPS | 5,793 QPS (uint8, dim128, 500K) | 221 QPS (float64, dim384, 500K) |
| Dense P50 latency | 1.351ms (uint8, dim128, 500K) | 36.626ms (float64, dim384, 500K) |

## Dtype Performance Ranking (averaged across all dims/counts)

| Rank | Dtype | Avg Ingest | Avg Dense QPS | Avg Dense P50 |
|------|-------|-----------|--------------|--------------|
| 1 | uint8 | 887,295 vec/s | 4,036 QPS | 2.33ms |
| 2 | turboquant | 294,938 vec/s | 2,391 QPS | 7.34ms |
| 3 | float32 | 310,343 vec/s | 2,864 QPS | 3.38ms |
| 4 | int8 | 922,400 vec/s | 2,111 QPS | 4.94ms |
| 5 | int32 | 330,775 vec/s | 1,873 QPS | 7.62ms |
| 6 | float16 | 491,630 vec/s | 1,544 QPS | 8.73ms |
| 7 | complex128 | 104,483 vec/s | 1,494 QPS | 11.38ms |
| 8 | int16 | 574,973 vec/s | 1,435 QPS | 11.52ms |
| 9 | complex64 | 184,162 vec/s | 1,268 QPS | 13.62ms |
| 10 | float64 | 183,259 vec/s | 1,260 QPS | 14.35ms |
| 11 | uint64 | 175,910 vec/s | 1,093 QPS | 14.39ms |
| 12 | int64 | 187,249 vec/s | 734 QPS | 15.32ms |
| 13 | uint16 | 520,145 vec/s | 617 QPS | 15.18ms |
| 14 | uint32 | 330,900 vec/s | 585 QPS | 16.45ms |

## Search Mode Comparison (float32, dim128, 500K vectors)

| Mode | QPS | P50 | P95 | P99 |
|------|-----|-----|-----|-----|
| dense | 963 | 7.475ms | 10.251ms | 29.171ms |
| hybrid | 956 | 8.072ms | 10.642ms | 13.774ms |
| filtered | 561 | 7.526ms | 11.447ms | 47.530ms |
| filteredbool | 601 | 7.701ms | 10.726ms | 118.610ms |
| filteredstring | 55 | 137.959ms | 245.438ms | 458.009ms |
| sparse | 7,047 | 1.103ms | 1.538ms | 1.739ms |
| byid | 1,019 | 7.337ms | 10.342ms | 27.868ms |
| graphrag | 950 | 8.043ms | 10.689ms | 17.571ms |
| globalgraphrag | 862 | 8.831ms | 11.612ms | 13.000ms |
| recommend | 953 | 8.058ms | 10.694ms | 28.967ms |
| geo | 49 | 151.064ms | 304.392ms | 479.307ms |
| temporal | 800 | 9.537ms | 13.313ms | 15.162ms |
| learnedindex | 1,007 | 7.296ms | 10.098ms | 41.376ms |

## Scale Behavior

Ingest throughput drops sharply at scale due to HNSW graph construction overhead:

| Count | Avg Ingest (float32, dim128) | Notes |
|-------|-------------------------------|-------|
| 10K | 469,962 vec/s | Warm cache, small graph |
| 50K | 1,193,498 vec/s | Peak throughput (better batch utilization) |
| 100K | 92,402 vec/s | HNSW edge construction dominates |
| 500K | 53,443 vec/s | Fully constrained by HNSW build |

## Memory Usage

16GB was sufficient for all 136 configurations including 500K vectors at dim 384 with complex128 (the largest data type). No OOM or ResourceExhausted errors were encountered.

## Optimization Impact (dim 128, 50K vectors)

Baseline vs optimized Go baseline implementations. All changes in `internal/simd/simd_baseline.go` and `internal/simd/dispatch.go`.

| Dtype | Mode | Before (QPS) | After (QPS) | Improvement | Change |
|-------|------|-------------|-------------|-------------|--------|
| int32 | dense | 2,060 | 2,688 | +30.5% | int64 accumulator for dot/Euclidean |
| int32 | hybrid | 1,419 | 2,015 | +42.0% | (eliminates float64 conversions) |
| uint32 | dense | 662 | 1,195 | **+80.4%** | uint64 accumulator for dot/Euclidean |
| uint32 | hybrid | 488 | 754 | **+54.5%** | (eliminates float64 conversions) |
| int64 | dense | 901 | 1,073 | +19.0% | 8x unrolled Euclidean + 4x unrolled cosine |
| int64 | hybrid | 434 | 648 | **+49.2%** | (better instruction-level parallelism) |
| uint64 | dense | 696 | 3,552 | **+410.3%** | 8x unrolled Euclidean + 4x unrolled cosine |
| uint64 | hybrid | 458 | 3,781 | **+726.3%** | (was scalar loop, now 8x/4x unrolled) |
| uint8 | dense | 4,612 | 4,553 | -1.3% | AVX2 dispatch fix (within margin) |
| uint16 | dense | 753 | 879 | **+16.7%** | AVX2 dispatch fix |

**Key Insight**: The uint64 integer types saw the largest gains because their distance computations were previously bottlenecked by scalar loops. The 8x unrolled Euclidean eliminates loop-carried dependencies for ~8x better instruction-level parallelism. The uint32 accumulator change (uint64 instead of float64) avoids 6 conversions per element.

## Notes

- **sparse** search mode is consistently 5-8x faster than dense across all configurations
- **filteredstring** is the slowest search mode (full text scan per query)
- **geo** search is slow across all configurations (~50 QPS) — quadtree Haversine overhead was partially reduced (bounding-box-only prefilter, removing double Haversine computation)
- **int16/uint16/int32/uint32/int64/uint64** no longer have disproportionately high search latency — the Go baseline implementations were optimized with integer accumulators and unrolled loops
- **turboquant** variants all produce identical JSON dtype labels but perform differently in practice (2-bit, 4-bit, 8-bit packs)
- Small counts (10K) show artificially high ingest rates due to minimal HNSW work
- All results are from CPU mode on a single node
