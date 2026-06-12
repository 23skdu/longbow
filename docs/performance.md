# Longbow Performance Benchmark Results

**Generated**: 2026-06-12
**Platform**: Linux x86_64 (1 NUMA node, 16 cores)
**Memory**: 16 GB allocated
**Test Tool**: `scripts/unified_benchmark.py` (CPU mode)
**Queries**: 500 per test configuration
**Range**: Dims 128,384 | 16 datatypes | Counts 10K, 50K, 100K, 500K | All 13 search modes

## Summary

All 128 configurations completed with **zero errors** across all search modes.

| Metric | Best | Worst |
|--------|------|-------|
| Ingest throughput | 3,263,137 vec/s (uint8, dim128, 50K) | 38,440 vec/s (complex128, dim384, 500K) |
| Dense search QPS | 4,678 QPS (uint8, dim384, 100K) | 59 QPS (turboquant, dim128, 500K) |
| Dense P50 latency | 1.666ms (uint8, dim384, 10K) | 240.079ms (turboquant, dim128, 500K) |

## Dtype Performance Ranking (averaged across all dims/counts)

| Rank | Dtype | Avg Ingest | Avg Dense QPS | Avg Dense P50 |
|------|-------|-----------|--------------|--------------|
| 1 | uint8 | 877,513 vec/s | 3,692 QPS | 5.21ms |
| 2 | float32 | 294,417 vec/s | 2,788 QPS | 5.94ms |
| 3 | int8 | 762,194 vec/s | 2,320 QPS | 8.97ms |
| 4 | int32 | 307,478 vec/s | 2,234 QPS | 11.34ms |
| 5 | turboquant | 254,619 vec/s | 2,030 QPS | 28.31ms |
| 6 | float16 | 453,834 vec/s | 1,887 QPS | 9.96ms |
| 7 | complex128 | 93,476 vec/s | 1,720 QPS | 19.11ms |
| 8 | complex64 | 159,542 vec/s | 1,192 QPS | 30.89ms |
| 9 | float64 | 175,331 vec/s | 1,185 QPS | 35.48ms |
| 10 | int16 | 544,350 vec/s | 1,015 QPS | 21.42ms |
| 11 | uint64 | 160,754 vec/s | 756 QPS | 38.93ms |
| 12 | int64 | 151,201 vec/s | 696 QPS | 28.78ms |
| 13 | uint32 | 300,821 vec/s | 633 QPS | 29.05ms |
| 14 | uint16 | 490,226 vec/s | 617 QPS | 29.38ms |

## Search Mode Comparison (float32, dim128, 500K vectors)

| Mode | QPS | P50 | P95 | P99 |
|------|-----|-----|-----|-----|
| sparse | 6,713 | 2.339ms | 2.997ms | 4.303ms |
| learnedindex | 3,239 | 4.778ms | 5.872ms | 6.353ms |
| recommend | 3,160 | 4.955ms | 5.975ms | 6.499ms |
| byid | 3,061 | 5.096ms | 6.230ms | 6.792ms |
| dense | 2,885 | 5.347ms | 7.171ms | 8.576ms |
| hybrid | 2,876 | 5.548ms | 7.108ms | 8.640ms |
| globalgraphrag | 2,383 | 6.461ms | 8.574ms | 9.936ms |
| graphrag | 1,924 | 7.119ms | 13.789ms | 16.401ms |
| filteredstring | 1,069 | 8.347ms | 13.296ms | 224.103ms |
| filteredbool | 1,014 | 3.093ms | 7.545ms | 375.363ms |
| temporal | 781 | 18.727ms | 27.988ms | 31.152ms |
| filtered | 634 | 3.260ms | 5.427ms | 667.528ms |
| geo | 54 | 273.455ms | 512.058ms | 599.595ms |

## Scale Behavior

Ingest throughput drops sharply at scale due to HNSW graph construction overhead:

| Count | Avg Ingest (float32, dim128) | Notes |
|-------|-------------------------------|-------|
| 10K | 604,263 vec/s | Warm cache, small graph |
| 50K | 993,435 vec/s | Peak throughput (better batch utilization) |
| 100K | 92,600 vec/s | HNSW edge construction dominates |
| 500K | 53,606 vec/s | Fully constrained by HNSW build |

## Optimization Impact (v0.2.2-rc1)

Go baseline optimizations in `internal/simd/simd_baseline.go` and `internal/simd/dispatch.go`:

| Optimization | Files Changed | Notable Gains |
|-------------|---------------|--------------|
| uint64 4x→8x unrolled Euclidean + scalar→4x unrolled cosine | simd_baseline.go | +709% dense QPS at dim128 500K |
| int32/uint32: float64→int64/uint64 accumulators | simd_baseline.go | +76% int32 dense QPS at dim128 50K |
| int64 4x→8x unrolled Euclidean + scalar→4x unrolled cosine | simd_baseline.go | +51% dense QPS at dim128 50K |
| uint8/uint16 AVX2 dispatch fix (dot product) | dispatch.go | Prevents fallback-to-unrolled on AVX2 systems |
| Geo quadtree: bounding-box-only prefilter (remove double Haversine) | geo_search.go | Eliminates redundant per-point Haversine in quadtree |

## Known Issues

- **turboquant** at 500K count shows degraded search performance (59 QPS) — HNSW build with turboquant distance may need tuning at scale
- **geo** search remains the slowest mode (54 QPS) — Haversine distance is not SIMD-accelerated
- **filtered** search modes have high P99 tail latency (>200ms) due to occasional slow filter evaluations
- **uint64** and **uint16** have the lowest avg dense QPS — memory bandwidth limited for 8-byte elements

## Notes

- **sparse** search mode is 2-3x faster than dense across all configurations
- **learnedindex** (learned IVF) closely matches dense QPS with competitive latency
- **temporal** search has elevated latency due to SegmentTree construction overhead per query
- **filteredstring** was optimized in a prior commit (raw Arrow buffer access) and shows competitive median latency
- All results are from CPU mode on a single node
