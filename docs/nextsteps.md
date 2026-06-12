# Recommended Next Steps (Updated 2026-06-12)

Based on the full benchmark matrix (128 configs, 16 dtypes, 2 dims, 4 counts, all query modes, zero errors).

## Priority 1: Turboquant at Scale

**Problem**: turboquant search drops to 59 QPS at 500K (dim128) — a catastrophic regression from smaller counts (~2,000 QPS at 100K).

**Suspected cause**: The HNSW build uses turboquant-decompressed vectors for distance computation. At 500K, the memory bandwidth pressure from decompressing on every distance call becomes the bottleneck.

**Fix options**:
- Implement SIMD-accelerated turboquant distance (skip decompression, compute directly on packed bits)
- Reduce efConstruction for turboquant at 500K+ via adaptive parameter tuning
- Profile HNSW build with pprof to confirm bottleneck (profiles available in `profiles/`)

## Priority 2: Geo Search SIMD Haversine

**Problem**: Geo search averages 54 QPS at 500K — slowest mode by 10x.

**Status**: Quadtree double-Haversine was eliminated in v0.2.2-rc1. But `haversineBatchAVX2` is a stub that calls scalar Go.

**Fix**: Break Haversine into batched sin/cos/sqrt/atan2 passes using the existing SIMD primitives (`SinFloat32`, `CosFloat32`, `Atan2Float32` — all have real AVX2/AVX-512/Neon implementations).

**Expected gain**: 2-4x

## Priority 3: HNSW Build Profiling

**Problem**: Ingest drops from 993K vec/s (50K) to 53K vec/s (500K) for float32. ~95% of time spent on HNSW construction.

**Existing data**: 1,792 pprof profiles collected across all configs in `profiles/`.

**Suggested analysis**:
- Profile `profiles/*_500000_profile_*.pprof` to identify exact HNSW bottleneck
- Compare distance computation vs graph traversal vs allocation
- Consider batched graph construction (Vamana-style) for bulk loads

## Priority 4: Filtered String Auto-Indexing

**Problem**: `filteredstring` has high P99 tail latency (224ms at 500K) due to full metadata scan.

**Fix**: Implement inverted index or bloom filter for string attribute columns.

## Priority 5: Filter P99 Tail Latency

**Problem**: All `filtered*` modes have P99 > 200ms at 500K (float32, dim128). The 99th percentile is 20-100x the median.

**Fix**: Investigate filter evaluation timeout/retry logic; add a fast-path for common filter patterns.

## Priority 6: Benchmark Infrastructure

- Add automatic heatmap generation to benchmark script
- Compare runs with statistical significance (multiple runs, stddev reporting)
- Add `--turboquant-bits` to differentiate turboquant2/4/8 in results

## Priority 7: Memory Profiling

- Analyze heap profiles in `profiles/` for memory fragmentation at scale
- Focus on transition between 100K and 500K where ingest drops 2x

## Data Available

- 1,792 pprof profiles (7 per config: cpu, heap, allocs, goroutine, threadcreate, block, mutex)
- 128-config JSON matrix in `data/perf_logs/perf_matrix_cpu_20260612_125608.json`
- 3 targeted retest runs (uint32, uint64, int64) for cross-validation
