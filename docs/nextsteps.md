# Longbow Performance Optimization Status

## Current Benchmark Results (2026-03-16)

### SIMD Microbenchmarks (Apple M3 Pro)
| Operation | ns/op | MB/s |
|----------|-------|-------|
| Euclidean128 | 26.92 | 18,229 |
| Euclidean384 | 85.20 | 17,571 |
| Euclidean768 | 131.4 | 23,371 |
| Euclidean1536 | 322.5 | 19,053 |

### Store Benchmarks (22GB)
| Config | DoPut | DoGet |
|--------|-------|-------|
| int64 128 5k | 902 MB/s | 1,035 MB/s |
| int64 384 5k | 1,240 MB/s | 1,063 MB/s |
| float32 128 5k | 737 MB/s | 913 MB/s |
| float32 384 5k | 69 MB/s | 121 MB/s |

---

## 5-Part Performance Improvement Plan

### Priority 1: Investigate float32 384 DoPut Regression (68 MB/s → 1000+ MB/s target)
**Status**: Needs verification after SIMD fix

**Current State**: 68.65 MB/s (was 1,693 MB/s)
**Target**: 1,000+ MB/s

**Actions**:
1. Verify SIMD dispatch fix is being used in DoPut path
2. Check if EnsureChunk is creating new arena each time
3. Profile DoPut to confirm SIMD kernels are invoked

### Priority 2: Investigate int64 DoGet Regression (1,035 MB/s → 1,500+ MB/s target)
**Status**: Investigation needed

**Current State**: 1,035 MB/s (was 1,785 MB/s)
**Target**: 1,500+ MB/s

**Actions**:
1. Run pprof on actual DoGet benchmark
2. Check if cache behavior changed due to arena layout
3. Consider adding prefetching for sequential reads

### Priority 3: Implement Batch SIMD for DoPut Ingestion
**Status**: Proposed

**Current State**: Single-vector insertion
**Target**: Batch insertion with SIMD vectorization

**Actions**:
1. Implement batch vector preparation pipeline
2. Add SIMD vectorized copy for batch insertion
3. Use lock-free queues for parallel batching

### Priority 4: Optimize HNSW Search with SIMD
**Status**: Proposed

**Target**: Improve search QPS

**Actions**:
1. Implement SIMD batch distance computation in search path
2. Add Metal GPU acceleration for search (if available)
3. Optimize ef parameter selection

### Priority 5: Add Comprehensive Benchmark Suite to CI
**Status**: Proposed

**Target**: Catch regressions early

**Actions**:
1. Add performance benchmarks to CI pipeline
2. Set up baseline metrics collection
3. Add alerts for >5% regression

---

## Completed Optimizations ✅

### SIMD Optimizations
- Fixed float32 384/768/1536-dimension SIMD dispatch
- Fixed AVX2 dispatch for all dimensions

### Arena Improvements
- Power-of-2 slab sizes
- Modulo replaced with bit operations
- Fast path in SlabArena.Alloc()

### Arena Support
- Uint16Arena, Uint32Arena support

---

Last Updated: 2026-03-16
