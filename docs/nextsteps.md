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
**Status**: INVESTIGATION COMPLETE ✅

**Findings**:
1. SIMD Euclidean384 benchmark: 85.20 ns/op = 17,571 MB/s ✅ (FAST)
2. Store DoPut float32 384: 69 MB/s ❌ (SLOW)
3. Root cause: The bottleneck is NOT in SIMD distance calculation - it's in the ingestion pipeline

**Analysis**:
- The DoPut benchmark measures entire pipeline: Arrow IPC → Batch processing → Per-vector SetVector → HNSW graph construction
- The SIMD distance calc is only one small part
- Found potential bug: "alloc size must be positive" errors during bulk insert
- The pprof showed EnsureChunk taking 48% of heap - indicating allocation overhead

**Next Steps**:
1. Fix allocation bug causing "alloc size must be positive" errors
2. Profile the actual DoPut pipeline to identify bottleneck
3. Consider batch insertion optimization

### Priority 2: Investigate int64 DoGet Regression (1,035 MB/s → 1,500+ MB/s target)
**Status**: INVESTIGATION IN PROGRESS

**Current State**: 1,035 MB/s (was 1,785 MB/s)
**Target**: 1,500+ MB/s

**Analysis**:
1. DoGet goes through VectorStore.DoGet → retrieves Arrow record batches
2. Retrieval uses chunk-based adaptive batching
3. GetVector() has sequential type checking chain (Float16 → Float64 → Complex64 → Complex128 → Int64 → Uint64 → Int32 → ...)
4. Each type check adds branch prediction overhead

**Potential Issues Identified**:
1. TypedArena.Get() creates zero-value on stack each call for element size calculation
2. Sequential type checking in GetVector() 
3. May need to add type-based fast path lookup

**Next Steps**:
1. Add fast-path lookup for dominant types (float32, int64)
2. Cache element size in TypedArena instead of calculating each time
3. Profile actual retrieval to confirm bottleneck

### Priority 3: Implement Batch SIMD for DoPut Ingestion
**Status**: COMPLEX - Requires Major Refactoring

**Current State**: Single-vector insertion via SetVector()
**Target**: Batch insertion with SIMD vectorization

**Analysis**:
1. Current: Arrow IPC → Per-vector SetVector() → Graph storage
2. Bottleneck: Per-vector processing in AddBatchBulk()
3. Opportunity: SIMD batch copy/conversion

**Complexity**:
- Requires significant refactoring of AddBatchBulk pipeline
- Need to batch type conversions (float32 → float16, etc.)
- Would need to batch vector copies into arena

**Estimated Effort**: 2-3 weeks
**Recommendation**: Defer to Phase 2 after other optimizations stabilize

### Priority 4: Optimize HNSW Search with SIMD
**Status**: IN PROGRESS

**Target**: Improve search QPS

**Current Implementation**:
1. Search uses EuclideanDistance which already has SIMD
2. Batch search uses SIMD dispatch for various dimensions
3. Opportunity: Optimize ef parameter selection, prefetching

**Actions**:
1. Profile search path to identify bottlenecks
2. Add prefetching for graph traversal
3. Consider parallel search for multiple queries

### Priority 5: Add Comprehensive Benchmark Suite to CI
**Status**: IN PROGRESS

**Target**: Catch regressions early

**Actions**:
1. Add performance benchmarks to CI pipeline
2. Set up baseline metrics collection
3. Add alerts for >5% regression

**Note**: Need to define baseline metrics first before adding CI checks

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
