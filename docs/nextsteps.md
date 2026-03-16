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
**Status**: IMPLEMENTED - Foundation laid ✅

**Progress**:
- [x] Added `SetVectorsBatch()` method in GraphData for efficient batch vector copying
- [x] Added `prefetch()` function for HNSW search optimization
- [x] Implemented prefetching in `searchLayer()` for better cache locality
- [ ] Optimize AddBatchBulk() to use SetVectorsBatch (future enhancement)

**Details**:
- SetVectorsBatch() enables batch copying of multiple vectors in the same chunk
- This reduces per-vector overhead in the hot path
- Future: Refactor AddBatchBulk() to group vectors by chunk and use this method

### Priority 4: HNSW Search Prefetching (Optimization)
**Status**: IN PROGRESS - Code-level investigation complete ✅

**Key Findings**:
1. **searchLayer()** (line 1349-1676): Main search bottleneck
2. **Memory access pattern**: 
   - Gets neighbors sequentially via `data.GetNeighbors(layer, curr.ID, nil)` (line 1631)
   - Each neighbor triggers `distComputer(n)` which calls `getVectorWithData()`
   - This causes many random memory accesses

3. **Optimization Opportunities**:
   - **Neighbor prefetching**: Prefetch neighbor data while computing distances
   - **Vector prefetching**: Prefetch vector data before distance calculation
   - **Cache locality**: Process neighbors in batches

**Implementation Plan**:
1. Add prefetch hints for neighbor list access
2. Batch distance calculations for multiple neighbors  
3. Use SIMD batch distance computation

**Progress**:
- [x] Code analysis complete
- [ ] Implement neighbor prefetching
- [ ] Implement vector prefetching
- [ ] Test and benchmark

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
