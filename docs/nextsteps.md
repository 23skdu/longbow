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
**Status**: COMPLETED ✅

**Implementation**:
- CI benchmark workflow already exists (benchmark.yml)
- **Enhanced**: Added specific benchmark categories
  - SIMD benchmarks (Euclidean distance operations)
  - Search benchmarks (HNSW search performance)
  - Insert benchmarks (bulk insert performance)
- **Improvements**:
  - Run separate benchmark suites for each category
  - Compare with benchstat for accurate delta detection
  - Set regression threshold to 5% (was 10%)
  - Fail CI on regression detection (was warning only)
- **Process**: Baseline metrics from main branch vs PR branch

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

## Incomplete Features & Technical Debt

### High Priority

#### 1. GPU/CUDA Support - Incomplete
**Files**: `internal/gpu/*.go`, `internal/gpu/cuda/cgo.go`

**Issues**:
- CUDA memory operations return "not implemented yet" errors:
  - `internal/gpu/memory.go:170` - CUDA memory free
  - `internal/gpu/memory.go:175` - Metal memory free
  - `internal/gpu/memory.go:185-200` - CUDA/Metal memcpy
- FAISS GPU CGO bindings exist but runtime linking incomplete
- GPU build requires `-tags=gpu` but runtime fails without proper CUDA libraries

**Action**: Complete CUDA memory management implementation or document as build-only feature

#### 2. Aligned Sharded Mutex - Incomplete
**File**: `internal/store/aligned_sharded_mutex.go:226`

**Issue**: "For now, just update the count (actual resize not implemented)"

**Action**: Implement resize functionality for AlignedShardedMutex

#### 3. Graph Store - Community Detection Not Implemented
**File**: `internal/store/graph_store.go:151`

**Issue**: "Simplified: return number of subjects as proxy for communities if not implemented"

**Action**: Implement proper community detection algorithm

#### 4. BruteForceIndex - Missing Implementation
**File**: `internal/store/adaptive_index.go:142-143`

**Issue**: `SearchVectorsWithBitmap` returns `nil, nil` - not implemented

**Action**: Implement bitmap-filtered search for BruteForceIndex

#### 5. DiskGraph - Arrow Serialization Not Implemented
**File**: `internal/store/graph_store_test.go:139`

**Issue**: "Arrow serialization not implemented"

**Action**: Implement Arrow serialization for GraphData

#### 6. Recall Validation - Not Implemented
**File**: `internal/store/arrow_validation_test.go:121`

**Issue**: "For now, recall should be 0 (not implemented)"

**Action**: Implement recall validation metrics

### Medium Priority

#### 7. PQ Encoder Training - Not Implemented
**Files**: `internal/pq/encoder_test.go:42-131`

**Issue**: PQ training returns "not implemented" and tests are skipped

**Action**: Implement Product Quantization training

#### 8. NUMA Support - Stub on Non-Linux
**Files**: 
- `internal/store/numa_allocator_stub.go`
- `internal/store/memory.go`
- `internal/store/numa_pin_linux.go`

**Issue**: NUMA allocation/pinning only works on Linux, stubs elsewhere

**Action**: Either implement cross-platform NUMA or document platform limitations

#### 9. Stream Aggregator - Stub Implementations
**File**: `internal/sharding/stream_aggregator.go`

**Issues**:
- Multiple methods return `nil, nil` (lines 126, 185, 290)
- Not fully implemented

**Action**: Complete stream aggregation implementation

#### 10. S3 Backend Integration - Tests Skipped
**File**: `internal/storage/s3_backend_test.go`

**Issue**: All S3 tests skipped unless `S3_TEST_ENDPOINT` is set

**Action**: Add S3 integration test CI or document S3 requirements

#### 11. ONNX Metal Runtime - Stub on Non-macOS
**Files**:
- `internal/onnx/metal/stub.go`
- `internal/onnx/metal/reranker_stub.go`

**Issue**: Metal backend only works on macOS ARM64

**Action**: Document platform limitations or implement cross-platform ONNX

### Lower Priority (Known Limitations)

#### 12. Filter Evaluator - SIMD Not Enabled
**File**: `internal/query/filter_evaluator_test.go`

**Issue**: Many SIMD filter tests are skipped (lines 372-1291)

**Action**: Enable SIMD filter operations

#### 13. Rate Limiter Integration - Skipped
**File**: `internal/store/rate_limit_integration_test.go:11`

**Issue**: Test skipped "due to refactor"

**Action**: Fix or remove skipped test

#### 14. Structured Errors - Skipped
**File**: `internal/store/structured_errors_test.go:9`

**Issue**: Test skipped "due to refactor"

**Action**: Fix or remove skipped test

#### 15. Vector Extraction Test - Skipped
**File**: `internal/store/vector_extraction_test.go:8`

**Issue**: Skipped "due to missing internal helpers"

**Action**: Fix or remove skipped test

#### 16. Vector Search Action Test - Skipped
**File**: `internal/store/vector_search_action_test.go:8`

**Issue**: Skipped "due to undefined mocks"

**Action**: Fix or remove skipped test

#### 17. Arrow Neighbors Test - Skipped
**File**: `internal/store/arrow_neighbors_test.go:38`

**Issue**: Skipped "due to Arrow memory management issues"

**Action**: Fix Arrow memory management or remove test

#### 18. Dataset Map RCU Test - Pending
**File**: `internal/store/dataset_map_rcu_test.go:183`

**Issue**: Test marked "Pending implementation"

**Action**: Implement or remove

#### 19. Generic Quantizer - Limited Type Support
**File**: `internal/store/generic_quantizer_test.go`

**Issues**:
- Float16 not supported (line 264)
- Int8 not supported (line 271)

**Action**: Extend quantizer type support

#### 20. SIMD AVX-512 - Many Tests Skipped
**Files**: 
- `internal/simd/simd_fma_portable_test.go`
- `internal/simd/simd_fma_test.go`

**Issue**: ~40+ tests skipped when AVX-512 not available

**Action**: Document AVX-512 requirements or add fallbacks

### Infrastructure Issues

#### 21. Docker Build - iouring Not Working
**Issue**: `Dockerfile` iouring build fails with "invalid reference to syscall.Sockaddr.sockaddr"

**Status**: Fixed by making BUILD_TAGS configurable

**Action**: Document iouring build requirements

#### 22. Default Metrics Integration Test - Failing
**File**: `internal/store/metrics_integration_test.go:47`

**Issue**: `longbow_hnsw_search_ops_total` counter not incrementing

**Action**: Investigate why search metrics not being recorded

---

## Stub Files (By Design - Conditional Compilation)

These files provide stubs when GPU/Metal features aren't compiled:

| File | When Active |
|------|-------------|
| `internal/gpu/stub.go` | `!gpu` |
| `internal/gpu/memory.go` | `!gpu` |
| `internal/store/store_gpu_stub.go` | `!gpu` |
| `internal/onnx/metal/stub.go` | `!gpu \|\| !darwin \|\| !arm64` |
| `internal/onnx/metal/reranker_stub.go` | `!gpu \|\| !darwin \|\| !arm64` |
| `internal/store/numa_allocator_stub.go` | non-Linux |

These are intentional - not bugs.

---

Last Updated: 2026-03-16
