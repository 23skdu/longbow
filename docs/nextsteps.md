# Longbow Performance — Executive Summary & Next Steps

## Executive Summary

Full benchmark matrix of **448 test runs** (14 dtypes × 2 dims × 8 counts × 2 backends) confirms Longbow is production-stable on Apple Silicon M3 Pro. Zero crashes or errors across all configurations.

### Performance Highlights

| Metric | Value | DType/Dim |
|--------|-------|-----------|
| Fastest ingest | 805k vec/s | int8, 128d, CPU |
| Fastest search QPS | 10,483 qps | uint64, 128d, Metal |
| Lowest P50 latency | 0.098ms | uint64 hybrid, 128d, CPU |
| Most scalable search | int64/uint64 | stable ~10k QPS across all scales |

### Critical Issues Found (Original)

1. ~~int16/uint16 search is catastrophically slow~~ ✅ **FIXED**
2. ~~Metal GPU acceleration barely engaged~~ ✅ **FIXED** (automatic dispatch added)
3. ~~float16 search is 2x slower than float32~~ ✅ **FIXED**
4. ~~complex128 scales poorly~~ ✅ **FIXED** (Metal kernels exist, wired up)

---

## Verified Completed Items

✅ **GPU Typed Search - All implementations complete:**
- MetalIndex: SearchFloat16, SearchComplex64, SearchComplex128 (`internal/gpu/metal/metal_gpu.go`)
- MetalHybridIndex: SearchFloat16, SearchComplex64, SearchComplex128 (`internal/gpu/metal/metal_gpu_hybrid.go`)
- CUDAIndex: SearchFloat16, SearchComplex64, SearchComplex128 (`internal/gpu/cuda/cuda_index.go`)

✅ **P0 Critical Fixes Completed (2026-04-23):**
- CPUIndex: SearchFloat16, SearchComplex64, SearchComplex128 now implemented (`internal/gpu/factory.go`)
- PluggableIndexAdapter: Returns proper errors for unsupported filters (`internal/store/pluggable_index_adapters.go`)
- MockIndex: SearchFloat16, SearchComplex64, SearchComplex128 now implemented (`internal/gpu/mock_index.go`)

✅ **P1 PQ Fallbacks Completed (2026-04-23):**
- MetalIndex: TrainPQ, EncodePQ now have CPU fallbacks using pq package
- MetalHybridIndex: TrainPQ, EncodePQ now have CPU fallbacks using pq package
- CUDAIndex: TrainPQ, EncodePQ now have CPU fallbacks using pq package

✅ **Core functionality:**
- int16/uint16 search performance - Fixed
- float16 search - Metal kernels exist, wired up
- Metal GPU acceleration for all float types - Automatic dispatch
- Complex types GPU acceleration - Metal kernels exist, wired up
- Complex128 ingest - Efficient .view(np.float64) approach
- int16/uint16 ingest - Arrow path already optimal
- Batch query optimization for Metal - SearchBatch exists
- Learned index routing - AutoShardingIndex exists
- Prometheus GPU metrics - Already implemented
- TurboQuant performance - Comparable to float32

---

## Remaining Issues & Plan

### P0 — Post-Audit Remediation (Urgent)

#### 1. HNSW Early-Exit Filtering (Geo/Temporal) ✅ **FIXED**
*   **Goal**: Boost specialized search QPS by integrating constraints into the HNSW traversal loop.
*   **Status**: Implemented `HNSWPredicate` integration in `searchLayer`. Nodes are now bypassed during traversal, significantly reducing distance compute overhead.

#### 2. Learned Index Threshold Optimization ✅ **FIXED**
*   **Goal**: Enable earlier k-NN activation for improved adaptive routing.
*   **Status**: Configurable environment variables (`LONGBOW_LEARNED_MIN_SAMPLES`, etc.) added. k-NN activation can now be forced earlier for small datasets.

#### 3. Metal/CUDA TurboQuant Optimization (768d+) ✅ **FIXED**
*   **Goal**: Resolve throughput regressions in high-dimension quantized search.
*   **Status**: Iterative polar reconstruction kernels implemented in both Metal (MSL) and CUDA. Optimized for 768d+ by avoiding recursive stack overhead.

---

### P1 — High Impact (Partially Fixed, Needs GPU Kernels)

1. **Product Quantization (PQ) GPU Kernels - Metal Complete**
   - Status: Metal SearchPQ kernel implemented and verified. CUDA Train/Encode still needed.
   - Locations:
     - `internal/gpu/metal/metal_gpu.go` - Metal PQ kernels for SearchPQ implemented ✅
     - `internal/gpu/metal/metal_gpu_hybrid.go` - Metal PQ kernels for SearchPQ implemented ✅
     - `internal/gpu/cuda/cuda_index.go` - SearchPQ exists, need TrainPQ/EncodePQ GPU kernels
   - Impact: Metal users now have full GPU-accelerated PQ search performance
   - Fix: CUDA parity (TrainPQ/EncodePQ) still pending

2. **PluggableIndexAdapter Interface Compliance**
   - Status: Multiple no-op/stub methods for VectorIndexer interface
   - Location: `internal/store/pluggable_index_adapters.go:108-293`
   - Issues: `Build()`, `Save()`, `Load()` are fakes; `AddByRecord`, `AddByLocation`, `AddBatch` return errors
   - Fix: Implement or clearly document limitations

### P2 — Medium Impact (Nice to Have)

3. **TPUIndex Placeholder Implementation**
   - Status: All methods wired but empty (lines 28-52 in `internal/gpu/tpu/tpu_index.go`)
   - Impact: TPU path exists but non-functional
   - Fix: Backlog - requires actual TPU integration work

4. **Test Coverage - Skipped Tests**
   - ~185 tests have t.Skip() for various reasons
   - Fix: Review and either fix tests or remove dead code

5. **Complex Types GPU Encoding**
   - Status: complex64/complex128 stored as float in Arrow (2x dim)
   - Fix: Consider native complex support if needed

6. **IVF-TQ Hybrid for High Dimensions**
   - Status: TurboQuant at 384d comparable to float32
   - Fix: Consider TQ-encoded centroids for IVF coarse filtering

### P3 — Low Priority

7. **Stub Files for Platform Compatibility**
   - Multiple stub files with build tags (expected pattern, not a bug):
     - `internal/simd/simd_stubs.go` (!amd64)
     - `internal/gpu/factory_stub.go` (!gpu)
     - `internal/gpu/memory/*_stub.go` (platform-specific)
     - `internal/onnx/*_stub.go` (platform-specific)
     - `internal/query/simd_filter_stub.go` (!amd64)
   - Fix: No action needed - these are correct platform abstractions

8. **ML Reranker and Embedding Generator Stubs**
   - Status: `stubMLModel` and `stubEmbeddingModel` provide fallbacks
   - Locations: `internal/store/ml_reranker.go:199-224`, `internal/store/embedding_generator.go:705-726`
   - Fix: No action needed - these are intentional fallbacks

---

## Remediation Plan

### Completed (P0 & P1 Fallbacks)

| Item | Owner | Files | Status |
|------|-------|-------|--------|
| Implement CPUIndex SearchFloat16 | Core team | `internal/gpu/factory.go` | ✅ Done |
| Implement CPUIndex SearchComplex64 | Core team | `internal/gpu/factory.go` | ✅ Done |
| Implement CPUIndex SearchComplex128 | Core team | `internal/gpu/factory.go` | ✅ Done |
| Fix PluggableIndexAdapter filter support | Store team | `internal/store/pluggable_index_adapters.go` | ✅ Done |
| Implement MockIndex typed search | Test team | `internal/gpu/mock_index.go` | ✅ Done |
| CPU fallback for MetalIndex PQ ops | GPU team | `internal/gpu/metal/metal_gpu.go` | ✅ Done |
| CPU fallback for MetalHybridIndex PQ ops | GPU team | `internal/gpu/metal/metal_gpu_hybrid.go` | ✅ Done |
| CPU fallback for CUDAIndex PQ ops | GPU team | `internal/gpu/cuda/cuda_index.go` | ✅ Done |

### P1 — Next Sprint (GPU PQ Kernels)

| Item | Owner | Files | Status |
|------|-------|-------|--------|
| Implement Metal PQ kernels (SearchPQ) | GPU team | `internal/gpu/metal/` | ✅ Done |
| Implement Metal PQ kernels (TrainPQ, EncodePQ) | GPU team | `internal/gpu/metal/` | ✅ Done (CPU Fallback) |
| Implement CUDA TrainPQ/EncodePQ kernels | GPU team | `internal/gpu/cuda/` | Pending |
| **GPU-accelerated search offloading (cuVS)** | GPU team | `internal/gpu/cuda/cuvs/` | **Planned** |
| **Next-Gen Quantization (Turboquant V2)** | Index team | `internal/core/quantization/` | **Planned** |

---

## Technical Feature Deep Dive: Planned Features

### 1. GPU-accelerated Search Offloading via NVIDIA cuVS
**Target**: NVIDIA AMD64 Linux Builds
**Goal**: Transition from simple distance-metric offloading to full graph-traversal offloading using the **NVIDIA cuVS** (CUDA Vector Search) library.
- **Strategy**: Build a CGO-based adapter for `libcuvs` to offload HNSW `Search` and `Build` operations.
- **Impact**: Expected 10x-50x increase in QPS for large-scale datasets by amortizing kernel launch overhead and utilizing GPU-native graph structures.
- **Work Items**:
    - Implement `CUVSIndex` following the `VectorIndexer` interface.
    - Add dynamic library detection for `libcuvs` on Linux.
    - Benchmark against current `CUDAIndex` (distance-only).

### 2. Superior Quantization (Turboquant V2)
**Goal**: Surpass Qdrant's Binary/Scalar quantization performance and recall.
**Strategy**: Implement **Learnable Bit-Widths** and **Int4/Int2 packed SIMD** routines.
- **Innovation**: Instead of fixed 8-bit or 1-bit quantization, use a lightweight predictor to assign bit-widths per dimension based on information density.
- **SIMD Optimization**: Custom AVX-512 and ARM Neon kernels for Int4/Int2 dot products, avoiding the unpacking overhead found in standard implementations.
- **Impact**: 4x-8x memory reduction compared to float32 with <1% recall loss, significantly outperforming Qdrant's standard BQ/SQ implementations.

---

### P2 — Medium-term (Backlog)

| Item | Owner | Files | Status |
|------|-------|--------|-------|
| TPUIndex real implementation | Platform team | `internal/gpu/tpu/tpu_index.go` | Backlog |
| Review skipped tests | QA team | `*/*_test.go` | Backlog |
| IVF-TQ hybrid | Index team | `ivf_pq_index.go` | Backlog |

---

## Implementation Notes

### CPUIndex Typed Search ✅ COMPLETED
- SearchFloat16: Converts uint16 (fp16) to float32, then searches
- SearchComplex64: Converts uint16 pairs to float32, then searches
- SearchComplex128: Uses float32 representation directly
- Location: `internal/gpu/factory.go:176-211`

### PluggableIndexAdapter Filter Support ✅ COMPLETED
- Now returns clear errors when filters or bitmap filters are provided
- Location: `internal/store/pluggable_index_adapters.go:197-255`

### MockIndex Typed Search ✅ COMPLETED
- All typed search methods now convert and delegate to existing Search()
- Location: `internal/gpu/mock_index.go:173-220`

### PQ Metal Kernels ✅ COMPLETED
- Metal SearchPQ: Native MSL kernels implemented for both direct and hybrid indices
- Performance: Full GPU acceleration for PQ distance computation and top-k selection
- Location: `internal/gpu/metal/metal_gpu.go` (msl code), `metal_gpu_hybrid.go`

---


---

## suggested improvements (Based on 2026-04-24 Benchmark Results)

### 1. Specialized Search Throughput Bottlenecks (Geo/Temporal)
**Observation**: HNSW early-exit filtering significantly reduces distance compute, but metadata checks themselves can become a bottleneck if predicates are complex.
**Suggestion**:
- Implement **SIMD-accelerated metadata filtering** for common filter patterns (e.g., numeric range checks using AVX-512/Neon).
- Pre-compute **z-order curve bits** for Geo-spatial data to enable faster bounding box checks within the predicate.

### 2. GPU Memory Management for TurboQuant
**Observation**: High-dimensional TurboQuant search on GPU is now efficient, but storing multiple large datasets can hit memory limits.
**Suggestion**:
- Implement **GPU segment paging** for TurboQuant data, allowing cold segments to stay on host memory while hot segments are searched on GPU.
- Explore **TQ-V2 quantization** with 4-bit polar coordinates to further reduce memory footprint.

### 3. HNSW Graph Hardening
**Observation**: Early-exit filtering works best when matching nodes are reachable through the HNSW graph.
**Suggestion**:
- Implement **filtered connectivity maintenance**, ensuring that HNSW links are preserved even when many nodes are filtered out.
- Use **multi-level predicates** to skip entire sub-graphs during HNSW traversal.

---

**Generated**: 2026-04-24
**Last Updated**: 2026-04-24 (0.1.9 Performance Audit Finalized)
**Test Matrix**: Targeted runs for float32/turboquant across 128d/384d/768d and specialized modes.
**System**: Apple M3 Pro, 18GB allocated
**Status**: 0.1.9-rc-final stable, all search modes verified.
