# Longbow Multi-Dimension Optimization Plan

**Date**: 2026-03-27
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)
**Scope**: Dimensions 128, 256, 384, 768, 1024, 1536, 2048, 3072
**Data Types**: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, turboquant

---

## Executive Summary

Enable optimized SIMD kernels for all supported dimensions (128-3072) across all data types. This plan addresses performance degradation at high dimensions (≥768) and ensures consistent QPS across the entire supported dimension range.

## Current State Analysis

| Dimension | float32 | float64 | int32 | int16 | int8 | complex64 | turboquant |
|-----------|---------|---------|-------|-------|------|-----------|------------|
| 128 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 256 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ⚠️ Needs verification |
| 384 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ⚠️ Needs verification |
| 768 | ✅ Optimized | ✅ Optimized | ⚠️ Partial | ⚠️ Partial | ⚠️ Partial | ⚠️ Partial | ❌ Not optimized |
| 1024 | ⚠️ Blocked only | ⚠️ Blocked only | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized |
| 1536 | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized |
| 2048 | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized |
| 3072 | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized | ❌ Not optimized |

---

## 10-Step Implementation Plan

### Step 1: Analyze Current SIMD Kernel Implementations

**Goal**: Document current state and identify gaps

**Tasks**:
- [ ] Catalog all existing SIMD kernels in `internal/simd/`
- [ ] Document dimension-specific implementations (128, 256, 384, 768, 1024, 1536, 2048, 3072)
- [ ] Identify which types have unoptimized paths
- [ ] Analyze blocked SIMD threshold (currently 1024)
- [ ] Review existing tests in `internal/simd/*_test.go`

**Deliverables**:
- Gap analysis matrix (dimension × type → optimization status)
- List of missing kernels to implement
- Estimated effort per kernel type

**Files to Analyze**:
- `internal/simd/simd_blocked.go`
- `internal/simd/distance_functions.go`
- `internal/simd/simd_amd64.go`
- `internal/simd/simd_arm64.go`
- `internal/simd/simd_generic.go`

---

### Step 2: Benchmark Baseline for All Dimensions/Types

**Goal**: Establish performance baseline before optimization

**Tasks**:
- [ ] Create benchmark script for full dimension × type matrix
- [ ] Run baseline benchmarks for all 8 dimensions × 13 types = 104 configurations
- [ ] Capture QPS, P50/P95/P99 latency, memory usage
- [ ] Identify performance cliffs and regression targets

**Deliverables**:
- Baseline performance matrix in CSV/JSON format
- Performance cliff identification (dimensions with >50% QPS drop)
- Target improvements per configuration

**Benchmark Command**:
```bash
./bin/benchmark-tool --dim=128,256,384,768,1024,1536,2048,3072 \
  --dtype=float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant \
  --scale=10000 --queries=200 --json=baseline.json
```

---

### Step 3: Implement Blocked SIMD for Missing Dimensions

**Goal**: Enable blocked processing for dimensions ≥768

**Tasks**:
- [ ] Update `blockedSimdThreshold` in `internal/simd/simd_blocked.go`
- [ ] Implement blocked versions for:
  - [ ] float64 (768, 1024, 1536, 2048, 3072)
  - [ ] int32 (768, 1024, 1536, 2048, 3072)
  - [ ] int16 (768, 1024, 1536, 2048, 3072)
  - [ ] int8 (768, 1024, 1536, 2048, 3072)
  - [ ] uint32/uint16/uint8 variants
  - [ ] complex64 (768, 1024, 1536, 2048, 3072)
  - [ ] complex128 (768, 1024, 1536, 2048, 3072)
  - [ ] turboquant (768, 1024, 1536, 2048, 3072)

**Deliverables**:
- Blocked SIMD implementations for all missing dimension/type combinations
- Unified dispatch logic to choose optimal path

**Key Changes**:
```go
// internal/simd/simd_blocked.go
const (
    blockedSimdThreshold256 = 256
    blockedSimdThreshold384 = 384
    blockedSimdThreshold768 = 768  // NEW
    blockedSimdThreshold1024 = 1024
    blockedSimdThreshold1536 = 1536  // NEW
    blockedSimdThreshold2048 = 2048  // NEW
    blockedSimdThreshold3072 = 3072  // NEW
)
```

---

### Step 4: Add Type-Specific Optimizations

**Goal**: Implement dimension-specific SIMD kernels for critical types

**Tasks**:
- [ ] Add unrolled kernels for high dimensions (768, 1536, 3072):
  - [ ] `euclidean768Unrolled4x`, `euclidean1536Unrolled4x`, `euclidean3072Unrolled4x`
  - [ ] `dot768Unrolled4x`, `dot1536Unrolled4x`, `dot3072Unrolled4x`
- [ ] Add complex number optimizations:
  - [ ] Complex dot product for 768, 1536, 3072
  - [ ] Complex Euclidean for 768, 1536, 3072
- [ ] Add turboquant optimizations:
  - [ ] Hadamard transform for 768, 1536, 3072
  - [ ] Rotation for 768, 1536, 3072

**Deliverables**:
- Type-specific optimized kernels
- Dispatch table updates

**Architecture**:
```
Dimension → Choose Implementation:
  128-384   → Direct SIMD (full unroll)
  512-768   → Blocked SIMD (256-byte blocks)
  1024+     → Tiled blocked (128-byte blocks + prefetch)
```

---

### Step 5: Add Unit Tests for All Kernel Variants

**Goal**: Ensure correctness across all dimension/type combinations

**Tasks**:
- [ ] Create comprehensive tests for each dimension:
  - [ ] `simd_768_test.go` - extend existing
  - [ ] `simd_1024_test.go` - create new
  - [ ] `simd_1536_test.go` - create new
  - [ ] `simd_2048_test.go` - create new
  - [ ] `simd_3072_test.go` - create new
- [ ] Add tests for all types:
  - [ ] int8, int16, int32, int64
  - [ ] uint8, uint16, uint32, uint64
  - [ ] float32, float64
  - [ ] complex64, complex128
  - [ ] turboquant
- [ ] Add property-based tests for random dimensions
- [ ] Add accuracy tests (verify against generic implementation)

**Deliverables**:
- Test coverage for all 104 dimension × type combinations
- Property-based tests for dimension invariance
- Accuracy verification tests

**Test Structure**:
```go
// TestEuclideanDim tests Euclidean distance for any dimension
func TestEuclideanDim(t *testing.T) {
    for _, dim := range []int{128, 256, 384, 768, 1024, 1536, 2048, 3072} {
        for _, dtype := range []string{"float32", "float64", "int32", ...} {
            // Test implementation vs generic
        }
    }
}
```

---

### Step 6: Add Prometheus Metrics for Performance Stability

**Goal**: Monitor kernel performance and detect regressions

**Tasks**:
- [ ] Add dimension-specific metrics:
  - [ ] `longbow_simd_kernel_duration_seconds` (histogram, labels: dtype, dimension, operation)
  - [ ] `longbow_simd_kernel_operations_total` (counter, labels: dtype, dimension)
  - [ ] `longbow_simd_fallback_total` (counter, tracks non-optimized paths)
- [ ] Add operation-specific metrics:
  - [ ] `longbow_hnsw_search_simd_duration_seconds`
  - [ ] `longbow_hybrid_sparse_simd_duration_seconds`
  - [ ] `longbow_distance_computation_total`

**Deliverables**:
- Prometheus metrics for all SIMD operations
- Dashboard queries for dimension-specific performance

**Metric Definitions**:
```go
// internal/metrics/simd_metrics.go
var (
    SimdKernelDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "longbow_simd_kernel_duration_seconds",
        Help:    "SIMD kernel execution duration in seconds",
        Buckets: []float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001},
    }, []string{"dtype", "dimension", "operation"})

    SimdKernelOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "longbow_simd_kernel_operations_total",
        Help: "Total number of SIMD kernel operations",
    }, []string{"dtype", "dimension", "operation"})

    SimdFallbackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "longbow_simd_fallback_total",
        Help: "Total number of times SIMD fell back to generic implementation",
    }, []string{"dtype", "dimension"})
)
```

---

### Step 7: Add Memory Pressure Metrics

**Goal**: Monitor memory usage during high-dimensional searches

**Tasks**:
- [ ] Add allocation tracking:
  - [ ] `longbow_search_allocation_bytes` (histogram)
  - [ ] `longbow_vector_copy_total` (counter, tracks zero-copy violations)
  - [ ] `longbow_arena_allocation_total`
- [ ] Add buffer pool metrics:
  - [ ] `longbow_buffer_pool_hits_total`
  - [ ] `longbow_buffer_pool_misses_total`
  - [ ] `longbow_buffer_pool_size_bytes`
- [ ] Add dimension-specific memory metrics:
  - [ ] `longbow_dimension_buffer_bytes` (labels: dimension)

**Deliverables**:
- Memory pressure metrics for all search paths
- Alerting thresholds for memory pressure

---

### Step 8: Integrate Metrics into Search Hot Paths

**Goal**: Ensure metrics are collected without performance impact

**Tasks**:
- [ ] Integrate metrics into `SearchVectorsWithBitmap`:
  - [ ] Add latency recording for SIMD operations
  - [ ] Add fallback tracking
- [ ] Integrate into HNSW search:
  - [ ] Record graph navigation SIMD usage
  - [ ] Track distance computation per candidate
- [ ] Integrate into hybrid search:
  - [ ] Track dense path SIMD usage
  - [ ] Track sparse path SIMD usage
- [ ] Add sampling for high-frequency metrics (avoid metric overhead)

**Deliverables**:
- Metrics collection in all hot paths
- Performance overhead <1%

---

### Step 9: Run Full Benchmark Matrix

**Goal**: Validate optimizations and measure improvements

**Tasks**:
- [ ] Run full benchmark suite:
  - [ ] All 8 dimensions × 13 types = 104 configurations
  - [ ] Scales: 1k, 5k, 10k, 25k, 50k
  - [ ] Search types: Dense, Hybrid, Filtered, ByID
- [ ] Compare against baseline (Step 2)
- [ ] Verify no regressions
- [ ] Measure improvement per configuration

**Deliverables**:
- Full benchmark results
- Improvement matrix (before vs after)
- Regression report

**Script**:
```bash
#!/bin/bash
# Run full matrix
for dim in 128 256 384 768 1024 1536 2048 3072; do
    for dtype in float32 float64 int8 int16 int32 int64 uint8 uint16 uint32 uint64 complex64 complex128 turboquant; do
        for scale in 1000 5000 10000 25000 50000; do
            ./bin/benchmark-tool --dim=$dim --dtype=$dtype --scale=$scale --queries=200
        done
    done
done
```

---

### Step 10: Document Results and Update performance.md

**Goal**: Final documentation of optimizations

**Tasks**:
- [ ] Update `docs/performance.md` with new benchmark results
- [ ] Add dimension-specific performance analysis
- [ ] Document optimization techniques used
- [ ] Add architecture diagram for kernel dispatch
- [ ] Update README with supported dimensions

**Deliverables**:
- Updated performance.md with full matrix results
- Performance analysis by dimension
- Architecture documentation

---

## Implementation Timeline

| Step | Description | Estimated Effort |
|------|-------------|------------------|
| 1 | Analyze current implementations | 2 hours |
| 2 | Benchmark baseline | 4 hours |
| 3 | Implement blocked SIMD | 8 hours |
| 4 | Type-specific optimizations | 16 hours |
| 5 | Unit tests | 8 hours |
| 6 | Prometheus metrics | 4 hours |
| 7 | Memory metrics | 4 hours |
| 8 | Integrate metrics | 4 hours |
| 9 | Run benchmarks | 8 hours |
| 10 | Documentation | 2 hours |

**Total Estimated**: 60 hours

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metric overhead | High | Use sampling, async collection |
| Test complexity | Medium | Automate test generation |
| Benchmark time | Medium | Parallel execution, CI caching |
| Regression | High | Baseline comparison, CI gates |

---

## Success Criteria

- [ ] All 8 dimensions × 13 types = 104 configurations tested
- [ ] No regressions vs baseline (within 5% variance)
- [ ] High dimensions (≥768) show ≥20% improvement
- [ ] Prometheus metrics available for all SIMD operations
- [ ] Memory metrics show zero-copy paths working
- [ ] Documentation complete

---

## Related Files

**SIMD Core**:
- `internal/simd/simd_blocked.go`
- `internal/simd/distance_functions.go`
- `internal/simd/simd_amd64.go`
- `internal/simd/simd_arm64.go`

**Tests**:
- `internal/simd/simd_128_test.go`
- `internal/simd/simd_384_test.go`
- `internal/simd/simd_768_test.go`

**Metrics**:
- `internal/metrics/simd_metrics.go` (create)
- `internal/metrics/metrics.go`

**Documentation**:
- `docs/performance.md`
- `docs/performance_metal.md`

---

**Last Updated**: 2026-03-27
