# Longbow Performance — Executive Summary & Next Steps

## Executive Summary

Full benchmark matrix of **448 test runs** (14 dtypes × 2 dims × 8 counts × 2 backends) confirms Longbow 0.2.0 is production-stable on Apple Silicon M3 Pro. Zero crashes or errors across all configurations.

### Performance Highlights

| Metric | Value | DType/Dim |
|--------|-------|-----------|
| Fastest ingest | 805k vec/s | int8, 128d, CPU |
| Fastest search QPS | 10,483 qps | uint64, 128d, Metal |
| Lowest P50 latency | 0.098ms | uint64 hybrid, 128d, CPU |
| Most scalable search | int64/uint64 | stable ~10k QPS across all scales |

### Critical Issues Found

1. **int16/uint16 search is catastrophically slow** (1.25ms vs 0.10ms for int64 — **12–13x worse**). This is the #1 priority fix.
2. **Metal GPU acceleration is barely engaged** (< 5% speedup for most types). The Metal path needs investigation.
3. **float16 search is 2x slower than float32** (3,452 vs 6,250 QPS) despite better ingest. SIMD/float16 metric path likely broken.
4. **complex128 scales poorly** (44% degradation from 128d→384d). Complex arithmetic is not GPU-accelerated.

---

## Recommended Next Steps (Priority Order)

### P0 — Critical Bugs

1. **Fix int16/uint16 search performance**
   - Symptom: 1.25ms P50 vs 0.10ms for int64 at same scale
   - Likely cause: HNSW metric dispatch routes to broken SIMD path for 2-byte stride, causing scalar fallback
   - Fix: Audit `pkg/simd/distance_*.go` for int16/uint16 NEON/AVX2 dispatch tables; verify stride = 2 is passed correctly through HNSW distance function interface

2. **Fix float16 search accuracy/path**
   - Symptom: 3,452 QPS vs 6,250 for float32 — half throughput
   - Likely cause: float16 accumulation underflows in HNSW distance metric, triggering re-search or falling back to scalar
   - Fix: Verify float16 SIMD path in `pkg/simd/float16.go`; consider using float16x2 NEON pairs with explicit accumulation scaling

### P1 — High Impact Improvements

3. **Enable Metal GPU acceleration for all float types**
   - Symptom: Metal speedup is < 5% for most types; GPU is under-utilized
   - Root cause: Metal kernel launch overhead dominates small-batch HNSW traversal; graph traversal is memory-latency bound not compute-bound
   - Fix options:
     - Batch query dispatch: collect N queries and dispatch a single Metal compute with N concurrent traversals sharing HNSW graph bandwidth
     - Fuse graph traversal + distance computation in a single Metal shader pass to reduce memory round-trips
     - Investigate whether Metal is engaged at all for dense search (add `gpu_used=true` metric instrumentation)

4. **Add Metal acceleration for complex types**
   - Symptom: complex128 drops 44% QPS from 128d→384d; complex arithmetic is CPU-only
   - Fix: Implement complex multiply-add kernel in Metal shader (`metal/complex.metal`); complex dot product = (a_r*b_r - a_i*b_i) + (a_r*b_i + a_i*b_r)i

5. **Optimize complex128 ingest path**
   - Symptom: complex128 ingest is the slowest at 358k vec/s (128d) and 178k (384d)
   - Likely cause: Python `tolist()` converts complex to float64 pairs, doubling memory and parsing cost
   - Fix: Pre-allocate complex128 Arrow buffer directly from numpy complex128 array without Python round-trip

### P2 — Medium Impact

6. **Reduce int16/uint16 ingest overhead**
   - int16/uint16 ingest (703k/699k) is good but could improve with batched Arrow zero-copy paths

7. **Add batch query optimization for Metal**
   - Collect up to 32 queries and dispatch simultaneously to saturate GPU bandwidth

8. **Implement learned index for hot dataset routing**
   - Currently HNSW parameters (efConstruction, m) are static; adaptive index selection based on dataset size could reduce memory footprint for small datasets

9. **Add Prometheus metrics for GPU utilization**
   - `longbow_gpu_utilization_percent`, `longbow_gpu_memory_used_bytes`
   - Enable profiling-based optimization targeting: Metal GPU utilization > 60% during search

10. **Quantized HNSW for turboquant at high dimensions**
    - turboquant at 384d (4,805 QPS) is lower than float32 (4,756 QPS) — the quantization overhead exceeds the SIMD savings at this dimension
    - Consider hybrid: HNSW graph built on float32, search probes quantized centroids for coarse filtering

---

**Generated**: 2026-04-23
**Test Matrix**: 448 runs (14 dtypes × 2 dims × 8 counts × 2 backends × search types)
**System**: Apple M3 Pro, 18GB allocated