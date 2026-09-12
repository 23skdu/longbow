# Next Steps & Roadmap

Based on benchmark results from 2026-09-11.

## Executive Summary

The CPU float32 dense 50k regression and CPU turboquant dense 500k regression are now resolved. Current focus shifts to addressing remaining critical regressions (CPU complex64 dense 500k, CPU float64 with emlgo, GPU turboquant small scale) while capitalizing on the massive GPU uint8 graphrag 500k gain. CPU temporal mode with emlgo shows consistent regressions that need investigation against the GPU temporal path which is consistently faster.

## Resolved (2026-09-11)

| Priority | Issue | Before | After | Fix |
|----------|-------|--------|-------|-----|
| P0 | CPU float32 dense 50k | -63% | -2.5% | Dispatch overhead fix |
| P0 | CPU turboquant dense 500k | -55% | -2.6% | PackedSize caching fix |
| P1 | CPU complex64 dense 10k | +10% | +0.02% | (resolved) |

## P0 — Critical

| # | Issue | Impact | Recommended Action | Owner |
|---|-------|--------|--------------------|-------|
| 1 | CPU complex64 dense 500k | -38% regression (404 vs 251 QPS) | Profile hot path at 500k scale, check if dispatch overhead returns at large N | |
| 2 | CPU complex128 dense 500k | +21% gain but P99 75ms | Investigate tail latency — likely allocation or GC pressure at large scale | |
| 3 | CPU float64 500k with emlgo | -21% dense, -41% sparse, -38% temporal | Disable emlgo for float64 at 500k or fix emlgo path for float64 | |
| 4 | GPU uint8 graphrag 500k | +290% gain (4981 vs 1276 QPS) | Validate result — potential measurement error or genuine breakthrough; run 3x to confirm | |
| 5 | GPU turboquant dense/graphrag 100k | -57% and -56% regression | Investigate GPU turboquant dispatch for small/medium scale — likely kernel launch overhead | |

## P1 — Important

| # | Issue | Impact | Recommended Action | Owner |
|---|-------|--------|--------------------|-------|
| 6 | CPU emlgo temporal mode | -18-36% across ALL dtypes at 10k and 100k | Compare CPU temporal dispatch path vs GPU temporal path (GPU is +3-18% faster) | |
| 7 | GPU emlgo temporal consistently faster | +3-18% | Extract GPU temporal optimization patterns and port to CPU temporal path | |
| 8 | CPU float64 memory 500k with emlgo | +47% memory (10632 vs 7172 MB) | Profile buffer allocation in emlgo for float64 — likely oversized scratch buffers | |
| 9 | GPU complex128 dense 100k | -50% regression (3419 vs 1718 QPS) | Profile GPU kernel for complex128 at 100k — likely shared memory or register pressure | |

## P2 — Improvement

| # | Issue | Impact | Recommended Action | Owner |
|---|-------|--------|--------------------|-------|
| 10 | CPU sparse search with emlgo | -5-17% slower at 100k | Compare CPU vs GPU sparse dispatch — GPU emlgo is +3-16% faster, find the divergence | |
| 11 | GPU sparse search with emlgo | +3-16% faster | Document GPU sparse dispatch pattern for CPU adoption | |
| 12 | CPU 10k scale emlgo overhead | -15-26% on graphrag/temporal | Consider disabling emlgo below 50k threshold — overhead exceeds benefit | |
| 13 | Benchmark variance | Non-monotonic scaling (float32 dense: 1244 QPS at 100k, 3398 at 500k) | Run 3x benchmark passes; investigate if turboquant activation is scale-dependent | |

## Infrastructure

| # | Need | Priority | Action |
|---|------|----------|--------|
| 14 | Statistical confidence | High | Implement 3x benchmark runs with mean/stdev reporting |
| 15 | Memory leak detection | High | Long-running soak test at 500k scale for 1+ hours |
| 16 | emlgo CI integration | Medium | Add emlgo benchmarks to CI pipeline with regression threshold alerts |

## Architecture Recommendations

1. **Conditional dispatch strategy**: Route emlgo for complex types (complex64/complex128) + turboquant, use standard paths for int/float at small scale (<50k).
2. **CPU temporal optimization**: GPU temporal path is consistently faster — extract dispatch patterns from GPU temporal and port to CPU temporal implementation.
3. **Scale-dependent activation**: Investigate emlgo activation threshold. At 10k scale, overhead exceeds benefit for most types. Consider 50k minimum threshold.
4. **GPU uint8 graphrag**: The +290% gain at 500k is either a breakthrough or a measurement artifact. Validate thoroughly before building on it.
5. **Float64 emlgo path**: Current emlgo implementation degrades float64 performance significantly at 500k. Either fix emlgo for float64 or exclude float64 from emlgo dispatch.

## Timeline

| Week | Focus |
|------|-------|
| 1 | Validate GPU uint8 graphrag result; fix CPU complex64 dense 500k regression |
| 2 | Investigate CPU vs GPU temporal divergence; fix CPU float64 emlgo path |
| 3 | Implement 3x benchmark runs; start memory soak test infrastructure |
| 4 | Conditional dispatch implementation; emlgo CI integration |
