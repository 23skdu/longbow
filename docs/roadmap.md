# Longbow Roadmap & Optimization Plan

This document consolidates the strategic roadmap (ADBC, Tensor Engine, EMLGo) with the ongoing performance optimization plan derived from benchmark analysis.

---

## 1. GPU TurboQuant Optimization

Optimizing GPU TQ search from O(N) brute-force toward O(ef·logN) HNSW graph traversal.

| Part | Goal | Status | Impact |
|------|------|--------|--------|
| 1 | Batched TQ kernel (single launch) | Done | Eliminates per-page loop |
| 2 | `__constant__` LUT tables | Done | Replaces sincosf with table lookups |
| 3 | HNSW greedy descent kernel | Done | O(N) → O(ef·logN) traversal |
| 4 | Explicit CUDA streams | Done | Async kernel/memcpy overlap |
| 5 | 32-byte warp-aligned PackedSize | Done | Coalesced memory access |
| 6 | CPU temporal emlgo investigation | Done | By design: emlgo adds 16-38% overhead for temporal search; use standard build |
| 7 | CPU float64 emlgo exclusion | Done | `LONGBOW_FLOAT64_EXCLUDE_EMLGO` env var |
| 8 | GPU complex128/complex64 CUDA kernels | Done | Native CUDA L2/dot/cosine kernels for complex types |

### Benchmark Results

| Config | Before | After | Change |
|--------|--------|-------|--------|
| GPU TQ 100k dense (emlgo) | 1363 QPS | 3494 QPS | **+156%** |

---

## 2. Performance Optimization Pipeline

Derived from 2026-09-11 A/B benchmark analysis. See [performance.md](performance.md) for raw data.

### Resolved Issues

| Issue | Fix | Date |
|-------|-----|------|
| GPU TQ 100k dense (emlgo) -57% | QJL correction asymmetry bug | 2026-09-12 |
| CPU TQ dense 500k -55% | PackedSize caching | 2026-09-11 |
| CPU float32 dense 50k -63% | Dispatch overhead fix | 2026-09-11 |
| Duplicate Dockerfile | Removed | 2026-09-12 |
| Dockerfile.metal missing ENTRYPOINT | Fixed | 2026-09-12 |
| docker-compose deprecated version key | Removed | 2026-09-12 |
| CPU temporal emlgo regression (-18-36%) | By design; use standard build for temporal | 2026-09-12 |
| CPU float64 emlgo memory (+47%) | `LONGBOW_FLOAT64_EXCLUDE_EMLGO` env var | 2026-09-12 |
| GPU uint8 graphrag 500k (+290%) | Validated: real speedup, not a regression | 2026-09-12 |

### Open Issues

| Priority | Issue | Impact |
|----------|-------|--------|
| P0 | CPU complex64 dense 500k | -38% regression |
| P0 | CPU complex128 dense 500k | P99 75ms tail latency |

---

## 3. Future Work

### Benchmark Infrastructure (Part 9)

- 3x benchmark runs with mean/stdev reporting
- Memory soak test (1+ hour at 500k)
- CI integration with regression threshold alerts

### Conditional Dispatch Strategy (Part 10)

- Route emlgo selectively by type and scale
- `LONGBOW_MATH_DISPATCH` env var for manual override
- Empirical rules: emlgo for complex types + TQ above 50k; standard for int/float below 50k
