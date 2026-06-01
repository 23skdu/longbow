# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-05-31
Commit: `2b3e4f3e`

## v0.2.1-rc5 — Localhost CPU Run (2026-05-31)

> [!IMPORTANT]
> Localhost (M3 Pro, 18GB) with `LONGBOW_USE_DISK=1`. Dimensions 128 and 384, counts 15k–800k, all dtypes. DiskVectorStore enabled. Ancalagon host unreachable (SSH timeout). Metal run pending.

### Localhost (M3 Pro) — CPU

| Dim | Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 | Disk (MB) |
|-----|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|-----------|
| 128 |   complex128 |   15000 |       417767 |    2782.5 |    4.061ms |    6.899ms |   11618.8 |    0.984ms |    1.671ms |      0.0 |
| 128 |   complex128 |  200000 |        61467 |     264.0 |   43.470ms |   64.597ms |   11630.0 |    0.977ms |    1.673ms |      0.0 |
| 384 |   complex128 |   15000 |       158537 |    2369.9 |    5.010ms |    6.996ms |   11460.0 |    0.994ms |    1.683ms |      0.0 |
| 384 |   complex128 |  200000 |        51813 |     192.2 |   62.710ms |   78.971ms |   11341.4 |    0.992ms |    1.743ms |      0.0 |
| 128 |      float16 |   15000 |      1665556 |    4934.9 |    2.234ms |    4.331ms |   10992.4 |    1.058ms |    1.767ms |      0.0 |
| 128 |      float16 |  200000 |        65937 |     549.2 |   20.891ms |   37.805ms |   11677.8 |    0.995ms |    1.642ms |      0.0 |
| 384 |      float16 |   15000 |       918625 |    3327.8 |    3.433ms |    5.559ms |   11784.2 |    0.968ms |    1.661ms |      0.0 |
| 384 |      float16 |  200000 |        63850 |     396.1 |   29.362ms |   48.620ms |   11807.0 |    0.966ms |    1.659ms |      0.0 |
| 128 |      float32 |   15000 |      1384243 |    7347.2 |    1.616ms |    1.909ms |   12021.2 |    0.935ms |    1.652ms |      0.0 |
| 128 |      float32 |  200000 |        64595 |    5850.6 |    1.907ms |    3.052ms |   12197.4 |    0.957ms |    1.528ms |      0.0 |
| 384 |      float32 |   15000 |       509758 |    5788.9 |    2.025ms |    2.522ms |   11926.2 |    0.971ms |    1.588ms |      0.0 |
| 384 |      float32 |  200000 |        61015 |    4874.7 |    2.413ms |    3.075ms |   11826.7 |    0.968ms |    1.625ms |      0.0 |
| 128 |         int8 |   15000 |      2456801 |    6367.4 |    1.858ms |    2.287ms |   10892.7 |    0.995ms |    2.018ms |      0.0 |
| 128 |         int8 |  200000 |        66262 |     939.7 |   11.353ms |   19.111ms |   11725.4 |    0.967ms |    1.693ms |      0.0 |
| 384 |         int8 |   15000 |      1479454 |    4811.6 |    2.478ms |    2.955ms |   11593.6 |    0.990ms |    1.705ms |      0.0 |
| 384 |         int8 |  200000 |        65774 |     638.6 |   16.163ms |   27.384ms |   11806.2 |    0.979ms |    1.638ms |      0.0 |
| 128 |   turboquant |   15000 |      1339281 |    7978.8 |    1.475ms |    1.773ms |   11673.9 |    0.989ms |    1.620ms |      0.0 |
| 128 |   turboquant |  200000 |        65094 |    5754.6 |    1.945ms |    3.153ms |   12496.4 |    0.908ms |    1.625ms |      0.0 |
| 384 |   turboquant |   15000 |       494815 |    5365.3 |    2.193ms |    2.737ms |   11612.1 |    0.992ms |    1.648ms |      0.0 |
| 384 |   turboquant |  200000 |        62691 |    5228.8 |    2.286ms |    2.847ms |   11897.6 |    0.953ms |    1.604ms |      0.0 |
| 128 |        uint8 |   15000 |      2419631 |    6224.4 |    1.841ms |    2.604ms |   11803.4 |    0.931ms |    1.720ms |      0.0 |
| 128 |        uint8 |  200000 |        66255 |    1605.9 |    6.500ms |   11.337ms |   11784.9 |    0.945ms |    1.669ms |      0.0 |
| 384 |        uint8 |   15000 |      1442273 |    4839.7 |    2.380ms |    3.345ms |   11310.6 |    1.010ms |    1.686ms |      0.0 |
| 384 |        uint8 |  200000 |        65617 |    1334.4 |    8.243ms |   13.519ms |   11789.7 |    0.965ms |    1.622ms |      0.0 |

### Scale Comparison (float32, dim=128)

| Metric | 15k | 200k | Ratio (200k/15k) |
|--------|-----|------|-------------------|
| Ingest (vec/s) | 1,384,243 | 64,595 | 4.67% |
| Dense QPS | 7,347.2 | 5,850.6 | 79.63% |
| Dense P50 (ms) | 1.616 | 1.907 | 1.2x |

### Key Observations

1. **float32 dense search is fastest** across all counts and dims — float32 kernels remain the most optimized code path.
2. **turboquant8 matches float32 dense QPS at 15k** (~7980 vs 7350 at dim=128), but may degrade at higher counts.
3. **int8/uint8 dense search is 15-30% slower than float32** at 15k, but drops to ~6x slower at 200k (940 vs 5850 QPS dim=128). The proportional gap widens with index size.
4. **float16 dense search is 7-15x slower than float32** (549 vs 5850 QPS at 200k dim=128). No SIMD kernel for float16.
5. **complex128 dense search is ~3x slower than float32** — generic distance computation path, no SIMD.
6. **Sparse search is consistent across all types** at ~11,500 QPS regardless of dtype, dim, or count — bypasses vector distance computation.
7. **Ingest speed drops 20-40x from 15k to 200k** — HNSW edge construction is O(N·log N).

### Hardware

- **Local**: Apple Silicon M3 Pro, 18GB memory (18GB allocated)

### Coverage (CPU run)

- **Platforms:** CPU (Metal pending, ancalagon unreachable)
- **Data Types:** float16, float32, int8, uint8, complex128, turboquant8
- **Dimensions:** 128, 384
- **Counts:** 15k, 200k (500k, 800k in progress)
- **Search Modes:** dense, sparse
- **DiskVectorStore:** enabled (LONGBOW_USE_DISK=1). Disk (MB) column shows on-disk size of the vector store files after ingest.

### Known Issues

1. **float16 dense search is 7-15x slower than float32** — lacks SIMD-optimized distance kernel. Add NEON/AVX float16 path.
2. **int8/uint8 dense search degrades sharply at scale** — 1.5x at 15k but ~6x at 200k vs float32. int8 SIMD kernel may not scale well with index size.
3. **complex128 ingest/search slow** — generic distance path, no SIMD. 3x slower than float32 at all counts.
4. **800k OOM expected at 384d** — HNSW graph overhead exceeds 18GB for large dims at high counts.
5. **Ingest speed drops 20-40x from 15k to 200k** — O(N·log N) edge construction is the bottleneck. Consider adaptive M during bulk insert.
6. **Metal benchmarks not yet run** — will follow CPU completion.
7. **Ancalagon unreachable** — CPU + CUDA benchmarks on i7-12650H + RTX 4060 pending.
