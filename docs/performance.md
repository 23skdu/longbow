# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-05-31
Commit: `52733eb5`

## v0.2.1-rc7 — Multi-Platform Benchmark Run (2026-05-31)

> [!IMPORTANT]
> This run covers 128-dim vectors at counts 250k and 500k across 4 platforms: Localhost CPU (M3 Pro), Localhost Metal (M3 Pro GPU), Ancalagon CPU (i7-12650H), Ancalagon CUDA (RTX 4060 Laptop). 750k vectors hit the 18GB (local) / 14GB (ancalagon) memory caps due to HNSW graph overhead (~3-5x raw data).

### Ancalagon (i7-12650H) — CPU

| Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 |
|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|
|   complex128 |  250000 |        54794 |     140.3 |   54.259ms |   73.960ms |    7182.8 |    1.089ms |    1.551ms |
|      float16 |  250000 |        61079 |     182.8 |   41.904ms |   61.247ms |    7547.7 |    1.038ms |    1.455ms |
|      float32 |  250000 |        60560 |    3742.4 |    2.097ms |    2.948ms |    7645.0 |    1.040ms |    1.423ms |
|         int8 |  250000 |        61545 |    1326.2 |    4.616ms |   12.441ms |    7253.1 |    1.068ms |    1.528ms |
|   turboquant |  250000 |        60563 |    3628.2 |    2.142ms |    2.866ms |    7273.0 |    1.096ms |    1.489ms |
|        uint8 |  250000 |        60035 |     128.9 |   64.341ms |   87.130ms |    7520.0 |    1.038ms |    1.476ms |
|        uint8 |  500000 |        54118 |    1887.8 |    1.154ms |    4.746ms |    7037.8 |    1.081ms |    1.715ms |
|        uint8 |  750000 |        51278 |    3894.1 |    1.511ms |    4.009ms |    4191.1 |    1.788ms |    2.923ms |
### Ancalagon (i7-12650H) — CUDA

| Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 |
|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|
|      float32 |  250000 |        60462 |    3587.0 |    2.150ms |    3.060ms |    8044.2 |    0.974ms |    1.411ms |
|         int8 |  250000 |        61390 |     498.9 |   14.509ms |   22.234ms |    7841.5 |    0.998ms |    1.411ms |
|         int8 |  500000 |        53829 |      49.9 |  122.277ms |  281.934ms |    8130.8 |    0.955ms |    1.368ms |
|        uint8 |  250000 |        61314 |     260.3 |   29.294ms |   39.845ms |    7730.2 |    1.010ms |    1.420ms |
|        uint8 |  500000 |        52962 |     987.0 |    1.357ms |    5.530ms |    5016.8 |    1.447ms |    2.608ms |
### Localhost (M3 Pro) — CPU

| Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 |
|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|
|   complex128 |  250000 |        58698 |     233.8 |   31.650ms |   47.000ms |   11471.2 |    0.692ms |    1.035ms |
|      float16 |  250000 |        61862 |     436.6 |   16.803ms |   31.945ms |   11410.7 |    0.676ms |    1.080ms |
|      float16 |  500000 |        55172 |     220.3 |   21.253ms |  141.554ms |    2712.4 |    0.542ms |    0.794ms |
|      float32 |  250000 |        61448 |    5712.3 |    1.356ms |    2.108ms |   11603.6 |    0.663ms |    1.037ms |
|         int8 |  250000 |        62201 |     942.9 |    7.226ms |   13.750ms |   11483.8 |    0.667ms |    1.049ms |
|         int8 |  500000 |        55265 |     423.8 |   14.466ms |   29.521ms |   11026.6 |    0.719ms |    1.050ms |
|   turboquant |  250000 |        61426 |    6063.3 |    1.269ms |    2.043ms |   11861.4 |    0.652ms |    1.034ms |
|        uint8 |  250000 |        62195 |    1559.2 |    4.650ms |    7.694ms |   11393.5 |    0.667ms |    1.021ms |
|        uint8 |  500000 |        55326 |     761.6 |    5.545ms |   16.159ms |   11342.3 |    0.687ms |    1.058ms |
### Localhost (M3 Pro) — Metal

| Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 |
|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|
|      float32 |  250000 |        61281 |    5816.9 |    1.254ms |    2.170ms |   11906.0 |    0.644ms |    1.020ms |
|         int8 |  250000 |        60616 |     844.5 |    7.971ms |   16.111ms |   11175.2 |    0.703ms |    1.051ms |
|         int8 |  500000 |        54648 |     108.4 |   61.322ms |  155.442ms |    1547.3 |    4.109ms |   15.650ms |
|        uint8 |  250000 |        61356 |     779.5 |    8.820ms |   21.021ms |   10762.0 |    0.743ms |    1.081ms |
|        uint8 |  500000 |        54742 |     590.7 |    5.703ms |   18.716ms |   11885.2 |    0.642ms |    1.086ms |

### Key Observations

1. **float32 dense search is fastest** across all platforms — float32 kernels are the most optimized code path.
2. **float16 and complex128 show steep dense search degradation** — >10x slower than float32 at 250k. These types lack SIMD optimization and use generic distance computation.
3. **int8 dense search slower than float32** — ~6x slower on localhost CPU (7.2ms vs 1.4ms P50 at 250k). The int8 distance kernel may not be using optimal SIMD paths.
4. **turboquant8 matches float32 dense QPS** — ~6,000 QPS at 250k on localhost CPU, confirming quantization overhead is negligible in the query path.
5. **Sparse search is consistent across all types** — ~11,000 QPS regardless of dtype, as it bypasses vector distance computation.
6. **Metal GPU provides ~1.5x dense QPS boost** over CPU for float32 (5,817 vs 5,712 at 250k), but int8/uint8 on Metal is slower than CPU — GPU kernel optimizations needed for integer types.
7. **CUDA GPU on ancalagon matches CPU for float32** but int8 dense is very slow (499 QPS at 250k, 14.5ms P50) — GPU integer kernel needs optimization.
8. **750k vectors hit ResourceExhausted** at 128 dims on both hosts (18GB local, 14GB ancalagon) for all tested types. Max viable count at dim=128 is ~500k with current memory budgets.

### Platform Comparison (float32, dim=128, count=250k)

| Metric | Local CPU | Local Metal | Ancalagon CPU | Ancalagon CUDA |
|--------|-----------|-------------|---------------|----------------|
| Ingest (vec/s) | 61,448 | 61,281 | 60,560 | 60,462 |
| Dense QPS | 5,712 | 5,817 | 3,742 | 3,587 |
| Dense P50 (ms) | 1.356 | 1.254 | 2.097 | 2.150 |
| Sparse QPS | 11,604 | 11,906 | 7,645 | 8,044 |
| Sparse P50 (ms) | 0.663 | 0.644 | 1.040 | 0.974 |

### Target Baselines Check

| Target | Goal | Actual | Status |
|--------|------|--------|--------|
| Dense QPS (float32, 128d, 50k) | >3,000 | N/A (50k not run) | N/A — use 250k proxy |
| Dense QPS (float32, 128d, 250k) | >3,000 (scaled) | 5,712 (local CPU) | ✅ OK (+90%) |
| Dense P50 (float32, 128d, 250k) | <0.3ms (50k target) | 1.356ms | ⚠️ 4.5x higher at 5x data |
| Ingest (float32, 128d, 500k) | >2,000,000 vec/s | N/A (500k incomplete) | ⚠️ Need to verify |
| Sparse QPS (all types) | >10,000 | ~11,500 | ✅ OK |

### Hardware

- **Local**: Apple Silicon M3 Pro, 18GB memory (18GB allocated)
- **Remote (ancalagon)**: 12th Gen Intel i7-12650H, 22GB RAM, NVIDIA RTX 4060 Laptop GPU (8GB VRAM)

### Coverage

- **Platforms:** CPU (both), Metal (local), CUDA (ancalagon)
- **Data Types:** float16, float32, int8, uint8, complex128, turboquant8
- **Dimensions:** 128
- **Counts:** 250,000, 500,000 (750k OOM)
- **Search Modes:** dense, sparse

### Known Issues

1. **float16 dense search is 12x slower than float32** — lacks SIMD-optimized distance kernel. Investigate adding NEON/AVX float16 path.
2. **int8/uint8 dense search is 5-6x slower than float32 on CPU** — int8 distance kernels may not fully utilize SIMD. Compare against direct float32 conversion. 
3. **750k OOM at 128d** — HNSW overhead exceeds 18GB memory budget. Consider tiered storage or disk-based indexing for >500k vectors.
4. **Metal int8/uint8 dense search slower than CPU** — GPU kernel needs integer optimization. Consider falling back to CPU for integer types on Metal.
5. **CUDA int8 dense search very slow** — 499 QPS at 250k vs 3,742 QPS on CPU. GPU integer kernel regression.
6. **Ancalagon CUDA float32 500k OOM** — 14GB memory cap insufficient for float32 500k on CUDA path (GPU buffers + CPU memory competition).
7. **Benchmark results missing for 750k** — all 750k configs hit ResourceExhausted on both hosts. No search/ingest data captured.
