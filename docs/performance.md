# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-05-29

## v0.2.1-rc5 — Int16/Uint16 Distance Kernel Fix (2026-05-29)

> [!IMPORTANT]
> **Int16/Uint16 Regression Fixed**: The int16/uint16 distance functions used `int64` accumulators (`int64 MUL` — 3-4 cycle latency on ARM64), while uint8/int8 used `float64 accumulators` (`float64 FMUL` — 1 cycle latency, FMA-fused). Switched int16/uint16 to `float64` accumulators, matching the uint8/int8 pattern. Results: **32x latency improvement** for int16 at count=5000.

### Performance Impact (Local CPU, M3)

| Type | Dim | Count | Metric | Before Fix | After Fix | Improvement |
|------|-----|-------|--------|-----------|-----------|-------------|
| int16 | 128 | 5,000 | p50 latency | 17.02 ms | **0.53 ms** | **32.1x faster** |
| int16 | 384 | 1,000 | p50 latency | 4.50 ms | **1.02 ms** | **4.4x faster** |
| uint16 | 384 | 5,000 | p50 latency | 17.89 ms | **1.02 ms** | **17.5x faster** |
| uint8 | 128 | 1,000 | p50 latency | 1.64 ms | **0.83 ms** | **2.0x faster** |

All integer types now consistently use `float64` accumulators, matching the fastest code path.

### Quick Sanity Results (CPU, dim=128, count=5000)

| Type | Local CPU (M3) | Remote CPU (Ancalagon x86_64) |
|------|---------------|------------------------------|
| float32 | 967 QPS (0.92ms) | 739 QPS (1.29ms) |
| int8 | 1,492 QPS (0.55ms) | 632 QPS (1.47ms) |
| int16 | **1,566 QPS (0.53ms)** | **1,389 QPS (0.67ms)** |
| uint8 | 1,087 QPS (0.83ms) | 642 QPS (1.48ms) |
| uint16 | 920 QPS (1.02ms) | 645 QPS (1.44ms) |

> [!NOTE]
> QPS values are per-worker from bench-tool's concurrent mode (10 workers × 100 queries each). The concurrent QPS aggregation has a known bug where only the last worker's QPS is recorded. **Latency (p50) is the reliable metric** for cross-run comparison.

### Key Stability Improvements

1. **Int16/Uint16 Distance Kernel**: Switched from `int64` to `float64` accumulators in 6 functions (`euclideanInt16Unrolled4x`, `dotInt16Unrolled4x`, `euclideanUint16Unrolled4x`, `dotUint16Unrolled4x`, `cosineDistanceInt16Unrolled4x`, `cosineDistanceUint16Unrolled4x`). This eliminates the 3-4 cycle `int64 MUL` bottleneck on ARM64 NEON, leveraging the fast 1-cycle `float64 FMUL` pipe.

2. **Benchmark Script Fix**: Resolved `-search-modes all` expansion bug in `scripts/unified_benchmark.py` — the literal string `"all"` was passed to bench-tool instead of expanding to actual mode names.

---

## v0.2.2-rc Auto-Sharding Stability & Large Scale (2026-05-28)

> [!IMPORTANT]
> **Auto-Sharding Validation**: Auto-sharding migration robustness has been fixed for missing vectors and memory leaks. The system can now successfully migrate, shard, and search datasets of 50,000+ vectors without OOM or panics.

### Large Scale Search Performance (uint64, dim=384, count=50,000)

| Mode             | QPS       | p50 (ms) | p95 (ms) | p99 (ms) | Platform       | Status     |
| ---------------- | --------- | -------- | -------- | -------- | -------------- | ---------- |
| **Dense Search** | 208.5 QPS | 36.545   | 63.477   | 84.543   | Local CPU (M3) | **STABLE** |

---

## v0.2.0-rc2 Release Candidate - Final Hardening (2026-05-05)

> [!IMPORTANT]
> **Performance Validation**: This update confirms that all P0 performance regressions in Dense and Temporal searches have been resolved. The current build significantly outperforms v0.1.9 targets across all critical search modes.

### Search Performance Breakdown (dim=128, count=5000)

| Mode                | Target (v0.1.9) | **Actual (v0.2.0-rc2)** | Platform                | Status             |
| ------------------- | --------------- | ----------------------- | ----------------------- | ------------------ |
| **Dense Search**    | > 20,000 QPS    | **30,576 QPS**          | Local CPU (M3)          | **OK (+52%)**      |
| **Dense Search**    | > 20,000 QPS    | **29,268 QPS**          | Local Metal (M3)        | **OK (+46%)**      |
| **Dense Search**    | > 20,000 QPS    | **29,223 QPS**          | Remote CPU (Ancalagon)  | **OK (+46%)**      |
| **Dense Search**    | > 20,000 QPS    | **30,013 QPS**          | Remote CUDA (Ancalagon) | **OK (+50%)**      |
| **Temporal Search** | > 12,000 QPS    | **29,389 QPS**          | Local CPU (M3)          | **OK (+145%)**     |
| **Temporal Search** | > 12,000 QPS    | **29,817 QPS**          | Local Metal (M3)        | **OK (+148%)**     |
| **Temporal Search** | > 12,000 QPS    | **19,886 QPS**          | Remote CPU (Ancalagon)  | **OK (+65%)**      |
| **Temporal Search** | > 12,000 QPS    | **20,096 QPS**          | Remote CUDA (Ancalagon) | **OK (+67%)**      |
| **Sparse Search**   | > 4,000 QPS     | **59,400 QPS**          | Local Metal (M3)        | **OK (14x above)** |
| **GraphRAG Search** | > 3,000 QPS     | **47,960 QPS**          | Local Metal (M3)        | **OK (15x above)** |
| **Geospatial**      | > 5,000 QPS     | **36,617 QPS**          | Local Metal (M3)        | **OK (+632%)**     |

### Latency Metrics (Local M3, dim=128, count=5000)

| Search Mode  | p50 (ms) | p95 (ms) | p99 (ms) |
| ------------ | -------- | -------- | -------- |
| Dense        | 0.228    | 0.493    | 0.757    |
| Sparse       | 0.129    | 0.250    | 0.372    |
| GraphRAG     | 0.156    | 0.276    | 0.338    |
| Temporal     | 0.246    | 0.493    | 0.756    |
| LearnedIndex | 2.039    | 2.731    | 2.821    |

### Ingestion Performance (vec/s)

| Platform     | Mode | float32 (128d) | Target  | Status         |
| ------------ | ---- | -------------- | ------- | -------------- |
| Darwin arm64 | CPU  | **459,418**    | 150,000 | **OK (+206%)** |
| Linux x86_64 | CPU  | **371,689**    | 150,000 | **OK (+147%)** |

---

## Target Baselines (v0.1.9 Parity)

- **Dense Search (Float32, 384d)**: > 20,000 QPS
- **Temporal Search**: > 12,000 QPS
- **Ingestion (Bulk)**: > 150,000 vec/s

---

## Hardware

- **Local**: Apple Silicon M3, 18GB memory
- **Remote (ancalagon)**: NVIDIA RTX 4060 Laptop GPU, 8GB VRAM, 22GB RAM, 16 cores (AMD64 Linux)

### Benchmark Matrix Coverage

- **Platforms:** CPU, Metal (local), CUDA (remote ancalagon)
- **Data Types:** float16, float32, float64, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant2, turboquant4, turboquant8
- **Dimensions:** 128, 384, 768, 1024, 3072
- **Counts:** 500, 1000, 5000, 15000, 50000, 100000
- **Search Modes:** dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal, learned_index
