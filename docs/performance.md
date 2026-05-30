# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-05-29

## v0.2.1-rc6 — QPS Fix (2026-05-29)

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

### QPS Aggregation Bug Fix

> [!IMPORTANT]
> **QPS values from all prior releases were inflated by a bench-tool aggregation bug**. Search modes (dense,hybrid,sparse,filtered,byid) ran as 5 concurrent goroutines, each computing QPS = 1000/avgLatency under contention from the other 4 modes. This produced QPS values ~5x higher than actual sustained throughput. The fix (`86b56fb7`) runs modes sequentially with total wall-clock QPS. **Latency (p50/p95/p99) was always accurate** since it's measured per-query.

### Corrected QPS — CPU (Apple M3, 10 GB memory)

| Dim | Dtype | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 | ByID QPS | ByID P50 |
|-----|-------|-------|----------------|-----------|-----------|------------|------------|--------------|--------------|----------|----------|
| 128 | float32 | 50k | 1,171,977 | 3,186 | 0.27ms | 3,697 | 0.27ms | 3,749 | 0.26ms | 3,777 | 0.25ms |
| 384 | float32 | 50k | 675,907 | 2,468 | 0.33ms | 2,962 | 0.33ms | 2,581 | 0.38ms | 2,575 | 0.38ms |
| 768 | float32 | 50k | 404,811 | 1,884 | 0.38ms | 2,503 | 0.39ms | 2,226 | 0.44ms | 2,236 | 0.44ms |
| 1024 | float32 | 50k | 317,493 | 1,748 | 0.40ms | 2,455 | 0.40ms | 2,252 | 0.44ms | 2,204 | 0.45ms |
| 3072 | float32 | 50k | 115,524 | 783 | 0.79ms | 1,275 | 0.77ms | 1,060 | 0.79ms | 1,247 | 0.79ms |
| 128 | int8 | 50k | 1,411,565 | 3,251 | 0.27ms | 3,538 | 0.28ms | 3,722 | 0.26ms | 3,649 | 0.27ms |
| 384 | int8 | 50k | 566,287 | 2,362 | 0.34ms | 2,967 | 0.33ms | 3,035 | 0.32ms | 2,917 | 0.34ms |
| 768 | int8 | 50k | 326,950 | 1,944 | 0.39ms | 2,380 | 0.42ms | 2,161 | 0.46ms | 2,188 | 0.45ms |
| 1024 | int8 | 50k | 307,779 | 1,761 | 0.39ms | 2,504 | 0.39ms | 2,340 | 0.42ms | 2,200 | 0.44ms |
| 3072 | int8 | 50k | 116,743 | 792 | 0.77ms | 1,268 | 0.77ms | 1,250 | 0.79ms | 1,235 | 0.79ms |
| 128 | int16 | 50k | 1,447,890 | 3,321 | 0.27ms | 3,759 | 0.26ms | 3,631 | 0.27ms | 3,673 | 0.27ms |
| 384 | int16 | 50k | 665,431 | 2,482 | 0.32ms | 2,828 | 0.34ms | 2,646 | 0.37ms | 2,609 | 0.37ms |
| 768 | int16 | 50k | 399,655 | 1,956 | 0.39ms | 2,670 | 0.37ms | 2,536 | 0.39ms | 2,434 | 0.40ms |
| 1024 | int16 | 50k | 302,319 | 1,785 | 0.38ms | 2,539 | 0.38ms | 2,525 | 0.39ms | 2,559 | 0.38ms |
| 3072 | int16 | 50k | 118,042 | 799 | 0.78ms | 1,261 | 0.78ms | 1,247 | 0.78ms | 1,047 | 0.80ms |
| 128 | float32 | 100k | 1,778,895 | 2,762 | 0.25ms | 4,027 | 0.24ms | 4,088 | 0.24ms | 4,086 | 0.24ms |
| 384 | float32 | 100k | 810,103 | 1,605 | 0.34ms | 2,956 | 0.33ms | 2,935 | 0.34ms | 2,881 | 0.34ms |
| 768 | float32 | 100k | 465,181 | 1,624 | 0.38ms | 2,558 | 0.38ms | 2,399 | 0.41ms | 2,217 | 0.44ms |
| 1024 | float32 | 100k | 350,119 | 1,466 | 0.40ms | 2,451 | 0.40ms | 2,258 | 0.44ms | 2,261 | 0.44ms |
| 3072 | float32 | 100k | 120,749 | 646 | 0.79ms | 1,274 | 0.77ms | 1,245 | 0.78ms | 1,228 | 0.79ms |
| 128 | int8 | 100k | 1,707,000 | 3,180 | 0.27ms | 3,596 | 0.27ms | 3,762 | 0.26ms | 3,806 | 0.26ms |
| 384 | int8 | 100k | 853,799 | 2,278 | 0.33ms | 2,885 | 0.34ms | 2,566 | 0.39ms | 2,592 | 0.38ms |
| 768 | int8 | 100k | 459,194 | 1,592 | 0.38ms | 2,423 | 0.41ms | 2,193 | 0.45ms | 2,403 | 0.41ms |
| 1024 | int8 | 100k | 358,184 | 1,443 | 0.39ms | 2,506 | 0.40ms | 2,252 | 0.43ms | 2,160 | 0.45ms |
| 3072 | int8 | 100k | 119,915 | 669 | 0.77ms | 1,254 | 0.79ms | 1,065 | 0.79ms | 1,242 | 0.79ms |
| 128 | int16 | 100k | 1,779,110 | 3,132 | 0.28ms | 3,672 | 0.27ms | 3,594 | 0.28ms | 3,619 | 0.27ms |
| 384 | int16 | 100k | 873,234 | 2,270 | 0.32ms | 2,791 | 0.35ms | 2,714 | 0.36ms | 2,751 | 0.36ms |
| 768 | int16 | 100k | 458,827 | 1,641 | 0.38ms | 2,679 | 0.37ms | 2,409 | 0.41ms | 2,286 | 0.43ms |
| 1024 | int16 | 100k | 350,850 | 1,466 | 0.39ms | 2,542 | 0.39ms | 2,285 | 0.43ms | 2,240 | 0.44ms |
| 3072 | int16 | 100k | 119,796 | 667 | 0.78ms | 1,257 | 0.78ms | 1,073 | 0.78ms | 1,268 | 0.78ms |
| 128 | float32 | 500k | 2,209,475 | 2,880 | 0.16ms | 7,513 | 0.13ms | 7,534 | 0.13ms | 7,538 | 0.13ms |
| 384 | float32 | 500k | 885,238 | 258 | 3.71ms | 180 | 5.55ms | 116 | 8.32ms | 118 | 8.46ms |
| 768 | float32 | 500k | 456,369 | 254 | 3.25ms | 237 | 4.14ms | 160 | 5.93ms | 123 | 7.89ms |
| 1024 | float32 | 500k | 349,949 | 261 | 3.08ms | 233 | 4.10ms | 158 | 6.16ms | 130 | 7.68ms |
| 128 | int8 | 500k | 2,368,439 | 2,280 | 0.28ms | 3,633 | 0.27ms | 3,742 | 0.26ms | 3,725 | 0.27ms |
| 384 | int8 | 500k | 870,973 | 292 | 3.37ms | 265 | 3.56ms | 228 | 4.38ms | 202 | 4.98ms |
| 768 | int8 | 500k | 456,427 | 221 | 4.07ms | 184 | 5.43ms | 132 | 6.97ms | 113 | 8.87ms |
| 1024 | int8 | 500k | 348,471 | 229 | 3.67ms | 197 | 5.08ms | 115 | 8.66ms | 112 | 8.86ms |
| 128 | int16 | 500k | 2,314,317 | 2,243 | 0.28ms | 3,649 | 0.27ms | 3,709 | 0.27ms | 3,532 | 0.28ms |
| 384 | int16 | 500k | 906,124 | 297 | 3.48ms | 233 | 4.33ms | 170 | 5.87ms | 150 | 6.55ms |
| 768 | int16 | 500k | 457,901 | 268 | 3.23ms | 249 | 4.08ms | 196 | 5.07ms | 196 | 5.08ms |
| 1024 | int16 | 500k | 345,312 | 230 | 3.41ms | 218 | 4.39ms | 149 | 6.40ms | 123 | 7.97ms |

> **Note:** Turboquant variants (2/4/8 bit) show nearly identical latency to float32 at the same dim/count — the quantization overhead is negligible in the query path. Full turboquant results in `data/perf_logs/perf_matrix_cpu_20260529_182652.md`. 3072×500k is OOM-bound on 16GB (6.1 GB raw float32 data + HNSW indexing + GC overhead exceeds memory). **Confirmed OOM limit**: even 3072×200k fails on both 16GB (M3 Pro) and 14GB (ancalagon) hosts across all dtypes — server process killed before ingest completes. Max viable count for 3072-dim: **100k vectors** (~3 GB raw float32, ~6 GB total physical). Streaming bench-tool fix applied (`a2df832`) — no longer pre-generates all chunks in RAM.

### Key Stability Improvements

1. **Int16/Uint16 Distance Kernel**: Switched from `int64` to `float64` accumulators in 6 functions (`euclideanInt16Unrolled4x`, `dotInt16Unrolled4x`, `euclideanUint16Unrolled4x`, `dotUint16Unrolled4x`, `cosineDistanceInt16Unrolled4x`, `cosineDistanceUint16Unrolled4x`). This eliminates the 3-4 cycle `int64 MUL` bottleneck on ARM64 NEON, leveraging the fast 1-cycle `float64 FMUL` pipe.

2. **Benchmark Script Fix**: Resolved `-search-modes all` expansion bug in `scripts/unified_benchmark.py` — the literal string `"all"` was passed to bench-tool instead of expanding to actual mode names.

3. **QPS Aggregation Fix** (`86b56fb7`): Search modes now run sequentially (not concurrently). QPS computed as `queries / totalElapsed` from wall-clock time, giving accurate sustained throughput.

4. **GC Tuner Conflict Fix** (`10470e1d`): `EnableAdaptiveGC()` no longer force-enables the AdaptiveGCController. When `LONGBOW_MAX_MEMORY > 0` (benchmark mode), only the single `GCTuner` (500ms interval, arena-aware) manages `debug.SetGCPercent()`, preventing thrash between two competing tuners.

5. **Benchmark Server Fast-Exit** (`415cdb63`): When `LONGBOW_SHUTDOWN_SKIP_FINAL_SNAPSHOT=true`, SIGTERM returns immediately from `main()` — no gRPC `GracefulStop()`, no `vectorStore.Close()`, no 120s snapshot timeout. Reduces per-config shutdown from 25-30s to <50ms.

6. **gRPC Message Size Scaling** (`10470e1d`): Server-side `GRPC_MAX_*_MSG_SIZE` and client-side `MaxCallRecvMsgSize`/`MaxCallSendMsgSize` raised to 20GB to support large payloads (500k×3072×4 = 5.7GB). Note: gRPC wire protocol caps individual messages at 4GB (4-byte length prefix); bench-tool now chunks ingest payloads to 25k-row batches.

---

---

## v0.2.0-rc2 Release Candidate - Final Hardening (2026-05-05)

> [!IMPORTANT]
> **QPS values in this section are inflated ~5x by the bench-tool concurrent-mode bug** (discovered and fixed in `86b56fb7`). **Latency (p50/p95/p99) values are accurate.** These results are preserved for historical reference of what the buggy tool reported.

### Search Performance Breakdown (dim=128, count=5000) [ARCHIVED — PREVIOUSLY INFLATED QPS]

All QPS values in this section are from the buggy concurrent-mode tool. Latency values are accurate.

| Mode                | Buggy QPS | Corrected QPS (from 50k run) | Platform           |
| ------------------- | --------- | ---------------------------- | ------------------ |
| **Dense Search**    | 30,576    | **3,186**                    | Local CPU (M3)     |
| **Hybrid Search**   | 30,000+   | **3,697**                    | Local CPU (M3)     |
| **Filtered Search** | 30,000+   | **3,749**                    | Local CPU (M3)     |
| **ByID Search**     | 30,000+   | **3,777**                    | Local CPU (M3)     |

*See full corrected matrix above for all dimensions, counts, and data types.*

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

## Target Baselines (v0.2.2, Corrected QPS)

> [!NOTE]
> All QPS targets revised downward from v0.1.9 era because the original measurements were inflated ~5x by the concurrent-mode bug. Latency targets are unchanged.

- **Dense Search (Float32, 128d, 50k)**: > 3,000 QPS (p50 < 0.3ms)
- **Dense Search (Float32, 384d, 50k)**: > 2,400 QPS (p50 < 0.4ms)
- **Dense Search (Float32, 768d, 50k)**: > 1,800 QPS (p50 < 0.5ms)
- **Dense Search (Float32, 3072d, 50k)**: > 750 QPS (p50 < 0.8ms)
- **Hybrid Search**: > 2,500 QPS (128d)
- **Filtered Search**: > 3,500 QPS (128d)
- **ByID Search**: > 3,500 QPS (128d)
- **Ingestion (Bulk, float32 128d, 500k)**: > 2,000,000 vec/s
- **Ingestion (Bulk, float32 3072d, 50k)**: > 100,000 vec/s

---

## Hardware

- **Local**: Apple Silicon M3 Pro, 18GB memory (10GB allocated for benchmarks)
- **Remote (ancalagon)**: NVIDIA RTX 4060 Laptop GPU, 8GB VRAM, 22GB RAM, 16 cores (AMD64 Linux)

### Benchmark Matrix Coverage

- **Platforms:** CPU, Metal (local), CUDA (remote ancalagon)
- **Data Types:** float32, int8, int16, turboquant2/4/8
- **Dimensions:** 128, 384, 768, 1024, 3072
- **Counts:** 50,000, 100,000, 500,000
- **Search Modes (CPU):** dense, hybrid, sparse, filtered, byid
- **Search Modes (temporal):** temporal_as_of, temporal_range, temporal_window

### Status

| Component | Local CPU (M3) | Local Metal | Ancalagon CPU | Ancalagon CUDA | Temporal/Special |
|-----------|---------------|-------------|---------------|----------------|-----------------|
| 84/90 configs | ✅ Done | ⏳ Pending | ⏳ Pending | ⏳ Pending | ⏳ Pending |
| 3072×500k (6 cfg) | 🔧 Fixing gRPC chunking | — | — | — | — |
