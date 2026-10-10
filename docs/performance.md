# Longbow Performance Benchmarks

**Date:** 2026-10-09 (50k TurboQuant & 100k CPU + CUDA rows re-measured; 10k / 250k / 500k rows still 2026-09-26)  
**Baseline Release Candidate:** `v0.2.5-rc1`  

> [!WARNING]
> **The TurboQuant rows at 250k and 500k still predate `a955a0c1`.** The 50k and 100k
> rows have been re-measured with full provenance, paired index times, and all 13 search modes.
>
> That commit fixed neighbour selection reading the float32 arena, which is empty for
> every element type other than float32. For TurboQuant, every candidate was rejected
> and each node was left with a single link. Measured at 40,000 vectors, `dim=128`,
> `MMax0=16`:
>
> | Revision | layer-0 edges | mean degree | reachable from entry point |
> |---|---|---|---|
> | `7f872022` (pre-fix) | 276,940 | 6.92 | **29,215 / 40,000 (73.0%)** |
> | `a955a0c1` (post-fix) | 628,013 | 15.70 | **39,251 / 40,000 (98.1%)** |
>
> So a quarter of the corpus was unreachable at any `ef` when the older numbers were
> taken. See `docs/roadmap.md` §9.2 for the bisection.
>
> **What profiling & re-measuring found (roadmap R9/R19/R27, Item 1):**
>
> - **CPU Profiling Decomposition (`pprof`)**:
>   Decomposition of `turboQuantDistanceAVX2Scratch` at 100k scale:
>   - **Recursive polar tree reconstruction**: 47.8% (hierarchical cos/sin LUT multiplication)
>   - **QJL sign correction (`tqApplyQJLCorrection`)**: 28.5% (branchless sign expansion)
>   - **Angle code unpacking**: 12.9% (bit-shifting nibbles into byte indices)
>   - **Scratch buffer management & radius unpack**: 6.5%
>   - **SIMD distance kernel (`l2SquaredAVX2`)**: 4.3% (pure AVX2 L2 evaluation)
>   - **Chunk resolution / table lookup**: < 1.0% (chunk views amortize resolution)
>   Over 89.2% of distance evaluation time is spent in scalar polar reconstruction and dequantization, while the SIMD distance kernel accounts for only 4.3%.
>
> - **Root Cause of Historical 1,248 QPS vs ~300–420 QPS Gap**:
>   Prior to `a955a0c1`, neighbour selection checked the empty float32 arena for TurboQuant, rejecting all candidates and leaving nodes with 1 edge (mean degree 6.92, 73.0% reachability). Search traversed an incomplete graph, early-exiting after ~1 hop, evaluating very few candidates and falsely reporting 1,248 QPS on a broken topology. On the fully connected graph (mean degree 15.8–16.0, 98.1%+ reachability), search properly evaluates the full beam of candidates across ~3.5 hops, resulting in the honest ~300–420 QPS (420.6 QPS at 50k, 300.0 QPS at 100k).
>
> - **Ingestion & Index Time**:
>   At 50k scale, TurboQuant Flight streaming ingestion achieves 356,729.8 vec/s (21.77 MB/s) with paired index construction time of 497.0s (100.6 vec/s construction throughput) and 1,479.5 MB peak RSS under active R8 quality gating.
>
> - **Batched TurboQuant SIMD Kernel Dispatch**:
>   Batched distance evaluation is unified with all other dtypes via `TurboQuantDistanceBatch` / `GetTurboQuantDistanceBatchFunc()`. Dispatched to `AVX-512`, `AVX2`, `NEON`, and `Generic` implementations:
>   - **4-way interleaved polar reconstruction**: Evaluates 4 candidate trees concurrently to overlap independent reconstruction chains and hide scalar load latencies.
>   - **Zero-allocation chunk buffer gathering**: `tqComputer.ComputeBatch` gathers resident chunk codes into pre-allocated `codesBuf` slices and delegates to `DistanceDirectCodesBatch`.
>   - **Exact bit-identity**: Every batched SIMD kernel produces bit-identical distance values to its corresponding single-vector kernel, preserving graph connectivity and search tie-breaking.
>
> - The old 2026-09-26 rows also lacked provenance; older configurations routed complex128 and TurboQuant to EMLGo for 50k ≤ N < 500k, whereas current rows reflect the native engine with EMLGo disabled.

### Methodology changes in the 2026-10-09 rows

These differ from the 2026-09-26 rows in ways that matter when comparing them:

- **All 13 search modes are now recorded at 100k.** The older table carried only 9
  (no `filteredbool`, `filteredstring`, `globalgraphrag`, `recommend`), so those
  rows have no baseline to compare against.
- **Both engines.** Every dtype has cpu and cuda rows. EMLGo is not exercised.
- **The R8 graph-quality gate is active** and may rebuild a degraded batch
  sequentially. That is the shipped behaviour and it is what the baseline should
  reflect, but it costs index time that §2 does not report.

## System Specifications

| Component | Detail |
|---|---|
| CPU | Intel Core i7-12650H (16 vCPUs, AVX2, x86_64) |
| Host Active Cores | 4 Unthrottled Cores (CPUs 12-15 pinned via `--cpu-affinity 12-15`) |
| Concurrency Workers | 4 Workers (`--workers 4`) |
| Host RAM | 23 GB System Memory |
| GPU | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89, CUDA 12.4) |
| Go Runtime | Go 1.27.2 (CGO enabled) |

### Engine Variants Evaluated

| Engine Variant | Binary | Dispatch Configuration |
|---|---|---|
| **CPU Standard** | `bin/longbow_main` | Pure Go / AVX2 SIMD dispatch |
| **CPU EMLGo** | `bin/longbow_emlgo` | `LONGBOW_MATH_DISPATCH=emlgo` |
| **GPU Standard** | `bin/longbow-cuda_main` | CUDA Accelerated Vector Engine |
| **GPU EMLGo** | `bin/longbow-cuda_emlgo` | CUDA + `LONGBOW_MATH_DISPATCH=emlgo` |

---

## 1. Streaming Chunk Upload Architecture

Streaming chunk upload support is now active and set as the default across all client SDKs, CLI tools, and benchmark binaries:

- **Go Client SDK (`client/client.go`)**: First-class `StreamUploader` struct with `NewStreamUploader`, `WriteChunked(record, maxChunkSize=10000)`, and `UploadTable(tbl, chunkSize=10000)`.
- **Go Benchmark Tool (`cmd/bench-tool/main.go`)**: Uses chunk streaming generator producing 10,000-row record batches written directly to the Flight stream with immediate release (`rec.Release()`). Memory consumption reduced from >6 GB to <60 MB at 1,000,000 vector scale.
- **Go CLI (`cmd/cli/main.go`)**: `uploadData`, `runImportArrow`, and `runImportArrowFromReader` chunk inputs into 10,000-row streaming batches.
- **IO Bench & Soak Test (`cmd/io-bench/main.go`, `cmd/soak_test/main.go`)**: Ingest pipelines converted to chunked streams.
- **Python SDK (`longbowclientsdk/src/longbow/client.py`)**: `insert` and `_upload_batch` default to streaming chunks via `max_chunksize=batch_size` (10,000 rows) and natively support `pyarrow.RecordBatchReader` streams.

---

## 2. Ingestion Throughput, Index Time & Memory Footprint

Ingestion throughput measures transport-side Flight streaming vectors into the dataset and packing them.
Index Time measures the wall-clock time from ingest completion until HNSW graph construction
finishes and the dataset is fully searchable (`check_readiness`). Tracking index time alongside ingest
throughput ensures that graph-construction bottlenecks and bulk-linkage regressions are visible
directly in the primary performance matrix (see `docs/roadmap.md` Item 3).

| Scale (Count) | Dim | Dtype | Engine | Ingestion (vec/s) | Ingestion (MB/s) | Index Time (s) | Peak RSS (MB) |
|---|---|---|---|---|---|---|---|
| 10,000 | 128 | complex128 | cpu | 65,521.3 | 127.97 MB/s | — | 193.0 MB |
| 10,000 | 128 | float32 | cpu | 185,325.3 | 90.49 MB/s | — | 160.7 MB |
| 10,000 | 128 | float32 | cuda | 166,944.6 | 81.52 MB/s | — | 160.7 MB |
| 10,000 | 128 | int8 | cpu | 256,789.0 | 31.35 MB/s | — | 152.7 MB |
| 10,000 | 128 | turboquant | cpu | 182,161.4 | 11.12 MB/s | — | 151.3 MB |
| 10,000 | 384 | complex128 | cpu | 22,909.4 | 134.23 MB/s | — | 278.9 MB |
| 10,000 | 384 | float32 | cpu | 69,450.8 | 101.73 MB/s | — | 182.2 MB |
| 10,000 | 384 | int8 | cpu | 98,161.7 | 35.95 MB/s | — | 158.1 MB |
| 10,000 | 384 | turboquant | cpu | 68,866.8 | 12.61 MB/s | — | 154.0 MB |
| 50,000 | 128 | complex128 | cpu | 96,197.7 | 187.89 MB/s | — | 364.8 MB |
| 50,000 | 128 | float32 | cpu | 260,218.1 | 127.06 MB/s | — | 203.7 MB |
| 50,000 | 128 | int8 | cpu | 294,673.1 | 35.97 MB/s | — | 163.4 MB |
| 50,000 | 128 | turboquant | cpu | 356,729.8 | 21.77 MB/s | 497.0s | 1,479.5 MB |
| 50,000 | 384 | complex128 | cpu | 35,284.7 | 206.75 MB/s | — | 794.5 MB |
| 50,000 | 384 | float32 | cpu | 94,081.6 | 137.81 MB/s | — | 311.1 MB |
| 50,000 | 384 | int8 | cpu | 119,548.3 | 43.78 MB/s | — | 190.3 MB |
| 50,000 | 384 | turboquant | cpu | 85,216.8 | 15.60 MB/s | — | 170.1 MB |
| 100,000 | 128 | complex128 | cpu | 87,695.1 | 171.28 MB/s | — | 579.7 MB |
| 100,000 | 128 | complex128 | cuda | 103,717.4 | 202.57 MB/s | — | 579.7 MB |
| 100,000 | 128 | complex64 | cpu | 159,772.0 | 78.01 MB/s | — | 257.4 MB |
| 100,000 | 128 | complex64 | cuda | 195,572.2 | 95.49 MB/s | — | 257.4 MB |
| 100,000 | 128 | float16 | cpu | 494,361.9 | 241.39 MB/s | — | 257.4 MB |
| 100,000 | 128 | float16 | cuda | 725,010.7 | 354.01 MB/s | — | 257.4 MB |
| 100,000 | 128 | float32 | cpu | 317,003.5 | 154.79 MB/s | 22.5s | 257.4 MB |
| 100,000 | 128 | float32 | cuda | 218,303.7 | 106.59 MB/s | — | 257.4 MB |
| 100,000 | 128 | float64 | cpu | 201,306.7 | 98.29 MB/s | — | 257.4 MB |
| 100,000 | 128 | float64 | cuda | 168,177.7 | 82.12 MB/s | — | 257.4 MB |
| 100,000 | 128 | int16 | cpu | 419,474.6 | 204.82 MB/s | — | 257.4 MB |
| 100,000 | 128 | int16 | cuda | 713,631.7 | 348.45 MB/s | — | 257.4 MB |
| 100,000 | 128 | int32 | cpu | 387,712.9 | 189.31 MB/s | — | 257.4 MB |
| 100,000 | 128 | int32 | cuda | 381,869.1 | 186.46 MB/s | — | 257.4 MB |
| 100,000 | 128 | int64 | cpu | 177,334.5 | 86.59 MB/s | — | 257.4 MB |
| 100,000 | 128 | int64 | cuda | 204,187.7 | 99.70 MB/s | — | 257.4 MB |
| 100,000 | 128 | int8 | cpu | 1,186,065.7 | 144.78 MB/s | — | 176.9 MB |
| 100,000 | 128 | int8 | cuda | 965,449.2 | 117.85 MB/s | — | 176.9 MB |
| 100,000 | 128 | turboquant | cpu | 398,313.3 | 24.31 MB/s | ~180s | 163.4 MB |
| 100,000 | 128 | turboquant | cuda | 264,584.3 | 16.15 MB/s | — | 163.4 MB |
| 100,000 | 128 | uint16 | cpu | 559,667.8 | 273.28 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint16 | cuda | 716,691.3 | 349.95 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint32 | cpu | 245,680.9 | 119.96 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint32 | cuda | 332,692.2 | 162.45 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint64 | cpu | 142,308.2 | 69.49 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint64 | cuda | 178,130.5 | 86.98 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint8 | cpu | 1,329,226.3 | 649.04 MB/s | — | 257.4 MB |
| 100,000 | 128 | uint8 | cuda | 1,103,042.0 | 538.59 MB/s | — | 257.4 MB |
| 250,000 | 128 | complex128 | cpu | 112,386.5 | 219.50 MB/s | — | 1,224.2 MB |
| 250,000 | 128 | complex128 | cuda | 119,648.2 | 233.69 MB/s | — | 1,224.2 MB |
| 250,000 | 128 | complex64 | cpu | 222,957.6 | 108.87 MB/s | — | 418.6 MB |
| 250,000 | 128 | complex64 | cuda | 240,446.7 | 117.41 MB/s | — | 418.6 MB |
| 250,000 | 128 | float16 | cpu | 812,184.0 | 396.57 MB/s | — | 418.6 MB |
| 250,000 | 128 | float16 | cuda | 696,094.6 | 339.89 MB/s | — | 418.6 MB |
| 250,000 | 128 | float32 | cpu | 260,094.7 | 127.00 MB/s | — | 418.6 MB |
| 250,000 | 128 | float32 | cuda | 391,694.9 | 191.26 MB/s | — | 418.6 MB |
| 250,000 | 128 | float64 | cpu | 227,529.3 | 111.10 MB/s | — | 418.6 MB |
| 250,000 | 128 | float64 | cuda | 258,533.4 | 126.24 MB/s | — | 418.6 MB |
| 250,000 | 128 | int16 | cpu | 465,644.3 | 227.37 MB/s | — | 418.6 MB |
| 250,000 | 128 | int16 | cuda | 969,045.6 | 473.17 MB/s | — | 418.6 MB |
| 250,000 | 128 | int32 | cpu | 501,623.8 | 244.93 MB/s | — | 418.6 MB |
| 250,000 | 128 | int32 | cuda | 428,805.2 | 209.38 MB/s | — | 418.6 MB |
| 250,000 | 128 | int64 | cpu | 155,817.5 | 76.08 MB/s | — | 418.6 MB |
| 250,000 | 128 | int64 | cuda | 226,335.4 | 110.52 MB/s | — | 418.6 MB |
| 250,000 | 128 | int8 | cpu | 237,384.4 | 28.98 MB/s | — | 217.1 MB |
| 250,000 | 128 | int8 | cuda | 1,989,312.3 | 242.84 MB/s | — | 217.1 MB |
| 250,000 | 128 | turboquant | cpu | 235,192.9 | 14.36 MB/s | — | 183.6 MB |
| 250,000 | 128 | turboquant | cuda | 486,607.5 | 29.70 MB/s | — | 183.6 MB |
| 250,000 | 128 | uint16 | cpu | 848,551.6 | 414.33 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint16 | cuda | 728,278.0 | 355.60 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint32 | cpu | 396,662.4 | 193.68 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint32 | cuda | 521,768.7 | 254.77 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint64 | cpu | 205,311.8 | 100.25 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint64 | cuda | 264,788.3 | 129.29 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint8 | cpu | 757,178.3 | 369.72 MB/s | — | 418.6 MB |
| 250,000 | 128 | uint8 | cuda | 1,698,933.2 | 829.56 MB/s | — | 418.6 MB |
| 250,000 | 384 | complex128 | cpu | 24,024.2 | 140.77 MB/s | — | 3,372.7 MB |
| 250,000 | 384 | float32 | cpu | 92,452.8 | 135.43 MB/s | — | 955.7 MB |
| 250,000 | 384 | int8 | cpu | 109,509.1 | 40.10 MB/s | — | 351.4 MB |
| 250,000 | 384 | turboquant | cpu | 101,181.1 | 18.53 MB/s | — | 250.7 MB |
| 1,000,000 | 128 | int8 | cpu | 261,434.5 | 31.91 MB/s | — | 418.6 MB |

---

## 3. Search Modalities Throughput (QPS) & Latencies

Measured with 4 concurrent workers pinned to CPUs 12-15. The 2026-10-09 rows at
100,000 cover all **13** search modalities; the 2026-09-26 rows at 10k, 50k, 250k
and 500k cover 9 and have no `filteredbool`, `filteredstring`, `globalgraphrag` or
`recommend` rows.

| Scale | Dim | Dtype | Engine | Mode | QPS | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 10,000 | 128 | complex128 | cpu | **dense** | 2,354.3 | 1.441 | 2.563 | 2.596 |
| 10,000 | 128 | complex128 | cpu | **hybrid** | 2,649.2 | 1.326 | 1.694 | 1.724 |
| 10,000 | 128 | complex128 | cpu | **filtered** | 1,735.8 | 1.430 | 6.129 | 6.493 |
| 10,000 | 128 | complex128 | cpu | **filteredbool** | 1,768.4 | 1.811 | 4.008 | 4.096 |
| 10,000 | 128 | complex128 | cpu | **filteredstring** | 1,477.4 | 2.214 | 3.881 | 3.891 |
| 10,000 | 128 | complex128 | cpu | **sparse** | 5,674.7 | 0.697 | 0.887 | 0.979 |
| 10,000 | 128 | complex128 | cpu | **byid** | 3,177.3 | 1.205 | 1.883 | 1.917 |
| 10,000 | 128 | complex128 | cpu | **graphrag** | 1,650.4 | 2.265 | 2.733 | 2.746 |
| 10,000 | 128 | complex128 | cpu | **globalgraphrag** | 1,527.2 | 2.382 | 3.366 | 3.604 |
| 10,000 | 128 | complex128 | cpu | **recommend** | 3,121.7 | 1.140 | 1.655 | 1.780 |
| 10,000 | 128 | complex128 | cpu | **geo** | 1,237.2 | 2.888 | 4.254 | 4.288 |
| 10,000 | 128 | complex128 | cpu | **temporal** | 2,073.0 | 1.649 | 2.304 | 2.704 |
| 10,000 | 128 | complex128 | cpu | **learnedindex** | 2,562.3 | 1.449 | 1.783 | 1.922 |
| 10,000 | 128 | float32 | cpu | **dense** | 2,806.2 | 1.144 | 3.574 | 3.681 |
| 10,000 | 128 | float32 | cpu | **hybrid** | 3,160.6 | 1.203 | 1.547 | 1.629 |
| 10,000 | 128 | float32 | cpu | **filtered** | 2,196.7 | 1.041 | 5.455 | 5.457 |
| 10,000 | 128 | float32 | cpu | **filteredbool** | 2,450.3 | 1.205 | 3.491 | 3.518 |
| 10,000 | 128 | float32 | cpu | **filteredstring** | 2,411.8 | 1.501 | 2.942 | 3.040 |
| 10,000 | 128 | float32 | cpu | **sparse** | 5,486.7 | 0.723 | 0.809 | 0.923 |
| 10,000 | 128 | float32 | cpu | **byid** | 3,656.8 | 0.952 | 1.337 | 1.597 |
| 10,000 | 128 | float32 | cpu | **graphrag** | 2,545.6 | 1.453 | 2.285 | 2.312 |
| 10,000 | 128 | float32 | cpu | **globalgraphrag** | 2,760.9 | 1.304 | 1.823 | 1.953 |
| 10,000 | 128 | float32 | cpu | **recommend** | 3,156.5 | 1.169 | 1.666 | 1.972 |
| 10,000 | 128 | float32 | cpu | **geo** | 1,171.9 | 3.128 | 4.038 | 4.085 |
| 10,000 | 128 | float32 | cpu | **temporal** | 2,262.9 | 1.563 | 2.351 | 2.394 |
| 10,000 | 128 | float32 | cpu | **learnedindex** | 3,629.0 | 1.004 | 1.299 | 1.388 |
| 10,000 | 128 | float32 | cuda | **dense** | 2,492.1 | 1.214 | 3.133 | 3.738 |
| 10,000 | 128 | float32 | cuda | **hybrid** | 2,665.2 | 1.448 | 1.717 | 1.835 |
| 10,000 | 128 | float32 | cuda | **filtered** | 1,818.0 | 1.222 | 5.792 | 6.971 |
| 10,000 | 128 | float32 | cuda | **filteredbool** | 1,844.7 | 1.565 | 4.081 | 5.130 |
| 10,000 | 128 | float32 | cuda | **filteredstring** | 1,693.0 | 2.143 | 3.744 | 4.058 |
| 10,000 | 128 | float32 | cuda | **sparse** | 3,154.1 | 0.879 | 2.989 | 3.142 |
| 10,000 | 128 | float32 | cuda | **byid** | 3,052.8 | 1.180 | 1.932 | 2.041 |
| 10,000 | 128 | float32 | cuda | **graphrag** | 1,889.5 | 1.819 | 3.212 | 3.499 |
| 10,000 | 128 | float32 | cuda | **globalgraphrag** | 1,789.5 | 1.986 | 3.233 | 3.313 |
| 10,000 | 128 | float32 | cuda | **recommend** | 1,973.3 | 1.464 | 3.911 | 4.020 |
| 10,000 | 128 | float32 | cuda | **geo** | 711.1 | 4.852 | 7.561 | 7.571 |
| 10,000 | 128 | float32 | cuda | **temporal** | 1,427.4 | 2.468 | 3.961 | 4.629 |
| 10,000 | 128 | float32 | cuda | **learnedindex** | 2,677.4 | 1.419 | 1.794 | 1.846 |
| 10,000 | 128 | int8 | cpu | **dense** | 3,109.9 | 1.305 | 1.660 | 1.986 |
| 10,000 | 128 | int8 | cpu | **hybrid** | 3,408.6 | 1.148 | 1.367 | 1.375 |
| 10,000 | 128 | int8 | cpu | **filtered** | 2,203.6 | 1.059 | 6.042 | 6.092 |
| 10,000 | 128 | int8 | cpu | **filteredbool** | 2,757.9 | 1.158 | 2.980 | 3.179 |
| 10,000 | 128 | int8 | cpu | **filteredstring** | 2,064.4 | 1.591 | 2.739 | 2.742 |
| 10,000 | 128 | int8 | cpu | **sparse** | 6,035.1 | 0.618 | 0.824 | 0.845 |
| 10,000 | 128 | int8 | cpu | **byid** | 4,130.1 | 0.890 | 1.303 | 1.415 |
| 10,000 | 128 | int8 | cpu | **graphrag** | 1,917.5 | 1.897 | 2.604 | 2.640 |
| 10,000 | 128 | int8 | cpu | **globalgraphrag** | 1,854.6 | 1.953 | 2.640 | 2.817 |
| 10,000 | 128 | int8 | cpu | **recommend** | 3,974.6 | 0.957 | 1.271 | 1.668 |
| 10,000 | 128 | int8 | cpu | **geo** | 668.9 | 6.080 | 8.626 | 8.799 |
| 10,000 | 128 | int8 | cpu | **temporal** | 1,121.7 | 3.269 | 4.496 | 4.786 |
| 10,000 | 128 | int8 | cpu | **learnedindex** | 3,579.0 | 0.918 | 1.475 | 1.722 |
| 10,000 | 128 | turboquant | cpu | **dense** | 3,166.4 | 0.857 | 3.354 | 3.620 |
| 10,000 | 128 | turboquant | cpu | **hybrid** | 3,641.8 | 1.068 | 1.314 | 1.317 |
| 10,000 | 128 | turboquant | cpu | **filtered** | 2,386.8 | 0.825 | 5.494 | 5.503 |
| 10,000 | 128 | turboquant | cpu | **filteredbool** | 1,852.9 | 1.714 | 3.852 | 3.878 |
| 10,000 | 128 | turboquant | cpu | **filteredstring** | 1,945.9 | 1.640 | 3.151 | 3.217 |
| 10,000 | 128 | turboquant | cpu | **sparse** | 4,478.0 | 0.866 | 0.936 | 0.978 |
| 10,000 | 128 | turboquant | cpu | **byid** | 3,745.8 | 1.111 | 1.571 | 1.655 |
| 10,000 | 128 | turboquant | cpu | **graphrag** | 3,267.1 | 1.019 | 1.640 | 1.818 |
| 10,000 | 128 | turboquant | cpu | **globalgraphrag** | 3,346.0 | 1.216 | 1.584 | 1.598 |
| 10,000 | 128 | turboquant | cpu | **recommend** | 4,025.6 | 0.895 | 1.465 | 1.498 |
| 10,000 | 128 | turboquant | cpu | **geo** | 1,266.0 | 2.888 | 3.667 | 3.736 |
| 10,000 | 128 | turboquant | cpu | **temporal** | 2,110.7 | 1.598 | 2.491 | 2.522 |
| 10,000 | 128 | turboquant | cpu | **learnedindex** | 3,820.2 | 1.026 | 1.332 | 1.339 |
| 10,000 | 384 | complex128 | cpu | **dense** | 1,557.0 | 2.251 | 3.569 | 3.577 |
| 10,000 | 384 | complex128 | cpu | **hybrid** | 1,445.0 | 2.595 | 3.604 | 3.615 |
| 10,000 | 384 | complex128 | cpu | **filtered** | 1,176.4 | 2.591 | 7.483 | 7.495 |
| 10,000 | 384 | complex128 | cpu | **filteredbool** | 1,029.3 | 3.103 | 5.388 | 5.436 |
| 10,000 | 384 | complex128 | cpu | **filteredstring** | 800.7 | 4.677 | 6.273 | 7.759 |
| 10,000 | 384 | complex128 | cpu | **sparse** | 4,840.6 | 0.856 | 0.978 | 0.980 |
| 10,000 | 384 | complex128 | cpu | **byid** | 2,470.6 | 1.443 | 2.434 | 2.492 |
| 10,000 | 384 | complex128 | cpu | **graphrag** | 1,041.9 | 3.402 | 3.932 | 5.474 |
| 10,000 | 384 | complex128 | cpu | **globalgraphrag** | 1,042.9 | 3.100 | 4.578 | 4.808 |
| 10,000 | 384 | complex128 | cpu | **recommend** | 2,151.9 | 1.547 | 2.489 | 3.146 |
| 10,000 | 384 | complex128 | cpu | **geo** | 1,247.1 | 3.019 | 3.559 | 3.614 |
| 10,000 | 384 | complex128 | cpu | **temporal** | 1,940.3 | 1.766 | 2.551 | 2.736 |
| 10,000 | 384 | complex128 | cpu | **learnedindex** | 1,701.9 | 2.185 | 2.623 | 3.990 |
| 10,000 | 384 | float32 | cpu | **dense** | 2,468.2 | 1.504 | 1.874 | 2.230 |
| 10,000 | 384 | float32 | cpu | **hybrid** | 2,717.2 | 1.217 | 1.923 | 2.063 |
| 10,000 | 384 | float32 | cpu | **filtered** | 1,659.8 | 1.719 | 6.641 | 6.687 |
| 10,000 | 384 | float32 | cpu | **filteredbool** | 1,860.5 | 1.574 | 4.163 | 5.229 |
| 10,000 | 384 | float32 | cpu | **filteredstring** | 1,794.4 | 1.886 | 3.516 | 3.544 |
| 10,000 | 384 | float32 | cpu | **sparse** | 5,137.6 | 0.746 | 0.858 | 0.862 |
| 10,000 | 384 | float32 | cpu | **byid** | 3,512.0 | 1.051 | 1.367 | 1.531 |
| 10,000 | 384 | float32 | cpu | **graphrag** | 2,105.5 | 1.693 | 2.473 | 2.488 |
| 10,000 | 384 | float32 | cpu | **globalgraphrag** | 1,746.0 | 1.663 | 4.944 | 5.038 |
| 10,000 | 384 | float32 | cpu | **recommend** | 2,340.4 | 1.516 | 2.652 | 3.101 |
| 10,000 | 384 | float32 | cpu | **geo** | 1,166.9 | 3.102 | 4.284 | 4.326 |
| 10,000 | 384 | float32 | cpu | **temporal** | 2,109.8 | 1.570 | 2.410 | 3.455 |
| 10,000 | 384 | float32 | cpu | **learnedindex** | 2,779.9 | 1.386 | 1.756 | 1.856 |
| 10,000 | 384 | int8 | cpu | **dense** | 2,628.9 | 1.285 | 1.846 | 3.861 |
| 10,000 | 384 | int8 | cpu | **hybrid** | 2,793.6 | 1.445 | 1.721 | 1.766 |
| 10,000 | 384 | int8 | cpu | **filtered** | 2,059.5 | 1.085 | 5.232 | 6.952 |
| 10,000 | 384 | int8 | cpu | **filteredbool** | 1,930.7 | 1.591 | 3.905 | 3.941 |
| 10,000 | 384 | int8 | cpu | **filteredstring** | 1,791.9 | 1.862 | 2.818 | 2.914 |
| 10,000 | 384 | int8 | cpu | **sparse** | 5,160.4 | 0.765 | 0.978 | 1.032 |
| 10,000 | 384 | int8 | cpu | **byid** | 4,349.1 | 0.833 | 1.511 | 1.545 |
| 10,000 | 384 | int8 | cpu | **graphrag** | 1,775.9 | 2.003 | 2.747 | 3.659 |
| 10,000 | 384 | int8 | cpu | **globalgraphrag** | 1,915.7 | 1.918 | 2.229 | 2.561 |
| 10,000 | 384 | int8 | cpu | **recommend** | 3,877.3 | 1.004 | 1.195 | 1.641 |
| 10,000 | 384 | int8 | cpu | **geo** | 1,150.2 | 3.172 | 4.177 | 4.557 |
| 10,000 | 384 | int8 | cpu | **temporal** | 1,240.2 | 2.809 | 4.443 | 4.825 |
| 10,000 | 384 | int8 | cpu | **learnedindex** | 2,999.2 | 1.366 | 1.473 | 1.819 |
| 10,000 | 384 | turboquant | cpu | **dense** | 1,853.2 | 1.821 | 3.264 | 3.288 |
| 10,000 | 384 | turboquant | cpu | **hybrid** | 1,816.2 | 2.026 | 2.261 | 2.508 |
| 10,000 | 384 | turboquant | cpu | **filtered** | 1,470.4 | 1.851 | 6.836 | 6.847 |
| 10,000 | 384 | turboquant | cpu | **filteredbool** | 1,727.8 | 1.888 | 4.017 | 4.021 |
| 10,000 | 384 | turboquant | cpu | **filteredstring** | 2,036.2 | 1.760 | 2.999 | 3.109 |
| 10,000 | 384 | turboquant | cpu | **sparse** | 4,978.5 | 0.817 | 0.919 | 0.930 |
| 10,000 | 384 | turboquant | cpu | **byid** | 2,400.4 | 1.510 | 1.840 | 2.593 |
| 10,000 | 384 | turboquant | cpu | **graphrag** | 1,865.8 | 1.906 | 3.166 | 3.255 |
| 10,000 | 384 | turboquant | cpu | **globalgraphrag** | 1,767.6 | 2.049 | 3.318 | 3.795 |
| 10,000 | 384 | turboquant | cpu | **recommend** | 2,073.1 | 1.614 | 2.749 | 3.447 |
| 10,000 | 384 | turboquant | cpu | **geo** | 1,118.5 | 3.378 | 4.984 | 5.126 |
| 10,000 | 384 | turboquant | cpu | **temporal** | 2,018.6 | 1.677 | 2.858 | 2.966 |
| 10,000 | 384 | turboquant | cpu | **learnedindex** | 1,808.9 | 1.819 | 2.626 | 3.397 |
| 50,000 | 128 | complex128 | cpu | **dense** | 1,353.7 | 2.561 | 3.527 | 3.592 |
| 50,000 | 128 | complex128 | cpu | **hybrid** | 775.9 | 4.712 | 5.527 | 5.574 |
| 50,000 | 128 | complex128 | cpu | **filtered** | 302.4 | 7.565 | 36.513 | 36.552 |
| 50,000 | 128 | complex128 | cpu | **filteredbool** | 153.3 | 19.586 | 33.135 | 41.689 |
| 50,000 | 128 | complex128 | cpu | **filteredstring** | 168.2 | 21.138 | 27.989 | 28.096 |
| 50,000 | 128 | complex128 | cpu | **sparse** | 4,717.3 | 0.784 | 0.997 | 1.090 |
| 50,000 | 128 | complex128 | cpu | **byid** | 3,276.6 | 1.139 | 1.272 | 1.320 |
| 50,000 | 128 | complex128 | cpu | **graphrag** | 235.4 | 17.767 | 19.561 | 19.901 |
| 50,000 | 128 | complex128 | cpu | **globalgraphrag** | 210.2 | 17.805 | 22.485 | 23.387 |
| 50,000 | 128 | complex128 | cpu | **recommend** | 180.1 | 20.454 | 20.935 | 20.941 |
| 50,000 | 128 | complex128 | cpu | **geo** | 322.4 | 11.745 | 15.611 | 15.644 |
| 50,000 | 128 | complex128 | cpu | **temporal** | 1,329.4 | 2.607 | 4.027 | 5.287 |
| 50,000 | 128 | complex128 | cpu | **learnedindex** | 203.8 | 18.889 | 20.590 | 20.845 |
| 50,000 | 128 | float32 | cpu | **dense** | 2,935.0 | 1.241 | 1.547 | 1.604 |
| 50,000 | 128 | float32 | cpu | **hybrid** | 1,216.3 | 1.401 | 5.045 | 5.615 |
| 50,000 | 128 | float32 | cpu | **filtered** | 660.0 | 1.005 | 29.180 | 29.322 |
| 50,000 | 128 | float32 | cpu | **filteredbool** | 1,099.4 | 1.475 | 14.711 | 14.863 |
| 50,000 | 128 | float32 | cpu | **filteredstring** | 788.5 | 1.632 | 8.960 | 18.096 |
| 50,000 | 128 | float32 | cpu | **sparse** | 5,138.1 | 0.771 | 1.030 | 1.043 |
| 50,000 | 128 | float32 | cpu | **byid** | 3,386.3 | 1.096 | 1.613 | 1.704 |
| 50,000 | 128 | float32 | cpu | **graphrag** | 1,739.4 | 1.695 | 2.140 | 5.992 |
| 50,000 | 128 | float32 | cpu | **globalgraphrag** | 1,918.2 | 1.858 | 2.446 | 3.026 |
| 50,000 | 128 | float32 | cpu | **recommend** | 698.7 | 5.111 | 6.483 | 8.025 |
| 50,000 | 128 | float32 | cpu | **geo** | 247.3 | 11.553 | 35.034 | 35.463 |
| 50,000 | 128 | float32 | cpu | **temporal** | 1,480.7 | 2.429 | 2.936 | 3.405 |
| 50,000 | 128 | float32 | cpu | **learnedindex** | 2,894.1 | 1.266 | 1.520 | 1.635 |
| 50,000 | 128 | int8 | cpu | **dense** | 1,210.8 | 2.891 | 4.046 | 5.346 |
| 50,000 | 128 | int8 | cpu | **hybrid** | 1,147.1 | 3.053 | 4.786 | 4.915 |
| 50,000 | 128 | int8 | cpu | **filtered** | 446.6 | 4.246 | 28.456 | 29.030 |
| 50,000 | 128 | int8 | cpu | **filteredbool** | 522.0 | 4.402 | 20.694 | 22.488 |
| 50,000 | 128 | int8 | cpu | **filteredstring** | 450.7 | 6.880 | 14.869 | 15.047 |
| 50,000 | 128 | int8 | cpu | **sparse** | 5,260.5 | 0.725 | 0.859 | 0.889 |
| 50,000 | 128 | int8 | cpu | **byid** | 3,979.1 | 1.002 | 1.177 | 1.205 |
| 50,000 | 128 | int8 | cpu | **graphrag** | 838.0 | 4.297 | 6.704 | 6.705 |
| 50,000 | 128 | int8 | cpu | **globalgraphrag** | 875.3 | 4.033 | 6.608 | 7.090 |
| 50,000 | 128 | int8 | cpu | **recommend** | 995.0 | 3.344 | 5.358 | 5.424 |
| 50,000 | 128 | int8 | cpu | **geo** | 335.6 | 11.265 | 13.527 | 14.357 |
| 50,000 | 128 | int8 | cpu | **temporal** | 931.8 | 3.712 | 5.119 | 5.123 |
| 50,000 | 128 | int8 | cpu | **learnedindex** | 784.7 | 3.930 | 5.873 | 6.865 |
| 50,000 | 128 | turboquant | cpu | **dense** | 420.6 | 9.440 | 9.764 | 12.091 |
| 50,000 | 128 | turboquant | cpu | **hybrid** | 409.5 | 9.603 | 10.236 | 13.359 |
| 50,000 | 128 | turboquant | cpu | **filtered** | 407.8 | 9.650 | 10.058 | 11.817 |
| 50,000 | 128 | turboquant | cpu | **filteredbool** | 408.4 | 9.690 | 10.090 | 12.417 |
| 50,000 | 128 | turboquant | cpu | **filteredstring** | 405.5 | 9.713 | 10.563 | 13.149 |
| 50,000 | 128 | turboquant | cpu | **sparse** | 6,219.9 | 0.627 | 0.973 | 1.738 |
| 50,000 | 128 | turboquant | cpu | **byid** | 418.2 | 9.498 | 9.956 | 12.201 |
| 50,000 | 128 | turboquant | cpu | **graphrag** | 378.0 | 10.555 | 11.087 | 11.776 |
| 50,000 | 128 | turboquant | cpu | **globalgraphrag** | 372.5 | 10.625 | 11.414 | 16.937 |
| 50,000 | 128 | turboquant | cpu | **recommend** | 274.4 | 14.496 | 15.198 | 17.288 |
| 50,000 | 128 | turboquant | cpu | **geo** | 535.3 | 6.378 | 17.152 | 24.452 |
| 50,000 | 128 | turboquant | cpu | **temporal** | 6,823.1 | 0.562 | 0.799 | 0.976 |
| 50,000 | 128 | turboquant | cpu | **learnedindex** | 388.6 | 10.227 | 10.722 | 12.959 |
| 50,000 | 384 | complex128 | cpu | **dense** | 837.1 | 4.708 | 6.266 | 6.295 |
| 50,000 | 384 | complex128 | cpu | **hybrid** | 654.9 | 5.583 | 8.009 | 8.259 |
| 50,000 | 384 | complex128 | cpu | **filtered** | 405.3 | 5.263 | 31.319 | 31.744 |
| 50,000 | 384 | complex128 | cpu | **filteredbool** | 408.9 | 6.944 | 22.744 | 26.299 |
| 50,000 | 384 | complex128 | cpu | **filteredstring** | 143.4 | 24.325 | 33.808 | 42.016 |
| 50,000 | 384 | complex128 | cpu | **sparse** | 4,193.6 | 0.935 | 1.050 | 1.078 |
| 50,000 | 384 | complex128 | cpu | **byid** | 2,921.9 | 1.350 | 1.482 | 1.482 |
| 50,000 | 384 | complex128 | cpu | **graphrag** | 504.5 | 7.682 | 11.089 | 13.260 |
| 50,000 | 384 | complex128 | cpu | **globalgraphrag** | 427.9 | 7.160 | 15.859 | 16.407 |
| 50,000 | 384 | complex128 | cpu | **recommend** | 232.6 | 15.836 | 16.177 | 18.596 |
| 50,000 | 384 | complex128 | cpu | **geo** | 306.1 | 12.168 | 14.949 | 15.006 |
| 50,000 | 384 | complex128 | cpu | **temporal** | 1,372.6 | 2.723 | 3.434 | 3.468 |
| 50,000 | 384 | complex128 | cpu | **learnedindex** | 219.9 | 18.866 | 20.107 | 20.677 |
| 50,000 | 384 | float32 | cpu | **dense** | 2,196.5 | 1.447 | 2.828 | 2.937 |
| 50,000 | 384 | float32 | cpu | **hybrid** | 2,159.0 | 1.779 | 2.045 | 2.125 |
| 50,000 | 384 | float32 | cpu | **filtered** | 749.8 | 1.323 | 25.753 | 25.779 |
| 50,000 | 384 | float32 | cpu | **filteredbool** | 990.1 | 2.073 | 13.982 | 13.993 |
| 50,000 | 384 | float32 | cpu | **filteredstring** | 1,139.5 | 2.221 | 9.237 | 9.290 |
| 50,000 | 384 | float32 | cpu | **sparse** | 4,790.3 | 0.783 | 1.104 | 1.114 |
| 50,000 | 384 | float32 | cpu | **byid** | 2,247.3 | 1.565 | 2.330 | 2.360 |
| 50,000 | 384 | float32 | cpu | **graphrag** | 1,638.7 | 2.242 | 3.138 | 3.343 |
| 50,000 | 384 | float32 | cpu | **globalgraphrag** | 1,529.1 | 2.258 | 3.409 | 4.061 |
| 50,000 | 384 | float32 | cpu | **recommend** | 1,466.6 | 2.350 | 3.468 | 3.547 |
| 50,000 | 384 | float32 | cpu | **geo** | 232.7 | 12.359 | 32.549 | 36.663 |
| 50,000 | 384 | float32 | cpu | **temporal** | 1,405.5 | 2.480 | 3.325 | 3.531 |
| 50,000 | 384 | float32 | cpu | **learnedindex** | 1,902.2 | 1.762 | 2.801 | 4.009 |
| 50,000 | 384 | int8 | cpu | **dense** | 2,892.5 | 1.111 | 1.602 | 3.913 |
| 50,000 | 384 | int8 | cpu | **hybrid** | 1,146.2 | 3.212 | 3.436 | 3.562 |
| 50,000 | 384 | int8 | cpu | **filtered** | 713.9 | 1.156 | 28.021 | 28.032 |
| 50,000 | 384 | int8 | cpu | **filteredbool** | 1,126.3 | 1.375 | 14.711 | 14.821 |
| 50,000 | 384 | int8 | cpu | **filteredstring** | 451.2 | 7.154 | 14.716 | 15.116 |
| 50,000 | 384 | int8 | cpu | **sparse** | 4,300.0 | 0.899 | 1.054 | 1.137 |
| 50,000 | 384 | int8 | cpu | **byid** | 3,506.8 | 1.044 | 1.270 | 1.277 |
| 50,000 | 384 | int8 | cpu | **graphrag** | 1,967.5 | 1.837 | 2.763 | 2.786 |
| 50,000 | 384 | int8 | cpu | **globalgraphrag** | 2,275.3 | 1.712 | 2.394 | 2.465 |
| 50,000 | 384 | int8 | cpu | **recommend** | 974.9 | 3.469 | 5.605 | 5.870 |
| 50,000 | 384 | int8 | cpu | **geo** | 209.3 | 13.745 | 34.497 | 47.764 |
| 50,000 | 384 | int8 | cpu | **temporal** | 881.0 | 3.928 | 5.718 | 5.734 |
| 50,000 | 384 | int8 | cpu | **learnedindex** | 2,742.3 | 1.282 | 1.671 | 1.674 |
| 50,000 | 384 | turboquant | cpu | **dense** | 1,700.3 | 2.269 | 2.954 | 2.989 |
| 50,000 | 384 | turboquant | cpu | **hybrid** | 1,622.5 | 2.268 | 2.640 | 2.837 |
| 50,000 | 384 | turboquant | cpu | **filtered** | 564.3 | 2.059 | 32.211 | 32.413 |
| 50,000 | 384 | turboquant | cpu | **filteredbool** | 833.8 | 2.143 | 17.417 | 17.522 |
| 50,000 | 384 | turboquant | cpu | **filteredstring** | 868.0 | 3.106 | 11.288 | 11.342 |
| 50,000 | 384 | turboquant | cpu | **sparse** | 4,454.1 | 0.853 | 1.056 | 1.058 |
| 50,000 | 384 | turboquant | cpu | **byid** | 1,671.9 | 2.055 | 3.214 | 3.905 |
| 50,000 | 384 | turboquant | cpu | **graphrag** | 1,563.1 | 2.303 | 2.826 | 2.869 |
| 50,000 | 384 | turboquant | cpu | **globalgraphrag** | 1,449.1 | 2.311 | 4.348 | 4.746 |
| 50,000 | 384 | turboquant | cpu | **recommend** | 1,801.4 | 2.052 | 2.285 | 2.353 |
| 50,000 | 384 | turboquant | cpu | **geo** | 301.3 | 12.524 | 15.006 | 15.008 |
| 50,000 | 384 | turboquant | cpu | **temporal** | 1,282.1 | 2.758 | 3.462 | 3.546 |
| 50,000 | 384 | turboquant | cpu | **learnedindex** | 1,570.7 | 2.341 | 2.677 | 2.782 |
| 100,000 | 128 | float32 | cpu | **dense** | 3,395.2 | 1.069 | 1.721 | 5.001 |
| 100,000 | 128 | float32 | cpu | **hybrid** | 647.9 | 7.928 | 9.241 | 11.206 |
| 100,000 | 128 | float32 | cpu | **sparse** | 6,865.5 | 0.596 | 0.774 | 0.831 |
| 100,000 | 128 | float32 | cpu | **filtered** | 2,461.9 | 1.041 | 1.512 | 2.107 |
| 100,000 | 128 | float32 | cpu | **filteredbool** | 2,785.3 | 1.215 | 1.875 | 2.278 |
| 100,000 | 128 | float32 | cpu | **filteredstring** | 2,042.4 | 1.540 | 2.380 | 3.099 |
| 100,000 | 128 | float32 | cpu | **byid** | 3,927.4 | 0.928 | 1.427 | 1.672 |
| 100,000 | 128 | float32 | cpu | **graphrag** | 1,788.4 | 1.828 | 4.473 | 6.910 |
| 100,000 | 128 | float32 | cpu | **globalgraphrag** | 2,205.7 | 1.732 | 2.634 | 3.061 |
| 100,000 | 128 | float32 | cpu | **recommend** | 454.5 | 8.442 | 10.945 | 19.282 |
| 100,000 | 128 | float32 | cpu | **geo** | 232.6 | 14.256 | 42.292 | 56.998 |
| 100,000 | 128 | float32 | cpu | **temporal** | 5,991.3 | 0.573 | 0.802 | 1.089 |
| 100,000 | 128 | float32 | cpu | **learnedindex** | 3,264.9 | 1.167 | 1.703 | 2.055 |
| 100,000 | 128 | float64 | cpu | **dense** | 1,150.9 | 1.258 | 9.567 | 11.998 |
| 100,000 | 128 | float64 | cpu | **hybrid** | 560.6 | 7.972 | 10.605 | 12.390 |
| 100,000 | 128 | float64 | cpu | **sparse** | 6,651.8 | 0.631 | 0.775 | 0.845 |
| 100,000 | 128 | float64 | cpu | **filtered** | 1,140.7 | 1.097 | 11.692 | 13.996 |
| 100,000 | 128 | float64 | cpu | **filteredbool** | 1,089.3 | 1.442 | 18.097 | 21.091 |
| 100,000 | 128 | float64 | cpu | **filteredstring** | 1,184.8 | 1.972 | 4.875 | 21.477 |
| 100,000 | 128 | float64 | cpu | **byid** | 1,300.7 | 1.054 | 13.121 | 14.386 |
| 100,000 | 128 | float64 | cpu | **graphrag** | 1,768.0 | 1.933 | 3.949 | 4.759 |
| 100,000 | 128 | float64 | cpu | **globalgraphrag** | 1,039.3 | 2.252 | 9.978 | 13.243 |
| 100,000 | 128 | float64 | cpu | **recommend** | 511.4 | 7.865 | 8.637 | 8.768 |
| 100,000 | 128 | float64 | cpu | **geo** | 229.4 | 14.507 | 42.200 | 60.671 |
| 100,000 | 128 | float64 | cpu | **temporal** | 6,053.5 | 0.554 | 0.800 | 1.248 |
| 100,000 | 128 | float64 | cpu | **learnedindex** | 1,860.3 | 1.265 | 9.090 | 10.228 |
| 100,000 | 128 | float16 | cpu | **dense** | 2,731.2 | 1.029 | 2.973 | 6.707 |
| 100,000 | 128 | float16 | cpu | **hybrid** | 953.2 | 3.854 | 7.745 | 9.034 |
| 100,000 | 128 | float16 | cpu | **sparse** | 6,392.8 | 0.647 | 0.813 | 1.006 |
| 100,000 | 128 | float16 | cpu | **filtered** | 2,090.0 | 1.009 | 3.121 | 5.833 |
| 100,000 | 128 | float16 | cpu | **filteredbool** | 1,281.9 | 1.191 | 9.314 | 14.785 |
| 100,000 | 128 | float16 | cpu | **filteredstring** | 1,383.9 | 1.375 | 7.768 | 9.945 |
| 100,000 | 128 | float16 | cpu | **byid** | 4,277.8 | 0.854 | 1.501 | 1.799 |
| 100,000 | 128 | float16 | cpu | **graphrag** | 1,865.5 | 1.678 | 4.802 | 5.558 |
| 100,000 | 128 | float16 | cpu | **globalgraphrag** | 1,781.8 | 1.698 | 3.898 | 9.173 |
| 100,000 | 128 | float16 | cpu | **recommend** | 508.6 | 7.510 | 9.296 | 19.616 |
| 100,000 | 128 | float16 | cpu | **geo** | 228.4 | 14.229 | 42.285 | 55.650 |
| 100,000 | 128 | float16 | cpu | **temporal** | 3,871.1 | 0.549 | 0.774 | 1.291 |
| 100,000 | 128 | float16 | cpu | **learnedindex** | 1,528.4 | 0.967 | 8.471 | 9.642 |
| 100,000 | 128 | int8 | cpu | **dense** | 1,162.2 | 3.316 | 4.839 | 6.064 |
| 100,000 | 128 | int8 | cpu | **hybrid** | 958.2 | 3.710 | 7.365 | 9.602 |
| 100,000 | 128 | int8 | cpu | **sparse** | 6,684.0 | 0.602 | 0.815 | 1.159 |
| 100,000 | 128 | int8 | cpu | **filtered** | 916.2 | 3.648 | 4.996 | 7.123 |
| 100,000 | 128 | int8 | cpu | **filteredbool** | 654.4 | 5.486 | 9.801 | 25.426 |
| 100,000 | 128 | int8 | cpu | **filteredstring** | 2,669.2 | 1.122 | 1.764 | 2.499 |
| 100,000 | 128 | int8 | cpu | **byid** | 4,353.0 | 0.847 | 1.407 | 1.592 |
| 100,000 | 128 | int8 | cpu | **graphrag** | 842.8 | 4.698 | 5.400 | 7.210 |
| 100,000 | 128 | int8 | cpu | **globalgraphrag** | 752.7 | 4.865 | 8.553 | 10.946 |
| 100,000 | 128 | int8 | cpu | **recommend** | 1,107.1 | 3.469 | 4.486 | 6.121 |
| 100,000 | 128 | int8 | cpu | **geo** | 231.1 | 14.214 | 42.943 | 54.045 |
| 100,000 | 128 | int8 | cpu | **temporal** | 3,842.6 | 0.550 | 0.814 | 1.620 |
| 100,000 | 128 | int8 | cpu | **learnedindex** | 1,066.3 | 3.573 | 4.747 | 5.366 |
| 100,000 | 128 | int16 | cpu | **dense** | 999.0 | 3.942 | 4.706 | 6.731 |
| 100,000 | 128 | int16 | cpu | **hybrid** | 908.1 | 4.326 | 4.859 | 7.677 |
| 100,000 | 128 | int16 | cpu | **sparse** | 6,646.7 | 0.628 | 0.789 | 0.896 |
| 100,000 | 128 | int16 | cpu | **filtered** | 743.6 | 4.354 | 8.792 | 15.740 |
| 100,000 | 128 | int16 | cpu | **filteredbool** | 561.0 | 6.940 | 7.366 | 11.703 |
| 100,000 | 128 | int16 | cpu | **filteredstring** | 368.5 | 10.137 | 14.397 | 30.992 |
| 100,000 | 128 | int16 | cpu | **byid** | 4,263.1 | 0.864 | 1.425 | 1.726 |
| 100,000 | 128 | int16 | cpu | **graphrag** | 727.2 | 5.517 | 6.104 | 7.131 |
| 100,000 | 128 | int16 | cpu | **globalgraphrag** | 661.5 | 5.630 | 9.716 | 12.591 |
| 100,000 | 128 | int16 | cpu | **recommend** | 927.3 | 4.161 | 5.441 | 6.673 |
| 100,000 | 128 | int16 | cpu | **geo** | 229.8 | 14.169 | 43.498 | 57.837 |
| 100,000 | 128 | int16 | cpu | **temporal** | 3,764.6 | 0.573 | 0.822 | 1.692 |
| 100,000 | 128 | int16 | cpu | **learnedindex** | 881.5 | 4.425 | 5.683 | 7.375 |
| 100,000 | 128 | int32 | cpu | **dense** | 994.5 | 3.895 | 6.601 | 9.828 |
| 100,000 | 128 | int32 | cpu | **hybrid** | 562.3 | 6.779 | 11.048 | 15.426 |
| 100,000 | 128 | int32 | cpu | **sparse** | 6,034.4 | 0.659 | 0.848 | 1.561 |
| 100,000 | 128 | int32 | cpu | **filtered** | 399.7 | 8.938 | 13.852 | 26.531 |
| 100,000 | 128 | int32 | cpu | **filteredbool** | 293.0 | 13.460 | 14.260 | 16.464 |
| 100,000 | 128 | int32 | cpu | **filteredstring** | 250.1 | 15.127 | 20.670 | 35.653 |
| 100,000 | 128 | int32 | cpu | **byid** | 4,141.6 | 0.880 | 1.465 | 1.724 |
| 100,000 | 128 | int32 | cpu | **graphrag** | 388.4 | 9.736 | 13.486 | 20.741 |
| 100,000 | 128 | int32 | cpu | **globalgraphrag** | 405.7 | 9.815 | 10.347 | 12.570 |
| 100,000 | 128 | int32 | cpu | **recommend** | 417.6 | 8.977 | 14.280 | 18.779 |
| 100,000 | 128 | int32 | cpu | **geo** | 244.6 | 14.336 | 38.434 | 54.963 |
| 100,000 | 128 | int32 | cpu | **temporal** | 3,727.7 | 0.576 | 0.884 | 1.268 |
| 100,000 | 128 | int32 | cpu | **learnedindex** | 439.5 | 9.030 | 9.424 | 11.779 |
| 100,000 | 128 | int64 | cpu | **dense** | 866.7 | 4.548 | 7.488 | 7.750 |
| 100,000 | 128 | int64 | cpu | **hybrid** | 350.2 | 11.518 | 13.916 | 21.670 |
| 100,000 | 128 | int64 | cpu | **sparse** | 6,577.5 | 0.637 | 0.780 | 0.843 |
| 100,000 | 128 | int64 | cpu | **filtered** | 292.6 | 13.119 | 13.868 | 17.548 |
| 100,000 | 128 | int64 | cpu | **filteredbool** | 182.3 | 21.244 | 24.783 | 38.790 |
| 100,000 | 128 | int64 | cpu | **filteredstring** | 157.0 | 24.944 | 25.538 | 55.505 |
| 100,000 | 128 | int64 | cpu | **byid** | 3,103.2 | 1.123 | 2.344 | 4.626 |
| 100,000 | 128 | int64 | cpu | **graphrag** | 282.2 | 14.049 | 15.534 | 16.855 |
| 100,000 | 128 | int64 | cpu | **globalgraphrag** | 272.0 | 14.069 | 17.248 | 28.056 |
| 100,000 | 128 | int64 | cpu | **recommend** | 307.4 | 12.980 | 13.738 | 15.508 |
| 100,000 | 128 | int64 | cpu | **geo** | 244.4 | 14.307 | 36.761 | 54.908 |
| 100,000 | 128 | int64 | cpu | **temporal** | 2,476.0 | 0.684 | 4.415 | 6.941 |
| 100,000 | 128 | int64 | cpu | **learnedindex** | 303.6 | 13.116 | 13.435 | 13.689 |
| 100,000 | 128 | uint8 | cpu | **dense** | 3,974.0 | 0.949 | 1.524 | 2.242 |
| 100,000 | 128 | uint8 | cpu | **hybrid** | 1,047.9 | 3.702 | 4.888 | 7.575 |
| 100,000 | 128 | uint8 | cpu | **sparse** | 6,670.8 | 0.622 | 0.817 | 0.954 |
| 100,000 | 128 | uint8 | cpu | **filtered** | 2,332.2 | 0.916 | 1.579 | 3.570 |
| 100,000 | 128 | uint8 | cpu | **filteredbool** | 3,196.0 | 1.051 | 1.676 | 2.111 |
| 100,000 | 128 | uint8 | cpu | **filteredstring** | 2,540.5 | 1.232 | 1.921 | 2.478 |
| 100,000 | 128 | uint8 | cpu | **byid** | 4,288.6 | 0.891 | 1.432 | 1.644 |
| 100,000 | 128 | uint8 | cpu | **graphrag** | 2,198.4 | 1.744 | 2.521 | 3.206 |
| 100,000 | 128 | uint8 | cpu | **globalgraphrag** | 1,725.6 | 1.948 | 4.280 | 6.314 |
| 100,000 | 128 | uint8 | cpu | **recommend** | 1,010.6 | 3.688 | 5.383 | 7.421 |
| 100,000 | 128 | uint8 | cpu | **geo** | 226.8 | 14.643 | 44.765 | 58.759 |
| 100,000 | 128 | uint8 | cpu | **temporal** | 3,691.4 | 0.582 | 0.883 | 1.299 |
| 100,000 | 128 | uint8 | cpu | **learnedindex** | 3,539.6 | 1.084 | 1.550 | 1.812 |
| 100,000 | 128 | uint16 | cpu | **dense** | 927.8 | 4.372 | 5.097 | 6.379 |
| 100,000 | 128 | uint16 | cpu | **hybrid** | 846.1 | 4.622 | 5.529 | 7.479 |
| 100,000 | 128 | uint16 | cpu | **sparse** | 6,589.4 | 0.609 | 0.808 | 0.917 |
| 100,000 | 128 | uint16 | cpu | **filtered** | 690.1 | 4.796 | 5.889 | 10.402 |
| 100,000 | 128 | uint16 | cpu | **filteredbool** | 522.8 | 7.353 | 7.907 | 12.698 |
| 100,000 | 128 | uint16 | cpu | **filteredstring** | 337.4 | 11.001 | 14.760 | 27.345 |
| 100,000 | 128 | uint16 | cpu | **byid** | 4,200.0 | 0.885 | 1.442 | 1.633 |
| 100,000 | 128 | uint16 | cpu | **graphrag** | 670.5 | 5.925 | 6.338 | 6.615 |
| 100,000 | 128 | uint16 | cpu | **globalgraphrag** | 609.5 | 5.991 | 10.840 | 13.875 |
| 100,000 | 128 | uint16 | cpu | **recommend** | 869.2 | 4.517 | 5.270 | 6.470 |
| 100,000 | 128 | uint16 | cpu | **geo** | 220.9 | 14.819 | 43.598 | 62.942 |
| 100,000 | 128 | uint16 | cpu | **temporal** | 3,652.5 | 0.609 | 0.931 | 1.565 |
| 100,000 | 128 | uint16 | cpu | **learnedindex** | 802.4 | 4.755 | 6.125 | 7.589 |
| 100,000 | 128 | uint32 | cpu | **dense** | 975.0 | 4.010 | 5.833 | 7.289 |
| 100,000 | 128 | uint32 | cpu | **hybrid** | 485.5 | 8.308 | 10.377 | 12.841 |
| 100,000 | 128 | uint32 | cpu | **sparse** | 6,240.2 | 0.667 | 0.807 | 0.913 |
| 100,000 | 128 | uint32 | cpu | **filtered** | 369.0 | 9.686 | 14.228 | 29.456 |
| 100,000 | 128 | uint32 | cpu | **filteredbool** | 259.1 | 14.674 | 18.324 | 36.532 |
| 100,000 | 128 | uint32 | cpu | **filteredstring** | 241.6 | 16.262 | 16.537 | 20.951 |
| 100,000 | 128 | uint32 | cpu | **byid** | 3,702.7 | 1.026 | 1.628 | 2.079 |
| 100,000 | 128 | uint32 | cpu | **graphrag** | 357.3 | 10.652 | 12.153 | 23.104 |
| 100,000 | 128 | uint32 | cpu | **globalgraphrag** | 359.1 | 10.636 | 15.122 | 23.333 |
| 100,000 | 128 | uint32 | cpu | **recommend** | 428.3 | 9.337 | 9.601 | 12.327 |
| 100,000 | 128 | uint32 | cpu | **geo** | 213.7 | 15.294 | 44.735 | 59.576 |
| 100,000 | 128 | uint32 | cpu | **temporal** | 3,549.3 | 0.601 | 0.874 | 1.554 |
| 100,000 | 128 | uint32 | cpu | **learnedindex** | 389.3 | 9.609 | 13.948 | 22.167 |
| 100,000 | 128 | uint64 | cpu | **dense** | 395.5 | 10.734 | 15.054 | 15.459 |
| 100,000 | 128 | uint64 | cpu | **hybrid** | 258.3 | 15.040 | 18.000 | 28.235 |
| 100,000 | 128 | uint64 | cpu | **sparse** | 6,521.8 | 0.633 | 0.798 | 0.965 |
| 100,000 | 128 | uint64 | cpu | **filtered** | 252.4 | 15.202 | 15.588 | 20.314 |
| 100,000 | 128 | uint64 | cpu | **filteredbool** | 156.7 | 24.777 | 26.777 | 43.821 |
| 100,000 | 128 | uint64 | cpu | **filteredstring** | 135.6 | 28.990 | 32.135 | 34.951 |
| 100,000 | 128 | uint64 | cpu | **byid** | 3,173.2 | 1.177 | 1.814 | 2.138 |
| 100,000 | 128 | uint64 | cpu | **graphrag** | 242.9 | 16.017 | 18.832 | 32.661 |
| 100,000 | 128 | uint64 | cpu | **globalgraphrag** | 246.7 | 16.058 | 16.688 | 18.420 |
| 100,000 | 128 | uint64 | cpu | **recommend** | 257.7 | 15.184 | 17.159 | 26.261 |
| 100,000 | 128 | uint64 | cpu | **geo** | 233.5 | 14.880 | 35.346 | 62.966 |
| 100,000 | 128 | uint64 | cpu | **temporal** | 3,618.9 | 0.601 | 0.885 | 1.395 |
| 100,000 | 128 | uint64 | cpu | **learnedindex** | 263.3 | 15.197 | 15.633 | 17.641 |
| 100,000 | 128 | complex64 | cpu | **dense** | 3,200.3 | 1.140 | 1.865 | 2.558 |
| 100,000 | 128 | complex64 | cpu | **hybrid** | 699.9 | 5.139 | 10.449 | 10.898 |
| 100,000 | 128 | complex64 | cpu | **sparse** | 6,740.8 | 0.618 | 0.772 | 0.805 |
| 100,000 | 128 | complex64 | cpu | **filtered** | 2,169.2 | 1.153 | 1.599 | 11.888 |
| 100,000 | 128 | complex64 | cpu | **filteredbool** | 2,339.9 | 1.414 | 2.162 | 16.605 |
| 100,000 | 128 | complex64 | cpu | **filteredstring** | 1,549.5 | 1.871 | 3.244 | 22.643 |
| 100,000 | 128 | complex64 | cpu | **byid** | 3,321.7 | 1.010 | 1.688 | 3.825 |
| 100,000 | 128 | complex64 | cpu | **graphrag** | 1,534.0 | 2.220 | 4.942 | 7.752 |
| 100,000 | 128 | complex64 | cpu | **globalgraphrag** | 1,790.4 | 2.108 | 3.124 | 4.211 |
| 100,000 | 128 | complex64 | cpu | **recommend** | 411.5 | 9.680 | 10.126 | 12.969 |
| 100,000 | 128 | complex64 | cpu | **geo** | 219.6 | 14.882 | 45.263 | 59.649 |
| 100,000 | 128 | complex64 | cpu | **temporal** | 5,013.2 | 0.665 | 0.961 | 1.539 |
| 100,000 | 128 | complex64 | cpu | **learnedindex** | 2,286.8 | 1.614 | 2.377 | 2.840 |
| 100,000 | 128 | complex128 | cpu | **dense** | 2,335.7 | 1.436 | 3.964 | 4.945 |
| 100,000 | 128 | complex128 | cpu | **hybrid** | 472.0 | 8.379 | 14.107 | 17.109 |
| 100,000 | 128 | complex128 | cpu | **sparse** | 6,482.7 | 0.643 | 0.781 | 0.837 |
| 100,000 | 128 | complex128 | cpu | **filtered** | 879.8 | 1.519 | 14.060 | 17.758 |
| 100,000 | 128 | complex128 | cpu | **filteredbool** | 704.1 | 1.704 | 20.814 | 23.307 |
| 100,000 | 128 | complex128 | cpu | **filteredstring** | 611.7 | 2.465 | 24.040 | 28.078 |
| 100,000 | 128 | complex128 | cpu | **byid** | 1,395.7 | 1.175 | 10.786 | 11.858 |
| 100,000 | 128 | complex128 | cpu | **graphrag** | 870.9 | 2.319 | 14.846 | 16.422 |
| 100,000 | 128 | complex128 | cpu | **globalgraphrag** | 857.5 | 2.324 | 15.036 | 15.853 |
| 100,000 | 128 | complex128 | cpu | **recommend** | 318.5 | 12.574 | 12.795 | 12.887 |
| 100,000 | 128 | complex128 | cpu | **geo** | 236.3 | 14.598 | 39.309 | 59.209 |
| 100,000 | 128 | complex128 | cpu | **temporal** | 5,388.5 | 0.566 | 0.834 | 1.890 |
| 100,000 | 128 | complex128 | cpu | **learnedindex** | 799.0 | 1.871 | 15.150 | 18.219 |
| 100,000 | 128 | turboquant | cpu | **dense** | 300.1 | 13.333 | 13.729 | 14.101 |
| 100,000 | 128 | turboquant | cpu | **hybrid** | 298.0 | 13.447 | 13.849 | 15.302 |
| 100,000 | 128 | turboquant | cpu | **sparse** | 6,203.5 | 0.676 | 0.830 | 0.886 |
| 100,000 | 128 | turboquant | cpu | **filtered** | 287.6 | 13.399 | 13.946 | 19.999 |
| 100,000 | 128 | turboquant | cpu | **filteredbool** | 292.8 | 13.495 | 14.007 | 16.192 |
| 100,000 | 128 | turboquant | cpu | **filteredstring** | 290.3 | 13.463 | 13.942 | 16.134 |
| 100,000 | 128 | turboquant | cpu | **byid** | 299.6 | 13.330 | 13.773 | 16.203 |
| 100,000 | 128 | turboquant | cpu | **graphrag** | 252.6 | 14.766 | 18.755 | 28.064 |
| 100,000 | 128 | turboquant | cpu | **globalgraphrag** | 276.0 | 14.529 | 14.953 | 15.485 |
| 100,000 | 128 | turboquant | cpu | **recommend** | 203.7 | 19.624 | 20.078 | 22.202 |
| 100,000 | 128 | turboquant | cpu | **geo** | 226.2 | 14.698 | 44.715 | 55.422 |
| 100,000 | 128 | turboquant | cpu | **temporal** | 5,534.2 | 0.593 | 0.898 | 2.368 |
| 100,000 | 128 | turboquant | cpu | **learnedindex** | 271.5 | 14.057 | 17.162 | 29.883 |
| 100,000 | 128 | turboquant4 | cpu | **dense** | 413.5 | 9.583 | 10.091 | 12.796 |
| 100,000 | 128 | turboquant4 | cpu | **hybrid** | 405.9 | 9.865 | 10.184 | 10.347 |
| 100,000 | 128 | turboquant4 | cpu | **sparse** | 6,340.8 | 0.647 | 0.812 | 0.897 |
| 100,000 | 128 | turboquant4 | cpu | **filtered** | 366.9 | 9.831 | 13.984 | 26.518 |
| 100,000 | 128 | turboquant4 | cpu | **filteredbool** | 401.4 | 9.765 | 10.146 | 13.753 |
| 100,000 | 128 | turboquant4 | cpu | **filteredstring** | 395.5 | 9.738 | 10.372 | 13.591 |
| 100,000 | 128 | turboquant4 | cpu | **byid** | 415.2 | 9.600 | 10.208 | 11.746 |
| 100,000 | 128 | turboquant4 | cpu | **graphrag** | 376.4 | 10.597 | 11.259 | 12.335 |
| 100,000 | 128 | turboquant4 | cpu | **globalgraphrag** | 376.5 | 10.597 | 11.163 | 13.189 |
| 100,000 | 128 | turboquant4 | cpu | **recommend** | 270.8 | 14.591 | 15.385 | 17.971 |
| 100,000 | 128 | turboquant4 | cpu | **geo** | 221.2 | 14.762 | 44.427 | 51.941 |
| 100,000 | 128 | turboquant4 | cpu | **temporal** | 3,434.5 | 0.675 | 3.903 | 6.436 |
| 100,000 | 128 | turboquant4 | cpu | **learnedindex** | 399.6 | 10.025 | 10.455 | 11.053 |
| 100,000 | 128 | turboquant8 | cpu | **dense** | 331.6 | 11.794 | 13.261 | 16.099 |
| 100,000 | 128 | turboquant8 | cpu | **hybrid** | 329.4 | 12.156 | 12.490 | 12.789 |
| 100,000 | 128 | turboquant8 | cpu | **sparse** | 6,329.4 | 0.639 | 0.826 | 0.890 |
| 100,000 | 128 | turboquant8 | cpu | **filtered** | 315.5 | 12.066 | 12.646 | 16.106 |
| 100,000 | 128 | turboquant8 | cpu | **filteredbool** | 322.9 | 12.128 | 12.641 | 17.985 |
| 100,000 | 128 | turboquant8 | cpu | **filteredstring** | 322.7 | 12.124 | 12.546 | 13.504 |
| 100,000 | 128 | turboquant8 | cpu | **byid** | 331.3 | 11.927 | 12.625 | 15.877 |
| 100,000 | 128 | turboquant8 | cpu | **graphrag** | 295.5 | 12.986 | 16.066 | 25.125 |
| 100,000 | 128 | turboquant8 | cpu | **globalgraphrag** | 306.0 | 13.082 | 13.602 | 13.891 |
| 100,000 | 128 | turboquant8 | cpu | **recommend** | 195.1 | 17.561 | 34.121 | 36.580 |
| 100,000 | 128 | turboquant8 | cpu | **geo** | 223.4 | 14.870 | 43.814 | 55.102 |
| 100,000 | 128 | turboquant8 | cpu | **temporal** | 5,580.7 | 0.590 | 0.919 | 1.346 |
| 100,000 | 128 | turboquant8 | cpu | **learnedindex** | 326.1 | 12.212 | 12.768 | 15.298 |
| 100,000 | 128 | float32 | cuda | **dense** | 3,589.9 | 1.024 | 1.701 | 2.492 |
| 100,000 | 128 | float32 | cuda | **hybrid** | 782.0 | 5.179 | 9.436 | 11.136 |
| 100,000 | 128 | float32 | cuda | **sparse** | 6,294.7 | 0.629 | 0.889 | 1.525 |
| 100,000 | 128 | float32 | cuda | **filtered** | 2,405.1 | 1.065 | 1.711 | 2.191 |
| 100,000 | 128 | float32 | cuda | **filteredbool** | 2,655.4 | 1.237 | 1.960 | 7.258 |
| 100,000 | 128 | float32 | cuda | **filteredstring** | 1,853.8 | 1.688 | 2.792 | 11.048 |
| 100,000 | 128 | float32 | cuda | **byid** | 3,557.0 | 0.982 | 1.621 | 3.215 |
| 100,000 | 128 | float32 | cuda | **graphrag** | 1,869.7 | 1.845 | 3.820 | 5.752 |
| 100,000 | 128 | float32 | cuda | **globalgraphrag** | 2,100.9 | 1.776 | 2.708 | 3.346 |
| 100,000 | 128 | float32 | cuda | **recommend** | 929.3 | 4.193 | 5.339 | 6.409 |
| 100,000 | 128 | float32 | cuda | **geo** | 214.9 | 14.814 | 46.476 | 59.606 |
| 100,000 | 128 | float32 | cuda | **temporal** | 5,693.3 | 0.578 | 0.981 | 1.596 |
| 100,000 | 128 | float32 | cuda | **learnedindex** | 3,095.0 | 1.190 | 1.874 | 2.617 |
| 100,000 | 128 | float64 | cuda | **dense** | 506.6 | 8.034 | 11.713 | 14.366 |
| 100,000 | 128 | float64 | cuda | **hybrid** | 463.1 | 9.505 | 11.863 | 21.344 |
| 100,000 | 128 | float64 | cuda | **sparse** | 6,427.2 | 0.650 | 0.779 | 0.851 |
| 100,000 | 128 | float64 | cuda | **filtered** | 496.8 | 8.114 | 11.531 | 15.947 |
| 100,000 | 128 | float64 | cuda | **filteredbool** | 392.6 | 10.645 | 16.775 | 29.149 |
| 100,000 | 128 | float64 | cuda | **filteredstring** | 261.6 | 15.445 | 21.425 | 27.421 |
| 100,000 | 128 | float64 | cuda | **byid** | 460.5 | 9.640 | 16.821 | 19.052 |
| 100,000 | 128 | float64 | cuda | **graphrag** | 317.2 | 12.247 | 22.331 | 33.626 |
| 100,000 | 128 | float64 | cuda | **globalgraphrag** | 392.0 | 10.100 | 18.564 | 24.776 |
| 100,000 | 128 | float64 | cuda | **recommend** | 343.7 | 11.324 | 13.279 | 18.244 |
| 100,000 | 128 | float64 | cuda | **geo** | 193.4 | 17.929 | 45.865 | 61.788 |
| 100,000 | 128 | float64 | cuda | **temporal** | 5,871.6 | 0.574 | 0.839 | 1.270 |
| 100,000 | 128 | float64 | cuda | **learnedindex** | 480.0 | 8.127 | 14.364 | 22.973 |
| 100,000 | 128 | float16 | cuda | **dense** | 1,146.9 | 2.730 | 6.810 | 7.622 |
| 100,000 | 128 | float16 | cuda | **hybrid** | 820.1 | 5.144 | 6.920 | 7.612 |
| 100,000 | 128 | float16 | cuda | **sparse** | 6,693.5 | 0.627 | 0.802 | 0.877 |
| 100,000 | 128 | float16 | cuda | **filtered** | 745.2 | 5.546 | 10.249 | 18.368 |
| 100,000 | 128 | float16 | cuda | **filteredbool** | 676.1 | 7.920 | 10.691 | 13.496 |
| 100,000 | 128 | float16 | cuda | **filteredstring** | 776.9 | 3.601 | 11.390 | 16.851 |
| 100,000 | 128 | float16 | cuda | **byid** | 4,164.7 | 0.909 | 1.444 | 1.733 |
| 100,000 | 128 | float16 | cuda | **graphrag** | 797.7 | 4.875 | 9.324 | 12.031 |
| 100,000 | 128 | float16 | cuda | **globalgraphrag** | 1,159.2 | 2.424 | 7.681 | 8.448 |
| 100,000 | 128 | float16 | cuda | **recommend** | 562.6 | 6.540 | 10.996 | 15.908 |
| 100,000 | 128 | float16 | cuda | **geo** | 232.8 | 14.028 | 42.895 | 53.370 |
| 100,000 | 128 | float16 | cuda | **temporal** | 3,673.4 | 0.585 | 0.836 | 1.609 |
| 100,000 | 128 | float16 | cuda | **learnedindex** | 949.3 | 4.128 | 7.584 | 9.479 |
| 100,000 | 128 | int8 | cuda | **dense** | 4,277.5 | 0.863 | 1.458 | 1.795 |
| 100,000 | 128 | int8 | cuda | **hybrid** | 1,173.9 | 3.013 | 5.694 | 8.211 |
| 100,000 | 128 | int8 | cuda | **sparse** | 5,563.9 | 0.655 | 1.205 | 3.437 |
| 100,000 | 128 | int8 | cuda | **filtered** | 2,651.3 | 0.921 | 1.554 | 1.973 |
| 100,000 | 128 | int8 | cuda | **filteredbool** | 3,400.3 | 0.972 | 1.549 | 2.558 |
| 100,000 | 128 | int8 | cuda | **filteredstring** | 2,604.4 | 1.132 | 1.806 | 3.170 |
| 100,000 | 128 | int8 | cuda | **byid** | 4,458.9 | 0.815 | 1.379 | 1.589 |
| 100,000 | 128 | int8 | cuda | **graphrag** | 2,547.7 | 1.498 | 2.206 | 2.501 |
| 100,000 | 128 | int8 | cuda | **globalgraphrag** | 1,864.3 | 1.684 | 5.045 | 7.482 |
| 100,000 | 128 | int8 | cuda | **recommend** | 1,185.3 | 3.205 | 4.322 | 5.389 |
| 100,000 | 128 | int8 | cuda | **geo** | 239.0 | 13.591 | 42.387 | 53.025 |
| 100,000 | 128 | int8 | cuda | **temporal** | 3,857.0 | 0.582 | 0.832 | 1.511 |
| 100,000 | 128 | int8 | cuda | **learnedindex** | 3,829.9 | 0.976 | 1.515 | 1.752 |
| 100,000 | 128 | int16 | cuda | **dense** | 1,319.7 | 2.809 | 4.453 | 5.096 |
| 100,000 | 128 | int16 | cuda | **hybrid** | 987.1 | 3.616 | 7.321 | 9.030 |
| 100,000 | 128 | int16 | cuda | **sparse** | 6,319.6 | 0.665 | 0.797 | 1.006 |
| 100,000 | 128 | int16 | cuda | **filtered** | 848.6 | 4.030 | 5.125 | 7.583 |
| 100,000 | 128 | int16 | cuda | **filteredbool** | 566.1 | 6.274 | 10.605 | 18.037 |
| 100,000 | 128 | int16 | cuda | **filteredstring** | 2,311.8 | 1.268 | 2.073 | 2.970 |
| 100,000 | 128 | int16 | cuda | **byid** | 4,257.8 | 0.878 | 1.444 | 1.697 |
| 100,000 | 128 | int16 | cuda | **graphrag** | 774.8 | 5.129 | 5.584 | 6.669 |
| 100,000 | 128 | int16 | cuda | **globalgraphrag** | 723.3 | 5.148 | 8.271 | 13.883 |
| 100,000 | 128 | int16 | cuda | **recommend** | 1,014.4 | 3.787 | 4.604 | 6.642 |
| 100,000 | 128 | int16 | cuda | **geo** | 231.6 | 14.060 | 42.427 | 56.026 |
| 100,000 | 128 | int16 | cuda | **temporal** | 3,860.9 | 0.557 | 0.838 | 1.573 |
| 100,000 | 128 | int16 | cuda | **learnedindex** | 972.1 | 3.981 | 4.995 | 6.822 |
| 100,000 | 128 | int32 | cuda | **dense** | 2,889.1 | 1.075 | 4.504 | 5.411 |
| 100,000 | 128 | int32 | cuda | **hybrid** | 1,070.4 | 3.645 | 5.221 | 7.158 |
| 100,000 | 128 | int32 | cuda | **sparse** | 6,733.4 | 0.620 | 0.775 | 0.880 |
| 100,000 | 128 | int32 | cuda | **filtered** | 2,744.0 | 0.937 | 1.539 | 1.918 |
| 100,000 | 128 | int32 | cuda | **filteredbool** | 3,175.0 | 1.061 | 1.701 | 2.167 |
| 100,000 | 128 | int32 | cuda | **filteredstring** | 272.3 | 13.842 | 20.807 | 32.787 |
| 100,000 | 128 | int32 | cuda | **byid** | 4,217.7 | 0.908 | 1.392 | 1.794 |
| 100,000 | 128 | int32 | cuda | **graphrag** | 2,381.9 | 1.595 | 2.320 | 2.815 |
| 100,000 | 128 | int32 | cuda | **globalgraphrag** | 2,374.0 | 1.621 | 2.295 | 2.862 |
| 100,000 | 128 | int32 | cuda | **recommend** | 462.0 | 8.257 | 10.854 | 18.087 |
| 100,000 | 128 | int32 | cuda | **geo** | 254.3 | 13.892 | 38.303 | 49.367 |
| 100,000 | 128 | int32 | cuda | **temporal** | 3,773.3 | 0.577 | 0.837 | 1.466 |
| 100,000 | 128 | int32 | cuda | **learnedindex** | 3,614.3 | 1.080 | 1.535 | 1.803 |
| 100,000 | 128 | int64 | cuda | **dense** | 343.5 | 11.607 | 12.001 | 13.901 |
| 100,000 | 128 | int64 | cuda | **hybrid** | 328.1 | 11.708 | 14.095 | 22.549 |
| 100,000 | 128 | int64 | cuda | **sparse** | 6,519.1 | 0.635 | 0.779 | 0.821 |
| 100,000 | 128 | int64 | cuda | **filtered** | 326.2 | 11.780 | 12.004 | 12.133 |
| 100,000 | 128 | int64 | cuda | **filteredbool** | 2,898.4 | 1.162 | 1.688 | 2.324 |
| 100,000 | 128 | int64 | cuda | **filteredstring** | 169.4 | 22.900 | 25.530 | 52.909 |
| 100,000 | 128 | int64 | cuda | **byid** | 3,837.5 | 0.980 | 1.426 | 1.715 |
| 100,000 | 128 | int64 | cuda | **graphrag** | 303.6 | 12.817 | 13.808 | 26.991 |
| 100,000 | 128 | int64 | cuda | **globalgraphrag** | 312.3 | 12.825 | 13.072 | 13.192 |
| 100,000 | 128 | int64 | cuda | **recommend** | 316.3 | 12.134 | 14.601 | 26.583 |
| 100,000 | 128 | int64 | cuda | **geo** | 254.9 | 13.924 | 35.904 | 53.148 |
| 100,000 | 128 | int64 | cuda | **temporal** | 3,850.0 | 0.570 | 0.821 | 1.212 |
| 100,000 | 128 | int64 | cuda | **learnedindex** | 333.9 | 11.908 | 12.340 | 14.736 |
| 100,000 | 128 | uint8 | cuda | **dense** | 3,023.9 | 1.012 | 4.096 | 5.261 |
| 100,000 | 128 | uint8 | cuda | **hybrid** | 1,319.4 | 2.875 | 4.272 | 5.292 |
| 100,000 | 128 | uint8 | cuda | **sparse** | 6,465.1 | 0.614 | 0.838 | 1.273 |
| 100,000 | 128 | uint8 | cuda | **filtered** | 2,811.7 | 0.955 | 1.596 | 2.006 |
| 100,000 | 128 | uint8 | cuda | **filteredbool** | 2,581.5 | 1.071 | 4.325 | 7.757 |
| 100,000 | 128 | uint8 | cuda | **filteredstring** | 2,747.3 | 1.111 | 1.704 | 2.158 |
| 100,000 | 128 | uint8 | cuda | **byid** | 4,257.1 | 0.887 | 1.456 | 1.758 |
| 100,000 | 128 | uint8 | cuda | **graphrag** | 2,411.0 | 1.558 | 2.323 | 2.803 |
| 100,000 | 128 | uint8 | cuda | **globalgraphrag** | 2,202.5 | 1.624 | 2.464 | 4.565 |
| 100,000 | 128 | uint8 | cuda | **recommend** | 1,075.4 | 3.319 | 5.601 | 10.794 |
| 100,000 | 128 | uint8 | cuda | **geo** | 237.6 | 13.958 | 42.714 | 53.567 |
| 100,000 | 128 | uint8 | cuda | **temporal** | 3,902.9 | 0.576 | 0.845 | 1.347 |
| 100,000 | 128 | uint8 | cuda | **learnedindex** | 3,668.9 | 1.042 | 1.533 | 1.795 |
| 100,000 | 128 | uint16 | cuda | **dense** | 3,934.0 | 0.965 | 1.547 | 1.984 |
| 100,000 | 128 | uint16 | cuda | **hybrid** | 885.6 | 3.989 | 7.562 | 10.721 |
| 100,000 | 128 | uint16 | cuda | **sparse** | 6,535.9 | 0.642 | 0.774 | 0.837 |
| 100,000 | 128 | uint16 | cuda | **filtered** | 2,454.4 | 0.939 | 1.640 | 2.630 |
| 100,000 | 128 | uint16 | cuda | **filteredbool** | 3,003.2 | 1.101 | 1.697 | 2.104 |
| 100,000 | 128 | uint16 | cuda | **filteredstring** | 401.2 | 9.624 | 10.011 | 13.308 |
| 100,000 | 128 | uint16 | cuda | **byid** | 4,197.7 | 0.898 | 1.464 | 1.646 |
| 100,000 | 128 | uint16 | cuda | **graphrag** | 1,857.4 | 1.751 | 4.630 | 8.843 |
| 100,000 | 128 | uint16 | cuda | **globalgraphrag** | 2,407.9 | 1.592 | 2.221 | 2.654 |
| 100,000 | 128 | uint16 | cuda | **recommend** | 997.4 | 3.836 | 5.189 | 6.302 |
| 100,000 | 128 | uint16 | cuda | **geo** | 235.5 | 13.839 | 41.899 | 52.236 |
| 100,000 | 128 | uint16 | cuda | **temporal** | 3,855.3 | 0.585 | 0.848 | 1.046 |
| 100,000 | 128 | uint16 | cuda | **learnedindex** | 3,697.5 | 1.034 | 1.488 | 1.823 |
| 100,000 | 128 | uint32 | cuda | **dense** | 1,457.0 | 2.237 | 5.152 | 8.671 |
| 100,000 | 128 | uint32 | cuda | **hybrid** | 1,276.3 | 3.091 | 3.929 | 5.227 |
| 100,000 | 128 | uint32 | cuda | **sparse** | 6,671.5 | 0.619 | 0.773 | 0.843 |
| 100,000 | 128 | uint32 | cuda | **filtered** | 809.1 | 3.911 | 8.606 | 13.762 |
| 100,000 | 128 | uint32 | cuda | **filteredbool** | 512.5 | 7.750 | 8.334 | 9.156 |
| 100,000 | 128 | uint32 | cuda | **filteredstring** | 283.9 | 13.492 | 16.252 | 23.585 |
| 100,000 | 128 | uint32 | cuda | **byid** | 3,093.8 | 1.055 | 3.704 | 5.315 |
| 100,000 | 128 | uint32 | cuda | **graphrag** | 482.9 | 7.804 | 12.008 | 15.981 |
| 100,000 | 128 | uint32 | cuda | **globalgraphrag** | 413.4 | 9.069 | 14.278 | 19.773 |
| 100,000 | 128 | uint32 | cuda | **recommend** | 471.0 | 8.456 | 9.035 | 9.222 |
| 100,000 | 128 | uint32 | cuda | **geo** | 230.2 | 14.251 | 42.478 | 55.245 |
| 100,000 | 128 | uint32 | cuda | **temporal** | 3,821.5 | 0.575 | 0.851 | 1.604 |
| 100,000 | 128 | uint32 | cuda | **learnedindex** | 464.0 | 8.592 | 8.988 | 9.500 |
| 100,000 | 128 | uint64 | cuda | **dense** | 713.0 | 5.572 | 9.723 | 10.436 |
| 100,000 | 128 | uint64 | cuda | **hybrid** | 230.7 | 17.494 | 22.537 | 24.050 |
| 100,000 | 128 | uint64 | cuda | **sparse** | 5,196.9 | 0.788 | 0.963 | 1.105 |
| 100,000 | 128 | uint64 | cuda | **filtered** | 208.3 | 17.999 | 20.098 | 42.541 |
| 100,000 | 128 | uint64 | cuda | **filteredbool** | 140.2 | 28.718 | 30.933 | 32.162 |
| 100,000 | 128 | uint64 | cuda | **filteredstring** | 126.0 | 29.849 | 38.911 | 57.260 |
| 100,000 | 128 | uint64 | cuda | **byid** | 2,351.5 | 1.633 | 2.391 | 2.813 |
| 100,000 | 128 | uint64 | cuda | **graphrag** | 241.8 | 15.943 | 19.559 | 29.990 |
| 100,000 | 128 | uint64 | cuda | **globalgraphrag** | 250.1 | 15.991 | 16.320 | 16.493 |
| 100,000 | 128 | uint64 | cuda | **recommend** | 240.4 | 15.173 | 24.502 | 40.603 |
| 100,000 | 128 | uint64 | cuda | **geo** | 225.0 | 15.127 | 39.845 | 62.346 |
| 100,000 | 128 | uint64 | cuda | **temporal** | 3,059.4 | 0.693 | 1.047 | 2.715 |
| 100,000 | 128 | uint64 | cuda | **learnedindex** | 231.2 | 17.647 | 18.361 | 19.160 |
| 100,000 | 128 | complex64 | cuda | **dense** | 511.2 | 9.321 | 11.161 | 13.565 |
| 100,000 | 128 | complex64 | cuda | **hybrid** | 500.7 | 9.469 | 10.718 | 11.591 |
| 100,000 | 128 | complex64 | cuda | **filtered** | 533.1 | 7.864 | 14.747 | 24.418 |
| 100,000 | 128 | complex64 | cuda | **byid** | 661.5 | 5.510 | 10.707 | 12.418 |
| 100,000 | 128 | complex64 | cuda | **graphrag** | 522.5 | 9.013 | 11.707 | 13.122 |
| 100,000 | 128 | complex64 | cuda | **learnedindex** | 552.1 | 7.214 | 13.021 | 20.086 |
| 100,000 | 128 | complex128 | cuda | **dense** | 378.1 | 8.316 | 20.410 | 31.238 |
| 100,000 | 128 | complex128 | cuda | **hybrid** | 326.9 | 12.458 | 19.348 | 27.336 |
| 100,000 | 128 | complex128 | cuda | **filtered** | 392.9 | 9.957 | 15.297 | 18.555 |
| 100,000 | 128 | complex128 | cuda | **byid** | 345.8 | 11.202 | 18.235 | 19.703 |
| 100,000 | 128 | complex128 | cuda | **graphrag** | 371.1 | 11.688 | 17.601 | 19.810 |
| 100,000 | 128 | complex128 | cuda | **learnedindex** | 360.6 | 12.477 | 16.797 | 25.968 |
| 100,000 | 128 | turboquant | cuda | **dense** | 365.9 | 10.895 | 11.470 | 12.754 |
| 100,000 | 128 | turboquant | cuda | **hybrid** | 362.0 | 11.032 | 11.512 | 14.761 |
| 100,000 | 128 | turboquant | cuda | **sparse** | 6,327.4 | 0.634 | 0.857 | 0.935 |
| 100,000 | 128 | turboquant | cuda | **filtered** | 345.7 | 11.042 | 11.439 | 14.717 |
| 100,000 | 128 | turboquant | cuda | **filteredbool** | 353.2 | 11.139 | 11.563 | 14.234 |
| 100,000 | 128 | turboquant | cuda | **filteredstring** | 353.5 | 11.021 | 11.501 | 16.235 |
| 100,000 | 128 | turboquant | cuda | **byid** | 374.4 | 10.641 | 11.153 | 13.387 |
| 100,000 | 128 | turboquant | cuda | **graphrag** | 341.2 | 11.706 | 12.480 | 14.292 |
| 100,000 | 128 | turboquant | cuda | **globalgraphrag** | 323.9 | 11.835 | 14.622 | 24.691 |
| 100,000 | 128 | turboquant | cuda | **recommend** | 254.1 | 15.526 | 16.737 | 19.375 |
| 100,000 | 128 | turboquant | cuda | **geo** | 226.4 | 14.645 | 43.792 | 56.386 |
| 100,000 | 128 | turboquant | cuda | **temporal** | 5,743.1 | 0.573 | 0.898 | 1.170 |
| 100,000 | 128 | turboquant | cuda | **learnedindex** | 363.4 | 10.993 | 11.458 | 14.289 |
| 100,000 | 128 | turboquant4 | cuda | **dense** | 495.2 | 7.892 | 9.019 | 11.284 |
| 100,000 | 128 | turboquant4 | cuda | **hybrid** | 483.7 | 8.055 | 9.005 | 9.819 |
| 100,000 | 128 | turboquant4 | cuda | **sparse** | 6,097.4 | 0.651 | 1.011 | 1.415 |
| 100,000 | 128 | turboquant4 | cuda | **filtered** | 456.5 | 7.989 | 9.708 | 13.256 |
| 100,000 | 128 | turboquant4 | cuda | **filteredbool** | 478.7 | 7.991 | 9.320 | 14.798 |
| 100,000 | 128 | turboquant4 | cuda | **filteredstring** | 471.2 | 8.004 | 9.034 | 13.541 |
| 100,000 | 128 | turboquant4 | cuda | **byid** | 486.8 | 7.904 | 9.604 | 12.120 |
| 100,000 | 128 | turboquant4 | cuda | **graphrag** | 447.9 | 8.709 | 9.642 | 12.625 |
| 100,000 | 128 | turboquant4 | cuda | **globalgraphrag** | 429.8 | 8.774 | 11.981 | 20.525 |
| 100,000 | 128 | turboquant4 | cuda | **recommend** | 375.9 | 10.427 | 11.535 | 12.961 |
| 100,000 | 128 | turboquant4 | cuda | **geo** | 220.0 | 14.872 | 45.526 | 64.983 |
| 100,000 | 128 | turboquant4 | cuda | **temporal** | 5,503.5 | 0.590 | 0.966 | 1.453 |
| 100,000 | 128 | turboquant4 | cuda | **learnedindex** | 472.1 | 8.192 | 9.417 | 10.962 |
| 100,000 | 128 | turboquant8 | cuda | **dense** | 384.0 | 10.260 | 11.349 | 13.708 |
| 100,000 | 128 | turboquant8 | cuda | **hybrid** | 383.7 | 10.353 | 11.035 | 13.670 |
| 100,000 | 128 | turboquant8 | cuda | **sparse** | 6,168.9 | 0.655 | 0.845 | 1.230 |
| 100,000 | 128 | turboquant8 | cuda | **filtered** | 356.3 | 10.361 | 12.507 | 14.182 |
| 100,000 | 128 | turboquant8 | cuda | **filteredbool** | 376.4 | 10.279 | 11.272 | 15.865 |
| 100,000 | 128 | turboquant8 | cuda | **filteredstring** | 370.3 | 10.404 | 11.609 | 14.510 |
| 100,000 | 128 | turboquant8 | cuda | **byid** | 390.0 | 10.158 | 11.213 | 13.292 |
| 100,000 | 128 | turboquant8 | cuda | **graphrag** | 346.2 | 11.249 | 13.628 | 22.836 |
| 100,000 | 128 | turboquant8 | cuda | **globalgraphrag** | 345.7 | 11.301 | 12.224 | 20.110 |
| 100,000 | 128 | turboquant8 | cuda | **recommend** | 391.9 | 10.187 | 10.929 | 11.652 |
| 100,000 | 128 | turboquant8 | cuda | **geo** | 217.6 | 14.995 | 43.214 | 58.599 |
| 100,000 | 128 | turboquant8 | cuda | **temporal** | 5,574.5 | 0.589 | 0.930 | 1.479 |
| 100,000 | 128 | turboquant8 | cuda | **learnedindex** | 379.8 | 10.476 | 11.408 | 12.596 |
| 250,000 | 128 | complex128 | cpu | **dense** | 292.0 | 13.360 | 16.373 | 27.176 |
| 250,000 | 128 | complex128 | cpu | **hybrid** | 202.4 | 15.375 | 29.760 | 29.998 |
| 250,000 | 128 | complex128 | cpu | **filtered** | 113.7 | 11.356 | 149.396 | 164.516 |
| 250,000 | 128 | complex128 | cpu | **filteredbool** | 150.3 | 18.501 | 88.689 | 89.309 |
| 250,000 | 128 | complex128 | cpu | **filteredstring** | 165.3 | 22.306 | 59.217 | 60.255 |
| 250,000 | 128 | complex128 | cpu | **sparse** | 4,079.9 | 0.951 | 1.121 | 1.130 |
| 250,000 | 128 | complex128 | cpu | **byid** | 2,865.5 | 1.250 | 1.566 | 1.575 |
| 250,000 | 128 | complex128 | cpu | **graphrag** | 511.1 | 8.630 | 10.634 | 10.995 |
| 250,000 | 128 | complex128 | cpu | **globalgraphrag** | 435.2 | 8.719 | 14.583 | 17.151 |
| 250,000 | 128 | complex128 | cpu | **recommend** | 342.7 | 10.773 | 10.880 | 10.884 |
| 250,000 | 128 | complex128 | cpu | **geo** | 52.5 | 74.199 | 79.853 | 90.118 |
| 250,000 | 128 | complex128 | cpu | **temporal** | 479.0 | 7.646 | 8.193 | 8.320 |
| 250,000 | 128 | complex128 | cpu | **learnedindex** | 339.8 | 10.668 | 12.236 | 13.177 |
| 250,000 | 128 | complex128 | cuda | **dense** | 229.1 | 26.147 | 42.390 | 54.406 |
| 250,000 | 128 | complex128 | cuda | **hybrid** | 333.3 | 17.386 | 38.177 | 44.576 |
| 250,000 | 128 | complex128 | cuda | **sparse** | 2,863.0 | 2.520 | 4.146 | 6.965 |
| 250,000 | 128 | complex128 | cuda | **filtered** | 67.2 | 21.285 | 612.980 | 616.494 |
| 250,000 | 128 | complex128 | cuda | **byid** | 2,014.8 | 3.319 | 6.182 | 6.921 |
| 250,000 | 128 | complex128 | cuda | **graphrag** | 364.0 | 17.849 | 33.121 | 34.138 |
| 250,000 | 128 | complex128 | cuda | **geo** | 62.5 | 88.638 | 271.394 | 297.748 |
| 250,000 | 128 | complex128 | cuda | **temporal** | 404.2 | 11.632 | 26.371 | 38.502 |
| 250,000 | 128 | complex128 | cuda | **learnedindex** | 400.8 | 13.717 | 25.801 | 31.795 |
| 250,000 | 128 | complex64 | cpu | **dense** | 396.2 | 9.538 | 25.227 | 28.892 |
| 250,000 | 128 | complex64 | cpu | **hybrid** | 331.4 | 15.627 | 36.934 | 38.478 |
| 250,000 | 128 | complex64 | cpu | **sparse** | 2,957.0 | 1.967 | 4.057 | 4.385 |
| 250,000 | 128 | complex64 | cpu | **filtered** | 65.7 | 24.717 | 621.004 | 631.020 |
| 250,000 | 128 | complex64 | cpu | **byid** | 303.9 | 16.839 | 37.839 | 38.470 |
| 250,000 | 128 | complex64 | cpu | **graphrag** | 321.3 | 15.401 | 37.070 | 52.045 |
| 250,000 | 128 | complex64 | cpu | **geo** | 82.6 | 87.230 | 108.636 | 132.259 |
| 250,000 | 128 | complex64 | cpu | **temporal** | 447.7 | 11.635 | 26.656 | 30.172 |
| 250,000 | 128 | complex64 | cpu | **learnedindex** | 468.2 | 10.590 | 36.754 | 50.902 |
| 250,000 | 128 | complex64 | cuda | **dense** | 568.0 | 7.693 | 21.084 | 25.165 |
| 250,000 | 128 | complex64 | cuda | **hybrid** | 372.2 | 16.266 | 33.065 | 35.466 |
| 250,000 | 128 | complex64 | cuda | **sparse** | 2,699.9 | 2.217 | 4.843 | 5.934 |
| 250,000 | 128 | complex64 | cuda | **filtered** | 63.2 | 28.254 | 622.556 | 631.473 |
| 250,000 | 128 | complex64 | cuda | **byid** | 329.1 | 17.206 | 42.208 | 43.653 |
| 250,000 | 128 | complex64 | cuda | **graphrag** | 240.3 | 24.212 | 45.513 | 48.383 |
| 250,000 | 128 | complex64 | cuda | **geo** | 77.0 | 86.907 | 115.563 | 122.995 |
| 250,000 | 128 | complex64 | cuda | **temporal** | 480.3 | 12.703 | 27.711 | 33.744 |
| 250,000 | 128 | complex64 | cuda | **learnedindex** | 286.6 | 20.642 | 39.620 | 44.870 |
| 250,000 | 128 | float16 | cpu | **dense** | 722.0 | 6.439 | 21.775 | 25.263 |
| 250,000 | 128 | float16 | cpu | **hybrid** | 345.4 | 17.326 | 39.682 | 45.610 |
| 250,000 | 128 | float16 | cpu | **sparse** | 2,610.4 | 2.720 | 4.513 | 4.810 |
| 250,000 | 128 | float16 | cpu | **filtered** | 71.4 | 11.027 | 637.152 | 650.754 |
| 250,000 | 128 | float16 | cpu | **byid** | 2,021.2 | 3.276 | 5.932 | 8.586 |
| 250,000 | 128 | float16 | cpu | **graphrag** | 545.7 | 8.203 | 23.003 | 23.870 |
| 250,000 | 128 | float16 | cpu | **geo** | 61.8 | 93.278 | 192.864 | 257.563 |
| 250,000 | 128 | float16 | cpu | **temporal** | 390.5 | 17.214 | 34.288 | 37.845 |
| 250,000 | 128 | float16 | cpu | **learnedindex** | 485.4 | 9.434 | 25.720 | 40.869 |
| 250,000 | 128 | float16 | cuda | **dense** | 631.1 | 9.439 | 19.464 | 20.893 |
| 250,000 | 128 | float16 | cuda | **hybrid** | 662.4 | 6.978 | 18.410 | 23.118 |
| 250,000 | 128 | float16 | cuda | **sparse** | 2,507.3 | 2.769 | 4.908 | 5.232 |
| 250,000 | 128 | float16 | cuda | **filtered** | 64.8 | 13.965 | 656.665 | 686.299 |
| 250,000 | 128 | float16 | cuda | **byid** | 1,941.3 | 3.552 | 5.389 | 6.896 |
| 250,000 | 128 | float16 | cuda | **graphrag** | 274.6 | 23.434 | 54.349 | 59.522 |
| 250,000 | 128 | float16 | cuda | **geo** | 73.4 | 93.639 | 125.904 | 137.078 |
| 250,000 | 128 | float16 | cuda | **temporal** | 317.7 | 16.509 | 31.476 | 36.628 |
| 250,000 | 128 | float16 | cuda | **learnedindex** | 290.8 | 12.624 | 32.947 | 35.811 |
| 250,000 | 128 | float32 | cpu | **dense** | 1,214.2 | 1.942 | 6.164 | 7.992 |
| 250,000 | 128 | float32 | cpu | **hybrid** | 379.1 | 11.059 | 12.086 | 13.567 |
| 250,000 | 128 | float32 | cpu | **filtered** | 169.2 | 1.452 | 129.096 | 139.631 |
| 250,000 | 128 | float32 | cpu | **filteredbool** | 146.8 | 17.364 | 82.187 | 83.413 |
| 250,000 | 128 | float32 | cpu | **filteredstring** | 159.8 | 20.890 | 55.969 | 55.979 |
| 250,000 | 128 | float32 | cpu | **sparse** | 4,519.2 | 0.858 | 0.981 | 1.012 |
| 250,000 | 128 | float32 | cpu | **byid** | 2,872.4 | 1.294 | 1.639 | 1.659 |
| 250,000 | 128 | float32 | cpu | **graphrag** | 859.1 | 1.681 | 12.330 | 12.556 |
| 250,000 | 128 | float32 | cpu | **globalgraphrag** | 767.8 | 2.128 | 12.958 | 13.399 |
| 250,000 | 128 | float32 | cpu | **recommend** | 296.6 | 12.394 | 12.863 | 12.883 |
| 250,000 | 128 | float32 | cpu | **geo** | 57.3 | 67.162 | 79.083 | 86.106 |
| 250,000 | 128 | float32 | cpu | **temporal** | 493.6 | 6.904 | 10.065 | 10.477 |
| 250,000 | 128 | float32 | cpu | **learnedindex** | 1,060.8 | 1.461 | 5.420 | 13.180 |
| 250,000 | 128 | float32 | cuda | **dense** | 137.0 | 38.767 | 72.514 | 75.105 |
| 250,000 | 128 | float32 | cuda | **hybrid** | 153.1 | 45.191 | 77.423 | 80.681 |
| 250,000 | 128 | float32 | cuda | **sparse** | 2,031.8 | 3.669 | 6.234 | 6.418 |
| 250,000 | 128 | float32 | cuda | **filtered** | 50.9 | 46.797 | 699.365 | 700.739 |
| 250,000 | 128 | float32 | cuda | **byid** | 139.2 | 41.455 | 75.400 | 84.015 |
| 250,000 | 128 | float32 | cuda | **graphrag** | 130.8 | 43.276 | 75.102 | 81.903 |
| 250,000 | 128 | float32 | cuda | **geo** | 68.5 | 92.617 | 215.782 | 223.218 |
| 250,000 | 128 | float32 | cuda | **temporal** | 423.7 | 13.774 | 27.546 | 28.845 |
| 250,000 | 128 | float32 | cuda | **learnedindex** | 135.7 | 38.088 | 78.491 | 88.700 |
| 250,000 | 128 | float64 | cpu | **dense** | 397.8 | 11.199 | 36.998 | 43.844 |
| 250,000 | 128 | float64 | cpu | **hybrid** | 281.3 | 26.980 | 47.273 | 48.393 |
| 250,000 | 128 | float64 | cpu | **sparse** | 2,106.7 | 3.093 | 5.919 | 6.044 |
| 250,000 | 128 | float64 | cpu | **filtered** | 61.5 | 22.578 | 684.383 | 690.325 |
| 250,000 | 128 | float64 | cpu | **byid** | 1,600.9 | 4.593 | 8.713 | 9.032 |
| 250,000 | 128 | float64 | cpu | **graphrag** | 208.0 | 26.313 | 44.484 | 46.772 |
| 250,000 | 128 | float64 | cpu | **geo** | 55.3 | 101.014 | 325.775 | 383.628 |
| 250,000 | 128 | float64 | cpu | **temporal** | 383.9 | 14.001 | 25.274 | 25.937 |
| 250,000 | 128 | float64 | cpu | **learnedindex** | 303.6 | 20.306 | 37.586 | 44.567 |
| 250,000 | 128 | float64 | cuda | **dense** | 457.4 | 11.221 | 24.853 | 29.406 |
| 250,000 | 128 | float64 | cuda | **hybrid** | 217.1 | 27.090 | 46.199 | 47.873 |
| 250,000 | 128 | float64 | cuda | **sparse** | 2,201.0 | 3.289 | 5.116 | 5.565 |
| 250,000 | 128 | float64 | cuda | **filtered** | 58.6 | 28.352 | 717.740 | 759.633 |
| 250,000 | 128 | float64 | cuda | **byid** | 1,972.1 | 3.524 | 5.341 | 5.580 |
| 250,000 | 128 | float64 | cuda | **graphrag** | 241.0 | 19.445 | 75.785 | 79.717 |
| 250,000 | 128 | float64 | cuda | **geo** | 65.1 | 105.264 | 150.122 | 151.579 |
| 250,000 | 128 | float64 | cuda | **temporal** | 371.7 | 13.571 | 31.197 | 38.305 |
| 250,000 | 128 | float64 | cuda | **learnedindex** | 310.4 | 16.663 | 32.758 | 35.475 |
| 250,000 | 128 | int16 | cpu | **dense** | 548.2 | 12.023 | 21.967 | 23.115 |
| 250,000 | 128 | int16 | cpu | **hybrid** | 583.3 | 10.040 | 22.138 | 25.596 |
| 250,000 | 128 | int16 | cpu | **sparse** | 2,338.4 | 3.044 | 4.475 | 4.883 |
| 250,000 | 128 | int16 | cpu | **filtered** | 62.1 | 10.338 | 729.294 | 731.580 |
| 250,000 | 128 | int16 | cpu | **byid** | 1,897.2 | 3.764 | 5.470 | 6.369 |
| 250,000 | 128 | int16 | cpu | **graphrag** | 550.4 | 10.528 | 25.596 | 27.197 |
| 250,000 | 128 | int16 | cpu | **geo** | 61.8 | 93.470 | 264.696 | 275.204 |
| 250,000 | 128 | int16 | cpu | **temporal** | 358.9 | 17.864 | 34.061 | 41.450 |
| 250,000 | 128 | int16 | cpu | **learnedindex** | 572.7 | 10.752 | 22.479 | 24.417 |
| 250,000 | 128 | int16 | cuda | **dense** | 545.6 | 11.624 | 19.668 | 20.601 |
| 250,000 | 128 | int16 | cuda | **hybrid** | 478.0 | 10.884 | 24.933 | 28.384 |
| 250,000 | 128 | int16 | cuda | **sparse** | 2,165.4 | 3.144 | 6.407 | 7.166 |
| 250,000 | 128 | int16 | cuda | **filtered** | 56.6 | 14.591 | 785.872 | 793.822 |
| 250,000 | 128 | int16 | cuda | **byid** | 1,825.3 | 3.541 | 7.059 | 8.271 |
| 250,000 | 128 | int16 | cuda | **graphrag** | 353.1 | 14.274 | 41.629 | 43.613 |
| 250,000 | 128 | int16 | cuda | **geo** | 65.5 | 108.058 | 135.031 | 150.016 |
| 250,000 | 128 | int16 | cuda | **temporal** | 320.4 | 15.923 | 35.106 | 40.782 |
| 250,000 | 128 | int16 | cuda | **learnedindex** | 448.4 | 12.821 | 28.740 | 37.004 |
| 250,000 | 128 | int32 | cpu | **dense** | 204.3 | 25.739 | 77.556 | 78.489 |
| 250,000 | 128 | int32 | cpu | **hybrid** | 246.2 | 22.740 | 45.099 | 48.148 |
| 250,000 | 128 | int32 | cpu | **sparse** | 2,396.0 | 2.596 | 4.679 | 7.029 |
| 250,000 | 128 | int32 | cpu | **filtered** | 57.5 | 27.522 | 685.733 | 693.392 |
| 250,000 | 128 | int32 | cpu | **byid** | 2,091.8 | 3.506 | 5.674 | 7.568 |
| 250,000 | 128 | int32 | cpu | **graphrag** | 236.3 | 20.250 | 47.794 | 48.121 |
| 250,000 | 128 | int32 | cpu | **geo** | 52.1 | 112.060 | 269.511 | 286.594 |
| 250,000 | 128 | int32 | cpu | **temporal** | 349.8 | 18.956 | 32.863 | 35.218 |
| 250,000 | 128 | int32 | cpu | **learnedindex** | 271.3 | 18.288 | 37.965 | 46.182 |
| 250,000 | 128 | int32 | cuda | **dense** | 491.8 | 8.705 | 24.491 | 25.906 |
| 250,000 | 128 | int32 | cuda | **hybrid** | 299.7 | 17.066 | 32.452 | 36.281 |
| 250,000 | 128 | int32 | cuda | **sparse** | 2,193.2 | 3.219 | 5.045 | 5.677 |
| 250,000 | 128 | int32 | cuda | **filtered** | 58.8 | 24.409 | 714.000 | 737.018 |
| 250,000 | 128 | int32 | cuda | **byid** | 2,058.4 | 3.410 | 5.436 | 6.977 |
| 250,000 | 128 | int32 | cuda | **graphrag** | 287.6 | 22.601 | 40.334 | 51.144 |
| 250,000 | 128 | int32 | cuda | **geo** | 49.9 | 120.085 | 296.459 | 330.881 |
| 250,000 | 128 | int32 | cuda | **temporal** | 360.3 | 17.353 | 31.949 | 36.254 |
| 250,000 | 128 | int32 | cuda | **learnedindex** | 260.0 | 20.635 | 42.276 | 44.041 |
| 250,000 | 128 | int64 | cpu | **dense** | 479.5 | 9.373 | 18.323 | 20.830 |
| 250,000 | 128 | int64 | cpu | **hybrid** | 293.2 | 19.562 | 36.075 | 40.816 |
| 250,000 | 128 | int64 | cpu | **sparse** | 2,323.8 | 3.140 | 5.352 | 6.162 |
| 250,000 | 128 | int64 | cpu | **filtered** | 55.1 | 32.058 | 658.505 | 678.221 |
| 250,000 | 128 | int64 | cpu | **byid** | 1,803.3 | 3.576 | 7.030 | 7.657 |
| 250,000 | 128 | int64 | cpu | **graphrag** | 224.8 | 25.075 | 54.054 | 60.468 |
| 250,000 | 128 | int64 | cpu | **geo** | 48.3 | 112.174 | 352.282 | 374.421 |
| 250,000 | 128 | int64 | cpu | **temporal** | 306.6 | 19.352 | 32.717 | 43.630 |
| 250,000 | 128 | int64 | cpu | **learnedindex** | 187.2 | 25.752 | 45.856 | 52.376 |
| 250,000 | 128 | int64 | cuda | **dense** | 345.5 | 11.403 | 25.436 | 29.089 |
| 250,000 | 128 | int64 | cuda | **hybrid** | 203.3 | 20.668 | 43.610 | 45.825 |
| 250,000 | 128 | int64 | cuda | **sparse** | 2,500.7 | 2.379 | 5.318 | 5.412 |
| 250,000 | 128 | int64 | cuda | **filtered** | 63.1 | 29.992 | 615.572 | 623.267 |
| 250,000 | 128 | int64 | cuda | **byid** | 1,854.3 | 3.931 | 6.286 | 6.987 |
| 250,000 | 128 | int64 | cuda | **graphrag** | 173.0 | 25.362 | 48.039 | 56.098 |
| 250,000 | 128 | int64 | cuda | **geo** | 58.2 | 94.029 | 293.120 | 301.800 |
| 250,000 | 128 | int64 | cuda | **temporal** | 420.0 | 15.954 | 31.279 | 33.981 |
| 250,000 | 128 | int64 | cuda | **learnedindex** | 195.2 | 24.515 | 48.655 | 55.323 |
| 250,000 | 128 | int8 | cpu | **dense** | 1,093.0 | 3.180 | 4.497 | 4.530 |
| 250,000 | 128 | int8 | cpu | **hybrid** | 516.4 | 7.222 | 8.003 | 8.138 |
| 250,000 | 128 | int8 | cpu | **filtered** | 146.0 | 4.825 | 142.846 | 142.875 |
| 250,000 | 128 | int8 | cpu | **filteredbool** | 158.0 | 12.657 | 85.129 | 85.157 |
| 250,000 | 128 | int8 | cpu | **filteredstring** | 140.0 | 20.219 | 61.792 | 61.794 |
| 250,000 | 128 | int8 | cpu | **sparse** | 4,320.1 | 0.888 | 1.104 | 1.174 |
| 250,000 | 128 | int8 | cpu | **byid** | 3,284.8 | 1.112 | 1.562 | 1.592 |
| 250,000 | 128 | int8 | cpu | **graphrag** | 677.3 | 5.473 | 6.009 | 6.045 |
| 250,000 | 128 | int8 | cpu | **globalgraphrag** | 649.5 | 5.649 | 6.026 | 9.133 |
| 250,000 | 128 | int8 | cpu | **recommend** | 792.7 | 4.543 | 5.577 | 5.628 |
| 250,000 | 128 | int8 | cpu | **geo** | 38.2 | 83.603 | 182.048 | 186.522 |
| 250,000 | 128 | int8 | cpu | **temporal** | 396.2 | 9.320 | 9.652 | 9.681 |
| 250,000 | 128 | int8 | cpu | **learnedindex** | 786.0 | 4.646 | 5.449 | 5.458 |
| 250,000 | 128 | int8 | cuda | **dense** | 557.0 | 9.547 | 20.815 | 21.738 |
| 250,000 | 128 | int8 | cuda | **hybrid** | 567.6 | 8.530 | 23.571 | 29.600 |
| 250,000 | 128 | int8 | cuda | **sparse** | 2,341.7 | 3.032 | 4.783 | 4.929 |
| 250,000 | 128 | int8 | cuda | **filtered** | 56.6 | 13.455 | 818.343 | 826.880 |
| 250,000 | 128 | int8 | cuda | **byid** | 2,022.7 | 2.976 | 4.548 | 5.221 |
| 250,000 | 128 | int8 | cuda | **graphrag** | 530.9 | 10.554 | 20.575 | 25.170 |
| 250,000 | 128 | int8 | cuda | **geo** | 55.6 | 110.492 | 242.673 | 255.508 |
| 250,000 | 128 | int8 | cuda | **temporal** | 376.6 | 16.451 | 30.835 | 39.362 |
| 250,000 | 128 | int8 | cuda | **learnedindex** | 600.3 | 7.504 | 20.304 | 23.488 |
| 250,000 | 128 | turboquant | cpu | **dense** | 1,916.0 | 1.781 | 3.099 | 3.110 |
| 250,000 | 128 | turboquant | cpu | **hybrid** | 1,977.9 | 1.878 | 2.266 | 2.965 |
| 250,000 | 128 | turboquant | cpu | **filtered** | 161.8 | 1.990 | 143.285 | 143.511 |
| 250,000 | 128 | turboquant | cpu | **filteredbool** | 258.1 | 3.313 | 77.562 | 77.570 |
| 250,000 | 128 | turboquant | cpu | **filteredstring** | 346.3 | 2.974 | 54.826 | 54.854 |
| 250,000 | 128 | turboquant | cpu | **sparse** | 4,053.0 | 0.975 | 1.083 | 1.089 |
| 250,000 | 128 | turboquant | cpu | **byid** | 1,982.8 | 1.877 | 2.104 | 2.328 |
| 250,000 | 128 | turboquant | cpu | **graphrag** | 1,513.9 | 2.422 | 3.466 | 3.791 |
| 250,000 | 128 | turboquant | cpu | **globalgraphrag** | 1,636.1 | 2.168 | 2.737 | 2.839 |
| 250,000 | 128 | turboquant | cpu | **recommend** | 1,790.8 | 1.912 | 2.532 | 3.255 |
| 250,000 | 128 | turboquant | cpu | **geo** | 41.3 | 78.830 | 152.037 | 157.499 |
| 250,000 | 128 | turboquant | cpu | **temporal** | 479.8 | 7.625 | 8.452 | 8.772 |
| 250,000 | 128 | turboquant | cpu | **learnedindex** | 1,602.5 | 2.295 | 2.774 | 2.820 |
| 250,000 | 128 | turboquant | cuda | **dense** | 1,003.0 | 6.379 | 13.639 | 16.234 |
| 250,000 | 128 | turboquant | cuda | **hybrid** | 927.5 | 5.034 | 13.021 | 16.957 |
| 250,000 | 128 | turboquant | cuda | **sparse** | 3,000.6 | 2.491 | 3.929 | 4.252 |
| 250,000 | 128 | turboquant | cuda | **filtered** | 73.1 | 6.693 | 643.373 | 653.107 |
| 250,000 | 128 | turboquant | cuda | **byid** | 910.1 | 5.704 | 13.793 | 14.687 |
| 250,000 | 128 | turboquant | cuda | **graphrag** | 882.6 | 6.329 | 14.951 | 17.136 |
| 250,000 | 128 | turboquant | cuda | **geo** | 64.1 | 97.611 | 253.484 | 263.044 |
| 250,000 | 128 | turboquant | cuda | **temporal** | 424.5 | 15.021 | 28.761 | 32.515 |
| 250,000 | 128 | turboquant | cuda | **learnedindex** | 669.8 | 5.819 | 33.149 | 37.842 |
| 250,000 | 128 | uint16 | cpu | **dense** | 471.7 | 10.880 | 21.721 | 26.182 |
| 250,000 | 128 | uint16 | cpu | **hybrid** | 406.7 | 12.879 | 25.354 | 28.689 |
| 250,000 | 128 | uint16 | cpu | **sparse** | 2,534.5 | 2.543 | 6.273 | 6.425 |
| 250,000 | 128 | uint16 | cpu | **filtered** | 70.2 | 11.343 | 641.737 | 650.336 |
| 250,000 | 128 | uint16 | cpu | **byid** | 1,490.4 | 4.823 | 6.981 | 7.479 |
| 250,000 | 128 | uint16 | cpu | **graphrag** | 457.5 | 14.509 | 28.510 | 32.009 |
| 250,000 | 128 | uint16 | cpu | **geo** | 59.3 | 98.736 | 250.368 | 253.705 |
| 250,000 | 128 | uint16 | cpu | **temporal** | 352.8 | 15.357 | 32.958 | 39.173 |
| 250,000 | 128 | uint16 | cpu | **learnedindex** | 575.0 | 9.426 | 21.874 | 24.209 |
| 250,000 | 128 | uint16 | cuda | **dense** | 524.0 | 12.336 | 21.575 | 24.016 |
| 250,000 | 128 | uint16 | cuda | **hybrid** | 469.1 | 11.966 | 22.597 | 33.652 |
| 250,000 | 128 | uint16 | cuda | **sparse** | 2,623.9 | 2.473 | 3.966 | 4.288 |
| 250,000 | 128 | uint16 | cuda | **filtered** | 74.3 | 14.115 | 596.489 | 607.740 |
| 250,000 | 128 | uint16 | cuda | **byid** | 1,863.0 | 4.158 | 5.400 | 5.576 |
| 250,000 | 128 | uint16 | cuda | **graphrag** | 460.1 | 11.453 | 23.625 | 26.150 |
| 250,000 | 128 | uint16 | cuda | **geo** | 54.0 | 102.438 | 315.788 | 374.560 |
| 250,000 | 128 | uint16 | cuda | **temporal** | 374.3 | 12.190 | 31.589 | 32.139 |
| 250,000 | 128 | uint16 | cuda | **learnedindex** | 541.5 | 7.370 | 22.246 | 22.921 |
| 250,000 | 128 | uint32 | cpu | **dense** | 395.5 | 16.623 | 26.729 | 31.520 |
| 250,000 | 128 | uint32 | cpu | **hybrid** | 254.3 | 21.522 | 40.666 | 44.242 |
| 250,000 | 128 | uint32 | cpu | **sparse** | 2,196.4 | 3.162 | 4.947 | 5.240 |
| 250,000 | 128 | uint32 | cpu | **filtered** | 60.5 | 30.649 | 634.318 | 651.229 |
| 250,000 | 128 | uint32 | cpu | **byid** | 1,955.2 | 3.602 | 5.703 | 7.506 |
| 250,000 | 128 | uint32 | cpu | **graphrag** | 234.4 | 25.664 | 49.248 | 59.792 |
| 250,000 | 128 | uint32 | cpu | **geo** | 45.8 | 115.554 | 375.771 | 446.463 |
| 250,000 | 128 | uint32 | cpu | **temporal** | 325.1 | 18.423 | 37.479 | 40.116 |
| 250,000 | 128 | uint32 | cpu | **learnedindex** | 235.0 | 18.646 | 42.664 | 53.798 |
| 250,000 | 128 | uint32 | cuda | **dense** | 300.5 | 17.126 | 37.324 | 43.799 |
| 250,000 | 128 | uint32 | cuda | **hybrid** | 283.7 | 18.559 | 39.645 | 43.311 |
| 250,000 | 128 | uint32 | cuda | **sparse** | 2,485.8 | 2.742 | 5.352 | 6.373 |
| 250,000 | 128 | uint32 | cuda | **filtered** | 60.0 | 21.277 | 710.706 | 727.215 |
| 250,000 | 128 | uint32 | cuda | **byid** | 1,996.2 | 3.372 | 6.151 | 7.283 |
| 250,000 | 128 | uint32 | cuda | **graphrag** | 266.3 | 20.000 | 40.872 | 42.559 |
| 250,000 | 128 | uint32 | cuda | **geo** | 59.1 | 96.730 | 258.714 | 278.669 |
| 250,000 | 128 | uint32 | cuda | **temporal** | 355.6 | 16.744 | 33.018 | 34.120 |
| 250,000 | 128 | uint32 | cuda | **learnedindex** | 267.3 | 19.650 | 38.678 | 49.009 |
| 250,000 | 128 | uint64 | cpu | **dense** | 328.6 | 16.845 | 37.131 | 39.357 |
| 250,000 | 128 | uint64 | cpu | **hybrid** | 171.4 | 29.222 | 60.903 | 65.577 |
| 250,000 | 128 | uint64 | cpu | **sparse** | 2,436.7 | 2.792 | 4.881 | 6.527 |
| 250,000 | 128 | uint64 | cpu | **filtered** | 49.2 | 46.186 | 737.367 | 752.140 |
| 250,000 | 128 | uint64 | cpu | **byid** | 1,570.0 | 4.605 | 8.548 | 8.682 |
| 250,000 | 128 | uint64 | cpu | **graphrag** | 147.1 | 43.502 | 90.057 | 98.825 |
| 250,000 | 128 | uint64 | cpu | **geo** | 77.0 | 93.537 | 123.173 | 124.869 |
| 250,000 | 128 | uint64 | cpu | **temporal** | 348.6 | 17.638 | 31.314 | 34.605 |
| 250,000 | 128 | uint64 | cpu | **learnedindex** | 201.9 | 33.202 | 52.939 | 72.383 |
| 250,000 | 128 | uint64 | cuda | **dense** | 406.8 | 10.735 | 25.347 | 28.544 |
| 250,000 | 128 | uint64 | cuda | **hybrid** | 449.2 | 11.912 | 29.063 | 32.311 |
| 250,000 | 128 | uint64 | cuda | **sparse** | 2,461.7 | 2.753 | 4.687 | 4.842 |
| 250,000 | 128 | uint64 | cuda | **filtered** | 69.6 | 8.948 | 648.993 | 662.757 |
| 250,000 | 128 | uint64 | cuda | **byid** | 1,853.5 | 3.525 | 7.079 | 7.815 |
| 250,000 | 128 | uint64 | cuda | **graphrag** | 473.2 | 12.156 | 25.335 | 28.389 |
| 250,000 | 128 | uint64 | cuda | **geo** | 60.3 | 102.172 | 196.898 | 208.648 |
| 250,000 | 128 | uint64 | cuda | **temporal** | 325.6 | 14.305 | 31.510 | 34.098 |
| 250,000 | 128 | uint64 | cuda | **learnedindex** | 434.0 | 10.575 | 41.827 | 50.114 |
| 250,000 | 128 | uint8 | cpu | **dense** | 570.5 | 10.671 | 19.593 | 21.334 |
| 250,000 | 128 | uint8 | cpu | **hybrid** | 579.6 | 11.635 | 19.764 | 23.071 |
| 250,000 | 128 | uint8 | cpu | **sparse** | 2,257.9 | 3.043 | 5.169 | 5.980 |
| 250,000 | 128 | uint8 | cpu | **filtered** | 68.2 | 12.863 | 659.467 | 670.747 |
| 250,000 | 128 | uint8 | cpu | **byid** | 1,861.3 | 3.944 | 5.423 | 6.571 |
| 250,000 | 128 | uint8 | cpu | **graphrag** | 401.5 | 13.918 | 26.506 | 28.379 |
| 250,000 | 128 | uint8 | cpu | **geo** | 50.6 | 109.934 | 322.565 | 338.949 |
| 250,000 | 128 | uint8 | cpu | **temporal** | 308.4 | 21.053 | 33.050 | 36.330 |
| 250,000 | 128 | uint8 | cpu | **learnedindex** | 624.6 | 11.444 | 21.750 | 23.953 |
| 250,000 | 128 | uint8 | cuda | **dense** | 537.8 | 11.408 | 27.006 | 28.024 |
| 250,000 | 128 | uint8 | cuda | **hybrid** | 757.2 | 8.177 | 19.083 | 20.713 |
| 250,000 | 128 | uint8 | cuda | **sparse** | 2,807.0 | 2.465 | 4.718 | 5.924 |
| 250,000 | 128 | uint8 | cuda | **filtered** | 75.0 | 9.695 | 580.183 | 614.705 |
| 250,000 | 128 | uint8 | cuda | **byid** | 1,833.6 | 3.892 | 6.741 | 7.717 |
| 250,000 | 128 | uint8 | cuda | **graphrag** | 607.7 | 9.574 | 18.997 | 20.689 |
| 250,000 | 128 | uint8 | cuda | **geo** | 52.4 | 94.226 | 355.419 | 383.091 |
| 250,000 | 128 | uint8 | cuda | **temporal** | 365.7 | 13.779 | 28.360 | 33.589 |
| 250,000 | 128 | uint8 | cuda | **learnedindex** | 647.7 | 8.805 | 18.700 | 22.180 |
| 250,000 | 384 | complex128 | cpu | **dense** | 204.5 | 16.227 | 27.838 | 29.841 |
| 250,000 | 384 | complex128 | cpu | **hybrid** | 153.9 | 20.977 | 36.294 | 48.376 |
| 250,000 | 384 | complex128 | cpu | **filtered** | 70.8 | 22.729 | 187.305 | 196.627 |
| 250,000 | 384 | complex128 | cpu | **filteredbool** | 62.4 | 43.854 | 139.376 | 143.817 |
| 250,000 | 384 | complex128 | cpu | **filteredstring** | 60.2 | 52.023 | 108.523 | 112.555 |
| 250,000 | 384 | complex128 | cpu | **sparse** | 4,399.1 | 0.848 | 1.353 | 1.412 |
| 250,000 | 384 | complex128 | cpu | **byid** | 2,368.0 | 1.519 | 2.035 | 2.178 |
| 250,000 | 384 | complex128 | cpu | **graphrag** | 158.6 | 22.180 | 26.998 | 30.318 |
| 250,000 | 384 | complex128 | cpu | **globalgraphrag** | 173.0 | 21.148 | 24.121 | 24.236 |
| 250,000 | 384 | complex128 | cpu | **recommend** | 202.9 | 18.266 | 18.521 | 18.557 |
| 250,000 | 384 | complex128 | cpu | **geo** | 49.7 | 76.773 | 91.314 | 91.452 |
| 250,000 | 384 | complex128 | cpu | **temporal** | 448.9 | 8.353 | 9.002 | 9.125 |
| 250,000 | 384 | complex128 | cpu | **learnedindex** | 220.4 | 15.836 | 24.675 | 24.677 |
| 250,000 | 384 | float32 | cpu | **dense** | 1,962.3 | 1.723 | 2.978 | 3.374 |
| 250,000 | 384 | float32 | cpu | **hybrid** | 675.9 | 4.651 | 8.462 | 8.885 |
| 250,000 | 384 | float32 | cpu | **filtered** | 153.4 | 1.760 | 149.194 | 151.896 |
| 250,000 | 384 | float32 | cpu | **filteredbool** | 238.0 | 2.911 | 78.201 | 78.661 |
| 250,000 | 384 | float32 | cpu | **filteredstring** | 270.1 | 3.509 | 51.399 | 59.312 |
| 250,000 | 384 | float32 | cpu | **sparse** | 4,224.8 | 0.988 | 1.074 | 1.104 |
| 250,000 | 384 | float32 | cpu | **byid** | 2,294.4 | 1.451 | 2.363 | 2.649 |
| 250,000 | 384 | float32 | cpu | **graphrag** | 1,062.2 | 3.241 | 5.646 | 5.703 |
| 250,000 | 384 | float32 | cpu | **globalgraphrag** | 1,095.2 | 2.749 | 5.538 | 5.806 |
| 250,000 | 384 | float32 | cpu | **recommend** | 962.5 | 3.760 | 3.998 | 3.999 |
| 250,000 | 384 | float32 | cpu | **geo** | 41.5 | 75.482 | 162.864 | 192.199 |
| 250,000 | 384 | float32 | cpu | **temporal** | 488.7 | 7.799 | 9.987 | 10.219 |
| 250,000 | 384 | float32 | cpu | **learnedindex** | 1,350.4 | 2.068 | 4.714 | 5.615 |
| 250,000 | 384 | int8 | cpu | **dense** | 696.2 | 4.896 | 7.247 | 8.201 |
| 250,000 | 384 | int8 | cpu | **hybrid** | 431.2 | 7.019 | 12.691 | 13.331 |
| 250,000 | 384 | int8 | cpu | **filtered** | 118.0 | 6.014 | 175.083 | 175.264 |
| 250,000 | 384 | int8 | cpu | **filteredbool** | 184.0 | 8.339 | 87.181 | 87.316 |
| 250,000 | 384 | int8 | cpu | **filteredstring** | 462.9 | 1.429 | 46.240 | 47.629 |
| 250,000 | 384 | int8 | cpu | **sparse** | 4,609.6 | 0.734 | 1.054 | 1.110 |
| 250,000 | 384 | int8 | cpu | **byid** | 2,773.0 | 1.272 | 1.894 | 2.364 |
| 250,000 | 384 | int8 | cpu | **graphrag** | 594.0 | 6.013 | 8.120 | 8.965 |
| 250,000 | 384 | int8 | cpu | **globalgraphrag** | 554.7 | 6.549 | 8.871 | 9.953 |
| 250,000 | 384 | int8 | cpu | **recommend** | 742.4 | 4.839 | 5.666 | 5.689 |
| 250,000 | 384 | int8 | cpu | **geo** | 51.6 | 74.500 | 87.899 | 92.408 |
| 250,000 | 384 | int8 | cpu | **temporal** | 391.8 | 9.470 | 10.051 | 10.251 |
| 250,000 | 384 | int8 | cpu | **learnedindex** | 740.2 | 5.026 | 6.121 | 6.462 |
| 250,000 | 384 | turboquant | cpu | **dense** | 540.1 | 6.728 | 7.869 | 7.873 |
| 250,000 | 384 | turboquant | cpu | **hybrid** | 530.1 | 6.883 | 7.149 | 7.211 |
| 250,000 | 384 | turboquant | cpu | **filtered** | 134.8 | 6.676 | 146.495 | 146.572 |
| 250,000 | 384 | turboquant | cpu | **filteredbool** | 205.4 | 6.680 | 80.594 | 80.614 |
| 250,000 | 384 | turboquant | cpu | **filteredstring** | 243.3 | 8.621 | 52.275 | 52.415 |
| 250,000 | 384 | turboquant | cpu | **sparse** | 4,374.3 | 0.897 | 1.002 | 1.016 |
| 250,000 | 384 | turboquant | cpu | **byid** | 564.9 | 6.350 | 6.728 | 6.761 |
| 250,000 | 384 | turboquant | cpu | **graphrag** | 491.5 | 7.362 | 10.645 | 11.093 |
| 250,000 | 384 | turboquant | cpu | **globalgraphrag** | 498.2 | 7.303 | 7.650 | 7.745 |
| 250,000 | 384 | turboquant | cpu | **recommend** | 560.6 | 6.365 | 6.781 | 6.841 |
| 250,000 | 384 | turboquant | cpu | **geo** | 40.4 | 72.196 | 226.683 | 229.373 |
| 250,000 | 384 | turboquant | cpu | **temporal** | 509.6 | 7.192 | 8.060 | 8.114 |
| 250,000 | 384 | turboquant | cpu | **learnedindex** | 523.0 | 6.897 | 7.358 | 7.395 |
| 1,000,000 | 128 | int8 | cpu | **dense** | 474.5 | 7.309 | 9.721 | 10.954 |
| 1,000,000 | 128 | int8 | cpu | **hybrid** | 429.5 | 7.964 | 10.968 | 11.569 |
| 1,000,000 | 128 | int8 | cpu | **filtered** | 39.7 | 9.410 | 575.643 | 575.649 |
| 1,000,000 | 128 | int8 | cpu | **filteredbool** | 65.7 | 13.690 | 301.098 | 301.101 |
| 1,000,000 | 128 | int8 | cpu | **filteredstring** | 68.4 | 25.077 | 220.269 | 220.308 |
| 1,000,000 | 128 | int8 | cpu | **sparse** | 4,491.0 | 0.908 | 1.011 | 1.021 |
| 1,000,000 | 128 | int8 | cpu | **byid** | 4,051.3 | 0.856 | 1.503 | 1.504 |
| 1,000,000 | 128 | int8 | cpu | **graphrag** | 358.3 | 9.915 | 12.086 | 13.115 |
| 1,000,000 | 128 | int8 | cpu | **globalgraphrag** | 362.4 | 10.317 | 12.836 | 12.894 |
| 1,000,000 | 128 | int8 | cpu | **recommend** | 482.0 | 7.549 | 8.460 | 8.468 |
| 1,000,000 | 128 | int8 | cpu | **geo** | 13.2 | 284.872 | 337.270 | 347.548 |
| 1,000,000 | 128 | int8 | cpu | **temporal** | 146.9 | 24.824 | 25.192 | 25.725 |
| 1,000,000 | 128 | int8 | cpu | **learnedindex** | 475.7 | 7.684 | 8.791 | 8.844 |

---

## 4. Hardware Profiling & Hotspot Analysis (Pprof Insights)

Continuous runtime CPU, heap allocation, and mutex profiling (`profiles/*.pprof`) during execution identified key operational insights:

### CPU Hotspots (Top Functions by Flat Duration)

1. **`simd.euclideanDistanceBatch4Way` (16.34% flat time)**: Dominates float32 search distance evaluation; 4-way unrolled AVX2 kernel provides high throughput.
2. **`simd.euclideanFloat64AVX2Kernel` (42.62% flat time)**: In `complex128` search, each 128-dim vector consists of 256 float64 elements, requiring heavy 256-bit SIMD processing.
3. **`store/index.(*ArrowHNSW).searchLayerFloat32` (9.60% flat, 90.27% cum)**: Core graph traversal loop traversing neighbor candidates.
4. **`CandidateHeapAdapter (down, Less, Swap)` (15.2% combined flat time)**: Priority queue maintenance during beam search represents a major non-SIMD compute consumer.
5. **`memory.(*SlabArena).GetWithGeneration` (11.25% flat time)**: Vector memory pointer dereferencing with generation checks.
6. **`prometheus.(*counter).Inc` & `hashAdd` (3.43% flat time)**: High-frequency metric counter increments on hot query paths.

### Lock & Contention Bottlenecks

- **`ArrowHNSW.AddConnectionsBatch` (56.94% mutex delay)**: Mutex serialization occurs when concurrent indexing workers update node neighbor lists in parallel.
- **`ArrowHNSW.AddConnection` (25.13% mutex delay)**: Fine-grained bidirectional edge linkage synchronization.

### Allocation Bottlenecks

- **`bytes.growSlice` (14.85% total alloc space)**: Slice expansion during batch payload serialization.
- **`SimpleBufferPool.Get` (10.93%) & protobuf decoding (10.72%)**: Flight RPC payload buffer management.
- **`NewBloomFilter` (10.24%)**: Temporary bloom filter structures allocated per batch.

### TurboQuant Distance Kernel Breakdown (`pprof` at 100k scale)

Profiling `turboQuantDistanceAVX2Scratch` across 100,000 vectors revealed the internal compute budget of TurboQuant distance calculations:

| Phase | Share (%) | Duration | Operation / Description |
|---|---|---|---|
| **Recursive Polar Reconstruction** | 47.8% | 890 ms | Hierarchical scalar tree traversal multiplying radii by cos/sin lookup tables |
| **QJL Sign Correction (`tqApplyQJLCorrection`)** | 28.5% | 530 ms | Branchless 1-bit sign correction across all dimensions |
| **Angle Code Unpacking** | 12.9% | 240 ms | Bit-shifting 4-bit nibbles into byte table indices |
| **Scratch Buffer & Radius Unpack** | 6.5% | 120 ms | Local buffer alignment and float32 radius scale restoration |
| **SIMD Distance Evaluation (`l2SquaredAVX2`)** | 4.3% | 80 ms | Vectorized AVX2 Euclidean distance computation |
| **Chunk Resolution & Chunk Views** | < 1.0% | < 10 ms | Zero-allocation Arrow record chunk view lookup (amortized) |

**Key Takeaway**: Over **89.2%** of the computational time in TurboQuant distance evaluation is consumed by scalar dequantization and coordinate reconstruction, while the SIMD distance computation accounts for only **4.3%**. Consequently, distance evaluation throughput is strictly bound by scalar reconstruction latency rather than SIMD vector throughput. This motivates Item 4 (candidate accumulation across hops to amortize batched kernel setup) and explains why TurboQuant search throughput is ~300–420 QPS rather than 2,000+ QPS.
