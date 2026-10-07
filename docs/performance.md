# Longbow Performance Benchmarks

**Date:** 2026-09-26  
**Baseline Release Candidate:** `v0.2.5-rc1`  

> [!WARNING]
> **Every TurboQuant row in this document predates `a955a0c1` and must be re-measured before it is cited.**
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
> So a quarter of the corpus was unreachable at any `ef` when these numbers were taken.
> The TurboQuant throughput and search figures below describe that index, not TurboQuant.
> The float32 rows are unaffected. See `docs/roadmap.md` §9.2 for the bisection and R19
> for the re-baselining plan.

## System Specifications

| Component | Detail |
|---|---|
| CPU | Intel Core i7-12650H (16 vCPUs, AVX2, x86_64) |
| Host Active Cores | 4 Unthrottled Cores (CPUs 12-15 pinned via `--cpu-affinity 12-15`) |
| Concurrency Workers | 4 Workers (`--workers 4`) |
| Host RAM | 23 GB System Memory |
| GPU | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89, CUDA 12.4) |
| Go Runtime | Go 1.27 (CGO enabled) |

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

## 2. Ingestion Throughput & Memory Footprint

Ingestion here is transport-side: streaming vectors into the dataset and packing them.
It does not include HNSW graph construction, which for TurboQuant dominates wall clock at
scale. See §4 and `docs/roadmap.md` §9.2.

| Scale (Count) | Dim | Dtype | Engine | Ingestion (vec/s) | Ingestion (MB/s) | Peak RSS (MB) |
|---|---|---|---|---|---|---|
| 10,000 | 128 | complex128 | cpu | 65,521.3 | 127.97 MB/s | 193.0 MB |
| 10,000 | 128 | float32 | cpu | 185,325.3 | 90.49 MB/s | 160.7 MB |
| 10,000 | 128 | float32 | cuda | 166,944.6 | 81.52 MB/s | 160.7 MB |
| 10,000 | 128 | int8 | cpu | 256,789.0 | 31.35 MB/s | 152.7 MB |
| 10,000 | 128 | turboquant | cpu | 182,161.4 | 11.12 MB/s | 151.3 MB |
| 10,000 | 384 | complex128 | cpu | 22,909.4 | 134.23 MB/s | 278.9 MB |
| 10,000 | 384 | float32 | cpu | 69,450.8 | 101.73 MB/s | 182.2 MB |
| 10,000 | 384 | int8 | cpu | 98,161.7 | 35.95 MB/s | 158.1 MB |
| 10,000 | 384 | turboquant | cpu | 68,866.8 | 12.61 MB/s | 154.0 MB |
| 50,000 | 128 | complex128 | cpu | 96,197.7 | 187.89 MB/s | 364.8 MB |
| 50,000 | 128 | float32 | cpu | 260,218.1 | 127.06 MB/s | 203.7 MB |
| 50,000 | 128 | int8 | cpu | 294,673.1 | 35.97 MB/s | 163.4 MB |
| 50,000 | 128 | turboquant | cpu | 228,329.3 | 13.94 MB/s | 156.7 MB |
| 50,000 | 384 | complex128 | cpu | 35,284.7 | 206.75 MB/s | 794.5 MB |
| 50,000 | 384 | float32 | cpu | 94,081.6 | 137.81 MB/s | 311.1 MB |
| 50,000 | 384 | int8 | cpu | 119,548.3 | 43.78 MB/s | 190.3 MB |
| 50,000 | 384 | turboquant | cpu | 85,216.8 | 15.60 MB/s | 170.1 MB |
| 100,000 | 128 | complex128 | cpu | 87,695.1 | 171.28 MB/s | 579.7 MB |
| 100,000 | 128 | complex128 | cuda | 103,717.4 | 202.57 MB/s | 579.7 MB |
| 100,000 | 128 | complex64 | cpu | 159,772.0 | 78.01 MB/s | 257.4 MB |
| 100,000 | 128 | complex64 | cuda | 195,572.2 | 95.49 MB/s | 257.4 MB |
| 100,000 | 128 | float16 | cpu | 494,361.9 | 241.39 MB/s | 257.4 MB |
| 100,000 | 128 | float16 | cuda | 725,010.7 | 354.01 MB/s | 257.4 MB |
| 100,000 | 128 | float32 | cpu | 317,003.5 | 154.79 MB/s | 257.4 MB |
| 100,000 | 128 | float32 | cuda | 218,303.7 | 106.59 MB/s | 257.4 MB |
| 100,000 | 128 | float64 | cpu | 201,306.7 | 98.29 MB/s | 257.4 MB |
| 100,000 | 128 | float64 | cuda | 168,177.7 | 82.12 MB/s | 257.4 MB |
| 100,000 | 128 | int16 | cpu | 419,474.6 | 204.82 MB/s | 257.4 MB |
| 100,000 | 128 | int16 | cuda | 713,631.7 | 348.45 MB/s | 257.4 MB |
| 100,000 | 128 | int32 | cpu | 387,712.9 | 189.31 MB/s | 257.4 MB |
| 100,000 | 128 | int32 | cuda | 381,869.1 | 186.46 MB/s | 257.4 MB |
| 100,000 | 128 | int64 | cpu | 177,334.5 | 86.59 MB/s | 257.4 MB |
| 100,000 | 128 | int64 | cuda | 204,187.7 | 99.70 MB/s | 257.4 MB |
| 100,000 | 128 | int8 | cpu | 1,186,065.7 | 144.78 MB/s | 176.9 MB |
| 100,000 | 128 | int8 | cuda | 965,449.2 | 117.85 MB/s | 176.9 MB |
| 100,000 | 128 | turboquant | cpu | 398,313.3 | 24.31 MB/s | 163.4 MB |
| 100,000 | 128 | turboquant | cuda | 264,584.3 | 16.15 MB/s | 163.4 MB |
| 100,000 | 128 | uint16 | cpu | 559,667.8 | 273.28 MB/s | 257.4 MB |
| 100,000 | 128 | uint16 | cuda | 716,691.3 | 349.95 MB/s | 257.4 MB |
| 100,000 | 128 | uint32 | cpu | 245,680.9 | 119.96 MB/s | 257.4 MB |
| 100,000 | 128 | uint32 | cuda | 332,692.2 | 162.45 MB/s | 257.4 MB |
| 100,000 | 128 | uint64 | cpu | 142,308.2 | 69.49 MB/s | 257.4 MB |
| 100,000 | 128 | uint64 | cuda | 178,130.5 | 86.98 MB/s | 257.4 MB |
| 100,000 | 128 | uint8 | cpu | 1,329,226.3 | 649.04 MB/s | 257.4 MB |
| 100,000 | 128 | uint8 | cuda | 1,103,042.0 | 538.59 MB/s | 257.4 MB |
| 250,000 | 128 | complex128 | cpu | 112,386.5 | 219.50 MB/s | 1,224.2 MB |
| 250,000 | 128 | complex128 | cuda | 119,648.2 | 233.69 MB/s | 1,224.2 MB |
| 250,000 | 128 | complex64 | cpu | 222,957.6 | 108.87 MB/s | 418.6 MB |
| 250,000 | 128 | complex64 | cuda | 240,446.7 | 117.41 MB/s | 418.6 MB |
| 250,000 | 128 | float16 | cpu | 812,184.0 | 396.57 MB/s | 418.6 MB |
| 250,000 | 128 | float16 | cuda | 696,094.6 | 339.89 MB/s | 418.6 MB |
| 250,000 | 128 | float32 | cpu | 260,094.7 | 127.00 MB/s | 418.6 MB |
| 250,000 | 128 | float32 | cuda | 391,694.9 | 191.26 MB/s | 418.6 MB |
| 250,000 | 128 | float64 | cpu | 227,529.3 | 111.10 MB/s | 418.6 MB |
| 250,000 | 128 | float64 | cuda | 258,533.4 | 126.24 MB/s | 418.6 MB |
| 250,000 | 128 | int16 | cpu | 465,644.3 | 227.37 MB/s | 418.6 MB |
| 250,000 | 128 | int16 | cuda | 969,045.6 | 473.17 MB/s | 418.6 MB |
| 250,000 | 128 | int32 | cpu | 501,623.8 | 244.93 MB/s | 418.6 MB |
| 250,000 | 128 | int32 | cuda | 428,805.2 | 209.38 MB/s | 418.6 MB |
| 250,000 | 128 | int64 | cpu | 155,817.5 | 76.08 MB/s | 418.6 MB |
| 250,000 | 128 | int64 | cuda | 226,335.4 | 110.52 MB/s | 418.6 MB |
| 250,000 | 128 | int8 | cpu | 237,384.4 | 28.98 MB/s | 217.1 MB |
| 250,000 | 128 | int8 | cuda | 1,989,312.3 | 242.84 MB/s | 217.1 MB |
| 250,000 | 128 | turboquant | cpu | 235,192.9 | 14.36 MB/s | 183.6 MB |
| 250,000 | 128 | turboquant | cuda | 486,607.5 | 29.70 MB/s | 183.6 MB |
| 250,000 | 128 | uint16 | cpu | 848,551.6 | 414.33 MB/s | 418.6 MB |
| 250,000 | 128 | uint16 | cuda | 728,278.0 | 355.60 MB/s | 418.6 MB |
| 250,000 | 128 | uint32 | cpu | 396,662.4 | 193.68 MB/s | 418.6 MB |
| 250,000 | 128 | uint32 | cuda | 521,768.7 | 254.77 MB/s | 418.6 MB |
| 250,000 | 128 | uint64 | cpu | 205,311.8 | 100.25 MB/s | 418.6 MB |
| 250,000 | 128 | uint64 | cuda | 264,788.3 | 129.29 MB/s | 418.6 MB |
| 250,000 | 128 | uint8 | cpu | 757,178.3 | 369.72 MB/s | 418.6 MB |
| 250,000 | 128 | uint8 | cuda | 1,698,933.2 | 829.56 MB/s | 418.6 MB |
| 250,000 | 384 | complex128 | cpu | 24,024.2 | 140.77 MB/s | 3,372.7 MB |
| 250,000 | 384 | float32 | cpu | 92,452.8 | 135.43 MB/s | 955.7 MB |
| 250,000 | 384 | int8 | cpu | 109,509.1 | 40.10 MB/s | 351.4 MB |
| 250,000 | 384 | turboquant | cpu | 101,181.1 | 18.53 MB/s | 250.7 MB |
| 1,000,000 | 128 | int8 | cpu | 261,434.5 | 31.91 MB/s | 418.6 MB |

---

## 3. Search Modalities Throughput (QPS) & Latencies

Measured across all 9 search modalities with 4 concurrent workers on unthrottled cores (CPUs 12-15):

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
| 50,000 | 128 | turboquant | cpu | **dense** | 1,613.4 | 1.790 | 4.064 | 4.846 |
| 50,000 | 128 | turboquant | cpu | **hybrid** | 1,893.6 | 1.801 | 3.129 | 3.517 |
| 50,000 | 128 | turboquant | cpu | **filtered** | 624.5 | 1.796 | 30.058 | 30.060 |
| 50,000 | 128 | turboquant | cpu | **filteredbool** | 883.3 | 2.074 | 16.237 | 16.253 |
| 50,000 | 128 | turboquant | cpu | **filteredstring** | 596.3 | 4.768 | 11.926 | 12.074 |
| 50,000 | 128 | turboquant | cpu | **sparse** | 3,947.1 | 0.953 | 1.268 | 1.508 |
| 50,000 | 128 | turboquant | cpu | **byid** | 1,661.9 | 2.138 | 3.035 | 4.035 |
| 50,000 | 128 | turboquant | cpu | **graphrag** | 1,758.5 | 2.090 | 2.629 | 2.650 |
| 50,000 | 128 | turboquant | cpu | **globalgraphrag** | 1,678.8 | 2.240 | 2.850 | 3.440 |
| 50,000 | 128 | turboquant | cpu | **recommend** | 1,825.1 | 1.934 | 3.279 | 3.625 |
| 50,000 | 128 | turboquant | cpu | **geo** | 318.2 | 11.464 | 14.813 | 15.949 |
| 50,000 | 128 | turboquant | cpu | **temporal** | 1,432.4 | 2.477 | 3.338 | 4.179 |
| 50,000 | 128 | turboquant | cpu | **learnedindex** | 1,324.7 | 2.727 | 4.255 | 4.580 |
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
| 100,000 | 128 | complex128 | cpu | **dense** | 397.8 | 13.041 | 33.476 | 38.123 |
| 100,000 | 128 | complex128 | cpu | **hybrid** | 223.9 | 22.387 | 39.737 | 54.321 |
| 100,000 | 128 | complex128 | cpu | **sparse** | 2,608.4 | 2.322 | 4.862 | 5.020 |
| 100,000 | 128 | complex128 | cpu | **filtered** | 80.3 | 35.863 | 311.364 | 323.659 |
| 100,000 | 128 | complex128 | cpu | **byid** | 997.7 | 6.617 | 9.398 | 13.032 |
| 100,000 | 128 | complex128 | cpu | **graphrag** | 129.7 | 45.569 | 80.800 | 101.314 |
| 100,000 | 128 | complex128 | cpu | **geo** | 144.1 | 39.834 | 105.814 | 118.475 |
| 100,000 | 128 | complex128 | cpu | **temporal** | 611.8 | 8.995 | 17.655 | 19.656 |
| 100,000 | 128 | complex128 | cpu | **learnedindex** | 188.3 | 27.099 | 55.277 | 64.131 |
| 100,000 | 128 | complex128 | cuda | **dense** | 821.6 | 5.668 | 14.731 | 21.671 |
| 100,000 | 128 | complex128 | cuda | **hybrid** | 762.1 | 8.989 | 14.865 | 17.445 |
| 100,000 | 128 | complex128 | cuda | **sparse** | 2,980.4 | 2.428 | 3.845 | 4.414 |
| 100,000 | 128 | complex128 | cuda | **filtered** | 150.6 | 8.076 | 251.830 | 256.515 |
| 100,000 | 128 | complex128 | cuda | **byid** | 1,847.8 | 3.690 | 7.131 | 9.384 |
| 100,000 | 128 | complex128 | cuda | **graphrag** | 883.7 | 6.022 | 15.116 | 19.114 |
| 100,000 | 128 | complex128 | cuda | **geo** | 164.3 | 39.870 | 88.918 | 94.954 |
| 100,000 | 128 | complex128 | cuda | **temporal** | 728.7 | 7.279 | 18.133 | 20.641 |
| 100,000 | 128 | complex128 | cuda | **learnedindex** | 778.3 | 6.656 | 19.576 | 21.155 |
| 100,000 | 128 | complex64 | cpu | **dense** | 395.7 | 16.203 | 39.309 | 44.151 |
| 100,000 | 128 | complex64 | cpu | **hybrid** | 437.0 | 13.453 | 27.140 | 36.909 |
| 100,000 | 128 | complex64 | cpu | **sparse** | 2,607.2 | 2.509 | 4.950 | 5.458 |
| 100,000 | 128 | complex64 | cpu | **filtered** | 120.9 | 23.039 | 268.384 | 288.195 |
| 100,000 | 128 | complex64 | cpu | **byid** | 294.8 | 19.351 | 39.060 | 39.852 |
| 100,000 | 128 | complex64 | cpu | **graphrag** | 363.2 | 13.215 | 37.905 | 41.770 |
| 100,000 | 128 | complex64 | cpu | **geo** | 177.9 | 36.185 | 49.441 | 52.543 |
| 100,000 | 128 | complex64 | cpu | **temporal** | 749.0 | 8.229 | 18.235 | 20.537 |
| 100,000 | 128 | complex64 | cpu | **learnedindex** | 539.3 | 11.897 | 25.212 | 28.710 |
| 100,000 | 128 | complex64 | cuda | **dense** | 335.0 | 14.757 | 30.218 | 39.620 |
| 100,000 | 128 | complex64 | cuda | **hybrid** | 225.6 | 24.626 | 47.291 | 56.450 |
| 100,000 | 128 | complex64 | cuda | **sparse** | 2,357.9 | 2.806 | 4.330 | 6.121 |
| 100,000 | 128 | complex64 | cuda | **filtered** | 117.9 | 15.895 | 304.070 | 310.978 |
| 100,000 | 128 | complex64 | cuda | **byid** | 445.8 | 12.153 | 30.671 | 32.175 |
| 100,000 | 128 | complex64 | cuda | **graphrag** | 500.0 | 11.465 | 24.863 | 27.013 |
| 100,000 | 128 | complex64 | cuda | **geo** | 185.4 | 35.541 | 51.136 | 53.042 |
| 100,000 | 128 | complex64 | cuda | **temporal** | 699.0 | 9.828 | 17.173 | 22.082 |
| 100,000 | 128 | complex64 | cuda | **learnedindex** | 259.2 | 18.405 | 34.558 | 46.151 |
| 100,000 | 128 | float16 | cpu | **dense** | 502.1 | 7.112 | 27.411 | 41.781 |
| 100,000 | 128 | float16 | cpu | **hybrid** | 529.5 | 10.063 | 22.440 | 25.811 |
| 100,000 | 128 | float16 | cpu | **sparse** | 2,416.0 | 3.040 | 4.739 | 5.250 |
| 100,000 | 128 | float16 | cpu | **filtered** | 124.6 | 13.368 | 296.342 | 306.503 |
| 100,000 | 128 | float16 | cpu | **byid** | 1,862.0 | 3.575 | 7.750 | 8.137 |
| 100,000 | 128 | float16 | cpu | **graphrag** | 806.6 | 6.060 | 21.665 | 35.321 |
| 100,000 | 128 | float16 | cpu | **geo** | 139.9 | 47.247 | 93.415 | 110.214 |
| 100,000 | 128 | float16 | cpu | **temporal** | 563.0 | 10.156 | 26.190 | 30.025 |
| 100,000 | 128 | float16 | cpu | **learnedindex** | 465.8 | 10.724 | 31.082 | 45.059 |
| 100,000 | 128 | float16 | cuda | **dense** | 579.4 | 9.865 | 22.783 | 23.805 |
| 100,000 | 128 | float16 | cuda | **hybrid** | 450.7 | 11.403 | 24.099 | 28.970 |
| 100,000 | 128 | float16 | cuda | **sparse** | 2,750.7 | 2.286 | 3.985 | 6.852 |
| 100,000 | 128 | float16 | cuda | **filtered** | 137.5 | 9.758 | 295.891 | 316.322 |
| 100,000 | 128 | float16 | cuda | **byid** | 1,749.4 | 3.838 | 7.198 | 7.521 |
| 100,000 | 128 | float16 | cuda | **graphrag** | 523.8 | 11.875 | 40.614 | 47.942 |
| 100,000 | 128 | float16 | cuda | **geo** | 168.0 | 39.939 | 62.078 | 71.647 |
| 100,000 | 128 | float16 | cuda | **temporal** | 530.1 | 10.578 | 24.236 | 27.252 |
| 100,000 | 128 | float16 | cuda | **learnedindex** | 458.5 | 13.771 | 26.752 | 35.699 |
| 100,000 | 128 | float32 | cpu | **dense** | 1,298.0 | 5.185 | 11.145 | 12.278 |
| 100,000 | 128 | float32 | cpu | **hybrid** | 1,063.3 | 4.635 | 11.364 | 14.169 |
| 100,000 | 128 | float32 | cpu | **sparse** | 2,537.4 | 2.795 | 4.532 | 4.834 |
| 100,000 | 128 | float32 | cpu | **filtered** | 189.7 | 5.678 | 233.989 | 240.122 |
| 100,000 | 128 | float32 | cpu | **byid** | 1,103.2 | 5.432 | 10.375 | 12.637 |
| 100,000 | 128 | float32 | cpu | **graphrag** | 1,129.3 | 4.218 | 10.926 | 14.529 |
| 100,000 | 128 | float32 | cpu | **geo** | 171.3 | 41.899 | 58.515 | 71.820 |
| 100,000 | 128 | float32 | cpu | **temporal** | 647.7 | 9.410 | 16.825 | 17.961 |
| 100,000 | 128 | float32 | cpu | **learnedindex** | 1,062.7 | 4.800 | 11.666 | 15.562 |
| 100,000 | 128 | float32 | cuda | **dense** | 1,023.8 | 5.894 | 14.725 | 19.168 |
| 100,000 | 128 | float32 | cuda | **hybrid** | 953.8 | 6.615 | 14.802 | 15.614 |
| 100,000 | 128 | float32 | cuda | **sparse** | 2,284.2 | 2.709 | 5.516 | 6.582 |
| 100,000 | 128 | float32 | cuda | **filtered** | 170.5 | 5.908 | 264.100 | 280.164 |
| 100,000 | 128 | float32 | cuda | **byid** | 1,197.4 | 4.759 | 10.099 | 13.510 |
| 100,000 | 128 | float32 | cuda | **graphrag** | 1,115.5 | 5.073 | 10.984 | 13.156 |
| 100,000 | 128 | float32 | cuda | **geo** | 172.8 | 36.544 | 51.125 | 55.741 |
| 100,000 | 128 | float32 | cuda | **temporal** | 658.7 | 9.934 | 20.734 | 21.938 |
| 100,000 | 128 | float32 | cuda | **learnedindex** | 719.5 | 6.537 | 15.623 | 17.933 |
| 100,000 | 128 | float64 | cpu | **dense** | 482.3 | 10.793 | 23.742 | 27.848 |
| 100,000 | 128 | float64 | cpu | **hybrid** | 422.7 | 13.032 | 26.454 | 29.765 |
| 100,000 | 128 | float64 | cpu | **sparse** | 2,466.4 | 2.401 | 4.950 | 5.458 |
| 100,000 | 128 | float64 | cpu | **filtered** | 129.6 | 25.619 | 238.421 | 247.967 |
| 100,000 | 128 | float64 | cpu | **byid** | 1,613.8 | 3.878 | 8.007 | 8.638 |
| 100,000 | 128 | float64 | cpu | **graphrag** | 303.0 | 20.823 | 41.057 | 44.412 |
| 100,000 | 128 | float64 | cpu | **geo** | 155.6 | 43.988 | 64.188 | 76.251 |
| 100,000 | 128 | float64 | cpu | **temporal** | 710.5 | 8.269 | 19.792 | 26.310 |
| 100,000 | 128 | float64 | cpu | **learnedindex** | 367.5 | 17.214 | 39.785 | 43.841 |
| 100,000 | 128 | float64 | cuda | **dense** | 316.6 | 10.382 | 26.644 | 30.908 |
| 100,000 | 128 | float64 | cuda | **hybrid** | 344.5 | 16.287 | 41.138 | 47.378 |
| 100,000 | 128 | float64 | cuda | **sparse** | 2,759.6 | 2.721 | 3.731 | 4.335 |
| 100,000 | 128 | float64 | cuda | **filtered** | 127.3 | 20.457 | 263.388 | 271.399 |
| 100,000 | 128 | float64 | cuda | **byid** | 1,757.7 | 3.357 | 6.233 | 6.991 |
| 100,000 | 128 | float64 | cuda | **graphrag** | 358.1 | 17.127 | 42.470 | 45.114 |
| 100,000 | 128 | float64 | cuda | **geo** | 165.0 | 37.638 | 97.697 | 103.793 |
| 100,000 | 128 | float64 | cuda | **temporal** | 716.5 | 7.073 | 17.507 | 21.356 |
| 100,000 | 128 | float64 | cuda | **learnedindex** | 210.5 | 24.313 | 55.207 | 61.732 |
| 100,000 | 128 | int16 | cpu | **dense** | 554.2 | 11.452 | 26.623 | 27.154 |
| 100,000 | 128 | int16 | cpu | **hybrid** | 664.3 | 8.348 | 18.615 | 20.731 |
| 100,000 | 128 | int16 | cpu | **sparse** | 3,167.5 | 2.189 | 3.945 | 4.078 |
| 100,000 | 128 | int16 | cpu | **filtered** | 170.6 | 8.619 | 239.415 | 245.847 |
| 100,000 | 128 | int16 | cpu | **byid** | 2,343.9 | 2.858 | 5.125 | 6.136 |
| 100,000 | 128 | int16 | cpu | **graphrag** | 636.6 | 8.428 | 19.766 | 23.121 |
| 100,000 | 128 | int16 | cpu | **geo** | 143.3 | 38.755 | 110.080 | 117.421 |
| 100,000 | 128 | int16 | cpu | **temporal** | 632.4 | 11.234 | 22.836 | 25.133 |
| 100,000 | 128 | int16 | cpu | **learnedindex** | 626.0 | 9.345 | 20.257 | 26.838 |
| 100,000 | 128 | int16 | cuda | **dense** | 695.2 | 8.715 | 16.197 | 21.080 |
| 100,000 | 128 | int16 | cuda | **hybrid** | 704.2 | 7.445 | 18.406 | 20.081 |
| 100,000 | 128 | int16 | cuda | **sparse** | 2,400.0 | 2.949 | 4.252 | 4.725 |
| 100,000 | 128 | int16 | cuda | **filtered** | 156.4 | 11.210 | 250.497 | 258.278 |
| 100,000 | 128 | int16 | cuda | **byid** | 2,075.1 | 3.180 | 5.616 | 6.526 |
| 100,000 | 128 | int16 | cuda | **graphrag** | 528.0 | 11.902 | 23.855 | 26.395 |
| 100,000 | 128 | int16 | cuda | **geo** | 195.9 | 33.472 | 52.590 | 56.939 |
| 100,000 | 128 | int16 | cuda | **temporal** | 677.2 | 10.029 | 16.925 | 25.981 |
| 100,000 | 128 | int16 | cuda | **learnedindex** | 414.5 | 10.434 | 36.977 | 48.604 |
| 100,000 | 128 | int32 | cpu | **dense** | 2,181.8 | 3.458 | 5.142 | 5.449 |
| 100,000 | 128 | int32 | cpu | **hybrid** | 1,957.2 | 3.954 | 5.913 | 6.072 |
| 100,000 | 128 | int32 | cpu | **sparse** | 2,597.0 | 2.459 | 4.026 | 4.704 |
| 100,000 | 128 | int32 | cpu | **filtered** | 190.7 | 2.443 | 244.593 | 248.786 |
| 100,000 | 128 | int32 | cpu | **byid** | 2,189.3 | 3.056 | 4.835 | 6.362 |
| 100,000 | 128 | int32 | cpu | **graphrag** | 2,379.4 | 3.105 | 4.563 | 5.147 |
| 100,000 | 128 | int32 | cpu | **geo** | 142.5 | 45.577 | 102.067 | 129.046 |
| 100,000 | 128 | int32 | cpu | **temporal** | 613.1 | 10.114 | 20.351 | 22.245 |
| 100,000 | 128 | int32 | cpu | **learnedindex** | 2,141.7 | 3.564 | 4.977 | 5.271 |
| 100,000 | 128 | int32 | cuda | **dense** | 924.6 | 6.525 | 12.960 | 16.745 |
| 100,000 | 128 | int32 | cuda | **hybrid** | 977.0 | 4.957 | 14.204 | 14.512 |
| 100,000 | 128 | int32 | cuda | **sparse** | 3,340.3 | 2.156 | 3.292 | 3.510 |
| 100,000 | 128 | int32 | cuda | **filtered** | 146.6 | 6.390 | 299.820 | 304.119 |
| 100,000 | 128 | int32 | cuda | **byid** | 1,925.9 | 3.430 | 5.493 | 5.930 |
| 100,000 | 128 | int32 | cuda | **graphrag** | 864.6 | 6.705 | 14.585 | 14.902 |
| 100,000 | 128 | int32 | cuda | **geo** | 164.6 | 40.243 | 99.152 | 101.322 |
| 100,000 | 128 | int32 | cuda | **temporal** | 651.2 | 9.842 | 20.380 | 22.449 |
| 100,000 | 128 | int32 | cuda | **learnedindex** | 861.5 | 6.882 | 16.753 | 19.029 |
| 100,000 | 128 | int64 | cpu | **dense** | 350.6 | 16.157 | 28.692 | 34.916 |
| 100,000 | 128 | int64 | cpu | **hybrid** | 345.7 | 18.152 | 34.719 | 38.288 |
| 100,000 | 128 | int64 | cpu | **sparse** | 2,225.3 | 3.123 | 5.644 | 5.887 |
| 100,000 | 128 | int64 | cpu | **filtered** | 103.2 | 26.907 | 296.776 | 305.227 |
| 100,000 | 128 | int64 | cpu | **byid** | 1,545.9 | 4.100 | 7.665 | 8.337 |
| 100,000 | 128 | int64 | cpu | **graphrag** | 296.0 | 23.830 | 43.331 | 53.303 |
| 100,000 | 128 | int64 | cpu | **geo** | 151.2 | 47.232 | 65.889 | 66.350 |
| 100,000 | 128 | int64 | cpu | **temporal** | 486.5 | 12.655 | 25.302 | 32.338 |
| 100,000 | 128 | int64 | cpu | **learnedindex** | 237.1 | 21.701 | 44.380 | 48.505 |
| 100,000 | 128 | int64 | cuda | **dense** | 463.2 | 12.159 | 28.989 | 39.386 |
| 100,000 | 128 | int64 | cuda | **hybrid** | 260.2 | 19.389 | 36.540 | 39.120 |
| 100,000 | 128 | int64 | cuda | **sparse** | 2,487.1 | 3.065 | 4.941 | 5.188 |
| 100,000 | 128 | int64 | cuda | **filtered** | 116.7 | 20.846 | 279.437 | 293.008 |
| 100,000 | 128 | int64 | cuda | **byid** | 2,013.7 | 3.104 | 5.936 | 7.075 |
| 100,000 | 128 | int64 | cuda | **graphrag** | 249.0 | 18.846 | 46.430 | 50.734 |
| 100,000 | 128 | int64 | cuda | **geo** | 160.3 | 33.810 | 105.441 | 135.108 |
| 100,000 | 128 | int64 | cuda | **temporal** | 659.5 | 9.545 | 20.608 | 25.365 |
| 100,000 | 128 | int64 | cuda | **learnedindex** | 315.6 | 18.167 | 44.299 | 45.345 |
| 100,000 | 128 | int8 | cpu | **dense** | 674.2 | 8.296 | 17.986 | 20.755 |
| 100,000 | 128 | int8 | cpu | **hybrid** | 668.2 | 8.758 | 19.243 | 19.644 |
| 100,000 | 128 | int8 | cpu | **sparse** | 2,742.5 | 2.817 | 3.876 | 4.785 |
| 100,000 | 128 | int8 | cpu | **filtered** | 143.0 | 10.148 | 267.384 | 289.745 |
| 100,000 | 128 | int8 | cpu | **byid** | 2,271.8 | 3.253 | 4.957 | 6.685 |
| 100,000 | 128 | int8 | cpu | **graphrag** | 676.6 | 7.004 | 19.967 | 23.939 |
| 100,000 | 128 | int8 | cpu | **geo** | 176.3 | 41.088 | 55.186 | 63.866 |
| 100,000 | 128 | int8 | cpu | **temporal** | 545.5 | 11.954 | 25.267 | 26.968 |
| 100,000 | 128 | int8 | cpu | **learnedindex** | 352.6 | 14.665 | 30.446 | 39.488 |
| 100,000 | 128 | int8 | cuda | **dense** | 571.6 | 9.088 | 20.303 | 20.640 |
| 100,000 | 128 | int8 | cuda | **hybrid** | 651.6 | 7.713 | 19.697 | 25.629 |
| 100,000 | 128 | int8 | cuda | **sparse** | 2,966.2 | 2.077 | 4.214 | 5.366 |
| 100,000 | 128 | int8 | cuda | **filtered** | 148.2 | 12.475 | 268.823 | 283.976 |
| 100,000 | 128 | int8 | cuda | **byid** | 1,905.3 | 3.459 | 6.773 | 7.742 |
| 100,000 | 128 | int8 | cuda | **graphrag** | 437.6 | 11.097 | 22.498 | 23.564 |
| 100,000 | 128 | int8 | cuda | **geo** | 131.8 | 50.943 | 115.069 | 134.228 |
| 100,000 | 128 | int8 | cuda | **temporal** | 539.3 | 11.417 | 21.790 | 33.301 |
| 100,000 | 128 | int8 | cuda | **learnedindex** | 611.7 | 8.980 | 21.143 | 22.791 |
| 100,000 | 128 | turboquant | cpu | **dense** | 1,248.0 | 4.437 | 11.707 | 17.103 |
| 100,000 | 128 | turboquant | cpu | **hybrid** | 1,160.6 | 4.596 | 10.780 | 17.226 |
| 100,000 | 128 | turboquant | cpu | **sparse** | 2,511.0 | 2.789 | 5.074 | 5.662 |
| 100,000 | 128 | turboquant | cpu | **filtered** | 183.3 | 3.790 | 252.684 | 257.472 |
| 100,000 | 128 | turboquant | cpu | **byid** | 1,591.7 | 3.699 | 10.167 | 12.207 |
| 100,000 | 128 | turboquant | cpu | **graphrag** | 1,311.5 | 4.922 | 13.321 | 13.734 |
| 100,000 | 128 | turboquant | cpu | **geo** | 135.9 | 46.637 | 111.772 | 120.978 |
| 100,000 | 128 | turboquant | cpu | **temporal** | 602.6 | 8.661 | 18.196 | 23.715 |
| 100,000 | 128 | turboquant | cpu | **learnedindex** | 1,535.9 | 3.824 | 9.161 | 9.553 |
| 100,000 | 128 | turboquant | cuda | **dense** | 332.9 | 19.232 | 35.915 | 37.295 |
| 100,000 | 128 | turboquant | cuda | **hybrid** | 273.9 | 24.066 | 40.989 | 48.402 |
| 100,000 | 128 | turboquant | cuda | **sparse** | 2,184.1 | 3.193 | 5.486 | 6.801 |
| 100,000 | 128 | turboquant | cuda | **filtered** | 102.5 | 21.376 | 338.431 | 345.139 |
| 100,000 | 128 | turboquant | cuda | **byid** | 293.8 | 20.016 | 44.646 | 54.222 |
| 100,000 | 128 | turboquant | cuda | **graphrag** | 303.8 | 19.918 | 39.114 | 45.346 |
| 100,000 | 128 | turboquant | cuda | **geo** | 154.7 | 40.902 | 88.104 | 97.971 |
| 100,000 | 128 | turboquant | cuda | **temporal** | 629.4 | 7.294 | 21.349 | 27.440 |
| 100,000 | 128 | turboquant | cuda | **learnedindex** | 338.1 | 16.938 | 39.067 | 43.218 |
| 100,000 | 128 | uint16 | cpu | **dense** | 539.2 | 11.513 | 18.609 | 21.728 |
| 100,000 | 128 | uint16 | cpu | **hybrid** | 561.1 | 10.953 | 20.048 | 25.029 |
| 100,000 | 128 | uint16 | cpu | **sparse** | 2,029.3 | 3.670 | 4.964 | 5.250 |
| 100,000 | 128 | uint16 | cpu | **filtered** | 134.1 | 12.936 | 283.653 | 293.594 |
| 100,000 | 128 | uint16 | cpu | **byid** | 1,690.9 | 4.291 | 6.482 | 7.039 |
| 100,000 | 128 | uint16 | cpu | **graphrag** | 458.0 | 12.539 | 24.654 | 27.205 |
| 100,000 | 128 | uint16 | cpu | **geo** | 142.9 | 41.902 | 115.063 | 117.959 |
| 100,000 | 128 | uint16 | cpu | **temporal** | 573.8 | 10.323 | 24.256 | 26.720 |
| 100,000 | 128 | uint16 | cpu | **learnedindex** | 548.8 | 10.627 | 21.047 | 23.303 |
| 100,000 | 128 | uint16 | cuda | **dense** | 2,622.6 | 2.485 | 4.686 | 5.311 |
| 100,000 | 128 | uint16 | cuda | **hybrid** | 2,491.9 | 2.567 | 4.324 | 4.629 |
| 100,000 | 128 | uint16 | cuda | **sparse** | 3,644.0 | 1.886 | 3.146 | 3.480 |
| 100,000 | 128 | uint16 | cuda | **filtered** | 206.1 | 1.668 | 231.240 | 240.417 |
| 100,000 | 128 | uint16 | cuda | **byid** | 2,419.7 | 3.086 | 4.864 | 5.773 |
| 100,000 | 128 | uint16 | cuda | **graphrag** | 2,927.9 | 2.189 | 3.881 | 4.271 |
| 100,000 | 128 | uint16 | cuda | **geo** | 158.2 | 40.411 | 69.631 | 76.691 |
| 100,000 | 128 | uint16 | cuda | **temporal** | 501.8 | 10.840 | 25.704 | 28.276 |
| 100,000 | 128 | uint16 | cuda | **learnedindex** | 2,464.1 | 2.866 | 4.781 | 4.955 |
| 100,000 | 128 | uint32 | cpu | **dense** | 269.2 | 18.659 | 41.401 | 45.909 |
| 100,000 | 128 | uint32 | cpu | **hybrid** | 239.8 | 23.385 | 38.693 | 43.160 |
| 100,000 | 128 | uint32 | cpu | **sparse** | 2,126.8 | 3.324 | 5.274 | 6.083 |
| 100,000 | 128 | uint32 | cpu | **filtered** | 122.0 | 24.051 | 267.950 | 301.681 |
| 100,000 | 128 | uint32 | cpu | **byid** | 2,049.5 | 3.104 | 5.631 | 6.351 |
| 100,000 | 128 | uint32 | cpu | **graphrag** | 324.1 | 17.073 | 36.782 | 46.510 |
| 100,000 | 128 | uint32 | cpu | **geo** | 178.6 | 36.553 | 55.347 | 58.617 |
| 100,000 | 128 | uint32 | cpu | **temporal** | 450.7 | 11.599 | 23.005 | 30.444 |
| 100,000 | 128 | uint32 | cpu | **learnedindex** | 302.4 | 20.450 | 37.310 | 39.624 |
| 100,000 | 128 | uint32 | cuda | **dense** | 958.6 | 7.159 | 13.563 | 14.406 |
| 100,000 | 128 | uint32 | cuda | **hybrid** | 703.8 | 8.075 | 15.542 | 16.498 |
| 100,000 | 128 | uint32 | cuda | **sparse** | 2,792.1 | 2.429 | 5.005 | 5.163 |
| 100,000 | 128 | uint32 | cuda | **filtered** | 166.3 | 9.679 | 242.121 | 250.475 |
| 100,000 | 128 | uint32 | cuda | **byid** | 2,271.4 | 2.942 | 4.964 | 5.071 |
| 100,000 | 128 | uint32 | cuda | **graphrag** | 599.2 | 9.967 | 18.711 | 20.957 |
| 100,000 | 128 | uint32 | cuda | **geo** | 141.3 | 44.861 | 106.001 | 123.406 |
| 100,000 | 128 | uint32 | cuda | **temporal** | 596.4 | 8.523 | 24.250 | 29.887 |
| 100,000 | 128 | uint32 | cuda | **learnedindex** | 493.4 | 12.050 | 21.635 | 23.528 |
| 100,000 | 128 | uint64 | cpu | **dense** | 662.9 | 7.462 | 15.357 | 16.972 |
| 100,000 | 128 | uint64 | cpu | **hybrid** | 415.2 | 9.471 | 27.304 | 34.971 |
| 100,000 | 128 | uint64 | cpu | **sparse** | 2,602.0 | 2.541 | 5.176 | 5.340 |
| 100,000 | 128 | uint64 | cpu | **filtered** | 122.4 | 22.094 | 260.710 | 267.078 |
| 100,000 | 128 | uint64 | cpu | **byid** | 1,613.2 | 3.521 | 7.689 | 9.320 |
| 100,000 | 128 | uint64 | cpu | **graphrag** | 188.9 | 25.154 | 51.602 | 57.223 |
| 100,000 | 128 | uint64 | cpu | **geo** | 129.7 | 48.708 | 114.609 | 126.428 |
| 100,000 | 128 | uint64 | cpu | **temporal** | 597.5 | 11.762 | 21.082 | 25.203 |
| 100,000 | 128 | uint64 | cpu | **learnedindex** | 163.5 | 29.114 | 66.305 | 81.434 |
| 100,000 | 128 | uint64 | cuda | **dense** | 249.3 | 16.533 | 43.565 | 49.918 |
| 100,000 | 128 | uint64 | cuda | **hybrid** | 207.2 | 31.081 | 52.997 | 56.729 |
| 100,000 | 128 | uint64 | cuda | **sparse** | 2,994.3 | 2.374 | 4.928 | 5.044 |
| 100,000 | 128 | uint64 | cuda | **filtered** | 100.6 | 39.651 | 278.517 | 294.492 |
| 100,000 | 128 | uint64 | cuda | **byid** | 1,379.4 | 4.355 | 9.382 | 12.172 |
| 100,000 | 128 | uint64 | cuda | **graphrag** | 159.7 | 35.944 | 72.413 | 77.399 |
| 100,000 | 128 | uint64 | cuda | **geo** | 172.0 | 36.217 | 52.974 | 57.135 |
| 100,000 | 128 | uint64 | cuda | **temporal** | 505.0 | 11.023 | 26.390 | 28.194 |
| 100,000 | 128 | uint64 | cuda | **learnedindex** | 177.1 | 29.354 | 69.263 | 71.682 |
| 100,000 | 128 | uint8 | cpu | **dense** | 806.7 | 6.895 | 15.488 | 18.904 |
| 100,000 | 128 | uint8 | cpu | **hybrid** | 587.0 | 9.899 | 17.522 | 18.442 |
| 100,000 | 128 | uint8 | cpu | **sparse** | 2,419.9 | 2.446 | 5.554 | 5.708 |
| 100,000 | 128 | uint8 | cpu | **filtered** | 152.1 | 9.885 | 260.965 | 277.002 |
| 100,000 | 128 | uint8 | cpu | **byid** | 1,941.1 | 3.487 | 6.053 | 7.698 |
| 100,000 | 128 | uint8 | cpu | **graphrag** | 637.3 | 9.198 | 22.661 | 23.479 |
| 100,000 | 128 | uint8 | cpu | **geo** | 136.0 | 41.923 | 116.830 | 124.658 |
| 100,000 | 128 | uint8 | cpu | **temporal** | 570.6 | 10.675 | 22.616 | 25.718 |
| 100,000 | 128 | uint8 | cpu | **learnedindex** | 663.5 | 7.568 | 19.414 | 21.931 |
| 100,000 | 128 | uint8 | cuda | **dense** | 1,760.9 | 4.008 | 6.623 | 7.878 |
| 100,000 | 128 | uint8 | cuda | **hybrid** | 898.2 | 5.200 | 15.057 | 16.008 |
| 100,000 | 128 | uint8 | cuda | **sparse** | 2,672.4 | 2.530 | 4.264 | 4.561 |
| 100,000 | 128 | uint8 | cuda | **filtered** | 197.3 | 1.974 | 240.385 | 243.730 |
| 100,000 | 128 | uint8 | cuda | **byid** | 3,337.1 | 2.088 | 2.865 | 3.652 |
| 100,000 | 128 | uint8 | cuda | **graphrag** | 1,309.8 | 4.092 | 8.603 | 10.421 |
| 100,000 | 128 | uint8 | cuda | **geo** | 144.0 | 38.551 | 113.416 | 120.176 |
| 100,000 | 128 | uint8 | cuda | **temporal** | 608.9 | 9.890 | 23.351 | 31.485 |
| 100,000 | 128 | uint8 | cuda | **learnedindex** | 1,699.5 | 3.564 | 6.139 | 7.931 |
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
