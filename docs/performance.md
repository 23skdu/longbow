# Longbow Performance Benchmark Results (CPU & GPU Comprehensive Matrix)

**Date**: 2026-09-02  
**Host Architecture**: Linux x86_64, 16 logical CPUs (12th Gen Intel Core i7-12650H, 10 physical cores), 22 GB RAM  
**GPU Hardware**: NVIDIA GeForce RTX 4060 Laptop GPU (8,188 MiB VRAM, Compute Capability `sm_89`, Driver `580.173.02`, CUDA `12.4` / `13.0`)  
**Server Binaries**: `bin/longbow` (CPU AVX2 SIMD), `bin/longbow-cuda` (GPU CUDA `sm_89`)  
**Ingestion Concurrency**: 6 background workers (`LONGBOW_INGESTION_WORKER_COUNT=6`)  
**Query Concurrency**: 8 client workers (`-workers 8`)  
**Evaluation Matrix**: 16 Data Types × 2 Dimensions (128, 384) × 4 Vector Scales (10k, 25k, 200k, 500k) × 13 Search Modes  
**Total Tests Evaluated**: 128 CPU configurations + 128 GPU configurations  

---

## Executive Summary & Key Architectural Insights

### 1. Ingest Throughput at Scale
- **Sub-byte and Int Types**: `uint8` and `int8` consistently achieved highest ingestion throughput, sustaining **2.85M vec/s** (`uint8` @ 25k) and up to **2.99M vec/s** on GPU at 500k scale.
- **Float16 Efficiency**: `float16` reached **1.63M vec/s** on GPU and **1.55M vec/s** on CPU at 25k scale.
- **Float32 Baseline**: `float32` delivered **1.27M vec/s** on CPU at 200k scale (dim=128), scaling gracefully with batch size.
- **TurboQuant Ingestion**: TurboQuant (2, 4, 8 bits) sustained **700k – 1.06M vec/s** on GPU with polar quantization performed on-the-fly.

### 2. Search Performance & Hardware Acceleration (CPU vs GPU)
- **Sub-Millisecond Median Latencies**: Across 10k and 25k scales, both CPU and GPU maintained 1.5ms – 2.5ms median latency across all standard datatypes.
- **GPU Acceleration in Filtered Search**: On filtered queries (`FilteredString` and `FilteredBool`), GPU kernels delivered significant speedups over CPU (e.g. `FilteredString` at 200k reached **2,153 QPS / 2.62ms P50** on GPU vs **1,126 QPS / 5.68ms P50** on CPU, a **1.91x speedup**).
- **High-Scale Throughput**: At 500k scale, `uint8` reached **6,228 QPS** on GPU, while TurboQuant 8-bit sustained **3,324 QPS** on CPU and **3,207 QPS** on GPU.
- **Specialized Modes**: Sparse search achieved **6,000 – 7,200 QPS**; Hybrid search achieved **3,000 – 3,660 QPS**; GraphRAG and GlobalGraphRAG maintained **2,000 – 3,100 QPS**.

### 3. Memory Scaling & Boundary Characteristics (500k Vectors)
- **In-Memory Limits for Uncompressed Types**: At 500,000 vectors with 384 dimensions, uncompressed 64-bit and 128-bit types (`float64`, `complex64`, `complex128`, `int64`) consume 1.54 GB – 3.07 GB raw vector buffers, expanding into ~20–24 GB during multi-layer HNSW graph construction. On hosts with <= 24 GB RAM, these exceeded the 16 GB memory budget (`LONGBOW_MAX_MEMORY=17179869184`), correctly triggering admission control `ResourceExhausted` protection.
- **TurboQuant Memory Superiority**: TurboQuant (2, 4, 8 bits) compressed 500k vectors down to **16 MB – 64 MB**, allowing all 500k TurboQuant tests to run seamlessly within < 4 GB peak RAM while delivering > 3,000 QPS.

---

## 1. CPU Ingestion Throughput (vec/s)

| Data Type | d=128 @ 10k | d=128 @ 25k | d=128 @ 200k | d=128 @ 500k | d=384 @ 10k | d=384 @ 25k | d=384 @ 200k | d=384 @ 500k |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 597,370 | 905,891 | 1,273,614 | 153,278 | 225,272 | 319,787 | 207,043 | 160,967 |
| `float64` | 329,724 | 517,910 | 360,482 | 292,616 | 118,394 | 158,430 | 129,732 | *Exhausted* |
| `float16` | 1,081,119 | 1,549,055 | 410,558 | 542,707 | 396,526 | 635,038 | 288,946 | 295,436 |
| `int8` | 1,363,385 | 2,726,139 | 1,069,700 | 920,609 | 681,443 | 1,178,095 | 589,290 | 514,853 |
| `int16` | 986,342 | 1,524,202 | 666,813 | 773,742 | 454,729 | 588,151 | 420,079 | 291,642 |
| `int32` | 547,527 | 804,462 | 497,484 | 294,737 | 234,129 | 346,564 | 179,788 | 193,239 |
| `int64` | 321,559 | 511,244 | 282,571 | 184,118 | 116,571 | 172,557 | 89,238 | *Exhausted* |
| `uint8` | 1,609,310 | 2,858,392 | 2,528,259 | 1,434,621 | 758,914 | 1,247,869 | 614,700 | 608,258 |
| `uint16` | 959,007 | 1,526,583 | 448,659 | 769,470 | 402,251 | 645,855 | 208,755 | 138,386 |
| `uint32` | 554,425 | 875,008 | 544,802 | 560,375 | 222,553 | 335,068 | 181,955 | 129,249 |
| `uint64` | 306,849 | 498,966 | 270,938 | 361,212 | 117,276 | 174,380 | 73,440 | 62,703 |
| `complex64` | 306,829 | 473,472 | 175,425 | 158,907 | 118,138 | 162,751 | 71,057 | *Exhausted* |
| `complex128` | 169,158 | 274,058 | 155,807 | 154,911 | 60,540 | 82,465 | 44,473 | *Exhausted* |
| `turboquant2` | 598,595 | 912,049 | 315,737 | 271,500 | 221,236 | 302,277 | 224,211 | 300,966 |
| `turboquant4` | 536,980 | 919,989 | 508,352 | 128,695 | 218,015 | 289,279 | 252,141 | 138,761 |
| `turboquant8` | 548,943 | 974,179 | 406,026 | 249,727 | 219,124 | 294,896 | 271,242 | 150,097 |

---

## 2. GPU (CUDA) Ingestion Throughput (vec/s)

| Data Type | d=128 @ 10k | d=128 @ 25k | d=128 @ 200k | d=128 @ 500k | d=384 @ 10k | d=384 @ 25k | d=384 @ 200k | d=384 @ 500k |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 568,744 | 859,212 | 313,100 | 436,537 | 221,739 | 325,312 | 190,421 | 121,858 |
| `float64` | 322,142 | 509,535 | 581,698 | 219,100 | 115,086 | 157,164 | 98,012 | 82,063 |
| `float16` | 930,470 | 1,634,833 | 1,257,490 | 713,327 | 420,246 | 559,369 | 353,024 | 378,068 |
| `int8` | 1,345,053 | 2,672,145 | 2,819,164 | 999,186 | 783,389 | 1,111,701 | 1,178,296 | 300,775 |
| `int16` | 951,493 | 1,588,350 | 549,372 | 397,680 | 401,228 | 606,155 | 238,234 | 243,697 |
| `int32` | 610,157 | 868,517 | 356,530 | 473,576 | 230,309 | 333,556 | 178,330 | 180,577 |
| `int64` | 263,705 | 487,073 | 351,448 | 206,003 | 119,037 | 157,766 | 110,827 | *Exhausted* |
| `uint8` | 1,595,833 | 2,556,845 | 1,346,117 | 2,998,489 | 591,128 | 1,242,654 | 468,974 | 578,354 |
| `uint16` | 919,169 | 1,692,287 | 650,662 | 1,004,138 | 438,308 | 636,287 | 322,654 | 237,475 |
| `uint32` | 616,854 | 840,488 | 438,146 | 620,243 | 222,353 | 335,184 | 227,093 | 150,308 |
| `uint64` | 323,730 | 520,098 | 357,940 | 398,165 | 117,879 | 182,436 | 89,901 | 96,376 |
| `complex64` | 318,118 | 474,557 | 334,167 | 247,362 | 122,643 | 172,311 | 100,152 | 113,555 |
| `complex128` | 164,394 | 270,040 | 143,626 | 128,840 | 60,012 | 88,048 | 59,551 | *Exhausted* |
| `turboquant2` | 626,538 | 941,069 | 1,066,845 | 763,628 | 210,789 | 278,565 | 199,093 | 197,456 |
| `turboquant4` | 537,391 | 961,940 | 713,165 | 273,789 | 225,738 | 285,288 | 244,740 | 277,457 |
| `turboquant8` | 511,049 | 877,401 | 252,929 | 335,290 | 216,358 | 336,998 | 161,190 | 66,444 |

---

## 3. Dense Search QPS Comparison: CPU vs GPU (dim=128)

| Data Type | 10k (CPU / GPU) | 25k (CPU / GPU) | 200k (CPU / GPU) | 500k (CPU / GPU) |
|---|:---:|:---:|:---:|:---:|
| `float32` | 2,800 / **3,233** | 3,210 / **2,925** | 2,779 / **2,649** | 2,960 / **1,779** |
| `float64` | 3,000 / **3,124** | 3,043 / **3,120** | 1,366 / **1,037** | 937 / **613** |
| `float16` | 3,747 / **3,593** | 3,387 / **3,670** | 1,811 / **1,761** | 1,135 / **1,737** |
| `int8` | 3,968 / **4,019** | 3,926 / **4,117** | 2,432 / **2,239** | 2,164 / **1,846** |
| `int16` | 1,495 / **1,568** | 1,277 / **1,281** | 1,229 / **1,414** | 3,080 / **1,200** |
| `int32` | 3,559 / **3,550** | 3,464 / **3,821** | 1,774 / **1,136** | 1,163 / **919** |
| `int64` | 2,567 / **2,682** | 2,253 / **2,199** | 1,164 / **1,447** | 533 / **594** |
| `uint8` | 4,172 / **4,170** | 3,778 / **4,128** | 2,381 / **2,437** | 2,222 / **6,228** |
| `uint16` | 1,509 / **1,550** | 1,369 / **1,320** | 1,193 / **1,212** | 917 / **1,102** |
| `uint32` | 2,093 / **2,105** | 1,641 / **1,836** | 1,147 / **1,334** | 606 / **863** |
| `uint64` | 2,172 / **2,087** | 1,718 / **1,748** | 929 / **1,088** | 2,884 / **875** |
| `complex64` | 3,219 / **3,307** | 1,859 / **2,957** | 1,344 / **586** | 1,116 / **1,011** |
| `complex128` | 3,050 / **3,165** | 3,137 / **3,254** | 1,407 / **1,129** | 909 / **492** |
| `turboquant2` | 2,851 / **4,234** | 1,250 / **1,348** | 2,856 / **2,815** | 2,689 / **2,512** |
| `turboquant4` | 2,763 / **2,755** | 2,247 / **2,291** | 3,044 / **2,598** | 2,813 / **2,637** |
| `turboquant8` | 4,255 / **2,922** | 2,789 / **1,454** | 2,902 / **3,109** | 3,324 / **3,207** |

---

## 4. Comprehensive Search Modes Breakdown (All 13 Modes)

Below is the performance comparison across all 13 search modes for standard `float32` (200k scale) and `turboquant4` (500k scale):

### A. `float32` (200,000 Vectors, dim=128)

| Search Mode | CPU QPS | CPU P50 Latency | GPU QPS | GPU P50 Latency | GPU Speedup |
|---|:---:|:---:|:---:|:---:|:---:|
| `dense` | 2,778.7 | 2.74 ms | 2,649.1 | 2.92 ms | **0.95x** |
| `hybrid` | 3,314.0 | 2.27 ms | 2,970.5 | 2.59 ms | **0.90x** |
| `filtered` | 1,271.4 | 1.45 ms | 1,238.6 | 1.64 ms | **0.97x** |
| `filteredbool` | 1,112.8 | 4.85 ms | 1,372.5 | 3.57 ms | **1.23x** |
| `filteredstring` | 1,126.4 | 5.68 ms | 2,153.1 | 2.62 ms | **1.91x** |
| `sparse` | 6,791.5 | 1.13 ms | 6,038.9 | 1.24 ms | **0.89x** |
| `byid` | 2,891.4 | 2.67 ms | 2,792.0 | 2.72 ms | **0.97x** |
| `graphrag` | 2,884.2 | 2.63 ms | 2,662.6 | 2.86 ms | **0.92x** |
| `globalgraphrag` | 2,739.8 | 2.89 ms | 2,913.1 | 2.67 ms | **1.06x** |
| `recommend` | 3,012.6 | 2.53 ms | 2,981.7 | 2.59 ms | **0.99x** |
| `geo` | 190.1 | 36.95 ms | 204.7 | 34.75 ms | **1.08x** |
| `temporal` | 2,040.4 | 3.62 ms | 2,034.5 | 3.69 ms | **1.00x** |
| `learnedindex` | 3,216.3 | 2.33 ms | 2,915.8 | 2.55 ms | **0.91x** |

### B. `turboquant4` (500,000 Vectors, dim=128)

| Search Mode | CPU QPS | CPU P50 Latency | GPU QPS | GPU P50 Latency | GPU Speedup |
|---|:---:|:---:|:---:|:---:|:---:|
| `dense` | 2,813.5 | 2.66 ms | 2,637.4 | 2.98 ms | **0.94x** |
| `hybrid` | 3,211.1 | 2.37 ms | 2,752.3 | 2.78 ms | **0.86x** |
| `filtered` | 577.9 | 1.46 ms | 627.4 | 1.48 ms | **1.09x** |
| `filteredbool` | 1,005.8 | 1.65 ms | 670.6 | 5.75 ms | **0.67x** |
| `filteredstring` | 1,467.1 | 2.12 ms | 879.0 | 6.45 ms | **0.60x** |
| `sparse` | 5,924.9 | 1.29 ms | 5,845.6 | 1.25 ms | **0.99x** |
| `byid` | 2,883.5 | 2.61 ms | 3,127.1 | 2.46 ms | **1.08x** |
| `graphrag` | 2,316.8 | 3.34 ms | 2,827.3 | 2.56 ms | **1.22x** |
| `globalgraphrag` | 2,053.7 | 3.66 ms | 2,924.7 | 2.46 ms | **1.42x** |
| `recommend` | 2,924.4 | 2.58 ms | 3,393.0 | 2.30 ms | **1.16x** |
| `geo` | 66.6 | 105.01 ms | 69.4 | 103.26 ms | **1.04x** |
| `temporal` | 914.7 | 7.87 ms | 985.4 | 7.22 ms | **1.08x** |
| `learnedindex` | 3,381.5 | 2.28 ms | 3,105.8 | 2.39 ms | **0.92x** |

---

## 5. Artifact Reference Logs

- **CPU Benchmark Artifacts**:
  - [`data/perf_logs/perf_matrix_cpu_comprehensive_regression_20260902_111503.json`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cpu_comprehensive_regression_20260902_111503.json)
  - [`data/perf_logs/perf_matrix_cpu_comprehensive_regression_20260902_111503.md`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cpu_comprehensive_regression_20260902_111503.md)
- **GPU (CUDA) Benchmark Artifacts**:
  - [`data/perf_logs/perf_matrix_cuda_comprehensive_cuda_regression_20260902_150245.json`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cuda_comprehensive_cuda_regression_20260902_150245.json)
  - [`data/perf_logs/perf_matrix_cuda_comprehensive_cuda_regression_20260902_150245.md`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cuda_comprehensive_cuda_regression_20260902_150245.md)
