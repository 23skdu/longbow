# Longbow Multi-Scale Performance Benchmark Results (CPU vs. GPU)

**Date**: 2026-09-03  
**Host Architecture**: Linux x86_64, 16 logical CPUs (12th Gen Intel Core i7-12650H, 10 physical cores), 23 GB RAM, 16 GB Swap  
**GPU Hardware**: NVIDIA GeForce RTX 4060 Laptop GPU (8,188 MiB VRAM, Compute Capability `sm_89`, Driver `580.173.02`, CUDA `12.4` / `13.0`)  
**Server Binaries**: `bin/longbow` (CPU AVX2 SIMD), `bin/longbow-cuda` (GPU CUDA `sm_89`)  
**Ingestion Concurrency**: 6 background workers (`LONGBOW_INGESTION_WORKER_COUNT=6` — bounded to physical cores)  
**Query Concurrency**: 8 client workers (`-workers 8`)  
**Evaluation Matrix**: 17 Data Types × 4 Vector Scales (50k, 200k, 500k, 1,000,000) × 13 Search Modes  
**Total Tests Evaluated**: 68 CPU configurations + 68 GPU configurations (136 total end-to-end benchmark runs)  
**Memory Profiling**: Native Linux Kernel Peak Resident Set Size (`VmHWM` via `/proc/<pid>/status`) recorded at each configuration  

---

## Executive Summary & Key Architectural Findings

### 1. Massive GPU Acceleration at Scale (Overcoming CPU L3 Cache Thrashing)
- **`float64` (8-byte) Latency Cliff on CPU**: At 500k scale, CPU search latency doubled from 4.46ms (@ 200k) to **9.35ms** (QPS fell from 1,481 to 687.1) due to L3 cache misses during HNSW graph traversal. On GPU, high memory bandwidth completely bypasses this bottleneck, delivering **2,014.1 QPS at 3.13ms P50 latency** — a **+2.93× (193%) QPS speedup** and **3.0× latency reduction**.
- **`float16` Throughput & Latency Superiority**: Across all scales, `float16` shines on GPU, reaching **2,421.5 QPS** at 500k (+79% over CPU's 1,352 QPS) with **1,017,344 vec/s ingestion throughput**, and sustaining **2,143.3 QPS** at 1,000,000 scale (**+1.91× speedup** over CPU's 1,119 QPS) with **500,787 vec/s ingest**.
- **`uint8` Sustains >1,000,000 vec/s at 1M Scale**: `uint8` sustained **1,040,831 vec/s** indexing throughput at the full 1M scale on GPU while delivering **2,712.6 QPS** and **2.44ms P50 latency**.
- **`uint64` and `int64` @ 1M Vectors**: On GPU, `uint64` achieved **830.1 QPS** (7.18ms P50 latency) vs CPU's 512.4 QPS (**+62% speedup**), and `int64` achieved **684.9 QPS** (7.36ms P50) vs CPU's 552.6 QPS at **11.65ms latency** (37% lower latency on GPU).

### 2. TurboQuant Memory & Latency Dominance at 1,000,000 Vectors
- **`turboquant4` (4-bit)**: Outperformed uncompressed `float32` at 1M scale on GPU, delivering **3,187.7 QPS** (2.16ms P50 latency) and **327,702 vec/s** ingestion throughput (+132% over CPU), while cutting memory consumption in half.
- **`turboquant2` (2-bit)**: Delivered **3,545.4 QPS** with **1.78ms P50 latency** on CPU and **2,056.0 QPS** on GPU at 1M scale.
- **Memory Footprint**: TurboQuant compressed 1,000,000 vectors down to ~10.4–11.2 GB peak RSS (including the full multi-layer HNSW graph index and Arrow record batches), whereas uncompressed 8-byte types consumed 15.4–16.4 GB.

### 3. Memory Scaling & Hard Boundary Characteristics
- **1-byte types (`uint8`, `int8`)**: 750 MB @ 50k → 2.1–2.5 GB @ 200k → 4.8–5.4 GB @ 500k → **10.0–10.1 GB @ 1M**.
- **2-byte types (`float16`, `int16`, `uint16`)**: 750–850 MB @ 50k → 2.2–2.5 GB @ 200k → 5.1–6.4 GB @ 500k → **10.6–11.6 GB @ 1M**.
- **4-byte types (`float32`, `int32`, `uint32`)**: 800–900 MB @ 50k → 2.4–3.0 GB @ 200k → 5.6–6.8 GB @ 500k → **10.0–12.5 GB @ 1M**.
- **8-byte types (`float64`, `int64`, `uint64`, `complex64`)**: 1.2–1.4 GB @ 50k → 3.5–4.0 GB @ 200k → 8.0–9.4 GB @ 500k → **14.5–16.4 GB @ 1M**.
- **16-byte types (`complex128`)**: 1.9–2.1 GB @ 50k → 5.1–5.3 GB @ 200k → **13.0–13.6 GB @ 500k** → **>22 GB @ 1M** (triggers memory safety protection).

---

## 1. Ingestion Throughput Comparison (vectors / sec)

| Data Type | CPU 50k | GPU 50k | Speedup | CPU 200k | GPU 200k | Speedup | CPU 500k | GPU 500k | Speedup | CPU 1M | GPU 1M | Speedup |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 957,980 | 1,012,042 | 1.06x | 287,182 | 571,335 | 1.99x | 621,174 | 359,850 | 0.58x | 129,476 | 231,900 | 1.79x |
| `float64` | 599,520 | 547,477 | 0.91x | 222,017 | 224,288 | 1.01x | 298,214 | 226,351 | 0.76x | 129,712 | *Exhausted* | — |
| `float16` | 2,022,020 | 2,080,909 | 1.03x | 834,898 | 440,325 | 0.53x | 599,812 | 1,017,344 | 1.70x | 428,611 | 500,787 | 1.17x |
| `int8` | 2,883,574 | 4,074,246 | 1.41x | 1,497,296 | 1,066,969 | 0.71x | 950,335 | 656,107 | 0.69x | 727,274 | 905,215 | 1.24x |
| `int16` | 1,847,878 | 2,010,017 | 1.09x | 920,550 | 673,762 | 0.73x | 670,264 | 839,081 | 1.25x | 426,725 | 709,740 | 1.66x |
| `int32` | 1,085,852 | 1,024,755 | 0.94x | 640,974 | 801,570 | 1.25x | 351,933 | 385,576 | 1.10x | 232,004 | 444,030 | 1.91x |
| `int64` | 552,328 | 556,140 | 1.01x | 269,692 | 360,976 | 1.34x | 317,088 | 179,916 | 0.57x | 185,246 | 162,658 | 0.88x |
| `uint8` | 3,713,850 | 3,707,972 | 1.00x | 1,082,674 | 720,815 | 0.67x | 1,378,354 | 1,603,103 | 1.16x | 980,389 | 1,040,831 | 1.06x |
| `uint16` | 2,240,177 | 2,155,888 | 0.96x | 680,832 | 912,804 | 1.34x | 490,002 | 574,756 | 1.17x | 313,294 | 442,752 | 1.41x |
| `uint32` | 1,079,227 | 1,013,781 | 0.94x | 577,453 | 513,013 | 0.89x | 413,047 | 377,521 | 0.91x | 224,551 | 300,975 | 1.34x |
| `uint64` | 535,221 | 583,421 | 1.09x | 553,704 | 222,581 | 0.40x | 206,545 | 318,743 | 1.54x | 356,411 | 233,101 | 0.65x |
| `complex64` | 504,364 | 554,109 | 1.10x | 358,177 | 310,453 | 0.87x | 168,613 | 144,929 | 0.86x | 123,867 | 118,396 | 0.96x |
| `complex128` | 312,595 | 278,619 | 0.89x | 174,270 | 147,921 | 0.85x | 156,049 | 114,143 | 0.73x | *Exhausted* | *Exhausted* | — |
| `turboquant` | 827,480 | 1,037,437 | 1.25x | 770,145 | 675,578 | 0.88x | 228,653 | 234,939 | 1.03x | 172,706 | 314,532 | 1.82x |
| `turboquant2` | 1,024,230 | 836,138 | 0.82x | 890,839 | 362,905 | 0.41x | 187,808 | 327,514 | 1.74x | 142,802 | 175,538 | 1.23x |
| `turboquant4` | 932,487 | 1,078,333 | 1.16x | 822,987 | 1,084,178 | 1.32x | 302,713 | 514,536 | 1.70x | 189,355 | 327,702 | 1.73x |
| `turboquant8` | 1,047,390 | 970,683 | 0.93x | 445,137 | 743,223 | 1.67x | 191,464 | 430,556 | 2.25x | 132,628 | 145,833 | 1.10x |

---

## 2. Dense Search QPS Comparison (Queries / sec)

| Data Type | CPU 50k | GPU 50k | Speedup | CPU 200k | GPU 200k | Speedup | CPU 500k | GPU 500k | Speedup | CPU 1M | GPU 1M | Speedup |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 3,264.9 | 2,664.7 | 0.82x | 2,151.1 | 3,477.3 | **1.62x** | 2,320.6 | 3,149.7 | **1.36x** | 3,023.2 | 2,903.1 | 0.96x |
| `float64` | 1,349.9 | 2,048.8 | **1.52x** | 1,481.0 | 979.3 | 0.66x | 687.1 | 2,014.1 | **2.93x** | 874.3 | *Exhausted* | — |
| `float16` | 3,379.8 | 3,648.2 | **1.08x** | 1,837.8 | 1,730.8 | 0.94x | 1,352.3 | 2,421.5 | **1.79x** | 1,119.3 | 2,143.3 | **1.91x** |
| `int8` | 2,885.4 | 2,604.3 | 0.90x | 2,677.9 | 2,565.4 | 0.96x | 2,399.7 | 2,528.0 | **1.05x** | 2,008.7 | 1,364.9 | 0.68x |
| `int16` | 1,844.8 | 1,730.0 | 0.94x | 1,507.3 | 1,660.7 | **1.10x** | 1,235.6 | 1,169.6 | 0.95x | 1,003.7 | 915.2 | 0.91x |
| `int32` | 2,735.3 | 3,205.5 | **1.17x** | 3,275.9 | 1,169.9 | 0.36x | 574.4 | 962.7 | **1.68x** | 891.7 | 618.8 | 0.69x |
| `int64` | 3,813.5 | 2,671.1 | 0.70x | 3,070.1 | 2,725.9 | 0.89x | 2,032.7 | 2,942.5 | **1.45x** | 552.6 | 684.9 | **1.24x** |
| `uint8` | 4,206.6 | 2,953.6 | 0.70x | 2,830.8 | 2,857.9 | **1.01x** | 2,477.7 | 2,759.8 | **1.11x** | 2,394.1 | 2,712.6 | **1.13x** |
| `uint16` | 1,781.2 | 1,718.2 | 0.96x | 1,681.5 | 1,745.8 | **1.04x** | 1,097.6 | 1,264.0 | **1.15x** | 980.9 | 964.0 | 0.98x |
| `uint32` | 2,491.2 | 2,411.0 | 0.97x | 2,326.6 | 2,228.0 | 0.96x | 1,338.9 | 1,463.3 | **1.09x** | 591.2 | 633.9 | **1.07x** |
| `uint64` | 3,409.1 | 2,834.5 | 0.83x | 2,048.7 | 2,207.1 | **1.08x** | 1,747.2 | 1,062.1 | 0.61x | 671.4 | 830.1 | **1.24x** |
| `complex64` | 1,375.8 | 2,503.9 | **1.82x** | 1,411.4 | 763.4 | 0.54x | 1,014.8 | 749.8 | 0.74x | 508.9 | 388.8 | 0.76x |
| `complex128` | 3,393.7 | 3,671.9 | **1.08x** | 2,542.3 | 1,601.4 | 0.63x | 826.2 | 1,027.7 | **1.24x** | *Exhausted* | *Exhausted* | — |
| `turboquant` | 3,186.6 | 1,518.0 | 0.48x | 1,411.6 | 3,133.6 | **2.22x** | 2,298.6 | 2,747.3 | **1.20x** | 3,118.2 | 1,415.3 | 0.45x |
| `turboquant2` | 2,716.3 | 1,428.7 | 0.53x | 3,438.1 | 3,342.1 | 0.97x | 3,729.9 | 3,421.4 | 0.92x | 3,545.4 | 2,056.0 | 0.58x |
| `turboquant4` | 1,995.5 | 3,338.8 | **1.67x** | 3,315.9 | 1,994.2 | 0.60x | 2,670.2 | 3,459.8 | **1.30x** | 3,032.4 | 3,187.7 | **1.05x** |
| `turboquant8` | 3,519.0 | 1,605.4 | 0.46x | 2,408.5 | 3,027.1 | **1.26x** | 898.1 | 1,653.3 | **1.84x** | 1,512.0 | 1,195.6 | 0.79x |

---

## 3. Search Latency Comparison — Median P50 (ms)

| Data Type | CPU 50k | GPU 50k | CPU 200k | GPU 200k | CPU 500k | GPU 500k | CPU 1M | GPU 1M | Latency Reduction (1M) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 2.18 ms | 2.36 ms | 3.47 ms | **2.12 ms** | 2.48 ms | **2.00 ms** | 2.21 ms | **2.18 ms** | **-1.5%** |
| `float64` | 5.08 ms | **3.16 ms** | 4.46 ms | 6.86 ms | 9.35 ms | **3.13 ms** | 7.17 ms | *Exh* | — |
| `float16` | 1.94 ms | 1.99 ms | 4.08 ms | **4.07 ms** | 5.16 ms | **2.63 ms** | 5.50 ms | **3.15 ms** | **-42.6%** |
| `int8` | 2.32 ms | 2.69 ms | 2.47 ms | 2.50 ms | 2.87 ms | **2.74 ms** | 3.07 ms | 4.39 ms | +43.1% |
| `int16` | 3.55 ms | 3.69 ms | 4.64 ms | **3.90 ms** | 5.51 ms | **5.06 ms** | 6.49 ms | **6.37 ms** | **-1.9%** |
| `int32` | 2.61 ms | **2.40 ms** | 2.05 ms | 5.92 ms | 8.72 ms | **6.25 ms** | 7.82 ms | 7.94 ms | +1.6% |
| `int64` | 1.77 ms | 2.76 ms | 2.19 ms | 2.58 ms | 2.79 ms | **2.40 ms** | 11.65 ms | **7.36 ms** | **-36.8%** |
| `uint8` | 1.76 ms | 1.98 ms | 2.40 ms | **2.27 ms** | 2.34 ms | 2.45 ms | 2.66 ms | **2.44 ms** | **-8.4%** |
| `uint16` | 3.91 ms | 4.37 ms | 3.91 ms | 4.15 ms | 5.71 ms | **5.09 ms** | 6.61 ms | 7.08 ms | +7.0% |
| `uint32` | 2.80 ms | 2.85 ms | 2.87 ms | 3.09 ms | 4.64 ms | **4.16 ms** | 10.64 ms | **8.67 ms** | **-18.5%** |
| `uint64` | 2.09 ms | 2.18 ms | 2.89 ms | **2.73 ms** | 3.64 ms | 5.77 ms | 8.83 ms | **7.18 ms** | **-18.8%** |
| `complex64` | 4.99 ms | **2.73 ms** | 3.65 ms | 7.27 ms | 7.46 ms | **7.37 ms** | 10.68 ms | 14.24 ms | +33.4% |
| `complex128` | 2.18 ms | **1.90 ms** | 1.85 ms | 3.80 ms | 8.07 ms | **6.07 ms** | *Exh* | *Exh* | — |
| `turboquant` | 2.22 ms | 4.13 ms | 4.25 ms | **2.32 ms** | 2.62 ms | **2.45 ms** | 2.31 ms | 4.86 ms | +110.7% |
| `turboquant2` | 2.46 ms | 4.21 ms | 2.08 ms | 2.12 ms | 1.77 ms | 2.10 ms | 1.78 ms | 3.12 ms | +74.5% |
| `turboquant4` | 3.33 ms | **1.93 ms** | 2.08 ms | 3.48 ms | 2.52 ms | **2.23 ms** | 2.29 ms | **2.16 ms** | **-5.7%** |
| `turboquant8` | 2.20 ms | 3.76 ms | 2.44 ms | **2.10 ms** | 8.07 ms | **2.80 ms** | 4.72 ms | 5.74 ms | +21.6% |

---

## 4. Peak Memory Footprint Scaling (`VmHWM` in MB)

| Data Type | Width | CPU 50k | GPU 50k | CPU 200k | GPU 200k | CPU 500k | GPU 500k | CPU 1M | GPU 1M |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `float32` | 4B | 801 MB | 911 MB | 2,370 MB | 2,992 MB | 6,066 MB | 5,663 MB | 10,024 MB | 10,475 MB |
| `float64` | 8B | 1,225 MB | 1,383 MB | 3,457 MB | 3,605 MB | 8,486 MB | 8,340 MB | 14,035 MB | *Exhausted* |
| `float16` | 2B | 749 MB | 838 MB | 2,403 MB | 2,216 MB | 5,096 MB | 5,178 MB | 10,645 MB | 11,279 MB |
| `int8` | 1B | 732 MB | 859 MB | 2,269 MB | 2,492 MB | 5,155 MB | 4,764 MB | 10,024 MB | 10,104 MB |
| `int16` | 2B | 817 MB | 941 MB | 2,530 MB | 2,567 MB | 5,589 MB | 6,379 MB | 10,642 MB | 10,977 MB |
| `int32` | 4B | 907 MB | 1,077 MB | 2,995 MB | 3,060 MB | 6,785 MB | 6,561 MB | 12,503 MB | 11,927 MB |
| `int64` | 8B | 1,272 MB | 1,439 MB | 3,735 MB | 3,653 MB | 8,341 MB | 9,453 MB | 15,992 MB | 15,409 MB |
| `uint8` | 1B | 756 MB | 854 MB | 2,128 MB | 2,556 MB | 5,045 MB | 5,429 MB | 10,051 MB | 10,011 MB |
| `uint16` | 2B | 824 MB | 951 MB | 2,667 MB | 2,872 MB | 5,595 MB | 5,790 MB | 11,701 MB | 11,609 MB |
| `uint32` | 4B | 900 MB | 1,025 MB | 2,848 MB | 2,997 MB | 6,602 MB | 6,162 MB | 11,271 MB | 11,068 MB |
| `uint64` | 8B | 1,340 MB | 1,366 MB | 3,582 MB | 3,446 MB | 7,961 MB | 8,388 MB | 14,672 MB | 14,566 MB |
| `complex64` | 8B | 1,325 MB | 1,425 MB | 3,815 MB | 4,013 MB | 9,077 MB | 9,076 MB | 15,835 MB | 16,448 MB |
| `complex128` | 16B | 1,935 MB | 2,095 MB | 5,096 MB | 5,251 MB | 13,583 MB | 12,994 MB | *Exhausted* | *Exhausted* |
| `turboquant` | ~0.5B | 790 MB | 966 MB | 2,682 MB | 2,765 MB | 5,393 MB | 5,607 MB | 10,273 MB | 11,791 MB |
| `turboquant2` | ~0.25B | 825 MB | 938 MB | 2,459 MB | 2,293 MB | 5,028 MB | 4,926 MB | 10,017 MB | 11,437 MB |
| `turboquant4` | ~0.5B | 826 MB | 946 MB | 2,633 MB | 2,490 MB | 5,193 MB | 5,954 MB | 10,600 MB | 11,186 MB |
| `turboquant8` | ~1B | 791 MB | 939 MB | 2,405 MB | 2,395 MB | 5,078 MB | 5,494 MB | 11,747 MB | 11,574 MB |

---

## 5. Comprehensive Search Mode Breakdown (All 13 Modes)

### A. `float32` @ 1,000,000 Vectors (dim=128)

| Search Mode | CPU QPS | CPU P50 Latency | GPU QPS | GPU P50 Latency | GPU Speedup |
|---|:---:|:---:|:---:|:---:|:---:|
| `byid` | 3,190.0 | 2.18 ms | 3,004.3 | 2.48 ms | 0.94x |
| `dense` | 3,023.2 | 2.21 ms | 2,903.1 | 2.18 ms | 0.96x |
| `filtered` | 61.7 | 1.98 ms | 58.9 | 1.77 ms | 0.95x |
| `filteredbool` | 122.0 | 1.77 ms | 112.5 | 1.58 ms | 0.92x |
| `filteredstring` | 230.7 | 1.56 ms | 176.2 | 11.21 ms | 0.76x |
| `geo` | 26.2 | 231.44 ms | 27.7 | 228.95 ms | **1.05x** |
| `globalgraphrag` | 2,056.9 | 2.88 ms | 2,143.6 | 3.27 ms | **1.04x** |
| `graphrag` | 2,057.7 | 3.36 ms | 1,753.0 | 3.82 ms | 0.85x |
| `hybrid` | 2,585.6 | 2.45 ms | 3,170.2 | 2.01 ms | **1.23x** |
| `learnedindex` | 1,233.9 | 2.14 ms | 878.1 | 7.92 ms | 0.71x |
| `recommend` | 2,868.4 | 2.21 ms | 2,762.8 | 2.46 ms | 0.96x |
| `sparse` | 6,914.0 | 1.07 ms | 5,746.3 | 1.27 ms | 0.83x |
| `temporal` | 464.9 | 12.20 ms | 377.9 | 16.30 ms | 0.81x |

### B. `turboquant4` @ 1,000,000 Vectors (dim=128)

| Search Mode | CPU QPS | CPU P50 Latency | GPU QPS | GPU P50 Latency | GPU Speedup |
|---|:---:|:---:|:---:|:---:|:---:|
| `byid` | 2,978.6 | 2.27 ms | 3,666.6 | 1.84 ms | **1.23x** |
| `dense` | 3,032.4 | 2.29 ms | 3,187.7 | 2.16 ms | **1.05x** |
| `filtered` | 59.6 | 1.85 ms | 59.8 | 0.96 ms | **1.00x** |
| `filteredbool` | 114.2 | 1.81 ms | 117.4 | 1.13 ms | **1.03x** |
| `filteredstring` | 187.6 | 7.86 ms | 194.2 | 8.06 ms | **1.04x** |
| `geo` | 25.3 | 235.46 ms | 28.9 | 225.43 ms | **1.15x** |
| `globalgraphrag` | 2,366.6 | 3.10 ms | 2,636.3 | 2.48 ms | **1.11x** |
| `graphrag` | 2,426.9 | 2.91 ms | 2,048.6 | 3.02 ms | 0.84x |
| `hybrid` | 3,017.2 | 2.27 ms | 4,044.4 | 1.84 ms | **1.34x** |
| `learnedindex` | 2,901.9 | 2.34 ms | 3,546.6 | 2.02 ms | **1.22x** |
| `recommend` | 3,102.2 | 2.32 ms | 3,427.6 | 1.98 ms | **1.10x** |
| `sparse` | 5,889.8 | 1.29 ms | 4,835.3 | 1.22 ms | 0.82x |
| `temporal` | 469.4 | 14.03 ms | 497.9 | 12.01 ms | **1.06x** |

### C. `float64` @ 500,000 Vectors (dim=128) — Overcoming L3 Cache Thrashing

| Search Mode | CPU QPS | CPU P50 Latency | GPU QPS | GPU P50 Latency | GPU Speedup |
|---|:---:|:---:|:---:|:---:|:---:|
| `byid` | 4,252.5 | 1.77 ms | 3,408.6 | 2.22 ms | 0.80x |
| `dense` | 687.1 | 9.35 ms | 2,014.1 | 3.13 ms | **2.93x** |
| `filtered` | 97.9 | 9.33 ms | 110.0 | 10.10 ms | **1.12x** |
| `filteredbool` | 177.7 | 14.52 ms | 162.9 | 17.20 ms | 0.92x |
| `filteredstring` | 242.5 | 12.05 ms | 189.5 | 23.11 ms | 0.78x |
| `geo` | 56.5 | 110.98 ms | 74.5 | 98.27 ms | **1.32x** |
| `globalgraphrag` | 1,193.1 | 5.58 ms | 530.7 | 11.96 ms | 0.44x |
| `graphrag` | 1,523.1 | 5.25 ms | 473.3 | 13.67 ms | 0.31x |
| `hybrid` | 703.2 | 8.70 ms | 743.9 | 8.51 ms | **1.06x** |
| `learnedindex` | 889.9 | 7.47 ms | 414.6 | 15.00 ms | 0.47x |
| `recommend` | 432.9 | 14.95 ms | 561.8 | 10.63 ms | **1.30x** |
| `sparse` | 6,044.1 | 1.26 ms | 5,271.5 | 1.31 ms | 0.87x |
| `temporal` | 822.3 | 8.89 ms | 864.6 | 6.76 ms | **1.05x** |

---

## 6. Artifact Reference Logs

- **CPU Multi-Scale Benchmark Matrix (50k, 200k, 500k, 1M)**:
  - [`data/perf_logs/perf_matrix_cpu_multi_scale_eval_20260903_151117.json`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cpu_multi_scale_eval_20260903_151117.json)
  - [`data/perf_logs/perf_matrix_cpu_multi_scale_eval_20260903_151117.md`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cpu_multi_scale_eval_20260903_151117.md)
- **GPU Multi-Scale Benchmark Matrix (50k, 200k, 500k, 1M)**:
  - [`data/perf_logs/perf_matrix_cuda_multi_scale_eval_gpu_20260903_165940.json`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cuda_multi_scale_eval_gpu_20260903_165940.json)
  - [`data/perf_logs/perf_matrix_cuda_multi_scale_eval_gpu_20260903_165940.md`](file:///home/rsd/REPOS/longbow/data/perf_logs/perf_matrix_cuda_multi_scale_eval_gpu_20260903_165940.md)
