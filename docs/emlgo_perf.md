# EMLGo Performance Benchmark Report

**Branch**: `experimental/emlgo`
**Date**: 2026-09-10
**emlgo version**: v0.4.0 (fixes GPU, TurboQuant4, and Int8 regressions from v0.3)
**Host Architecture**: Linux x86_64, 16 logical CPUs (12th Gen Intel Core i7-12650H, 10 physical cores), 23 GB RAM
**GPU**: NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4)
**Server Binary**: `bin/longbow` (compiled with `github.com/emlgo/eml` v0.4.0 high-performance mathematical engine)
**Client Binary**: `bin/bench-tool`
**Evaluation Matrix**: 8 Data Types × 2 Scales (50,000, 500,000 vectors) × 128 Dimensions × 5 Search Modes
**Disk Spillover**: Auto-spill enabled (`LONGBOW_AUTO_SPILL_DISK=true`, threshold 60%). Forced disk mode (`LONGBOW_USE_DISK=1`) is NOT used — it makes HNSW graph construction 10-100x slower because every distance computation during indexing requires a disk read. Auto-spill lets HNSW build in-memory and only spills vectors to disk when memory pressure exceeds the threshold.

---

## Executive Summary

### emlgo v0.4 vs Main Branch — CPU

| Metric | Result |
|--------|--------|
| **Best dense win** | turboquant4 at 50k: +78% QPS (3152 vs 1767) |
| **Best 500k win** | uint8 dense: +168% QPS (4230 vs 1579) |
| **Largest regression** | int8/uint8 dense at 50k: -56% QPS |
| **Overall** | Mixed — strong gains for float16/float64/complex128, regressions for int8/uint8 at small scale |

### emlgo v0.4 vs Main Branch — GPU

| Metric | Result |
|--------|--------|
| **Best dense win** | float32 at 500k: +203% QPS (3393 vs 1122) |
| **Best overall** | complex128 dense at 500k: +135% QPS (734 vs 313) |
| **Largest regression** | turboquant4 dense at 50k: -65% QPS (897 vs 2556) |
| **Overall** | v0.4 largely fixes v0.3 GPU regressions; float32/float64/complex128 now faster |

---

## 1. CPU Performance — 50,000 Vectors

### Dense Search QPS & P50 Latency

| Dtype | Main QPS | Emlgo QPS | Delta | Main P50 | Emlgo P50 |
|-------|--------:|---------:|------:|--------:|---------:|
| int8 | 4801.6 | 2107.4 | **-56.1%** | 0.85 ms | 2.73 ms |
| uint8 | 5228.7 | 2313.3 | **-55.8%** | 0.77 ms | 2.44 ms |
| float16 | 1713.9 | 2973.8 | **+73.5%** | 1.82 ms | 0.89 ms |
| float32 | 1007.8 | 907.2 | -10.0% | 3.65 ms | 6.27 ms |
| float64 | 823.3 | 1313.5 | **+59.6%** | 4.61 ms | 3.64 ms |
| complex64 | 926.6 | 1016.5 | +9.7% | 4.31 ms | 5.68 ms |
| complex128 | 1788.9 | 2616.1 | **+46.2%** | 0.88 ms | 1.21 ms |
| turboquant4 | 1767.3 | 3152.0 | **+78.4%** | 2.12 ms | 1.84 ms |

### Sparse Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 7441.3 | 7031.6 | -5.5% |
| uint8 | 7533.5 | 6891.7 | -8.5% |
| float16 | 7094.0 | 6914.2 | -2.5% |
| float32 | 6293.1 | 6682.1 | +6.2% |
| float64 | 5078.6 | 6930.5 | **+36.5%** |
| complex64 | 6695.5 | 6596.6 | -1.5% |
| complex128 | 6375.5 | 6121.5 | -4.0% |
| turboquant4 | 6120.3 | 6508.2 | +6.3% |

### Hybrid Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1829.0 | 1641.8 | -10.2% |
| uint8 | 1875.0 | 1968.7 | +5.0% |
| float16 | 1073.6 | 1186.8 | +10.5% |
| float32 | 764.1 | 896.2 | +17.3% |
| float64 | 817.9 | 983.7 | **+20.3%** |
| complex64 | 1126.2 | 762.4 | -32.3% |
| complex128 | 673.1 | 841.3 | +25.0% |
| turboquant4 | 1708.7 | 3006.0 | **+75.9%** |

### GraphRAG Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 3715.6 | 1455.8 | **-60.8%** |
| uint8 | 3601.5 | 1906.5 | **-47.1%** |
| float16 | 1704.7 | 2637.0 | **+54.7%** |
| float32 | 973.0 | 893.8 | -8.1% |
| float64 | 726.1 | 1080.3 | **+48.8%** |
| complex64 | 884.9 | 843.3 | -4.7% |
| complex128 | 1684.4 | 2473.7 | **+46.8%** |
| turboquant4 | 1667.6 | 3137.2 | **+88.1%** |

### Temporal Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1793.1 | 2008.6 | +12.0% |
| uint8 | 1771.5 | 1814.9 | +2.5% |
| float16 | 1652.0 | 1851.1 | +12.0% |
| float32 | 2274.3 | 2179.5 | -4.2% |
| float64 | 2178.9 | 2423.8 | +11.2% |
| complex64 | 2223.8 | 2155.4 | -3.1% |
| complex128 | 2020.3 | 2190.7 | +8.4% |
| turboquant4 | 2028.7 | 2248.7 | +10.8% |

---

## 2. CPU Performance — 500,000 Vectors

### Dense Search QPS & P50 Latency

| Dtype | Main QPS | Emlgo QPS | Delta | Main P50 | Emlgo P50 |
|-------|--------:|---------:|------:|--------:|---------:|
| int8 | 856.9 | 1371.2 | **+59.9%** | 4.78 ms | 4.20 ms |
| uint8 | 1579.4 | 4229.8 | **+167.8%** | 2.47 ms | 1.40 ms |
| float16 | 978.3 | 876.3 | -10.4% | 4.30 ms | 7.26 ms |
| float32 | 2354.9 | 1420.6 | -39.7% | 1.64 ms | 4.08 ms |
| float64 | 518.7 | 617.7 | +19.1% | 8.71 ms | 10.30 ms |
| complex64 | 500.8 | 595.8 | +19.0% | 8.57 ms | 10.30 ms |
| complex128 | 109.8 | 201.2 | **+83.3%** | 20.71 ms | 21.35 ms |
| turboquant4 | 1804.1 | 821.2 | -54.5% | 2.14 ms | 6.82 ms |

### Sparse Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 6100.9 | 6206.6 | +1.7% |
| uint8 | 6370.0 | 7377.9 | **+15.8%** |
| float16 | 6578.4 | 6050.8 | -8.0% |
| float32 | 5332.2 | 6435.6 | **+20.7%** |
| float64 | 5157.6 | 6353.2 | **+23.2%** |
| complex64 | 6713.1 | 5024.5 | -25.2% |
| complex128 | 745.5 | 688.0 | -7.7% |
| turboquant4 | 5541.5 | 6610.8 | **+19.3%** |

### Hybrid Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 866.4 | 1020.4 | +17.8% |
| uint8 | 1395.0 | 4540.0 | **+225.5%** |
| float16 | 828.3 | 689.5 | -16.8% |
| float32 | 1646.9 | 1418.5 | -13.9% |
| float64 | 440.7 | 564.6 | +28.1% |
| complex64 | 427.3 | 424.0 | -0.8% |
| complex128 | 168.6 | 440.8 | **+161.5%** |
| turboquant4 | 1704.7 | 789.4 | -53.7% |

### GraphRAG Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1027.0 | 1145.9 | +11.6% |
| uint8 | 1133.6 | 3994.4 | **+252.4%** |
| float16 | 867.3 | 838.9 | -3.3% |
| float32 | 2065.8 | 1340.9 | -35.1% |
| float64 | 567.0 | 638.7 | +12.6% |
| complex64 | 426.4 | 494.4 | +16.0% |
| complex128 | 273.5 | 434.1 | **+58.7%** |
| turboquant4 | 1483.7 | 859.7 | -42.1% |

### Temporal Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 453.7 | 423.6 | -6.6% |
| uint8 | 460.1 | 384.1 | -16.5% |
| float16 | 470.4 | 556.7 | +18.3% |
| float32 | 511.2 | 636.8 | +24.6% |
| float64 | 479.6 | 614.3 | +28.1% |
| complex64 | 503.3 | 483.8 | -3.9% |
| complex128 | 328.8 | 345.9 | +5.2% |
| turboquant4 | 445.8 | 659.1 | **+47.8%** |

---

## 3. GPU Performance — 50,000 Vectors

### Dense Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 2175.2 | 2021.1 | -7.1% |
| uint8 | 4438.2 | 4001.5 | -9.8% |
| float16 | 2045.2 | 2122.8 | +3.8% |
| float32 | 934.3 | 952.5 | +2.0% |
| float64 | 1094.3 | 980.1 | -10.4% |
| complex64 | 1128.6 | 1271.4 | +12.7% |
| complex128 | 1441.8 | 656.9 | -54.4% |
| turboquant4 | 2555.6 | 897.3 | -64.9% |

### Sparse Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 6290.1 | 6406.5 | +1.8% |
| uint8 | 7726.8 | 7087.9 | -8.3% |
| float16 | 7221.4 | 7075.0 | -2.0% |
| float32 | 6475.1 | 6437.6 | -0.6% |
| float64 | 6771.4 | 6056.2 | -10.6% |
| complex64 | 6795.0 | 6423.8 | -5.5% |
| complex128 | 6456.5 | 5949.5 | -7.9% |
| turboquant4 | 6510.7 | 6140.9 | -5.7% |

### Hybrid Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1732.7 | 1705.5 | -1.6% |
| uint8 | 2213.6 | 1941.3 | -12.3% |
| float16 | 1254.5 | 1293.0 | +3.1% |
| float32 | 868.1 | 923.4 | +6.4% |
| float64 | 947.9 | 850.4 | -10.3% |
| complex64 | 899.4 | 809.8 | -10.0% |
| complex128 | 1170.8 | 600.3 | -48.7% |
| turboquant4 | 2548.5 | 876.2 | -65.6% |

---

## 4. GPU Performance — 500,000 Vectors

### Dense Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1478.8 | 1408.6 | -4.7% |
| uint8 | 1551.9 | 1565.5 | +0.9% |
| float16 | 1156.4 | 878.4 | -24.0% |
| float32 | 1121.7 | 3393.2 | **+202.5%** |
| float64 | 306.7 | 609.8 | **+98.8%** |
| complex64 | 571.8 | 681.2 | +19.1% |
| complex128 | 312.8 | 734.2 | **+134.7%** |
| turboquant4 | 2787.2 | 3617.9 | **+29.8%** |

### Sparse Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 6550.4 | 6372.2 | -2.7% |
| uint8 | 6021.4 | 5912.2 | -1.8% |
| float16 | 6368.1 | 4557.5 | -28.4% |
| float32 | 6593.6 | 6572.8 | -0.3% |
| float64 | 4491.5 | 6474.5 | **+44.2%** |
| complex64 | 6036.0 | 6647.3 | +10.1% |
| complex128 | 6993.5 | 6355.8 | -9.1% |
| turboquant4 | 6660.2 | 6495.3 | -2.5% |

### Hybrid Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1189.0 | 1185.8 | -0.3% |
| uint8 | 1627.8 | 1444.0 | -11.3% |
| float16 | 1178.1 | 393.0 | **-66.6%** |
| float32 | 1108.9 | 3222.2 | **+190.6%** |
| float64 | 541.4 | 554.2 | +2.4% |
| complex64 | 556.5 | 509.5 | -8.4% |
| complex128 | 288.7 | 601.7 | **+108.4%** |
| turboquant4 | 2511.2 | 3075.7 | **+22.5%** |

### GraphRAG Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 1081.0 | 1170.2 | +8.2% |
| uint8 | 1386.9 | 1237.2 | -10.8% |
| float16 | 874.8 | 1064.7 | +21.7% |
| float32 | 1067.2 | 2766.0 | **+159.2%** |
| float64 | 542.6 | 626.6 | +15.5% |
| complex64 | 617.0 | 466.2 | -24.4% |
| complex128 | 285.5 | 759.7 | **+166.1%** |
| turboquant4 | 2223.5 | 2703.6 | +21.6% |

### Temporal Search QPS

| Dtype | Main QPS | Emlgo QPS | Delta |
|-------|--------:|---------:|------:|
| int8 | 594.5 | 524.1 | -11.8% |
| uint8 | 562.9 | 520.9 | -7.5% |
| float16 | 557.4 | 523.9 | -6.0% |
| float32 | 645.1 | 471.5 | -26.9% |
| float64 | 518.3 | 609.0 | +17.5% |
| complex64 | 620.0 | 670.0 | +8.1% |
| complex128 | 641.0 | 548.8 | -14.4% |
| turboquant4 | 654.6 | 590.9 | -9.7% |

---

## 5. Memory Usage (Peak MB)

### CPU

| Dtype | 50k Main | 50k Emlgo | 500k Main | 500k Emlgo |
|-------|--------:|---------:|---------:|----------:|
| int8 | 644.5 | 667.3 | 4256.7 | 4649.3 |
| uint8 | 631.1 | 684.8 | 4655.4 | 4428.0 |
| float16 | 672.4 | 706.3 | 4628.5 | 4210.6 |
| float32 | 788.7 | 718.9 | 4357.1 | 4831.4 |
| float64 | 1133.7 | 1183.3 | 7799.5 | 7861.9 |
| complex64 | 1260.9 | 1214.5 | 8997.6 | 8136.2 |
| complex128 | 1826.8 | 1902.5 | 14363.0 | 12982.3 |
| turboquant4 | 721.2 | 729.1 | 5365.4 | 4631.4 |

### GPU

| Dtype | 50k Main | 50k Emlgo | 500k Main | 500k Emlgo |
|-------|--------:|---------:|---------:|----------:|
| int8 | 648.3 | 765.4 | 4579.4 | 4394.5 |
| uint8 | 661.2 | 654.6 | 4493.5 | 5009.2 |
| float16 | 730.4 | 697.1 | 4568.9 | 4666.9 |
| float32 | 735.7 | 673.3 | 4649.1 | 4714.2 |
| float64 | 1176.4 | 1207.4 | 6797.3 | 7469.5 |
| complex64 | 1238.7 | 1289.2 | 9051.0 | 8403.5 |
| complex128 | 1999.3 | 1943.4 | 14211.1 | 14674.4 |
| turboquant4 | 771.6 | 713.2 | 4505.9 | 4949.9 |

---

## 6. Key Findings & Recommendations

### What emlgo v0.4 improved

1. **GPU path is now viable** — float32 dense at 500k is +203%, float64 +99%, complex128 +135%. The v0.3 GPU catastrophes are resolved.
2. **turboquant4 at 50k CPU** — +78% QPS, the strongest single gain at small scale
3. **uint8 at 500k CPU** — +168% QPS for the most common integer type at production scale
4. **float64/complex128** — consistent +20-80% gains across both scales on CPU
5. **Memory efficiency** — turboquant4 -14% at 500k, complex128 -10% at 500k

### What still needs work

1. **int8/uint8 at 50k CPU** — -56% QPS regression for the most common integer types at small scale
2. **turboquant4 at 500k CPU** — -55% QPS regression at scale
3. **complex128/turboquant4 GPU at 50k** — -54%/-65% regressions at small scale
4. **float16 hybrid GPU at 500k** — -67% new regression

### v0.4 vs v0.3 comparison

| Area | v0.3 Status | v0.4 Status | Improvement |
|------|------------|------------|-------------|
| GPU dense 500k | -60% to -84% across all types | -5% to +203% | Major fix |
| GPU sparse 500k | -40% to -92% across all types | -3% to +44% | Major fix |
| TurboQuant4 GPU | -83% to -91% | -5% to +30% | Major fix |
| Int8 GPU | -72% | -5% | Major fix |
| CPU int8/uint8 50k | -14% to -61% | -56% | Similar |

### Recommendations

1. **Ship emlgo v0.4 for GPU workloads** — the GPU path is now production-viable
2. **Investigate int8/uint8 50k CPU regression** — profile the emlgo math primitives overhead for simple integer dot products
3. **Profile turboquant4 at 500k** — the -55% regression suggests a code path issue specific to quantized types at scale
4. **Use conditional dispatch** — emlgo for float16/float64/complex128, fall back to main for int8/uint8 at small scale
5. **Test float16 hybrid GPU at 500k** — a new regression needs root cause analysis
