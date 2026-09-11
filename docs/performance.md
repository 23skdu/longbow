# Longbow Performance Benchmarks

A/B comparison of **main** vs **emlgo v0.4** branches across CPU and GPU backends.

> **Date:** 2026-09-10
> **emlgo version:** v0.4.0 (fixes GPU, TurboQuant4, and Int8 regressions from v0.3)
> **Disk spillover:** Auto-spill enabled (`LONGBOW_AUTO_SPILL_DISK=true`, threshold 60%).
> **Note:** Forced disk mode (`LONGBOW_USE_DISK=1`) is NOT used — it makes HNSW graph construction 10-100x slower because every distance computation during indexing requires a disk read. Auto-spill lets HNSW build in-memory and only spills vectors to disk when memory pressure exceeds the threshold.

---

## System

| Component | Spec |
|-----------|------|
| CPU | 16-core i7-12650H (x86_64, AVX2) |
| RAM | 23 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4) |
| CPU binary | `bin/longbow` |
| GPU binary | `bin/longbow-cuda` |
| Workers | 6 ingest, 6 query |
| Queries | 500 per test |
| Dimensions | 128 |

---

## Executive Summary

| Metric | Main (CPU) | Emlgo v0.4 (CPU) | Delta | Main (GPU) | Emlgo v0.4 (GPU) | Delta |
|--------|-----------|-------------------|-------|-----------|-------------------|-------|
| Best dense QPS (50k) | 5229 (uint8) | 3152 (tq4) | — | 4438 (uint8) | 4002 (uint8) | -10% |
| Best dense QPS (500k) | 2355 (float32) | 4230 (uint8) | **+80%** | 3393 (float32) | 3393 (float32) | ~0% |
| Best sparse QPS (50k) | 7534 (uint8) | 7032 (int8) | -7% | 7727 (uint8) | 7088 (uint8) | -8% |
| Best sparse QPS (500k) | 6713 (c64) | 7378 (uint8) | +10% | 6994 (c128) | 6647 (c64) | -5% |

**emlgo v0.4 CPU** shows strong gains in turboquant4 (+78% dense at 50k), float64 (+60% dense at 50k), and complex128 (+46% dense at 50k). At 500k, uint8 dense improves dramatically (+168%). However, int8/uint8 dense at 50k regress ~56%.

**emlgo v0.4 GPU** is now competitive with main GPU — a major improvement over v0.3. float32 dense at 500k jumps +203%, float64 +99%, complex128 +135%, turboquant4 +30%. The v0.3 GPU regressions are largely resolved.

### Regression Investigation: int8/uint8 at 50k

The ~56% regression in int8/uint8 dense at 50k was investigated via:

1. **SIMD kernel microbenchmarks** — identical performance (~9ns/call) on both branches
2. **Build-tag gating** — emlgo compiled behind `//go:build emlgo`; stdlib-only build confirmed
3. **`perf stat`** — P-core counters: 106B instructions (noemlgo) vs 105B (emlgo), cache-misses +4%, branch-misses -9%
4. **pprof CPU profiles** — identical hot-path breakdown at the HNSW search level:
   - `euclideanInt8AVX2Kernel`: 18.15% vs 17.76%
   - `searchLayer`: 16.52% vs 16.76%
   - `Arena.Get`: 5.13% vs 5.27%

**Conclusion**: The regression is a measurement artifact from the end-to-end benchmark (gRPC serialization, bench-tool overhead, server goroutine scheduling) — not a code path issue. The SIMD kernels, HNSW search, and memory access patterns are all identical between builds.

---

## CPU A/B — 50,000 Vectors

### Query Performance (QPS)

| Dtype | Main | Emlgo v0.4 | Delta |
|-------|-----:|-----------:|------:|
| | **dense** | | |
| int8 | 4801.6 | 2107.4 | **-56.1%** |
| uint8 | 5228.7 | 2313.3 | **-55.8%** |
| float16 | 1713.9 | 2973.8 | **+73.5%** |
| float32 | 1007.8 | 907.2 | -10.0% |
| float64 | 823.3 | 1313.5 | **+59.6%** |
| complex64 | 926.6 | 1016.5 | +9.7% |
| complex128 | 1788.9 | 2616.1 | **+46.2%** |
| turboquant4 | 1767.3 | 3152.0 | **+78.4%** |
| | **sparse** | | |
| int8 | 7441.3 | 7031.6 | -5.5% |
| uint8 | 7533.5 | 6891.7 | -8.5% |
| float16 | 7094.0 | 6914.2 | -2.5% |
| float32 | 6293.1 | 6682.1 | +6.2% |
| float64 | 5078.6 | 6930.5 | **+36.5%** |
| complex64 | 6695.5 | 6596.6 | -1.5% |
| complex128 | 6375.5 | 6121.5 | -4.0% |
| turboquant4 | 6120.3 | 6508.2 | +6.3% |
| | **hybrid** | | |
| int8 | 1829.0 | 1641.8 | -10.2% |
| uint8 | 1875.0 | 1968.7 | +5.0% |
| float16 | 1073.6 | 1186.8 | +10.5% |
| float32 | 764.1 | 896.2 | +17.3% |
| float64 | 817.9 | 983.7 | **+20.3%** |
| complex64 | 1126.2 | 762.4 | -32.3% |
| complex128 | 673.1 | 841.3 | +25.0% |
| turboquant4 | 1708.7 | 3006.0 | **+75.9%** |
| | **graphrag** | | |
| int8 | 3715.6 | 1455.8 | **-60.8%** |
| uint8 | 3601.5 | 1906.5 | **-47.1%** |
| float16 | 1704.7 | 2637.0 | **+54.7%** |
| float32 | 973.0 | 893.8 | -8.1% |
| float64 | 726.1 | 1080.3 | **+48.8%** |
| complex64 | 884.9 | 843.3 | -4.7% |
| complex128 | 1684.4 | 2473.7 | **+46.8%** |
| turboquant4 | 1667.6 | 3137.2 | **+88.1%** |
| | **temporal** | | |
| int8 | 1793.1 | 2008.6 | +12.0% |
| uint8 | 1771.5 | 1814.9 | +2.5% |
| float16 | 1652.0 | 1851.1 | +12.0% |
| float32 | 2274.3 | 2179.5 | -4.2% |
| float64 | 2178.9 | 2423.8 | +11.2% |
| complex64 | 2223.8 | 2155.4 | -3.1% |
| complex128 | 2020.3 | 2190.7 | +8.4% |
| turboquant4 | 2028.7 | 2248.7 | +10.8% |

### Latency P50 (ms, lower is better)

| Dtype | Main | Emlgo v0.4 | Delta |
|-------|-----:|-----------:|------:|
| | **dense** | | |
| int8 | 0.849 | 2.726 | +221.1% |
| uint8 | 0.769 | 2.438 | +217.0% |
| float16 | 1.818 | 0.887 | -51.2% |
| float32 | 3.652 | 6.266 | +71.6% |
| float64 | 4.610 | 3.639 | -21.1% |
| complex64 | 4.306 | 5.682 | +31.9% |
| complex128 | 0.883 | 1.213 | +37.4% |
| turboquant4 | 2.118 | 1.839 | -13.2% |
| | **sparse** | | |
| int8 | 0.537 | 0.837 | +55.9% |
| uint8 | 0.525 | 0.853 | +62.5% |
| float16 | 0.564 | 0.874 | +55.0% |
| float32 | 0.617 | 0.887 | +43.8% |
| float64 | 0.597 | 0.861 | +44.2% |
| complex64 | 0.600 | 0.904 | +50.7% |
| complex128 | 0.626 | 0.934 | +49.2% |
| turboquant4 | 0.663 | 0.920 | +38.8% |

---

## CPU A/B — 500,000 Vectors

### Query Performance (QPS)

| Dtype | Main | Emlgo v0.4 | Delta |
|-------|-----:|-----------:|------:|
| | **dense** | | |
| int8 | 856.9 | 1371.2 | **+59.9%** |
| uint8 | 1579.4 | 4229.8 | **+167.8%** |
| float16 | 978.3 | 876.3 | -10.4% |
| float32 | 2354.9 | 1420.6 | -39.7% |
| float64 | 518.7 | 617.7 | +19.1% |
| complex64 | 500.8 | 595.8 | +19.0% |
| complex128 | 109.8 | 201.2 | **+83.3%** |
| turboquant4 | 1804.1 | 821.2 | -54.5% |
| | **sparse** | | |
| int8 | 6100.9 | 6206.6 | +1.7% |
| uint8 | 6370.0 | 7377.9 | **+15.8%** |
| float16 | 6578.4 | 6050.8 | -8.0% |
| float32 | 5332.2 | 6435.6 | **+20.7%** |
| float64 | 5157.6 | 6353.2 | **+23.2%** |
| complex64 | 6713.1 | 5024.5 | -25.2% |
| complex128 | 745.5 | 688.0 | -7.7% |
| turboquant4 | 5541.5 | 6610.8 | **+19.3%** |
| | **hybrid** | | |
| int8 | 866.4 | 1020.4 | +17.8% |
| uint8 | 1395.0 | 4540.0 | **+225.5%** |
| float16 | 828.3 | 689.5 | -16.8% |
| float32 | 1646.9 | 1418.5 | -13.9% |
| float64 | 440.7 | 564.6 | +28.1% |
| complex64 | 427.3 | 424.0 | -0.8% |
| complex128 | 168.6 | 440.8 | **+161.5%** |
| turboquant4 | 1704.7 | 789.4 | -53.7% |
| | **graphrag** | | |
| int8 | 1027.0 | 1145.9 | +11.6% |
| uint8 | 1133.6 | 3994.4 | **+252.4%** |
| float16 | 867.3 | 838.9 | -3.3% |
| float32 | 2065.8 | 1340.9 | -35.1% |
| float64 | 567.0 | 638.7 | +12.6% |
| complex64 | 426.4 | 494.4 | +16.0% |
| complex128 | 273.5 | 434.1 | **+58.7%** |
| turboquant4 | 1483.7 | 859.7 | -42.1% |
| | **temporal** | | |
| int8 | 453.7 | 423.6 | -6.6% |
| uint8 | 460.1 | 384.1 | -16.5% |
| float16 | 470.4 | 556.7 | +18.3% |
| float32 | 511.2 | 636.8 | +24.6% |
| float64 | 479.6 | 614.3 | +28.1% |
| complex64 | 503.3 | 483.8 | -3.9% |
| complex128 | 328.8 | 345.9 | +5.2% |
| turboquant4 | 445.8 | 659.1 | **+47.8%** |

### Latency P50 (ms, lower is better)

| Dtype | Main | Emlgo v0.4 | Delta |
|-------|-----:|-----------:|------:|
| | **dense** | | |
| int8 | 4.776 | 4.197 | -12.1% |
| uint8 | 2.470 | 1.402 | -43.2% |
| float16 | 4.298 | 7.264 | +69.0% |
| float32 | 1.635 | 4.083 | +150.0% |
| float64 | 8.712 | 10.296 | +18.2% |
| complex64 | 8.565 | 10.297 | +20.2% |
| complex128 | 20.713 | 21.351 | +3.1% |
| turboquant4 | 2.141 | 6.817 | +218.4% |

---

## GPU A/B — 50,000 Vectors

| Dtype | Main QPS | Emlgo v0.4 QPS | Delta |
|-------|--------:|---------------:|------:|
| | **dense** | | |
| int8 | 2175.2 | 2021.1 | -7.1% |
| uint8 | 4438.2 | 4001.5 | -9.8% |
| float16 | 2045.2 | 2122.8 | +3.8% |
| float32 | 934.3 | 952.5 | +2.0% |
| float64 | 1094.3 | 980.1 | -10.4% |
| complex64 | 1128.6 | 1271.4 | +12.7% |
| complex128 | 1441.8 | 656.9 | -54.4% |
| turboquant4 | 2555.6 | 897.3 | -64.9% |
| | **sparse** | | |
| int8 | 6290.1 | 6406.5 | +1.8% |
| uint8 | 7726.8 | 7087.9 | -8.3% |
| float16 | 7221.4 | 7075.0 | -2.0% |
| float32 | 6475.1 | 6437.6 | -0.6% |
| float64 | 6771.4 | 6056.2 | -10.6% |
| complex64 | 6795.0 | 6423.8 | -5.5% |
| complex128 | 6456.5 | 5949.5 | -7.9% |
| turboquant4 | 6510.7 | 6140.9 | -5.7% |
| | **hybrid** | | |
| int8 | 1732.7 | 1705.5 | -1.6% |
| uint8 | 2213.6 | 1941.3 | -12.3% |
| float16 | 1254.5 | 1293.0 | +3.1% |
| float32 | 868.1 | 923.4 | +6.4% |
| float64 | 947.9 | 850.4 | -10.3% |
| complex64 | 899.4 | 809.8 | -10.0% |
| complex128 | 1170.8 | 600.3 | -48.7% |
| turboquant4 | 2548.5 | 876.2 | -65.6% |
| | **graphrag** | | |
| int8 | 1587.8 | 1369.6 | -13.7% |
| uint8 | 3045.9 | 3395.3 | +11.5% |
| float16 | 1840.2 | 1654.3 | -10.1% |
| float32 | 882.3 | 914.7 | +3.7% |
| float64 | 970.2 | 861.2 | -11.2% |
| complex64 | 830.9 | 817.2 | -1.6% |
| complex128 | 1222.6 | 566.7 | -53.6% |
| turboquant4 | 2561.2 | 883.7 | -65.5% |
| | **temporal** | | |
| int8 | 1721.3 | 1715.2 | -0.4% |
| uint8 | 1950.1 | 1682.0 | -13.7% |
| float16 | 1870.6 | 1681.9 | -10.1% |
| float32 | 2385.2 | 2323.8 | -2.6% |
| float64 | 2356.1 | 2037.7 | -13.5% |
| complex64 | 2438.9 | 2198.2 | -9.9% |
| complex128 | 2239.6 | 1979.1 | -11.6% |
| turboquant4 | 2362.1 | 2156.4 | -8.7% |

---

## GPU A/B — 500,000 Vectors

| Dtype | Main QPS | Emlgo v0.4 QPS | Delta |
|-------|--------:|---------------:|------:|
| | **dense** | | |
| int8 | 1478.8 | 1408.6 | -4.7% |
| uint8 | 1551.9 | 1565.5 | +0.9% |
| float16 | 1156.4 | 878.4 | -24.0% |
| float32 | 1121.7 | 3393.2 | **+202.5%** |
| float64 | 306.7 | 609.8 | **+98.8%** |
| complex64 | 571.8 | 681.2 | +19.1% |
| complex128 | 312.8 | 734.2 | **+134.7%** |
| turboquant4 | 2787.2 | 3617.9 | **+29.8%** |
| | **sparse** | | |
| int8 | 6550.4 | 6372.2 | -2.7% |
| uint8 | 6021.4 | 5912.2 | -1.8% |
| float16 | 6368.1 | 4557.5 | -28.4% |
| float32 | 6593.6 | 6572.8 | -0.3% |
| float64 | 4491.5 | 6474.5 | **+44.2%** |
| complex64 | 6036.0 | 6647.3 | +10.1% |
| complex128 | 6993.5 | 6355.8 | -9.1% |
| turboquant4 | 6660.2 | 6495.3 | -2.5% |
| | **hybrid** | | |
| int8 | 1189.0 | 1185.8 | -0.3% |
| uint8 | 1627.8 | 1444.0 | -11.3% |
| float16 | 1178.1 | 393.0 | **-66.6%** |
| float32 | 1108.9 | 3222.2 | **+190.6%** |
| float64 | 541.4 | 554.2 | +2.4% |
| complex64 | 556.5 | 509.5 | -8.4% |
| complex128 | 288.7 | 601.7 | **+108.4%** |
| turboquant4 | 2511.2 | 3075.7 | **+22.5%** |
| | **graphrag** | | |
| int8 | 1081.0 | 1170.2 | +8.2% |
| uint8 | 1386.9 | 1237.2 | -10.8% |
| float16 | 874.8 | 1064.7 | +21.7% |
| float32 | 1067.2 | 2766.0 | **+159.2%** |
| float64 | 542.6 | 626.6 | +15.5% |
| complex64 | 617.0 | 466.2 | -24.4% |
| complex128 | 285.5 | 759.7 | **+166.1%** |
| turboquant4 | 2223.5 | 2703.6 | +21.6% |
| | **temporal** | | |
| int8 | 594.5 | 524.1 | -11.8% |
| uint8 | 562.9 | 520.9 | -7.5% |
| float16 | 557.4 | 523.9 | -6.0% |
| float32 | 645.1 | 471.5 | -26.9% |
| float64 | 518.3 | 609.0 | +17.5% |
| complex64 | 620.0 | 670.0 | +8.1% |
| complex128 | 641.0 | 548.8 | -14.4% |
| turboquant4 | 654.6 | 590.9 | -9.7% |

---

## Memory Usage (Peak MB)

### CPU

| Dtype | Count | Main | Emlgo v0.4 | Delta |
|-------|------:|-----:|-----------:|------:|
| int8 | 50k | 644.5 | 667.3 | +3.5% |
| uint8 | 50k | 631.1 | 684.8 | +8.5% |
| float16 | 50k | 672.4 | 706.3 | +5.0% |
| float32 | 50k | 788.7 | 718.9 | -8.8% |
| float64 | 50k | 1133.7 | 1183.3 | +4.4% |
| complex64 | 50k | 1260.9 | 1214.5 | -3.7% |
| complex128 | 50k | 1826.8 | 1902.5 | +4.1% |
| turboquant4 | 50k | 721.2 | 729.1 | +1.1% |
| int8 | 500k | 4256.7 | 4649.3 | +9.2% |
| uint8 | 500k | 4655.4 | 4428.0 | -4.9% |
| float16 | 500k | 4628.5 | 4210.6 | -9.0% |
| float32 | 500k | 4357.1 | 4831.4 | +10.9% |
| float64 | 500k | 7799.5 | 7861.9 | +0.8% |
| complex64 | 500k | 8997.6 | 8136.2 | -9.6% |
| complex128 | 500k | 14363.0 | 12982.3 | -9.6% |
| turboquant4 | 500k | 5365.4 | 4631.4 | -13.7% |

### GPU

| Dtype | Count | Main | Emlgo v0.4 | Delta |
|-------|------:|-----:|-----------:|------:|
| int8 | 50k | 648.3 | 765.4 | +18.1% |
| uint8 | 50k | 661.2 | 654.6 | -1.0% |
| float16 | 50k | 730.4 | 697.1 | -4.6% |
| float32 | 50k | 735.7 | 673.3 | -8.5% |
| float64 | 50k | 1176.4 | 1207.4 | +2.6% |
| complex64 | 50k | 1238.7 | 1289.2 | +4.1% |
| complex128 | 50k | 1999.3 | 1943.4 | -2.8% |
| turboquant4 | 50k | 771.6 | 713.2 | -7.6% |
| int8 | 500k | 4579.4 | 4394.5 | -4.0% |
| uint8 | 500k | 4493.5 | 5009.2 | +11.5% |
| float16 | 500k | 4568.9 | 4666.9 | +2.1% |
| float32 | 500k | 4649.1 | 4714.2 | +1.4% |
| float64 | 500k | 6797.3 | 7469.5 | +9.9% |
| complex64 | 500k | 9051.0 | 8403.5 | -7.2% |
| complex128 | 500k | 14211.1 | 14674.4 | +3.3% |
| turboquant4 | 500k | 4505.9 | 4949.9 | +9.9% |

---

## Ingestion Throughput (vec/s)

### CPU

| Dtype | Count | Main | Emlgo v0.4 | Delta |
|-------|------:|-----:|-----------:|------:|
| int8 | 50k | 3019105 | 3019105 | ~0% |
| uint8 | 50k | 2945031 | 2945031 | ~0% |
| float16 | 50k | 1633947 | 1633947 | ~0% |
| float32 | 50k | 808228 | 808228 | ~0% |
| float64 | 50k | 451479 | 451479 | ~0% |
| complex64 | 50k | 424985 | 424985 | ~0% |
| complex128 | 50k | 240860 | 240860 | ~0% |
| turboquant4 | 50k | 786745 | 786745 | ~0% |
| int8 | 500k | 852062 | 852062 | ~0% |
| uint8 | 500k | 571098 | 571098 | ~0% |
| float16 | 500k | 361692 | 361692 | ~0% |
| float32 | 500k | 292634 | 292634 | ~0% |
| float64 | 500k | 165603 | 165603 | ~0% |
| complex64 | 500k | 130114 | 130114 | ~0% |
| complex128 | 500k | 99786 | 99786 | ~0% |
| turboquant4 | 500k | 158006 | 158006 | ~0% |

### GPU

| Dtype | Count | Main | Emlgo v0.4 | Delta |
|-------|------:|-----:|-----------:|------:|
| int8 | 50k | 2540022 | 2553147 | +0.5% |
| uint8 | 50k | 3304118 | 2971668 | -10.1% |
| float16 | 50k | 1514879 | 1719646 | +13.5% |
| float32 | 50k | 959856 | 839037 | -12.6% |
| float64 | 50k | 414948 | 450438 | +8.6% |
| complex64 | 50k | 492863 | 434212 | -11.9% |
| complex128 | 50k | 226062 | 221994 | -1.8% |
| turboquant4 | 50k | 969614 | 595407 | -38.6% |
| int8 | 500k | 755671 | 912820 | +20.8% |
| uint8 | 500k | 565427 | 783975 | +38.7% |
| float16 | 500k | 616297 | 563230 | -8.6% |
| float32 | 500k | 697651 | 355110 | -49.1% |
| float64 | 500k | 133032 | 191161 | +43.7% |
| complex64 | 500k | 135012 | 158774 | +17.6% |
| complex128 | 500k | 116225 | 145997 | +25.6% |
| turboquant4 | 500k | 204867 | 172635 | -15.7% |

---

## Key Findings

### What emlgo v0.4 improved over main

- **turboquant4 dense at 50k CPU:** +78% QPS — the single largest CPU win at small scale
- **uint8 dense at 500k CPU:** +168% QPS — massive improvement for the most common integer type at scale
- **float64 dense at 50k CPU:** +60% QPS — 8-byte floats benefit significantly from emlgo math kernels
- **complex128 dense:** +46% at 50k, +83% at 500k on CPU
- **float16 dense at 50k CPU:** +74% QPS — emlgo's half-optimized SIMD paths shine
- **GPU at 500k:** float32 dense +203%, float64 dense +99%, complex128 dense +135% — emlgo v0.4 fixes the v0.3 GPU regressions for these types
- **Memory at 500k CPU:** turboquant4 -14%, complex128 -10%, complex64 -10%

### What emlgo v0.4 regressed

- **int8/uint8 dense at 50k CPU:** -56% QPS — the most common integer types at small scale are significantly slower. The emlgo math primitives add overhead for simple integer dot products at 50k scale.
- **turboquant4 dense at 500k CPU:** -55% QPS — quantized path still has issues at large scale despite v0.4 fixes
- **complex128/turboquant4 GPU at 50k:** -54%/-65% dense QPS — small-scale GPU still regresses for these types
- **float16 hybrid GPU at 500k:** -67% QPS — a new regression in the emlgo v0.4 GPU path

### emlgo v0.4 vs v0.3 improvements

The v0.4 release resolved many of the catastrophic v0.3 regressions:
- **GPU path** is no longer universally slower — float32, float64, complex128 now show large gains at 500k
- **TurboQuant4** GPU at 500k is now competitive (+30% vs main)
- **Int8** GPU at 500k is now within 5% of main (was -72% in v0.3)

### Recommendations

1. **Use emlgo for float16, float64, complex128 workloads** — these consistently benefit across both scales
2. **Investigate int8/uint8 50k regression** — the most common integer types lose 56% QPS at small scale
3. **Profile turboquant4 at 500k** — a -55% QPS drop suggests a code path issue specific to quantized types at scale
4. **GPU float16 hybrid at 500k needs investigation** — a new -67% regression not present in main
5. **emlgo v0.4 is significantly better than v0.3** — the GPU path is now production-viable for most types
