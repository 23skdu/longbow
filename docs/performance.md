# Longbow Performance Benchmarks

**Date:** 2026-09-11

## System Specifications

| Component | Detail |
|---|---|
| CPU | 16-core i7-12650H (x86_64, AVX2) |
| RAM | 23 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4) |

| Binary | Description |
|---|---|
| `bin/longbow_main` | CPU standard build |
| `bin/longbow_emlgo` | CPU emlgo build |
| `bin/longbow-cuda_main` | GPU standard build |
| `bin/longbow-cuda_emlgo` | GPU emlgo build |

**Configuration:** 8 workers, 500 queries, 128 dimensions, auto-spill enabled (threshold 60%).

---

## 1. Executive Summary

The emlgo build delivers significant gains on select dtype/count combinations but exhibits notable regressions on others. On CPU, emlgo's standout wins are **float32 100k dense (+40.34%)**, **complex128 100k dense (+159.31%)**, and **turboquant 10k dense (+28.60%)**. The worst CPU regressions include **complex64 500k dense (-37.96%)**, **float64 100k dense (-28.69%)**, and **int8 100k dense (-25.83%)**.

On GPU, emlgo excels at larger counts — **uint8 500k dense (+159.12%)**, **uint8 500k graphrag (+290.43%)**, **complex128 500k sparse (+176.96%)**, and **uint8 500k hybrid (+181.13%)**. The deepest GPU regressions are **turboquant 100k dense (-57.40%)**, **turboquant 100k graphrag (-55.75%)**, and **complex128 100k dense (-49.75%)**.

A clear pattern emerges: emlgo tends to regress at the 100k count for many dtypes (especially on CPU), but often recovers or surpasses standard at 500k. GPU performance with emlgo is broadly stronger than CPU, particularly for sparse and hybrid workloads.

---

## 2. CPU A/B — Dense Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 4151.56 | 4078.80 | -1.75% | 2868.70 | 2127.61 | -25.83% | 1532.96 | 1579.34 | +3.03% |
| uint8 | 4327.27 | 4539.58 | +4.91% | 3009.98 | 2394.09 | -20.46% | 2168.69 | 2592.86 | +19.56% |
| float16 | 3829.30 | 3796.61 | -0.85% | 2016.67 | 1494.48 | -25.89% | 1417.47 | 1458.71 | +2.91% |
| float32 | 3464.98 | 3657.06 | +5.54% | 1243.54 | 1745.15 | +40.34% | 3398.44 | 3312.34 | -2.53% |
| float64 | 3537.18 | 3390.44 | -4.15% | 1403.64 | 1000.90 | -28.69% | 741.55 | 586.09 | -20.96% |
| complex64 | 3546.21 | 3547.00 | +0.02% | 1496.08 | 1188.66 | -20.55% | 404.28 | 250.83 | -37.96% |
| complex128 | 3580.29 | 3414.55 | -4.63% | 1347.00 | 3492.86 | +159.31% | 440.65 | 533.03 | +20.97% |
| turboquant | 3416.65 | 4393.78 | +28.60% | 1885.38 | 1566.94 | -16.89% | 3339.05 | 3253.33 | -2.57% |

---

## 3. CPU A/B — Sparse Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 7691.31 | 7244.40 | -5.81% | 8059.08 | 6659.29 | -17.37% | 6568.58 | 6184.63 | -5.85% |
| uint8 | 7779.66 | 7632.69 | -1.89% | 8275.25 | 6860.08 | -17.10% | 6670.13 | 6638.07 | -0.48% |
| float16 | 7687.07 | 8040.86 | +4.60% | 7972.37 | 6367.62 | -20.13% | 6787.01 | 6397.02 | -5.75% |
| float32 | 8030.47 | 7449.41 | -7.24% | 7781.57 | 6724.05 | -13.59% | 6691.23 | 6553.03 | -2.07% |
| float64 | 8257.33 | 7067.92 | -14.40% | 8221.24 | 6872.16 | -16.41% | 6853.20 | 4018.28 | -41.37% |
| complex64 | 7583.73 | 7086.57 | -6.56% | 7863.69 | 6655.60 | -15.36% | 2290.72 | 2337.32 | +2.03% |
| complex128 | 7802.03 | 7623.63 | -2.29% | 6500.99 | 7080.05 | +8.91% | 3913.14 | 3581.34 | -8.48% |
| turboquant | 8434.82 | 8153.67 | -3.33% | 7276.20 | 4988.01 | -31.45% | 7060.92 | 6638.10 | -5.99% |

---

## 4. CPU A/B — Hybrid Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 4452.95 | 4055.79 | -8.92% | 2409.93 | 1765.07 | -26.76% | 1139.92 | 1013.40 | -11.10% |
| uint8 | 4506.51 | 4482.92 | -0.52% | 2519.46 | 1979.02 | -21.45% | 1854.20 | 1971.57 | +6.33% |
| float16 | 4506.51 | 4283.23 | -4.95% | 1893.83 | 1552.79 | -18.01% | 1472.87 | 1319.05 | -10.44% |
| float32 | 4206.51 | 3550.90 | -15.59% | 1182.23 | 1615.27 | +36.63% | 3564.19 | 3302.56 | -7.34% |
| float64 | 4262.78 | 3777.91 | -11.37% | 1117.40 | 987.11 | -11.66% | 460.35 | 458.74 | -0.35% |
| complex64 | 4021.90 | 3608.24 | -10.29% | 1151.82 | 895.06 | -22.29% | 359.36 | 217.52 | -39.47% |
| complex128 | 4007.43 | 3788.32 | -5.47% | 1270.76 | 2256.26 | +77.55% | 455.28 | 303.65 | -33.31% |
| turboquant | 4264.26 | 4415.05 | +3.54% | 1744.19 | 1529.52 | -12.31% | 3759.33 | 3347.36 | -10.96% |

---

## 5. CPU A/B — Graphrag Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 3718.56 | 3146.49 | -15.38% | 2223.23 | 1652.15 | -25.69% | 1409.73 | 494.37 | -64.93% |
| uint8 | 3417.26 | 3022.33 | -11.56% | 2472.31 | 1676.74 | -32.18% | 1358.59 | 1452.93 | +6.94% |
| float16 | 3706.06 | 3067.76 | -17.22% | 1756.16 | 1453.99 | -17.21% | 1392.69 | 1026.06 | -26.33% |
| float32 | 3569.44 | 3501.28 | -1.91% | 1214.14 | 1727.90 | +42.31% | 3294.37 | 3001.59 | -8.89% |
| float64 | 3412.18 | 2719.53 | -20.30% | 1163.39 | 838.85 | -27.90% | 484.52 | 478.05 | -1.34% |
| complex64 | 3470.22 | 2736.72 | -21.14% | 1375.22 | 1030.50 | -25.07% | 347.39 | 285.10 | -17.93% |
| complex128 | 3632.44 | 2901.96 | -20.11% | 1776.11 | 2577.29 | +45.11% | 337.44 | 439.07 | +30.12% |
| turboquant | 3761.40 | 3940.17 | +4.75% | 1648.34 | 1596.70 | -3.13% | 3435.43 | 2543.24 | -25.97% |

---

## 6. CPU A/B — Temporal Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 3333.97 | 2518.75 | -24.45% | 2247.47 | 1753.02 | -22.00% | 702.62 | 645.72 | -8.10% |
| uint8 | 3345.74 | 2644.11 | -20.97% | 2112.55 | 1345.57 | -36.31% | 703.67 | 690.24 | -1.91% |
| float16 | 3196.88 | 2540.59 | -20.53% | 2226.93 | 1493.32 | -32.94% | 683.44 | 452.83 | -33.74% |
| float32 | 3953.70 | 3319.76 | -16.03% | 2611.77 | 2126.74 | -18.57% | 793.69 | 739.94 | -6.77% |
| float64 | 4225.22 | 3125.36 | -26.03% | 2461.17 | 2042.69 | -17.00% | 806.57 | 498.51 | -38.19% |
| complex64 | 4051.07 | 3210.83 | -20.74% | 2505.99 | 2059.27 | -17.83% | 270.38 | 543.06 | +100.85% |
| complex128 | 4196.89 | 3297.69 | -21.43% | 2517.05 | 2027.63 | -19.44% | 650.54 | 595.23 | -8.50% |
| turboquant | 4102.77 | 3325.73 | -18.94% | 2558.97 | 1974.67 | -22.83% | 727.86 | 561.86 | -22.81% |

---

## 7. GPU A/B — Dense Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 3724.46 | 4101.62 | +10.13% | 1949.28 | 2157.82 | +10.70% | 1636.40 | 1561.56 | -4.57% |
| uint8 | 4480.37 | 4091.39 | -8.68% | 2249.76 | 2742.08 | +21.88% | 1912.61 | 4955.98 | +159.12% |
| float16 | 3859.76 | 3609.35 | -6.49% | 2234.30 | 1957.81 | -12.37% | 1230.60 | 1302.99 | +5.88% |
| float32 | 4061.74 | 3359.23 | -17.30% | 2459.91 | 2322.96 | -5.57% | 2879.88 | 3335.94 | +15.84% |
| float64 | 3305.64 | 3440.98 | +4.09% | 1059.12 | 1159.52 | +9.48% | 659.33 | 414.07 | -37.20% |
| complex64 | 3305.18 | 3551.05 | +7.44% | 1029.21 | 1455.87 | +41.45% | 609.08 | 505.18 | -17.06% |
| complex128 | 3447.51 | 3230.18 | -6.30% | 3418.53 | 1717.79 | -49.75% | 348.87 | 796.73 | +128.38% |
| turboquant | 3413.99 | 3804.77 | +11.45% | 3198.66 | 1362.56 | -57.40% | 2969.05 | 3367.91 | +13.43% |

---

## 8. GPU A/B — Sparse Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 6959.62 | 7086.87 | +1.83% | 6628.50 | 7366.93 | +11.14% | 6106.81 | 5218.43 | -14.55% |
| uint8 | 7619.63 | 7393.32 | -2.97% | 4998.65 | 7493.74 | +49.92% | 5944.20 | 6508.36 | +9.49% |
| float16 | 7254.44 | 6836.90 | -5.76% | 6807.97 | 7031.34 | +3.28% | 6544.70 | 6770.71 | +3.45% |
| float32 | 6911.43 | 7597.80 | +9.93% | 6694.27 | 7320.60 | +9.36% | 6504.35 | 7541.68 | +15.95% |
| float64 | 6834.38 | 8122.56 | +18.85% | 6999.27 | 7481.47 | +6.89% | 6883.81 | 7242.55 | +5.21% |
| complex64 | 6855.28 | 6781.18 | -1.08% | 6926.18 | 7564.40 | +9.21% | 5719.01 | 5648.58 | -1.23% |
| complex128 | 7075.06 | 6905.28 | -2.40% | 6659.13 | 7263.99 | +9.08% | 2103.11 | 5824.83 | +176.96% |
| turboquant | 7070.93 | 7421.55 | +4.96% | 6810.12 | 7216.80 | +5.97% | 6124.66 | 7078.98 | +15.58% |

---

## 9. GPU A/B — Hybrid Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 4102.17 | 3893.33 | -5.09% | 1669.16 | 1915.52 | +14.76% | 1191.49 | 1623.96 | +36.30% |
| uint8 | 4338.10 | 4099.26 | -5.51% | 1734.97 | 2177.22 | +25.49% | 1625.36 | 4569.34 | +181.13% |
| float16 | 3841.40 | 3948.68 | +2.79% | 1421.70 | 1661.64 | +16.88% | 1076.52 | 1309.34 | +21.63% |
| float32 | 3931.64 | 3810.88 | -3.07% | 2362.21 | 2181.18 | -7.66% | 3136.99 | 3512.28 | +11.96% |
| float64 | 2946.38 | 4219.00 | +43.19% | 805.18 | 1008.91 | +25.30% | 623.11 | 659.43 | +5.83% |
| complex64 | 3371.48 | 3795.68 | +12.58% | 819.63 | 1139.51 | +39.03% | 650.83 | 688.46 | +5.78% |
| complex128 | 3492.83 | 3612.41 | +3.42% | 1175.34 | 1049.85 | -10.68% | 642.42 | 699.71 | +8.92% |
| turboquant | 3622.94 | 3597.72 | -0.70% | 2909.19 | 1316.52 | -54.75% | 2823.36 | 3782.78 | +33.98% |

---

## 10. GPU A/B — Graphrag Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2790.72 | 3008.97 | +7.82% | 1518.08 | 1757.49 | +15.77% | 1278.87 | 1280.01 | +0.09% |
| uint8 | 2576.19 | 2659.58 | +3.24% | 1566.16 | 1955.80 | +24.88% | 1275.76 | 4980.96 | +290.43% |
| float16 | 2954.08 | 2827.08 | -4.30% | 1658.84 | 1954.32 | +17.81% | 1067.67 | 1137.73 | +6.56% |
| float32 | 3550.21 | 3514.98 | -0.99% | 2328.15 | 2134.99 | -8.30% | 2880.88 | 3551.39 | +23.27% |
| float64 | 2620.51 | 2905.33 | +10.87% | 911.82 | 1527.02 | +67.47% | 672.57 | 626.12 | -6.91% |
| complex64 | 2578.29 | 2843.45 | +10.28% | 859.10 | 1183.84 | +37.80% | 632.83 | 725.38 | +14.63% |
| complex128 | 2645.93 | 2799.58 | +5.81% | 2765.58 | 2197.59 | -20.54% | 734.77 | 716.91 | -2.43% |
| turboquant | 3589.26 | 3575.92 | -0.37% | 2924.51 | 1294.04 | -55.75% | 2124.10 | 3429.28 | +61.45% |

---

## 11. GPU A/B — Temporal Search

| dtype | 10k std | 10k emlgo | 10k delta | 100k std | 100k emlgo | 100k delta | 500k std | 500k emlgo | 500k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2394.73 | 2432.20 | +1.56% | 1649.11 | 1781.39 | +8.02% | 646.47 | 698.24 | +8.01% |
| uint8 | 2457.27 | 2463.46 | +0.25% | 1622.48 | 1685.79 | +3.90% | 638.00 | 677.92 | +6.26% |
| float16 | 2450.96 | 2471.99 | +0.86% | 1560.62 | 1773.84 | +13.66% | 647.46 | 668.45 | +3.24% |
| float32 | 3119.43 | 3392.18 | +8.74% | 1933.87 | 2272.81 | +17.53% | 729.38 | 753.87 | +3.36% |
| float64 | 3050.99 | 3370.19 | +10.46% | 2081.30 | 2290.39 | +10.05% | 687.54 | 729.67 | +6.13% |
| complex64 | 2908.32 | 3167.20 | +8.90% | 1829.86 | 2199.67 | +20.21% | 585.50 | 575.84 | -1.65% |
| complex128 | 3183.81 | 3343.05 | +5.00% | 1944.61 | 2048.35 | +5.33% | 481.15 | 549.53 | +14.21% |
| turboquant | 3076.77 | 3506.52 | +13.97% | 1988.07 | 2258.78 | +13.62% | 684.54 | 766.95 | +12.04% |

---

## 12. Cross-Config Best QPS

For each (dtype, count, search type), the winning build is listed.

### Dense Search

| dtype | 10k Winner | 100k Winner | 500k Winner |
|---|---|---|---|
| int8 | std (4151.56) | std (2868.70) | emlgo (1579.34) |
| uint8 | emlgo (4539.58) | std (3009.98) | emlgo (2592.86) |
| float16 | std (3829.30) | std (2016.67) | emlgo (1458.71) |
| float32 | emlgo (3657.06) | emlgo (1745.15) | std (3398.44) |
| float64 | std (3537.18) | std (1403.64) | std (741.55) |
| complex64 | emlgo (3547.00) | std (1496.08) | std (404.28) |
| complex128 | std (3580.29) | emlgo (3492.86) | emlgo (533.03) |
| turboquant | emlgo (4393.78) | std (1885.38) | std (3339.05) |

### Sparse Search

| dtype | 10k Winner | 100k Winner | 500k Winner |
|---|---|---|---|
| int8 | std (7691.31) | std (8059.08) | std (6568.58) |
| uint8 | std (7779.66) | std (8275.25) | std (6670.13) |
| float16 | emlgo (8040.86) | std (7972.37) | std (6787.01) |
| float32 | std (8030.47) | std (7781.57) | std (6691.23) |
| float64 | std (8257.33) | std (8221.24) | std (6853.20) |
| complex64 | std (7583.73) | std (7863.69) | emlgo (2337.32) |
| complex128 | std (7802.03) | emlgo (7080.05) | std (3913.14) |
| turboquant | std (8434.82) | std (7276.20) | std (7060.92) |

### Hybrid Search

| dtype | 10k Winner | 100k Winner | 500k Winner |
|---|---|---|---|
| int8 | std (4452.95) | std (2409.93) | std (1139.92) |
| uint8 | std (4506.51) | std (2519.46) | emlgo (1971.57) |
| float16 | std (4506.51) | std (1893.83) | std (1472.87) |
| float32 | std (4206.51) | emlgo (1615.27) | std (3564.19) |
| float64 | std (4262.78) | std (1117.40) | std (460.35) |
| complex64 | std (4021.90) | std (1151.82) | std (359.36) |
| complex128 | std (4007.43) | emlgo (2256.26) | std (455.28) |
| turboquant | emlgo (4415.05) | std (1744.19) | std (3759.33) |

### Graphrag Search

| dtype | 10k Winner | 100k Winner | 500k Winner |
|---|---|---|---|
| int8 | std (3718.56) | std (2223.23) | std (1409.73) |
| uint8 | std (3417.26) | std (2472.31) | emlgo (1452.93) |
| float16 | std (3706.06) | std (1756.16) | std (1392.69) |
| float32 | std (3569.44) | emlgo (1727.90) | std (3294.37) |
| float64 | std (3412.18) | std (1163.39) | std (484.52) |
| complex64 | std (3470.22) | std (1375.22) | std (347.39) |
| complex128 | std (3632.44) | emlgo (2577.29) | emlgo (439.07) |
| turboquant | emlgo (3940.17) | std (1648.34) | std (3435.43) |

### Temporal Search

| dtype | 10k Winner | 100k Winner | 500k Winner |
|---|---|---|---|
| int8 | std (3333.97) | std (2247.47) | std (702.62) |
| uint8 | std (3345.74) | std (2112.55) | std (703.67) |
| float16 | std (3196.88) | std (2226.93) | std (683.44) |
| float32 | std (3953.70) | std (2611.77) | std (793.69) |
| float64 | std (4225.22) | std (2461.17) | std (806.57) |
| complex64 | std (4051.07) | std (2505.99) | emlgo (543.06) |
| complex128 | std (4196.89) | std (2517.05) | std (650.54) |
| turboquant | std (4102.77) | std (2558.97) | std (727.86) |

---

## 13. Peak Memory

| dtype | Count | CPU std (MB) | CPU emlgo (MB) | GPU std (MB) | GPU emlgo (MB) |
|---|---|---|---|---|---|
| int8 | 10k | 448.18 | 432.30 | 421.23 | 525.36 |
| int8 | 100k | 1140.31 | 1105.29 | 1068.67 | 1203.38 |
| int8 | 500k | 4572.18 | 4305.14 | 4577.48 | 4233.65 |
| uint8 | 10k | 476.12 | 420.89 | 452.79 | 579.72 |
| uint8 | 100k | 1175.09 | 1088.05 | 1050.53 | 1183.24 |
| uint8 | 500k | 4651.71 | 4692.32 | 4314.17 | 4653.01 |
| float16 | 10k | 442.59 | 487.95 | 477.35 | 546.52 |
| float16 | 100k | 1224.50 | 1178.62 | 1173.96 | 1301.70 |
| float16 | 500k | 4861.31 | 4482.06 | 4618.37 | 4759.70 |
| float32 | 10k | 357.88 | 372.33 | 355.41 | 481.28 |
| float32 | 100k | 1153.50 | 1143.28 | 1195.94 | 1274.13 |
| float32 | 500k | 5197.42 | 4745.66 | 4722.07 | 5481.51 |
| float64 | 10k | 613.65 | 588.61 | 591.11 | 726.89 |
| float64 | 100k | 1858.98 | 2008.74 | 1862.75 | 2037.95 |
| float64 | 500k | 7171.96 | 10632.59 | 7520.42 | 8181.87 |
| complex64 | 10k | 559.34 | 611.95 | 649.12 | 720.55 |
| complex64 | 100k | 2074.55 | 1824.08 | 1846.02 | 2068.87 |
| complex64 | 500k | 7294.88 | 8932.79 | 8854.07 | 7534.86 |
| complex128 | 10k | 788.21 | 799.47 | 818.19 | 879.47 |
| complex128 | 100k | 3484.52 | 2678.87 | 3317.77 | 3185.10 |
| complex128 | 500k | 14093.76 | 15211.54 | 14845.71 | 14836.03 |
| turboquant | 10k | 383.94 | 371.24 | 365.94 | 493.22 |
| turboquant | 100k | 1230.52 | 1177.25 | 1179.57 | 1359.45 |
| turboquant | 500k | 5314.37 | 4167.35 | 5348.85 | 4315.80 |

---

## 14. Key Findings

### What emlgo Improves

- **CPU float32 dense at 100k** (+40.34%) and **CPU complex128 dense at 100k** (+159.31%) are the largest single-count gains, suggesting emlgo optimizes memory-bound medium-count workloads for wider types.
- **GPU sparse search** is broadly improved — emlgo wins on 18 of 24 sparse combinations, with standout gains at 500k (complex128 +176.96%, uint8 +9.49%, float32 +15.95%).
- **GPU hybrid and graphrag** at 500k show strong emlgo wins: uint8 hybrid +181.13%, uint8 graphrag +290.43%, float32 graphrag +23.27%, turboquant graphrag +61.45%.
- **GPU temporal search** is consistently better with emlgo — every dtype at every count wins or is within ~2%, with gains up to +13.97% (turboquant 10k).
- CPU turboquant 10k dense gains +28.60% with emlgo.
- Memory usage at 500k is often lower with emlgo (int8, float16, float32, turboquant), indicating better large-index memory efficiency.

### What emlgo Regresses

- **CPU 100k count is the weak spot** — emlgo loses at 100k across nearly every CPU search type and dtype, with deltas often -15% to -30%. This is the most consistent regression pattern.
- **CPU temporal search** is almost entirely regressive for emlgo, losing on 22 of 24 combinations. The 500k complex64 outlier (+100.85%) is an exception.
- **CPU sparse search** at 100k is uniformly worse with emlgo, with float64 500k sparse hitting -41.37%.
- **GPU turboquant 100k** regresses sharply across dense (-57.40%), hybrid (-54.75%), and graphrag (-55.75%).
- **GPU complex128 100k dense** drops -49.75% — a significant regression for a type that otherwise benefits from emlgo at 500k.
- **float64 500k CPU dense** (-20.96%) and **GPU dense** (-37.20%) regress, with emlgo memory usage spiking to 10.6 GB (vs 7.2 GB std) on CPU — suggesting spill or allocation overhead.
- **complex64 500k CPU dense** (-37.96%) and **CPU hybrid** (-39.47%) are the worst complex64 regressions.

### Historical Comparisons

- emlgo shows a consistent "U-shaped" performance curve: competitive or winning at 10k, regressing at 100k, then recovering at 500k for many dtypes. This pattern is visible across CPU dense, hybrid, and graphrag.
- GPU builds benefit more uniformly from emlgo than CPU builds, likely due to better memory coalescing or reduced host-device transfer overhead in the emlgo code paths.
- The turboquant dtype is volatile with emlgo — it has some of the best gains (CPU dense 10k +28.60%) and some of the worst regressions (GPU dense 100k -57.40%), suggesting the quantization path interacts poorly with emlgo at specific scales.
- Peak memory for emlgo at 500k is generally lower than standard on CPU (except float64 and complex128), indicating the emlgo build uses more compact representations for common integer and float types at scale.
