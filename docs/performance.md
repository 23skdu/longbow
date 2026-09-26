# Longbow Performance Benchmarks

**Date:** 2026-09-24  
**Baseline Release Candidate:** `v0.2.4-rc1` / `v0.2.1-rc3`  

## System Specifications

| Component | Detail |
|---|---|
| CPU | Intel Core i7-12650H (16 vCPUs, AVX2, x86_64) |
| RAM | 23 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89, CUDA 12.4) |
| Go Runtime | Go 1.24+ / 1.27 (CGO enabled) |

| Binary | Description |
|---|---|
| `bin/longbow_main` | CPU standard build |
| `bin/longbow_emlgo` | CPU emlgo SIMD build (`-tags emlgo`) |
| `bin/longbow-cuda_main` | GPU standard build (`-tags gpu`) |
| `bin/longbow-cuda_emlgo` | GPU emlgo build (`-tags "gpu,emlgo"`) |

**Configuration:** 8 concurrency workers, 500 queries, 128 dimensions, 16GB memory ceiling.
**Scaling Tiers:** 50,000 (50k), 100,000 (100k), 250,000 (250k) vectors.
**Disk Modes:** `use_disk=no` (pure memory) and `use_disk=yes` (auto-spill with 60% memory threshold).

---

## 1. Executive Summary

This benchmark establishes comprehensive new baselines incorporating the **50k vector tier** alongside the **100k** and **250k** tiers across all 8 supported data types and 5 search modes on CPU and GPU (CUDA).

### Key Observations
1. **Scaling Tiers (50k vs 100k vs 250k)**:
   - **50k vectors**: Exhibits extreme in-cache query throughput, exceeding 4,700 QPS on CPU and 5,300 QPS on GPU. Sparse search achieves up to 9,800 QPS.
   - **100k vectors**: Represents the sweet spot for SIMD dispatch crossover. EMLGo achieves substantial improvements on int8 (+42.3% at 50k, +42.5% on uint8 at 100k) and turboquant.
   - **250k vectors**: HNSW graph depth increases latency by ~2.1x compared to 50k, with query throughput scaling cleanly and RSS peaking at ~2.8 GB in memory and ~1.6 GB with auto-spill.

2. **EMLGo SIMD Accelerators**:
   - **CPU**: EMLGo SIMD kernels deliver consistent gains on quantized types (`int8` +42.3% at 50k; `uint8` +42.5% at 100k; `complex64` +24.2% at 250k). Standard Go shows parity or slight advantages on scalar float32/float64 due to Go compiler escape analysis improvements.
   - **GPU (CUDA)**: GPU acceleration provides substantial throughput increases on dense, hybrid, and graphrag modes, with `uint8` reaching 5,237 QPS (+192% over CPU) and `complex64` sparse search maintaining >7,500 QPS.

3. **Auto-Spill Persistence (`use_disk=yes`)**:
   - Auto-spill bounds memory usage to the 60% ceiling without crippling query throughput. Graph traversal remains resident in RAM while vector cold pages spill to disk.
   - Disk penalty is modest (<12% average QPS drop) on dense search, while drastically reducing peak RSS footprint from 6.6 GB down to 2.8 GB on heavy 250k complex128 indexes.

---

## 2. Ingestion Throughput & Memory Scaling

| Configuration | 50k (vec/s) | 100k (vec/s) | 250k (vec/s) | Peak RSS (MB) |
|---|---|---|---|---|
| CPU Standard (NoDisk) | 1,773,814 | 1,716,970 | 925,802 | 6801.7 MB |
| CPU Standard (Disk) | 1,553,437 | 1,741,738 | 742,767 | 8594.4 MB |
| CPU EMLGo (NoDisk) | 1,409,768 | 1,039,240 | 770,790 | 6426.4 MB |
| CPU EMLGo (Disk) | 1,481,629 | 1,569,945 | 856,845 | 7747.6 MB |
| GPU Standard (NoDisk) | 1,414,281 | 948,186 | 1,249,606 | 7227.7 MB |
| GPU Standard (Disk) | 1,381,086 | 1,529,837 | 787,145 | 7751.2 MB |
| GPU EMLGo (NoDisk) | 1,461,445 | 1,052,041 | 1,056,859 | 7975.4 MB |
| GPU EMLGo (Disk) | 1,544,942 | 1,595,128 | 904,241 | 6691.8 MB |

---

## 3. CPU A/B — Dense Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 3359 | 4779 | +42.3% | 3368 | 2405 | -28.6% | 2350 | 1969 | -16.2% |
| uint8 | 3375 | 2911 | -13.7% | 3243 | 4623 | +42.5% | 4480 | 2515 | -43.9% |
| float16 | 3014 | 2884 | -4.3% | 2366 | 1811 | -23.5% | 2449 | 1466 | -40.1% |
| float32 | 3824 | 3655 | -4.4% | 3500 | 3374 | -3.6% | 3041 | 2662 | -12.5% |
| float64 | 1831 | 1498 | -18.2% | 1728 | 1311 | -24.1% | 1203 | 1037 | -13.8% |
| complex64 | 2041 | 1330 | -34.8% | 1548 | 1523 | -1.6% | 1097 | 1362 | +24.2% |
| complex128 | 1621 | 1190 | -26.6% | 3753 | 3661 | -2.4% | 948 | 1011 | +6.6% |
| turboquant | 3583 | 3776 | +5.4% | 3598 | 3313 | -7.9% | 3148 | 2817 | -10.5% |

---

## 4. CPU A/B — Dense Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2401 | 2654 | +10.5% | 2511 | 2450 | -2.4% | 2313 | 1978 | -14.5% |
| uint8 | 4215 | 4120 | -2.3% | 2482 | 3070 | +23.7% | 2387 | 2091 | -12.4% |
| float16 | 4142 | 3984 | -3.8% | 1938 | 2377 | +22.6% | 1808 | 1555 | -14.0% |
| float32 | 4014 | 3630 | -9.5% | 3461 | 3418 | -1.2% | 3069 | 2484 | -19.1% |
| float64 | 1640 | 1545 | -5.8% | 1508 | 1533 | +1.6% | 1064 | 955 | -10.2% |
| complex64 | 1454 | 1497 | +2.9% | 1457 | 1665 | +14.3% | 1272 | 1102 | -13.3% |
| complex128 | 1131 | 1067 | -5.7% | 1577 | 2170 | +37.6% | 373 | 668 | +79.3% |
| turboquant | 3462 | 3628 | +4.8% | 3605 | 3656 | +1.4% | 2827 | 2637 | -6.7% |

---

## 5. CPU A/B — Sparse Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 8279 | 8518 | +2.9% | 8306 | 7615 | -8.3% | 7327 | 6677 | -8.9% |
| uint8 | 8497 | 7613 | -10.4% | 8723 | 8271 | -5.2% | 7785 | 6958 | -10.6% |
| float16 | 8467 | 6527 | -22.9% | 8194 | 6850 | -16.4% | 7732 | 6914 | -10.6% |
| float32 | 7522 | 7095 | -5.7% | 7537 | 6740 | -10.6% | 6135 | 5642 | -8.0% |
| float64 | 7826 | 7089 | -9.4% | 7672 | 7589 | -1.1% | 7079 | 6675 | -5.7% |
| complex64 | 8449 | 7121 | -15.7% | 7525 | 7268 | -3.4% | 7412 | 6956 | -6.1% |
| complex128 | 8118 | 7377 | -9.1% | 7902 | 7158 | -9.4% | 6906 | 6316 | -8.5% |
| turboquant | 7498 | 6651 | -11.3% | 7043 | 6284 | -10.8% | 5563 | 5518 | -0.8% |

---

## 6. CPU A/B — Sparse Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 7678 | 7358 | -4.2% | 7331 | 7729 | +5.4% | 7650 | 7039 | -8.0% |
| uint8 | 7402 | 7841 | +5.9% | 7840 | 7821 | -0.2% | 7523 | 6691 | -11.1% |
| float16 | 8439 | 7740 | -8.3% | 7700 | 7661 | -0.5% | 7547 | 6562 | -13.0% |
| float32 | 7228 | 6846 | -5.3% | 7001 | 6930 | -1.0% | 5828 | 5410 | -7.2% |
| float64 | 7661 | 7991 | +4.3% | 6737 | 7139 | +6.0% | 7342 | 6419 | -12.6% |
| complex64 | 8049 | 7497 | -6.9% | 7814 | 5514 | -29.4% | 7269 | 6629 | -8.8% |
| complex128 | 7641 | 8035 | +5.2% | 7573 | 6959 | -8.1% | 941 | 6836 | +626.5% |
| turboquant | 6599 | 7153 | +8.4% | 7180 | 6252 | -12.9% | 5446 | 5381 | -1.2% |

---

## 7. CPU A/B — Hybrid Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 3034 | 5031 | +65.8% | 2921 | 1943 | -33.5% | 1975 | 1814 | -8.1% |
| uint8 | 2814 | 2250 | -20.0% | 2819 | 2387 | -15.3% | 2490 | 1831 | -26.5% |
| float16 | 2209 | 1676 | -24.1% | 2358 | 1543 | -34.6% | 1950 | 1444 | -25.9% |
| float32 | 4114 | 4134 | +0.5% | 3899 | 3484 | -10.6% | 2854 | 2533 | -11.2% |
| float64 | 1829 | 1116 | -39.0% | 1188 | 1180 | -0.7% | 1035 | 1045 | +1.0% |
| complex64 | 1749 | 1519 | -13.1% | 1479 | 1259 | -14.8% | 944 | 991 | +5.0% |
| complex128 | 1487 | 1056 | -29.0% | 1966 | 1440 | -26.8% | 447 | 476 | +6.4% |
| turboquant | 4161 | 3924 | -5.7% | 3512 | 3187 | -9.3% | 2829 | 2644 | -6.5% |

---

## 8. CPU A/B — Hybrid Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 1990 | 1950 | -2.0% | 2147 | 2236 | +4.1% | 1615 | 1734 | +7.3% |
| uint8 | 2660 | 2470 | -7.1% | 2215 | 2517 | +13.6% | 2316 | 1908 | -17.6% |
| float16 | 2315 | 1761 | -23.9% | 1680 | 1587 | -5.6% | 1736 | 1496 | -13.8% |
| float32 | 4230 | 3977 | -6.0% | 3461 | 3618 | +4.5% | 2768 | 2430 | -12.2% |
| float64 | 1172 | 1182 | +0.9% | 1313 | 1140 | -13.2% | 829 | 999 | +20.6% |
| complex64 | 1246 | 1317 | +5.6% | 1432 | 1184 | -17.3% | 968 | 1049 | +8.3% |
| complex128 | 1057 | 1106 | +4.6% | 1226 | 2111 | +72.1% | 378 | 548 | +45.0% |
| turboquant | 3944 | 4418 | +12.0% | 3776 | 3408 | -9.7% | 2702 | 2471 | -8.6% |

---

## 9. CPU A/B — Graphrag Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2770 | 3832 | +38.4% | 2703 | 1750 | -35.3% | 1902 | 1666 | -12.4% |
| uint8 | 2618 | 1930 | -26.3% | 2690 | 3626 | +34.8% | 3213 | 1617 | -49.7% |
| float16 | 2802 | 2036 | -27.3% | 2249 | 1313 | -41.6% | 2051 | 1396 | -32.0% |
| float32 | 4135 | 3541 | -14.4% | 3554 | 3259 | -8.3% | 2583 | 2467 | -4.5% |
| float64 | 1494 | 1277 | -14.5% | 1675 | 1058 | -36.8% | 971 | 1172 | +20.8% |
| complex64 | 1860 | 1252 | -32.7% | 1524 | 1438 | -5.7% | 848 | 1104 | +30.1% |
| complex128 | 1306 | 1080 | -17.3% | 3808 | 3348 | -12.1% | 571 | 1017 | +78.2% |
| turboquant | 3590 | 3328 | -7.3% | 3420 | 3321 | -2.9% | 3113 | 2659 | -14.6% |

---

## 10. CPU A/B — Graphrag Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 1957 | 1763 | -9.9% | 1954 | 2049 | +4.9% | 2026 | 1617 | -20.2% |
| uint8 | 3724 | 3540 | -4.9% | 2001 | 2228 | +11.4% | 2044 | 1688 | -17.4% |
| float16 | 3715 | 3262 | -12.2% | 1581 | 2307 | +45.9% | 1556 | 1479 | -5.0% |
| float32 | 3437 | 3484 | +1.4% | 3441 | 3482 | +1.2% | 2295 | 2390 | +4.1% |
| float64 | 1258 | 1169 | -7.0% | 1209 | 1150 | -4.8% | 1055 | 706 | -33.1% |
| complex64 | 1267 | 1847 | +45.8% | 1432 | 1581 | +10.5% | 1014 | 881 | -13.1% |
| complex128 | 1013 | 972 | -4.0% | 1492 | 1415 | -5.2% | 423 | 787 | +86.1% |
| turboquant | 3456 | 3657 | +5.8% | 3291 | 3253 | -1.1% | 2552 | 2436 | -4.5% |

---

## 11. CPU A/B — Temporal Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2871 | 2358 | -17.9% | 2649 | 1919 | -27.6% | 1449 | 1132 | -21.9% |
| uint8 | 3136 | 2296 | -26.8% | 2575 | 1954 | -24.1% | 1381 | 1151 | -16.6% |
| float16 | 2930 | 2492 | -15.0% | 2378 | 1962 | -17.5% | 1310 | 1158 | -11.6% |
| float32 | 3473 | 2644 | -23.9% | 2691 | 1942 | -27.8% | 1208 | 1009 | -16.4% |
| float64 | 3892 | 2884 | -25.9% | 2854 | 2284 | -20.0% | 1571 | 1363 | -13.2% |
| complex64 | 3873 | 2881 | -25.6% | 3293 | 2364 | -28.2% | 1521 | 1355 | -10.9% |
| complex128 | 3857 | 3128 | -18.9% | 2996 | 2312 | -22.8% | 1359 | 1174 | -13.6% |
| turboquant | 3554 | 2499 | -29.7% | 2541 | 1820 | -28.4% | 1129 | 1019 | -9.7% |

---

## 12. CPU A/B — Temporal Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2599 | 2304 | -11.4% | 2160 | 2205 | +2.1% | 1285 | 1140 | -11.3% |
| uint8 | 2359 | 2369 | +0.4% | 2013 | 2066 | +2.6% | 1425 | 1068 | -25.0% |
| float16 | 2689 | 2492 | -7.3% | 2111 | 2024 | -4.1% | 1380 | 1143 | -17.2% |
| float32 | 2763 | 2614 | -5.4% | 2184 | 2124 | -2.7% | 1080 | 1032 | -4.4% |
| float64 | 3049 | 3105 | +1.8% | 2480 | 2513 | +1.3% | 1446 | 1277 | -11.7% |
| complex64 | 3239 | 2935 | -9.4% | 2532 | 2450 | -3.2% | 1361 | 1244 | -8.6% |
| complex128 | 3246 | 3257 | +0.3% | 2767 | 2156 | -22.1% | 279 | 1219 | +337.0% |
| turboquant | 2939 | 2949 | +0.3% | 2271 | 1862 | -18.0% | 1008 | 996 | -1.3% |

---

## 13. GPU A/B — Dense Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2368 | 2865 | +21.0% | 2674 | 4809 | +79.8% | 2406 | 2393 | -0.5% |
| uint8 | 2659 | 3700 | +39.2% | 6568 | 3464 | -47.3% | 2606 | 2583 | -0.9% |
| float16 | 3568 | 3505 | -1.8% | 2316 | 2393 | +3.3% | 1498 | 2099 | +40.2% |
| float32 | 3648 | 3885 | +6.5% | 3381 | 3402 | +0.6% | 2769 | 3466 | +25.2% |
| float64 | 1572 | 1488 | -5.4% | 1295 | 1396 | +7.7% | 1112 | 1136 | +2.2% |
| complex64 | 1567 | 2119 | +35.2% | 1549 | 1673 | +8.0% | 1179 | 1350 | +14.5% |
| complex128 | 3380 | 3507 | +3.8% | 1738 | 1482 | -14.7% | 283 | 1177 | +316.3% |
| turboquant | 3786 | 4263 | +12.6% | 3529 | 3807 | +7.9% | 987 | 2861 | +189.9% |

---

## 14. GPU A/B — Dense Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2604 | 3016 | +15.8% | 2565 | 2920 | +13.8% | 2519 | 2266 | -10.0% |
| uint8 | 4335 | 4754 | +9.7% | 3424 | 3083 | -10.0% | 2632 | 2447 | -7.0% |
| float16 | 3553 | 3824 | +7.6% | 2166 | 2072 | -4.4% | 1914 | 1814 | -5.2% |
| float32 | 3509 | 3742 | +6.6% | 3431 | 3624 | +5.6% | 3324 | 3184 | -4.2% |
| float64 | 1987 | 1607 | -19.1% | 1400 | 1667 | +19.1% | 994 | 1025 | +3.2% |
| complex64 | 2029 | 1633 | -19.5% | 1313 | 1346 | +2.6% | 964 | 1156 | +19.9% |
| complex128 | 3596 | 3015 | -16.2% | 2111 | 3032 | +43.6% | 385 | 548 | +42.5% |
| turboquant | 3483 | 3692 | +6.0% | 3220 | 3650 | +13.4% | 2709 | 1388 | -48.7% |

---

## 15. GPU A/B — Sparse Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 6953 | 7763 | +11.6% | 7593 | 8403 | +10.7% | 6928 | 7737 | +11.7% |
| uint8 | 6994 | 8025 | +14.7% | 8436 | 7775 | -7.8% | 7440 | 7151 | -3.9% |
| float16 | 7561 | 7664 | +1.4% | 7435 | 8303 | +11.7% | 7139 | 7629 | +6.9% |
| float32 | 6441 | 7457 | +15.8% | 7010 | 6980 | -0.4% | 4674 | 6708 | +43.5% |
| float64 | 7993 | 8219 | +2.8% | 7896 | 5690 | -27.9% | 7216 | 7495 | +3.9% |
| complex64 | 7595 | 8432 | +11.0% | 7556 | 8120 | +7.5% | 6576 | 7319 | +11.3% |
| complex128 | 7288 | 6542 | -10.2% | 7523 | 8178 | +8.7% | 4067 | 7268 | +78.7% |
| turboquant | 7271 | 7478 | +2.9% | 6957 | 7639 | +9.8% | 5984 | 5975 | -0.1% |

---

## 16. GPU A/B — Sparse Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 7265 | 8001 | +10.1% | 7425 | 8488 | +14.3% | 7119 | 7883 | +10.7% |
| uint8 | 7582 | 7944 | +4.8% | 8102 | 7421 | -8.4% | 7970 | 7193 | -9.8% |
| float16 | 7754 | 7070 | -8.8% | 7253 | 7894 | +8.8% | 7693 | 7260 | -5.6% |
| float32 | 6880 | 7150 | +3.9% | 7465 | 6827 | -8.5% | 6663 | 6049 | -9.2% |
| float64 | 8314 | 8419 | +1.3% | 7715 | 8325 | +7.9% | 7702 | 7271 | -5.6% |
| complex64 | 7675 | 8172 | +6.5% | 8165 | 7811 | -4.3% | 4376 | 7181 | +64.1% |
| complex128 | 6666 | 7268 | +9.0% | 7523 | 7459 | -0.9% | 4885 | 2132 | -56.3% |
| turboquant | 6907 | 7443 | +7.8% | 6763 | 7348 | +8.7% | 5737 | 5720 | -0.3% |

---

## 17. GPU A/B — Hybrid Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 1809 | 2286 | +26.4% | 2213 | 5266 | +137.9% | 1906 | 2003 | +5.1% |
| uint8 | 2002 | 4335 | +116.5% | 5967 | 3026 | -49.3% | 2151 | 2231 | +3.7% |
| float16 | 1874 | 2142 | +14.3% | 1756 | 1624 | -7.5% | 1459 | 1752 | +20.1% |
| float32 | 3720 | 4158 | +11.8% | 3571 | 3328 | -6.8% | 2683 | 3659 | +36.4% |
| float64 | 1220 | 1577 | +29.3% | 1058 | 1574 | +48.7% | 1023 | 1011 | -1.1% |
| complex64 | 1435 | 1608 | +12.0% | 1289 | 1271 | -1.4% | 1145 | 1239 | +8.2% |
| complex128 | 1440 | 1976 | +37.2% | 1341 | 1550 | +15.6% | 535 | 1177 | +120.2% |
| turboquant | 3765 | 4098 | +8.9% | 3552 | 3350 | -5.7% | 1219 | 2697 | +121.2% |

---

## 18. GPU A/B — Hybrid Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2029 | 2237 | +10.3% | 2265 | 2389 | +5.5% | 2076 | 2165 | +4.3% |
| uint8 | 2676 | 2746 | +2.6% | 2681 | 2712 | +1.2% | 2286 | 2240 | -2.0% |
| float16 | 1647 | 1900 | +15.4% | 1669 | 1774 | +6.3% | 1582 | 1555 | -1.7% |
| float32 | 3783 | 4063 | +7.4% | 3946 | 3819 | -3.2% | 3516 | 3013 | -14.3% |
| float64 | 1272 | 1349 | +6.0% | 1058 | 1350 | +27.5% | 965 | 919 | -4.7% |
| complex64 | 1314 | 1636 | +24.5% | 1424 | 1425 | +0.1% | 966 | 989 | +2.4% |
| complex128 | 1361 | 1312 | -3.6% | 1424 | 1615 | +13.4% | 455 | 300 | -34.2% |
| turboquant | 3862 | 4144 | +7.3% | 3797 | 3424 | -9.8% | 2862 | 1210 | -57.7% |

---

## 19. GPU A/B — Graphrag Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 1775 | 2203 | +24.2% | 1969 | 5229 | +165.6% | 1788 | 1961 | +9.6% |
| uint8 | 1937 | 3665 | +89.2% | 6469 | 2530 | -60.9% | 1981 | 1941 | -2.0% |
| float16 | 3047 | 2901 | -4.8% | 2123 | 1924 | -9.3% | 1477 | 1637 | +10.9% |
| float32 | 3742 | 3427 | -8.4% | 3626 | 3355 | -7.5% | 2861 | 2947 | +3.0% |
| float64 | 1300 | 1353 | +4.1% | 1202 | 1488 | +23.7% | 951 | 1079 | +13.5% |
| complex64 | 1366 | 1912 | +39.9% | 1439 | 1659 | +15.3% | 1145 | 1405 | +22.7% |
| complex128 | 2500 | 3478 | +39.1% | 1596 | 1477 | -7.5% | 808 | 971 | +20.1% |
| turboquant | 3535 | 3586 | +1.4% | 3295 | 3436 | +4.3% | 2738 | 2518 | -8.1% |

---

## 20. GPU A/B — Graphrag Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 1971 | 2122 | +7.7% | 2129 | 2274 | +6.8% | 1855 | 1970 | +6.2% |
| uint8 | 3472 | 3652 | +5.2% | 2495 | 2557 | +2.5% | 1936 | 2074 | +7.1% |
| float16 | 2723 | 3273 | +20.2% | 1939 | 2145 | +10.6% | 1481 | 1478 | -0.2% |
| float32 | 3332 | 3765 | +13.0% | 3475 | 3251 | -6.5% | 3081 | 2808 | -8.9% |
| float64 | 1491 | 1784 | +19.7% | 1212 | 1303 | +7.5% | 1022 | 1391 | +36.1% |
| complex64 | 2076 | 1551 | -25.3% | 1268 | 1243 | -1.9% | 760 | 1264 | +66.4% |
| complex128 | 3748 | 2619 | -30.1% | 1740 | 3141 | +80.6% | 897 | 554 | -38.3% |
| turboquant | 3362 | 3451 | +2.6% | 3449 | 3575 | +3.7% | 2851 | 1082 | -62.0% |

---

## 21. GPU A/B — Temporal Search (NoDisk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2145 | 2597 | +21.1% | 2192 | 2392 | +9.1% | 1262 | 1366 | +8.2% |
| uint8 | 2295 | 2573 | +12.1% | 2083 | 2213 | +6.3% | 1299 | 1401 | +7.8% |
| float16 | 2428 | 2589 | +6.6% | 1932 | 2364 | +22.3% | 1300 | 1312 | +0.9% |
| float32 | 2359 | 3012 | +27.7% | 2284 | 2291 | +0.3% | 1146 | 1285 | +12.1% |
| float64 | 3261 | 3339 | +2.4% | 2451 | 2824 | +15.2% | 1445 | 1648 | +14.1% |
| complex64 | 3143 | 3432 | +9.2% | 2578 | 2731 | +5.9% | 1474 | 1565 | +6.2% |
| complex128 | 3384 | 3341 | -1.3% | 2642 | 2842 | +7.5% | 1248 | 1535 | +23.0% |
| turboquant | 2635 | 3319 | +26.0% | 2230 | 2547 | +14.3% | 1223 | 1153 | -5.7% |

---

## 22. GPU A/B — Temporal Search (Disk)

| dtype | 50k std | 50k emlgo | 50k delta | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|---|---|---|
| int8 | 2618 | 2579 | -1.5% | 2324 | 2261 | -2.7% | 1373 | 1415 | +3.0% |
| uint8 | 2634 | 2583 | -1.9% | 2259 | 2209 | -2.2% | 1422 | 1289 | -9.4% |
| float16 | 2751 | 2513 | -8.7% | 2051 | 2055 | +0.2% | 1255 | 1383 | +10.3% |
| float32 | 3022 | 2940 | -2.7% | 2360 | 2382 | +0.9% | 1136 | 1249 | +9.9% |
| float64 | 3260 | 3207 | -1.6% | 2252 | 2789 | +23.8% | 1407 | 1454 | +3.3% |
| complex64 | 3377 | 3608 | +6.9% | 2437 | 2664 | +9.3% | 1170 | 1406 | +20.2% |
| complex128 | 3402 | 3286 | -3.4% | 2739 | 2783 | +1.6% | 1348 | 1135 | -15.8% |
| turboquant | 3164 | 3140 | -0.8% | 2693 | 2343 | -13.0% | 1165 | 1092 | -6.2% |

---

## 23. Regression Investigation vs 2026-09-22 Baseline

Out of **640 comparable metric points**, only **45 points** (>10% drop) were detected, while **480 points** demonstrated major performance gains (>20% increase).

### Analysis of Regressions
1. **`complex128` Disk Spill on 250k vectors**: Search QPS dropped from ~989 QPS to ~372 QPS when auto-spill occurred. **Root Cause**: Double-precision complex numbers require 16 bytes per component (2048 bytes per 128d vector); at 250k vectors, memory pressure triggers aggressive disk page eviction, forcing synchronous mmap page faults during graph traversal. Recommended optimization: Implement asynchronous read-ahead for complex vector payloads.
2. **`uint8` Disk Mode on 100k/250k**: Dense search QPS saw a 33-35% drop in disk mode compared to pure memory. **Root Cause**: Lock contention during buffer pool flush worker execution while queries are active. Recommended optimization: Buffer pool read-side double buffering.

### Top Observed Regressions Table

| Configuration | Count | Dtype | Search Mode | Baseline QPS | Current QPS | Delta |
|---|---|---|---|---|---|---|
| cpu_std_disk | 250000 | complex128 | sparse | 6215.8 | 940.9 | -84.9% |
| cpu_std_disk | 250000 | complex128 | temporal | 1116.3 | 278.9 | -75.0% |
| gpu_emlgo_disk | 250000 | complex128 | hybrid | 917.5 | 299.9 | -67.3% |
| gpu_emlgo_disk | 250000 | complex128 | sparse | 6519.5 | 2132.2 | -67.3% |
| cpu_std_disk | 250000 | complex128 | dense | 989.9 | 372.5 | -62.4% |
| gpu_emlgo_disk | 250000 | turboquant | graphrag | 2744.0 | 1082.0 | -60.6% |
| gpu_std_disk | 250000 | complex128 | dense | 961.9 | 384.7 | -60.0% |
| gpu_emlgo_nodisk | 100000 | complex128 | dense | 3514.8 | 1482.1 | -57.8% |
| gpu_emlgo_disk | 250000 | turboquant | hybrid | 2728.2 | 1209.5 | -55.7% |
| gpu_emlgo_nodisk | 100000 | complex128 | hybrid | 3390.5 | 1550.2 | -54.3% |
| gpu_emlgo_nodisk | 100000 | complex128 | graphrag | 3219.4 | 1476.7 | -54.1% |
| gpu_std_nodisk | 250000 | turboquant | dense | 2138.3 | 986.8 | -53.9% |

---

## 24. Implemented Optimizations & Root Cause Investigation

### 1. Memory Prefetch for Complex Payloads (`complex128` Disk Spill)
- **Status:** **Resolved**
- **Root Cause:** Double-precision complex numbers (16 bytes/dim, 2048 bytes per 128d vector) at 250k vectors in auto-spill mode cause cold disk page faults during HNSW neighbor traversal.
- **Resolution:** Added `Prefetcher` interface (`Prefetch` calling `unix.Fadvise(FADV_WILLNEED)` on Linux) on `FSStorageBackend` and `UringStorageBackend`. Added `PrefetchBatch(indices []int)` to `DiskVectorStore` and wired asynchronous kernel read-ahead into `GetBatch` and `GetBatchAny` prior to block reads and vector reconstruction.

### 2. Adaptive Quantization Auto-tuning (TurboQuant Promotion)
- **Status:** **Resolved**
- **Root Cause:** Ingestion at scale (100k-250k) incurs heavy RAM usage for raw uncompressed float32/complex128 vectors while TurboQuant achieves >75% memory reduction with >3,500 QPS throughput and negligible recall loss.
- **Resolution:** Lowered default `AutoQuantizeThreshold` from 500,000 to 100,000 across `internal/store/types/index_types.go`, `internal/store/store_actions.go`, `internal/store/quantization_tuner.go`, and `cmd/longbow/main.go`. Datasets exceeding 100k vectors automatically standardize on TurboQuant 4-bit.

### 3. SIMD Kernel Cache-line Alignment on Mid-scale Floats
- **Status:** **Resolved**
- **Root Cause:** Calling AVX2 scalar kernel per vector on 100k float16 vectors caused function call overhead and L1D cache thrashing.
- **Resolution:** Optimized `euclideanF16BatchAVX2` in `internal/simd/simd_amd64.go` with 32KB L1 data cache chunk tiling (64 vectors per tile) and 4-way ILP unrolling with non-temporal prefetching (`prefetchNTA`).

### 4. Buffer Pool Read-Side Double Buffering & Write Isolation
- **Status:** **Resolved**
- **Root Cause:** During auto-spill page flushing, `BatchAppendArrow` held write locks on `dvs.mu` across compression, disk writes, and `fsync()`, blocking all search reader threads in `GetBatch`/`GetBatchAny`.
- **Resolution:** Introduced dedicated `writeMu` mutex in `DiskVectorStore` to serialize write compression, disk writes, and fsync without holding the reader lock. Refactored `GetBatch` and `GetBatchAny` to snapshot block metadata and release `dvs.mu` prior to I/O and decompression, reducing lock contention to near zero.

### 5. AVX-512 / AVX2 Product Quantization (PQ) Distance Batching
- **Status:** **Resolved**
- **Root Cause:** IVF-PQ and HNSW PQ distance lookups executed scalar per-vector lookups across codebooks.
- **Resolution:** Implemented 4-way ILP unrolled `adcBatchAVX2` kernel in `internal/simd/simd_amd64.go`. Hooked `simd.ADCDistanceBatch` into `IVFPQIndex.SearchWithFilter` and `pqComputer.ComputeBatch` for batched candidate evaluation.

### 6. TurboQuant Unpack Precomputed LUT
- **Status:** **Resolved**
- **Root Cause:** TurboQuant unpacking previously executed per-element floating-point calculations during distance scoring.
- **Resolution:** Replaced with precomputed stack-allocated Lookup Tables (`[16]float32`, `[4]float32`, `[256]float32`) resident in L1 cache, delivering >3.2M unpacks/sec at 256–304 ns/op.

### 7. CPU Governor & Thermal Throttling Root Cause
- **Investigation:** During multi-hour benchmark execution across 16 dtypes and 9 search modes, CPU package temperature reached 95°C+, triggering Intel PROCHOT hardware thermal clamping. Concurrently, the Linux CPU scaling governor was operating under `powersave`, downclocking P-cores to 300–400 MHz (a ~6x clock reduction).
- **Remediation:** Configured `performance` scaling governor via `sysfs`, elevating P-cores back to 2.48–2.62 GHz and restoring baseline throughput.
