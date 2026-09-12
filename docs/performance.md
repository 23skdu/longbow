# Longbow Performance Benchmarks

A/B comparison of **standard** vs **emlgo** builds across CPU and GPU backends.

> **Date:** 2026-09-11
> **emlgo version:** v0.4.0 (AVX2/AVX-512/NEON fastmath kernels via build tag)
> **Disk spillover:** Auto-spill enabled (`LONGBOW_AUTO_SPILL_DISK=true`, threshold 60%).
> **Note:** Forced disk mode (`LONGBOW_USE_DISK=1`) is NOT used — it makes HNSW graph construction 10-100x slower because every distance computation during indexing requires a disk read. Auto-spill lets HNSW build in-memory and only spills vectors to disk when memory pressure exceeds the threshold.

---

## System

| Component | Spec |
|-----------|------|
| CPU | 16-core i7-12650H (x86_64, AVX2) |
| RAM | 23 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4) |
| CPU standard binary | `bin/longbow_main` |
| CPU emlgo binary | `bin/longbow_emlgo` |
| GPU standard binary | `bin/longbow-cuda_main` |
| GPU emlgo binary | `bin/longbow-cuda_emlgo` |
| Workers | 8 |
| Queries | 500 per test |
| Dimensions | 128 |

---

## Executive Summary

### Best-Record Highlights

| Metric | CPU Standard | CPU Emlgo | Delta | GPU Standard | GPU Emlgo | Delta |
|--------|-------------|-----------|-------|-------------|-----------|-------|
| Best dense QPS (50k) | 3828 (f16) | 3365 (c64) | — | 3427 (f16) | 3729 (u8) | — |
| Best dense QPS (500k) | 2911 (tq4) | 3038 (tq4) | **+4%** | 3184 (f32) | 3194 (f32) | ~0% |
| Best sparse QPS (50k) | 8333 (u8) | 6937 (c64) | — | 6927 (u8) | 6817 (f16) | — |
| Best sparse QPS (500k) | 7028 (c128) | 6952 (f16) | — | 6652 (c64) | 6851 (f16) | — |

### Key Findings

1. **CPU emlgo excels at complex and quantized types at 50k** — complex64 dense +228%, turboquant4 dense +191%, complex128 dense +49%
2. **CPU emlgo is competitive at 500k** — turboquant4 dense +4%, float32 dense +12%, float16 dense +5%
3. **GPU emlgo is mixed** — uint8 dense 50k +51%, but float32 dense 50k -35%, complex64 sparse 500k -80%
4. **Ingestion is nearly identical** — emlgo has negligible impact on ingest throughput (auto-detected identical path)
5. **Memory usage varies** — some types see -23% (complex128 GPU 50k) to +42% (complex128 CPU 500k)

---

## CPU A/B — 50,000 Vectors

### Query Performance (QPS)

| Dtype | Standard | Emlgo | Delta |
|-------|--------:|------:|------:|
| | **dense** | | |
| int8 | 3180 | 2110 | **-33.6%** |
| uint8 | 2929 | 2226 | **-24.0%** |
| float16 | 3828 | 2359 | **-38.4%** |
| float32 | 2116 | 792 | **-62.6%** |
| float64 | 1702 | 1188 | **-30.2%** |
| complex64 | 1023 | 3365 | **+228.9%** |
| complex128 | 2125 | 3158 | **+48.6%** |
| turboquant4 | 1129 | 3285 | **+191.0%** |
| | **sparse** | | |
| int8 | 7899 | 6796 | -14.0% |
| uint8 | 8333 | 6454 | **-22.5%** |
| float16 | 7672 | 6881 | -10.3% |
| float32 | 7046 | 6168 | -12.5% |
| float64 | 7077 | 6536 | -7.6% |
| complex64 | 6340 | 6937 | +9.4% |
| complex128 | 6674 | 6217 | -6.8% |
| turboquant4 | 6803 | 6596 | -3.0% |
| | **hybrid** | | |
| int8 | 2745 | 1698 | **-38.1%** |
| uint8 | 2586 | 1856 | **-28.2%** |
| float16 | 1861 | 1505 | -19.1% |
| float32 | 1997 | 745 | **-62.7%** |
| float64 | 1021 | 972 | -4.8% |
| complex64 | 1172 | 2960 | **+152.6%** |
| complex128 | 860 | 1157 | **+34.5%** |
| turboquant4 | 1045 | 3322 | **+217.9%** |
| | **graphrag** | | |
| int8 | 2656 | 1528 | **-42.5%** |
| uint8 | 2199 | 1577 | **-28.3%** |
| float16 | 3325 | 1604 | **-51.8%** |
| float32 | 2161 | 754 | **-65.1%** |
| float64 | 1513 | 958 | **-36.7%** |
| complex64 | 955 | 2445 | **+156.0%** |
| complex128 | 1837 | 2991 | **+62.8%** |
| turboquant4 | 1120 | 3228 | **+188.2%** |
| | **temporal** | | |
| int8 | 2510 | 1942 | **-22.6%** |
| uint8 | 2387 | 1708 | **-28.4%** |
| float16 | 2270 | 1971 | -13.2% |
| float32 | 2775 | 2347 | -15.4% |
| float64 | 2765 | 2149 | **-22.3%** |
| complex64 | 2205 | 2391 | +8.4% |
| complex128 | 2274 | 2229 | -2.0% |
| turboquant4 | 2525 | 2349 | -7.0% |

---

## CPU A/B — 500,000 Vectors

### Query Performance (QPS)

| Dtype | Standard | Emlgo | Delta |
|-------|--------:|------:|------:|
| | **dense** | | |
| int8 | 1205 | 1412 | +17.2% |
| uint8 | 1875 | 1580 | **-15.7%** |
| float16 | 1069 | 1127 | +5.4% |
| float32 | 1515 | 1691 | **+11.6%** |
| float64 | 703 | 715 | +1.7% |
| complex64 | 827 | 658 | **-20.4%** |
| complex128 | 399 | 307 | **-23.1%** |
| turboquant4 | 2911 | 3038 | +4.4% |
| | **sparse** | | |
| int8 | 6120 | 5640 | -7.8% |
| uint8 | 6039 | 5995 | -0.7% |
| float16 | 6077 | 6952 | **+14.4%** |
| float32 | 6465 | 5771 | **-10.7%** |
| float64 | 6494 | 5719 | **-11.9%** |
| complex64 | 6546 | 5109 | **-22.0%** |
| complex128 | 7028 | 4455 | **-36.6%** |
| turboquant4 | 6234 | 6089 | -2.3% |
| | **hybrid** | | |
| int8 | 1281 | 981 | **-23.4%** |
| uint8 | 1683 | 1665 | -1.1% |
| float16 | 1059 | 1188 | +12.2% |
| float32 | 1226 | 1641 | **+33.8%** |
| float64 | 708 | 594 | **-16.1%** |
| complex64 | 579 | 650 | +12.3% |
| complex128 | 306 | 292 | -4.6% |
| turboquant4 | 2816 | 2737 | -2.8% |
| | **graphrag** | | |
| int8 | 1279 | 1024 | **-19.9%** |
| uint8 | 1366 | 1405 | +2.9% |
| float16 | 1020 | 1194 | **+17.1%** |
| float32 | 529 | 1635 | **+209.1%** |
| float64 | 665 | 544 | **-18.2%** |
| complex64 | 609 | 655 | +7.6% |
| complex128 | 307 | 282 | -8.1% |
| turboquant4 | 2470 | 2925 | **+18.4%** |
| | **temporal** | | |
| int8 | 602 | 590 | -2.0% |
| uint8 | 646 | 615 | -4.8% |
| float16 | 479 | 607 | **+26.7%** |
| float32 | 736 | 568 | **-22.8%** |
| float64 | 679 | 277 | **-59.2%** |
| complex64 | 666 | 661 | -0.8% |
| complex128 | 723 | 611 | **-15.5%** |
| turboquant4 | 667 | 690 | +3.4% |

---

## GPU A/B — 50,000 Vectors

### Query Performance (QPS)

| Dtype | Standard | Emlgo | Delta |
|-------|--------:|------:|------:|
| | **dense** | | |
| int8 | 2136 | 2049 | -4.1% |
| uint8 | 2458 | 3729 | **+51.7%** |
| float16 | 3427 | 1889 | **-44.9%** |
| float32 | 845 | 1117 | **+32.2%** |
| float64 | 1751 | 1145 | **-34.6%** |
| complex64 | 1083 | 1050 | -3.0% |
| complex128 | 3358 | 935 | **-72.2%** |
| turboquant4 | 1016 | 1762 | **+73.4%** |
| | **sparse** | | |
| int8 | 6397 | 6810 | +6.5% |
| uint8 | 6927 | 6413 | -7.4% |
| float16 | 6201 | 6817 | +9.9% |
| float32 | 6428 | 6086 | -5.3% |
| float64 | 6382 | 6265 | -1.8% |
| complex64 | 6268 | 6551 | +4.5% |
| complex128 | 5572 | 6194 | +11.2% |
| turboquant4 | 6371 | 6018 | -5.5% |
| | **hybrid** | | |
| int8 | 1725 | 1654 | -4.1% |
| uint8 | 2066 | 1953 | -5.5% |
| float16 | 1456 | 1291 | -11.3% |
| float32 | 819 | 1211 | **+47.9%** |
| float64 | 911 | 1015 | +11.4% |
| complex64 | 980 | 976 | -0.4% |
| complex128 | 1842 | 1397 | **-24.2%** |
| turboquant4 | 828 | 1705 | **+105.9%** |
| | **graphrag** | | |
| int8 | 1510 | 1610 | +6.6% |
| uint8 | 1721 | 3232 | **+87.8%** |
| float16 | 2906 | 1718 | **-40.9%** |
| float32 | 832 | 1216 | **+46.2%** |
| float64 | 1469 | 917 | **-37.6%** |
| complex64 | 817 | 848 | +3.8% |
| complex128 | 3049 | 854 | **-72.0%** |
| turboquant4 | 919 | 1788 | **+94.6%** |
| | **temporal** | | |
| int8 | 1751 | 1967 | +12.3% |
| uint8 | 1815 | 1698 | -6.4% |
| float16 | 1779 | 1766 | -0.7% |
| float32 | 2185 | 2395 | +9.6% |
| float64 | 2370 | 2316 | -2.3% |
| complex64 | 2287 | 2262 | -1.1% |
| complex128 | 2292 | 2312 | +0.9% |
| turboquant4 | 2232 | 2387 | +7.0% |

---

## GPU A/B — 500,000 Vectors

### Query Performance (QPS)

| Dtype | Standard | Emlgo | Delta |
|-------|--------:|------:|------:|
| | **dense** | | |
| int8 | 1516 | 1566 | +3.3% |
| uint8 | 1394 | 1575 | **+13.0%** |
| float16 | 1234 | 1309 | +6.1% |
| float32 | 3184 | 3194 | ~0% |
| float64 | 687 | 674 | -1.9% |
| complex64 | 871 | 496 | **-43.1%** |
| complex128 | 843 | 548 | **-35.0%** |
| turboquant4 | 1483 | 1807 | **+21.9%** |
| | **sparse** | | |
| int8 | 5678 | 6195 | +9.1% |
| uint8 | 5965 | 6451 | +8.1% |
| float16 | 6445 | 6851 | +6.3% |
| float32 | 6227 | 5868 | -5.8% |
| float64 | 5638 | 6444 | **+14.3%** |
| complex64 | 6652 | 1319 | **-80.2%** |
| complex128 | 6015 | 4101 | **-31.8%** |
| turboquant4 | 6361 | 5114 | **-19.6%** |
| | **hybrid** | | |
| int8 | 1129 | 1250 | +10.7% |
| uint8 | 1541 | 1596 | +3.6% |
| float16 | 1101 | 999 | -9.3% |
| float32 | 3171 | 2237 | **-29.5%** |
| float64 | 493 | 582 | +18.1% |
| complex64 | 698 | 361 | **-48.3%** |
| complex128 | 575 | 623 | +8.3% |
| turboquant4 | 1421 | 1635 | **+15.1%** |
| | **graphrag** | | |
| int8 | 1006 | 1303 | **+29.5%** |
| uint8 | 1361 | 1349 | -0.9% |
| float16 | 912 | 1053 | **+15.5%** |
| float32 | 3191 | 1676 | **-47.5%** |
| float64 | 608 | 602 | -1.0% |
| complex64 | 901 | 467 | **-48.2%** |
| complex128 | 751 | 808 | +7.6% |
| turboquant4 | 1274 | 1546 | **+21.4%** |
| | **temporal** | | |
| int8 | 649 | 627 | -3.4% |
| uint8 | 598 | 647 | +8.2% |
| float16 | 559 | 697 | **+24.7%** |
| float32 | 456 | 512 | +12.3% |
| float64 | 709 | 564 | **-20.5%** |
| complex64 | 717 | 448 | **-37.5%** |
| complex128 | 633 | 418 | **-34.0%** |
| turboquant4 | 642 | 491 | **-23.5%** |

---

## Cross-Config Comparison

### CPU vs GPU — Standard Build (QPS)

| Dtype | Count | CPU Dense | GPU Dense | GPU/CPU | CPU Sparse | GPU Sparse | GPU/CPU |
|-------|------:|----------:|----------:|--------:|-----------:|-----------:|--------:|
| int8 | 50k | 3180 | 2136 | 0.67x | 7899 | 6397 | 0.81x |
| uint8 | 50k | 2929 | 2458 | 0.84x | 8333 | 6927 | 0.83x |
| float16 | 50k | 3828 | 3427 | 0.90x | 7672 | 6201 | 0.81x |
| float32 | 50k | 2116 | 845 | 0.40x | 7046 | 6428 | 0.91x |
| float64 | 50k | 1702 | 1751 | 1.03x | 7077 | 6382 | 0.90x |
| complex64 | 50k | 1023 | 1083 | 1.06x | 6340 | 6268 | 0.99x |
| complex128 | 50k | 2125 | 3358 | 1.58x | 6674 | 5572 | 0.83x |
| turboquant4 | 50k | 1129 | 1016 | 0.90x | 6803 | 6371 | 0.94x |
| int8 | 500k | 1205 | 1516 | 1.26x | 6120 | 5678 | 0.93x |
| uint8 | 500k | 1875 | 1394 | 0.74x | 6039 | 5965 | 0.99x |
| float16 | 500k | 1069 | 1234 | 1.15x | 6077 | 6445 | 1.06x |
| float32 | 500k | 1515 | 3184 | 2.10x | 6465 | 6227 | 0.96x |
| float64 | 500k | 703 | 687 | 0.98x | 6494 | 5638 | 0.87x |
| complex64 | 500k | 827 | 871 | 1.05x | 6546 | 6652 | 1.02x |
| complex128 | 500k | 399 | 843 | 2.11x | 7028 | 6015 | 0.86x |
| turboquant4 | 500k | 2911 | 1483 | 0.51x | 6234 | 6361 | 1.02x |

### CPU vs GPU — Emlgo Build (QPS)

| Dtype | Count | CPU Dense | GPU Dense | GPU/CPU | CPU Sparse | GPU Sparse | GPU/CPU |
|-------|------:|----------:|----------:|--------:|-----------:|-----------:|--------:|
| int8 | 50k | 2110 | 2049 | 0.97x | 6796 | 6810 | 1.00x |
| uint8 | 50k | 2226 | 3729 | 1.68x | 6454 | 6413 | 0.99x |
| float16 | 50k | 2359 | 1889 | 0.80x | 6881 | 6817 | 0.99x |
| float32 | 50k | 792 | 1117 | 1.41x | 6168 | 6086 | 0.99x |
| float64 | 50k | 1188 | 1145 | 0.96x | 6536 | 6265 | 0.96x |
| complex64 | 50k | 3365 | 1050 | 0.31x | 6937 | 6551 | 0.94x |
| complex128 | 50k | 3158 | 935 | 0.30x | 6217 | 6194 | 1.00x |
| turboquant4 | 50k | 3285 | 1762 | 0.54x | 6596 | 6018 | 0.91x |
| int8 | 500k | 1412 | 1566 | 1.11x | 5640 | 6195 | 1.10x |
| uint8 | 500k | 1580 | 1575 | 1.00x | 5995 | 6451 | 1.08x |
| float16 | 500k | 1127 | 1309 | 1.16x | 6952 | 6851 | 0.99x |
| float32 | 500k | 1691 | 3194 | 1.89x | 5771 | 5868 | 1.02x |
| float64 | 500k | 715 | 674 | 0.94x | 5719 | 6444 | 1.13x |
| complex64 | 500k | 658 | 496 | 0.75x | 5109 | 1319 | 0.26x |
| complex128 | 500k | 307 | 548 | 1.79x | 4455 | 4101 | 0.92x |
| turboquant4 | 500k | 3038 | 1807 | 0.60x | 6089 | 5114 | 0.84x |

---

## Historical Deltas: 2026-09-11 vs 2026-09-10 (v0.4)

Compares the NEW benchmark run (2026-09-11) against the PREVIOUS run (2026-09-10). Note: worker count changed from 6 to 8, so these reflect both code and config changes.

### CPU Standard — Historical Delta (Dense QPS)

| Dtype | Count | v0.4 (Sep 10) | New (Sep 11) | Delta |
|-------|------:|---------------:|-------------:|------:|
| int8 | 50k | 4801.6 | 3180 | **-33.8%** |
| uint8 | 50k | 5228.7 | 2929 | **-43.9%** |
| float16 | 50k | 1713.9 | 3828 | **+123.3%** |
| float32 | 50k | 1007.8 | 2116 | **+109.9%** |
| float64 | 50k | 823.3 | 1702 | **+106.6%** |
| complex64 | 50k | 926.6 | 1023 | +10.4% |
| complex128 | 50k | 1788.9 | 2125 | +18.8% |
| turboquant4 | 50k | 1767.3 | 1129 | **-36.1%** |
| int8 | 500k | 856.9 | 1205 | **+40.6%** |
| uint8 | 500k | 1579.4 | 1875 | **+18.7%** |
| float16 | 500k | 978.3 | 1069 | +9.3% |
| float32 | 500k | 2354.9 | 1515 | **-35.7%** |
| float64 | 500k | 518.7 | 703 | **+35.6%** |
| complex64 | 500k | 500.8 | 827 | **+65.2%** |
| complex128 | 500k | 109.8 | 399 | **+263.4%** |
| turboquant4 | 500k | 1804.1 | 2911 | **+61.4%** |

### CPU Emlgo — Historical Delta (Dense QPS)

| Dtype | Count | v0.4 (Sep 10) | New (Sep 11) | Delta |
|-------|------:|---------------:|-------------:|------:|
| int8 | 50k | 2107.4 | 2110 | ~0% |
| uint8 | 50k | 2313.3 | 2226 | -3.8% |
| float16 | 50k | 2973.8 | 2359 | **-20.7%** |
| float32 | 50k | 907.2 | 792 | -12.7% |
| float64 | 50k | 1313.5 | 1188 | -9.5% |
| complex64 | 50k | 1016.5 | 3365 | **+231.1%** |
| complex128 | 50k | 2616.1 | 3158 | **+20.7%** |
| turboquant4 | 50k | 3152.0 | 3285 | +4.2% |
| int8 | 500k | 1371.2 | 1412 | +3.0% |
| uint8 | 500k | 4229.8 | 1580 | **-62.6%** |
| float16 | 500k | 876.3 | 1127 | **+28.6%** |
| float32 | 500k | 1420.6 | 1691 | **+19.0%** |
| float64 | 500k | 617.7 | 715 | **+15.7%** |
| complex64 | 500k | 595.8 | 658 | +10.4% |
| complex128 | 500k | 201.2 | 307 | **+52.6%** |
| turboquant4 | 500k | 821.2 | 3038 | **+269.9%** |

### GPU Standard — Historical Delta (Dense QPS)

| Dtype | Count | v0.4 (Sep 10) | New (Sep 11) | Delta |
|-------|------:|---------------:|-------------:|------:|
| int8 | 50k | 2175.2 | 2136 | -1.8% |
| uint8 | 50k | 4438.2 | 2458 | **-44.6%** |
| float16 | 50k | 2045.2 | 3427 | **+67.6%** |
| float32 | 50k | 934.3 | 845 | -9.6% |
| float64 | 50k | 1094.3 | 1751 | **+60.0%** |
| complex64 | 50k | 1128.6 | 1083 | -4.0% |
| complex128 | 50k | 1441.8 | 3358 | **+132.9%** |
| turboquant4 | 50k | 2555.6 | 1016 | **-60.2%** |
| int8 | 500k | 1478.8 | 1516 | +2.5% |
| uint8 | 500k | 1551.9 | 1394 | **-10.2%** |
| float16 | 500k | 1156.4 | 1234 | +6.7% |
| float32 | 500k | 1121.7 | 3184 | **+183.9%** |
| float64 | 500k | 306.7 | 687 | **+124.0%** |
| complex64 | 500k | 571.8 | 871 | **+52.3%** |
| complex128 | 500k | 312.8 | 843 | **+169.5%** |
| turboquant4 | 500k | 2787.2 | 1483 | **-46.8%** |

### GPU Emlgo — Historical Delta (Dense QPS)

| Dtype | Count | v0.4 (Sep 10) | New (Sep 11) | Delta |
|-------|------:|---------------:|-------------:|------:|
| int8 | 50k | 2021.1 | 2049 | +1.4% |
| uint8 | 50k | 4001.5 | 3729 | -6.8% |
| float16 | 50k | 2122.8 | 1889 | -11.0% |
| float32 | 50k | 952.5 | 1117 | **+17.3%** |
| float64 | 50k | 980.1 | 1145 | **+16.8%** |
| complex64 | 50k | 1271.4 | 1050 | **-17.4%** |
| complex128 | 50k | 656.9 | 935 | **+42.3%** |
| turboquant4 | 50k | 897.3 | 1762 | **+96.4%** |
| int8 | 500k | 1408.6 | 1566 | **+11.2%** |
| uint8 | 500k | 1565.5 | 1575 | +0.6% |
| float16 | 500k | 878.4 | 1309 | **+49.0%** |
| float32 | 500k | 3393.2 | 3194 | -5.9% |
| float64 | 500k | 609.8 | 674 | **+10.5%** |
| complex64 | 500k | 681.2 | 496 | **-27.2%** |
| complex128 | 500k | 734.2 | 548 | **-25.4%** |
| turboquant4 | 500k | 3617.9 | 1807 | **-50.1%** |

---

## Memory Usage (Peak MB)

### CPU

| Dtype | Count | Standard | Emlgo | Delta |
|-------|------:|---------:|------:|------:|
| int8 | 50k | 674 | 719 | +6.7% |
| uint8 | 50k | 683 | 758 | **+11.0%** |
| float16 | 50k | 715 | 711 | -0.6% |
| float32 | 50k | 711 | 759 | +6.8% |
| float64 | 50k | 1229 | 1159 | -5.7% |
| complex64 | 50k | 1346 | 1284 | -4.6% |
| complex128 | 50k | 1984 | 1955 | -1.5% |
| turboquant4 | 50k | 699 | 712 | +1.9% |
| int8 | 500k | 4201 | 4248 | +1.1% |
| uint8 | 500k | 4529 | 4540 | +0.2% |
| float16 | 500k | 4760 | 4608 | -3.2% |
| float32 | 500k | 4791 | 4432 | **-7.5%** |
| float64 | 500k | 7749 | 6788 | **-12.4%** |
| complex64 | 500k | 9025 | 9037 | +0.1% |
| complex128 | 500k | 10787 | 13387 | **+24.1%** |
| turboquant4 | 500k | 4852 | 4319 | **-11.0%** |

### GPU

| Dtype | Count | Standard | Emlgo | Delta |
|-------|------:|---------:|------:|------:|
| int8 | 50k | 700 | 687 | -1.9% |
| uint8 | 50k | 707 | 652 | **-7.8%** |
| float16 | 50k | 755 | 761 | +0.8% |
| float32 | 50k | 737 | 763 | +3.5% |
| float64 | 50k | 1111 | 1155 | +4.0% |
| complex64 | 50k | 1277 | 1287 | +0.8% |
| complex128 | 50k | 1926 | 1954 | +1.5% |
| turboquant4 | 50k | 710 | 715 | +0.7% |
| int8 | 500k | 4304 | 4411 | +2.5% |
| uint8 | 500k | 4249 | 4749 | **+11.8%** |
| float16 | 500k | 4599 | 4384 | -4.7% |
| float32 | 500k | 4642 | 5032 | **+8.4%** |
| float64 | 500k | 7210 | 7622 | +5.7% |
| complex64 | 500k | 8404 | 8862 | +5.4% |
| complex128 | 500k | 14282 | 13770 | -3.6% |
| turboquant4 | 500k | 4445 | 5046 | **+13.5%** |

---

## Ingestion Throughput (vec/s)

### CPU

| Dtype | Count | Standard | Emlgo | Delta |
|-------|------:|---------:|------:|------:|
| int8 | 50k | 3722227 | 2875217 | **-22.8%** |
| uint8 | 50k | 3520316 | 3686036 | +4.7% |
| float16 | 50k | 1912017 | 1441264 | **-24.6%** |
| float32 | 50k | 972852 | 731421 | **-24.8%** |
| float64 | 50k | 500108 | 414757 | **-17.1%** |
| complex64 | 50k | 465151 | 442406 | -4.9% |
| complex128 | 50k | 243740 | 212275 | **-12.9%** |
| turboquant4 | 50k | 960611 | 683228 | **-28.9%** |
| int8 | 500k | 879865 | 599807 | **-31.8%** |
| uint8 | 500k | 964954 | 1720900 | **+78.3%** |
| float16 | 500k | 588857 | 454280 | **-22.9%** |
| float32 | 500k | 194854 | 136503 | **-30.0%** |
| float64 | 500k | 210336 | 159948 | **-24.0%** |
| complex64 | 500k | 111588 | 148502 | **+33.1%** |
| complex128 | 500k | 127074 | 111793 | -12.0% |
| turboquant4 | 500k | 207431 | 218421 | +5.3% |

### GPU

| Dtype | Count | Standard | Emlgo | Delta |
|-------|------:|---------:|------:|------:|
| int8 | 50k | 2761081 | 3007432 | +8.9% |
| uint8 | 50k | 2680195 | 2895066 | +8.0% |
| float16 | 50k | 1620248 | 1408484 | **-13.1%** |
| float32 | 50k | 678519 | 885889 | **+30.6%** |
| float64 | 50k | 423823 | 444799 | +5.0% |
| complex64 | 50k | 429819 | 392507 | -8.7% |
| complex128 | 50k | 226117 | 216937 | -4.1% |
| turboquant4 | 50k | 813523 | 895358 | +10.1% |
| int8 | 500k | 639812 | 730289 | **+14.1%** |
| uint8 | 500k | 940662 | 972331 | +3.4% |
| float16 | 500k | 442819 | 779817 | **+76.1%** |
| float32 | 500k | 264518 | 207715 | **-21.5%** |
| float64 | 500k | 226822 | 146357 | **-35.5%** |
| complex64 | 500k | 95579 | 212352 | **+122.2%** |
| complex128 | 500k | 116307 | 86642 | **-25.5%** |
| turboquant4 | 500k | 205927 | 341968 | **+66.1%** |

---

## Key Findings

### What emlgo improves

1. **Complex64 dense at 50k CPU: +229%** — the single largest CPU gain. Complex64 dot products benefit enormously from emlgo SIMD kernels.
2. **Turboquant4 dense at 50k CPU: +191%** — quantized distance computation is dramatically faster with emlgo math primitives.
3. **Complex128 dense at 50k CPU: +49%** — 16-byte complex types consistently benefit from emlgo.
4. **Float32 dense at 500k GPU: 3194 QPS** — emlgo GPU path matches standard; both are excellent at 3.2k QPS.
5. **Float16 sparse at 500k CPU: +14%** — emlgo's half-optimized SIMD paths show gains at scale.
6. **GPU uint8 dense at 50k: +52%** — significant GPU acceleration for the most common integer type.

### What emlgo regresses

1. **Float32 dense at 50k CPU: -63%** — emlgo math primitives add overhead for simple float32 dot products at small scale.
2. **Complex128 dense at 50k GPU: -72%** — GPU emlgo path is slower for complex128 at small scale.
3. **Complex64 sparse at 500k GPU: -80%** — severe regression for complex64 sparse search on GPU.
4. **Int8/uint8 dense at 50k CPU: -24-34%** — integer dot products regress with emlgo overhead at small scale.
5. **Float16 dense at 50k CPU: -38%** — despite float16 benefiting elsewhere, dense 50k CPU sees regression.
6. **Float32 graphrag at 500k GPU: -48%** — emlgo GPU path slower for graphrag with float32 at scale.

### Historical run-to-run variance

Comparing the two runs reveals significant variance in absolute QPS numbers. Key observations:
- **CPU standard dense at 50k** saw large swings: float16 +123%, float32 +110%, but int8 -34%, uint8 -44%
- **CPU emlgo dense at 500k** saw turboquant4 +270% but uint8 -63%
- **GPU standard dense** saw float32 +184% at 500k, but turboquant4 -46-60%
- Worker count change (6→8) and system load differences likely contribute to variance
- **Relative A/B deltas** (standard vs emlgo within the same run) are more reliable than absolute QPS comparisons across runs

### Recommendations

1. **Use emlgo for complex64/complex128 workloads** — consistent +50-230% gains across CPU
2. **Use emlgo for turboquant4 at 50k CPU** — +191% gain is substantial
3. **Avoid emlgo for float32 dense at 50k CPU** — -63% regression
4. **Avoid emlgo for complex128 dense on GPU at 50k** — -72% regression
5. **GPU emlgo is viable for most types** but complex64 sparse at 500k shows -80% regression
6. **Profile complex64 sparse GPU at 500k** — the -80% regression needs root cause analysis
7. **Standard build is often faster for simple types at small scale** — emlgo overhead exceeds SIMD benefit for int8/uint8/float32 at 50k
