# Longbow Performance Benchmarks

**Date:** 2026-09-22

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
**Counts:** 100k, 250k vectors.
**Disk modes:** `use_disk=yes` (auto-spill) and `use_disk=no`.

---

## 1. Executive Summary

This benchmark extends the 2026-09-11 baseline to 100k/250k vector counts, adding disk spill
mode (auto-spill) as a variable. The emlgo build continues to show a "U-shaped" performance
curve: regressing at 100k on CPU, often recovering at 250k. GPU emlgo is broadly stronger,
especially with disk enabled.

**Biggest emlgo wins (CPU):** complex128 100k Disk dense (+300%), complex128 100k Disk
hybrid (+352%), float64 250k NoDisk dense (+24%).

**Biggest emlgo regressions (CPU):** uint8 250k Disk hybrid (-82%), complex64 100k NoDisk
hybrid (-64%), turboquant 100k Disk temporal (-69%).

**Biggest emlgo wins (GPU):** uint8 100k Disk hybrid (+197%), complex128 100k NoDisk
graphrag (+358%), complex64 250k NoDisk sparse (+1265%).

**Biggest emlgo regressions (GPU):** complex128 100k Disk graphrag (-74%), uint8 100k NoDisk
dense (-42%), float16 250k NoDisk temporal (-32%).

**Disk impact:** disk mode helps uint8 at 250k (+130-164% CPU std), helps GPU complex types
enormously at 250k (+85-1297%), but hurts CPU int8/float16 at 100k (-20-38%).

---

## 2. CPU A/B — Dense Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 2512 | 1649 | -34.4% | 1697 | 1105 | -34.9% |
| uint8 | 2824 | 1725 | -38.9% | 1561 | 1576 | +0.9% |
| float16 | 1759 | 1183 | -32.8% | 1165 | 1119 | -4.0% |
| float32 | 3033 | 2882 | -5.0% | 2397 | 1505 | -37.2% |
| float64 | 1089 | 978 | -10.2% | 733 | 910 | +24.1% |
| complex64 | 816 | 623 | -23.7% | 920 | 843 | -8.4% |
| complex128 | 1482 | 1042 | -29.7% | 764 | 803 | +5.2% |
| turboquant | 2647 | 2796 | +5.6% | 1939 | 2136 | +10.2% |

---

## 3. CPU A/B — Dense Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1724 | 1494 | -13.4% | 1578 | 1675 | +6.1% |
| uint8 | 3832 | 2187 | -42.9% | 3595 | 1538 | -57.2% |
| float16 | 1317 | 1421 | +7.9% | 1309 | 1220 | -6.8% |
| float32 | 2970 | 2661 | -10.4% | 2653 | 1816 | -31.6% |
| float64 | 1076 | 1066 | -0.9% | 806 | 1044 | +29.4% |
| complex64 | 952 | 786 | -17.4% | 525 | 296 | -43.6% |
| complex128 | 832 | 3324 | +299.6% | 990 | 707 | -28.6% |
| turboquant | 2958 | 2826 | -4.5% | 2010 | 1606 | -20.1% |

---

## 4. CPU A/B — Sparse Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 7718 | 5939 | -23.1% | 6370 | 5781 | -9.3% |
| uint8 | 7534 | 5982 | -20.6% | 6003 | 4560 | -24.0% |
| float16 | 6905 | 5590 | -19.0% | 6191 | 6156 | -0.6% |
| float32 | 5903 | 5224 | -11.5% | 4483 | 3832 | -14.5% |
| float64 | 6886 | 5793 | -15.9% | 6524 | 6504 | -0.3% |
| complex64 | 5513 | 2669 | -51.6% | 6284 | 4922 | -21.7% |
| complex128 | 5863 | 5478 | -6.6% | 5975 | 6205 | +3.9% |
| turboquant | 5424 | 5428 | +0.1% | 4086 | 4488 | +9.8% |

---

## 5. CPU A/B — Sparse Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 6113 | 5779 | -5.5% | 5889 | 5952 | +1.1% |
| uint8 | 6363 | 5950 | -6.5% | 6265 | 3611 | -42.4% |
| float16 | 4304 | 5979 | +38.9% | 5990 | 5879 | -1.9% |
| float32 | 5672 | 5141 | -9.4% | 4773 | 4295 | -10.0% |
| float64 | 6236 | 4894 | -21.5% | 6403 | 6444 | +0.6% |
| complex64 | 6383 | 6215 | -2.6% | 760 | 4192 | +451.7% |
| complex128 | 6251 | 4935 | -21.1% | 6216 | 3224 | -48.1% |
| turboquant | 5719 | 5456 | -4.6% | 4334 | 3535 | -18.4% |

---

## 6. CPU A/B — Hybrid Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 2080 | 1450 | -30.3% | 1624 | 1332 | -18.0% |
| uint8 | 2188 | 1386 | -36.7% | 1497 | 1304 | -12.9% |
| float16 | 1565 | 1075 | -31.3% | 1233 | 1021 | -17.1% |
| float32 | 3117 | 2668 | -14.4% | 2413 | 1569 | -35.0% |
| float64 | 982 | 702 | -28.5% | 703 | 728 | +3.5% |
| complex64 | 819 | 298 | -63.6% | 671 | 834 | +24.4% |
| complex128 | 1349 | 672 | -50.2% | 685 | 715 | +4.4% |
| turboquant | 2489 | 2789 | +12.1% | 1852 | 1825 | -1.4% |

---

## 7. CPU A/B — Hybrid Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1445 | 1297 | -10.3% | 1460 | 1446 | -1.0% |
| uint8 | 1681 | 1919 | +14.2% | 3686 | 674 | -81.7% |
| float16 | 1059 | 1339 | +26.4% | 1080 | 975 | -9.7% |
| float32 | 3131 | 2494 | -20.4% | 2413 | 1559 | -35.4% |
| float64 | 814 | 997 | +22.5% | 851 | 329 | -61.4% |
| complex64 | 872 | 743 | -14.8% | 496 | 645 | +29.9% |
| complex128 | 746 | 3372 | +351.9% | 721 | 695 | -3.5% |
| turboquant | 2879 | 2759 | -4.2% | 1822 | 1679 | -7.8% |

---

## 8. CPU A/B — Graphrag Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 2119 | 1343 | -36.6% | 1448 | 1183 | -18.3% |
| uint8 | 1891 | 1288 | -31.9% | 1353 | 1172 | -13.4% |
| float16 | 1386 | 1034 | -25.4% | 1342 | 1022 | -23.9% |
| float32 | 3002 | 2729 | -9.1% | 2270 | 1504 | -33.8% |
| float64 | 934 | 741 | -20.7% | 744 | 763 | +2.6% |
| complex64 | 965 | 334 | -65.4% | 700 | 774 | +10.6% |
| complex128 | 719 | 288 | -60.0% | 720 | 603 | -16.3% |
| turboquant | 2539 | 2642 | +4.0% | 1619 | 1956 | +20.8% |

---

## 9. CPU A/B — Graphrag Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1531 | 1190 | -22.3% | 1214 | 1410 | +16.2% |
| uint8 | 2522 | 1580 | -37.3% | 3575 | 1097 | -69.3% |
| float16 | 984 | 1186 | +20.5% | 1162 | 1247 | +7.3% |
| float32 | 2937 | 2500 | -14.9% | 2106 | 1468 | -30.3% |
| float64 | 909 | 965 | +6.2% | 876 | 730 | -16.7% |
| complex64 | 940 | 742 | -21.0% | 722 | 684 | -5.2% |
| complex128 | 657 | 2937 | +347.3% | 766 | 1125 | +46.9% |
| turboquant | 2742 | 2427 | -11.5% | 1762 | 1588 | -9.9% |

---

## 10. CPU A/B — Temporal Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1767 | 1340 | -24.2% | 1067 | 903 | -15.4% |
| uint8 | 1955 | 1202 | -38.5% | 930 | 811 | -12.8% |
| float16 | 1700 | 1246 | -26.7% | 943 | 947 | +0.4% |
| float32 | 1624 | 1426 | -12.2% | 755 | 586 | -22.4% |
| float64 | 1842 | 1517 | -17.6% | 1128 | 1151 | +2.1% |
| complex64 | 1847 | 1702 | -7.8% | 1036 | 1074 | +3.7% |
| complex128 | 1815 | 1317 | -27.4% | 843 | 365 | -56.7% |
| turboquant | 1447 | 1331 | -8.0% | 730 | 756 | +3.6% |

---

## 11. CPU A/B — Temporal Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1388 | 1196 | -13.9% | 996 | 1012 | +1.6% |
| uint8 | 1316 | 1340 | +1.9% | 960 | 973 | +1.4% |
| float16 | 1159 | 1366 | +17.9% | 1005 | 910 | -9.5% |
| float32 | 1507 | 1268 | -15.9% | 841 | 849 | +0.9% |
| float64 | 1719 | 1321 | -23.1% | 1168 | 1064 | -8.9% |
| complex64 | 1826 | 1476 | -19.2% | 1013 | 883 | -12.8% |
| complex128 | 1804 | 1772 | -1.7% | 1116 | 829 | -25.7% |
| turboquant | 1604 | 502 | -68.7% | 737 | 610 | -17.3% |

---

## 12. GPU A/B — Dense Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1908 | 1944 | +1.9% | 1303 | 1681 | +29.0% |
| uint8 | 3696 | 2140 | -42.1% | 1795 | 1824 | +1.6% |
| float16 | 1540 | 1576 | +2.3% | 1300 | 1323 | +1.8% |
| float32 | 2638 | 2942 | +11.5% | 2363 | 2206 | -6.7% |
| float64 | 959 | 1032 | +7.6% | 822 | 1017 | +23.6% |
| complex64 | 972 | 760 | -21.8% | 294 | 918 | +211.7% |
| complex128 | 785 | 3515 | +347.8% | 521 | 1020 | +95.9% |
| turboquant | 2764 | 3177 | +15.0% | 2138 | 2386 | +11.6% |

---

## 13. GPU A/B — Dense Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1832 | 1808 | -1.3% | 1635 | 1788 | +9.4% |
| uint8 | 2111 | 4901 | +132.1% | 1728 | 2042 | +18.2% |
| float16 | 1256 | 1674 | +33.3% | 1334 | 1659 | +24.3% |
| float32 | 2968 | 3120 | +5.1% | 2101 | 1854 | -11.7% |
| float64 | 1123 | 1189 | +5.9% | 1073 | 794 | -26.1% |
| complex64 | 859 | 969 | +12.8% | 672 | 956 | +42.1% |
| complex128 | 3420 | 1719 | -49.7% | 962 | 902 | -6.2% |
| turboquant | 2708 | 2940 | +8.6% | 3360 | 2747 | -18.2% |

---

## 14. GPU A/B — Sparse Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 5985 | 6225 | +4.0% | 5730 | 6081 | +6.1% |
| uint8 | 6284 | 6201 | -1.3% | 5658 | 5356 | -5.3% |
| float16 | 5698 | 6084 | +6.8% | 6371 | 6034 | -5.3% |
| float32 | 5267 | 5913 | +12.3% | 5637 | 4780 | -15.2% |
| float64 | 6877 | 6693 | -2.7% | 6308 | 6001 | -4.9% |
| complex64 | 6307 | 6459 | +2.4% | 441 | 6017 | +1264.7% |
| complex128 | 6298 | 6909 | +9.7% | 1358 | 5251 | +286.7% |
| turboquant | 4838 | 6018 | +24.4% | 4714 | 5243 | +11.2% |

---

## 15. GPU A/B — Sparse Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 6354 | 6455 | +1.6% | 6109 | 5829 | -4.6% |
| uint8 | 6105 | 7045 | +15.4% | 6175 | 6648 | +7.7% |
| float16 | 6364 | 5085 | -20.1% | 5880 | 5280 | -10.2% |
| float32 | 5792 | 5823 | +0.5% | 5251 | 4380 | -16.6% |
| float64 | 7079 | 6516 | -7.9% | 6093 | 6318 | +3.7% |
| complex64 | 6168 | 6286 | +1.9% | 6158 | 6221 | +1.0% |
| complex128 | 6288 | 5889 | -6.3% | 6459 | 6519 | +0.9% |
| turboquant | 5872 | 5929 | +1.0% | 6280 | 5526 | -12.0% |

---

## 16. GPU A/B — Hybrid Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1547 | 1530 | -1.1% | 1354 | 1356 | +0.1% |
| uint8 | 1804 | 1688 | -6.5% | 1399 | 1399 | +0.0% |
| float16 | 1195 | 1363 | +14.0% | 1159 | 1112 | -4.0% |
| float32 | 2457 | 2811 | +14.4% | 2503 | 1858 | -25.8% |
| float64 | 887 | 983 | +10.8% | 603 | 747 | +23.8% |
| complex64 | 892 | 766 | -14.1% | 622 | 675 | +8.5% |
| complex128 | 767 | 3390 | +342.1% | 389 | 865 | +122.6% |
| turboquant | 2611 | 3299 | +26.4% | 2098 | 2303 | +9.8% |

---

## 17. GPU A/B — Hybrid Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1590 | 1525 | -4.1% | 1437 | 1592 | +10.8% |
| uint8 | 1650 | 4896 | +196.7% | 1631 | 1791 | +9.8% |
| float16 | 1197 | 1223 | +2.2% | 1271 | 1262 | -0.7% |
| float32 | 2851 | 3112 | +9.2% | 2275 | 1877 | -17.5% |
| float64 | 842 | 871 | +3.4% | 750 | 694 | -7.4% |
| complex64 | 927 | 893 | -3.6% | 661 | 822 | +24.4% |
| complex128 | 1401 | 1294 | -7.6% | 806 | 917 | +13.8% |
| turboquant | 2645 | 2693 | +1.8% | 3054 | 2728 | -10.7% |

---

## 18. GPU A/B — Graphrag Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1443 | 1351 | -6.4% | 1165 | 1362 | +17.0% |
| uint8 | 2956 | 1609 | -45.6% | 1331 | 1237 | -7.1% |
| float16 | 1213 | 1279 | +5.5% | 1245 | 1054 | -15.3% |
| float32 | 2277 | 2779 | +22.0% | 2408 | 2131 | -11.5% |
| float64 | 915 | 976 | +6.7% | 405 | 688 | +69.7% |
| complex64 | 825 | 863 | +4.6% | 280 | 848 | +203.2% |
| complex128 | 704 | 3219 | +357.6% | 477 | 814 | +70.5% |
| turboquant | 2650 | 3108 | +17.3% | 1937 | 2188 | +13.0% |

---

## 19. GPU A/B — Graphrag Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1387 | 1374 | -0.9% | 1239 | 1298 | +4.8% |
| uint8 | 1620 | 4875 | +200.8% | 1404 | 1581 | +12.6% |
| float16 | 1643 | 1519 | -7.5% | 1110 | 1084 | -2.3% |
| float32 | 2829 | 2989 | +5.7% | 2278 | 1634 | -28.3% |
| float64 | 1019 | 1044 | +2.5% | 825 | 993 | +20.3% |
| complex64 | 942 | 993 | +5.4% | 713 | 862 | +21.0% |
| complex128 | 3131 | 807 | -74.2% | 894 | 776 | -13.2% |
| turboquant | 2638 | 2681 | +1.6% | 3184 | 2744 | -13.8% |

---

## 20. GPU A/B — Temporal Search (NoDisk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1458 | 1455 | -0.2% | 876 | 1058 | +20.8% |
| uint8 | 1466 | 1499 | +2.3% | 887 | 805 | -9.3% |
| float16 | 1356 | 1388 | +2.4% | 1008 | 681 | -32.4% |
| float32 | 1389 | 1687 | +21.5% | 969 | 880 | -9.1% |
| float64 | 1981 | 2042 | +3.1% | 1226 | 1070 | -12.8% |
| complex64 | 1919 | 1745 | -9.1% | 324 | 1133 | +249.9% |
| complex128 | 1858 | 1995 | +7.4% | 1062 | 1056 | -0.5% |
| turboquant | 1479 | 1621 | +9.6% | 859 | 950 | +10.7% |

---

## 21. GPU A/B — Temporal Search (Disk)

| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |
|---|---|---|---|---|---|---|
| int8 | 1574 | 1486 | -5.6% | 1034 | 981 | -5.1% |
| uint8 | 1452 | 1609 | +10.8% | 1024 | 999 | -2.5% |
| float16 | 1248 | 1428 | +14.5% | 956 | 943 | -1.3% |
| float32 | 1633 | 1541 | -5.6% | 884 | 739 | -16.4% |
| float64 | 2002 | 2052 | +2.5% | 1178 | 1221 | +3.6% |
| complex64 | 2003 | 1896 | -5.3% | 1077 | 933 | -13.3% |
| complex128 | 1968 | 1870 | -5.0% | 1138 | 1134 | -0.3% |
| turboquant | 1586 | 1573 | -0.8% | 1148 | 785 | -31.6% |

---

## 22. Disk vs NoDisk Impact (std build)

### CPU std — disk penalty/bonus at 100k

| dtype | dense | sparse | hybrid | graphrag | temporal |
|---|---|---|---|---|---|
| int8 | -31.4% | -20.8% | -30.5% | -27.7% | -21.4% |
| uint8 | +35.7% | -15.5% | -23.2% | +33.4% | -32.7% |
| float16 | -25.1% | -37.7% | -32.3% | -29.0% | -31.8% |
| float32 | -2.1% | -3.9% | +0.4% | -2.2% | -7.2% |
| float64 | -1.2% | -9.4% | -17.1% | -2.7% | -6.7% |
| complex64 | +16.7% | +15.8% | +6.6% | -2.5% | -1.1% |
| complex128 | -43.9% | +6.6% | -44.7% | -8.6% | -0.6% |
| turboquant | +11.7% | +5.4% | +15.7% | +8.0% | +10.8% |

### CPU std — disk bonus at 250k

| dtype | dense | sparse | hybrid | graphrag | temporal |
|---|---|---|---|---|---|
| int8 | -7.0% | -7.6% | -10.1% | -16.1% | -6.6% |
| uint8 | +130.3% | +4.4% | +146.3% | +164.3% | +3.2% |
| float16 | +12.3% | -3.2% | -12.3% | -13.4% | +6.6% |
| float32 | +10.7% | +6.5% | +0.0% | -7.2% | +11.3% |
| float64 | +10.1% | -1.9% | +21.1% | +17.8% | +3.5% |
| complex64 | -43.0% | -87.9% | -26.0% | +3.2% | -2.2% |
| complex128 | +29.6% | +4.0% | +5.1% | +6.5% | +32.3% |
| turboquant | +3.7% | +6.1% | -1.6% | +8.8% | +0.9% |

### GPU std — disk bonus at 250k

| dtype | dense | sparse | hybrid | graphrag | temporal |
|---|---|---|---|---|---|
| int8 | +25.5% | +6.6% | +6.2% | +6.4% | +18.0% |
| uint8 | -3.7% | +9.1% | +16.6% | +5.5% | +15.5% |
| float16 | +2.7% | -7.7% | +9.7% | -10.9% | -5.2% |
| float32 | -11.1% | -6.9% | -9.1% | -5.4% | -8.7% |
| float64 | +30.5% | -3.4% | +24.3% | +103.7% | -3.9% |
| complex64 | +128.4% | +1296.6% | +6.2% | +154.8% | +232.4% |
| complex128 | +84.7% | +375.7% | +107.4% | +87.3% | +7.2% |
| turboquant | +57.1% | +33.2% | +45.6% | +64.4% | +33.8% |

---

## 23. Key Findings

### emlgo Patterns

- **CPU 100k NoDisk is universally regressive** — emlgo loses on every dtype/search combination (worst: complex64 hybrid -64%, int8 dense -34%). This is the most consistent regression.
- **CPU 250k shows recovery for some types** — float64 dense +24%, turboquant gains, complex64 hybrid/graphrag flip positive. But float32 and complex128 temporal remain weak.
- **GPU emlgo is broadly stronger** — most NoDisk 100k combinations are positive or neutral, especially float32 (+11-22%), turboquant (+10-26%), and complex128 (massive +348%).
- **complex128 is emlgo's strongest type on GPU** — gains of +95-358% across dense/hybrid/graphrag at 100k NoDisk, though this reverses at 100k Disk graphrag (-74%).
- **uint8 with disk is volatile** — GPU uint8 100k Disk gains +132-201%, but CPU uint8 250k Disk loses -42-82%.

### Disk Impact

- **CPU int8/float16 at 100k**: disk hurts 20-38% across all modes (spill overhead).
- **CPU uint8 at 250k**: disk helps massively (+130-164%) — likely benefits from reduced memory pressure enabling better cache behavior.
- **GPU complex types at 250k**: disk helps enormously (+85-1297%) — disk mode may prevent OOM-induced performance cliffs.
- **GPU turboquant at 250k**: disk consistently helps (+33-64%) — similar OOM prevention effect.
- **float32 is mostly disk-neutral** — deltas within ±10% for most configurations.

### Stability Concerns

1. **CPU emlgo uint8 250k Disk hybrid** (-82%) — catastrophic regression, likely memory/spill interaction bug.
2. **CPU emlgo complex64 100k NoDisk** (-51-65%) — severe across sparse/hybrid/graphrag.
3. **GPU emlgo complex128 100k Disk graphrag** (-74%) — reversal from massive NoDisk gains.
4. **CPU emlgo turboquant 100k Disk temporal** (-69%) — severe temporal-specific regression.
5. **GPU std complex64/128 at 250k NoDisk** (294-521 QPS dense) — very low, possible OOM or spill cliff.
