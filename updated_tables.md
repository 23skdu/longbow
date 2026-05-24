## Performance (dim=128, count=50000)
| Platform | Dtype | Search Mode | Ingest (vec/s) | QPS | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|
| Local (M3) | complex128 | Dense | 1277 | 1603 | 4.84 | 7.14 | 9.57 |
| Local (M3) | complex128 | Sparse | 1277 | 11250 | 0.67 | 1.11 | 1.37 |
| Local (M3) | complex128 | Temporal | 1277 | 2383 | 3.05 | 5.22 | 6.57 |
| Local (M3) | float32 | Dense | 1512 | 1259 | 6.23 | 9.55 | 11.34 |
| Local (M3) | float32 | Sparse | 1512 | 11798 | 0.67 | 1.03 | 1.25 |
| Local (M3) | float32 | Temporal | 1512 | 2497 | 2.96 | 5.07 | 6.39 |
| Local (M3) | int8 | Dense | 1816 | 2013 | 3.78 | 5.95 | 9.08 |
| Local (M3) | int8 | Sparse | 1816 | 11998 | 0.65 | 1.02 | 1.20 |
| Local (M3) | int8 | Temporal | 1816 | 1381 | 4.75 | 10.60 | 14.49 |
| Local (M3) | turboquant | Dense | 9895 | 2647 | 2.95 | 4.52 | 5.21 |
| Local (M3) | turboquant | Sparse | 9895 | 11746 | 0.66 | 1.07 | 1.24 |
| Local (M3) | turboquant | Temporal | 9895 | 2441 | 3.00 | 5.05 | 7.60 |
| Remote (Ancalagon) | complex128 | Dense | 1091 | 1008 | 7.51 | 11.93 | 15.19 |
| Remote (Ancalagon) | complex128 | Sparse | 1091 | 6413 | 1.02 | 1.43 | 3.70 |
| Remote (Ancalagon) | complex128 | Temporal | 1091 | 2301 | 3.21 | 5.67 | 6.31 |
| Remote (Ancalagon) | float32 | Dense | 1278 | 834 | 9.05 | 14.14 | 22.32 |
| Remote (Ancalagon) | float32 | Sparse | 1278 | 7839 | 1.02 | 1.37 | 1.52 |
| Remote (Ancalagon) | float32 | Temporal | 1278 | 2301 | 3.13 | 5.70 | 6.50 |
| Remote (Ancalagon) | int8 | Dense | 1280 | 1128 | 6.22 | 11.12 | 26.11 |
| Remote (Ancalagon) | int8 | Sparse | 1280 | 8078 | 0.97 | 1.43 | 1.61 |
| Remote (Ancalagon) | int8 | Temporal | 1280 | 1410 | 5.01 | 9.86 | 14.22 |
| Remote (Ancalagon) | turboquant | Dense | 6572 | 1111 | 6.72 | 11.30 | 16.89 |
| Remote (Ancalagon) | turboquant | Sparse | 6572 | 7835 | 0.99 | 1.50 | 1.70 |
| Remote (Ancalagon) | turboquant | Temporal | 6572 | 2203 | 3.30 | 5.74 | 7.00 |


## Performance (dim=128, count=100000)
| Platform | Dtype | Search Mode | Ingest (vec/s) | QPS | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|
| Local (M3) | complex128 | Dense | 1929 | 1393 | 5.67 | 8.14 | 10.26 |
| Local (M3) | complex128 | Sparse | 1929 | 11617 | 0.67 | 1.05 | 1.22 |
| Local (M3) | complex128 | Temporal | 1929 | 2225 | 3.28 | 5.42 | 7.78 |
| Local (M3) | float32 | Dense | 838 | 951 | 8.00 | 13.48 | 17.91 |
| Local (M3) | float32 | Sparse | 838 | 8456 | 0.85 | 1.68 | 2.03 |
| Local (M3) | float32 | Temporal | 838 | 2082 | 3.38 | 6.36 | 8.10 |
| Local (M3) | int8 | Dense | 3836 | 1774 | 4.20 | 7.15 | 11.76 |
| Local (M3) | int8 | Sparse | 3836 | 8114 | 0.90 | 1.72 | 2.30 |
| Local (M3) | int8 | Temporal | 3836 | 1128 | 6.32 | 11.62 | 17.14 |
| Local (M3) | turboquant | Dense | 8258 | 2655 | 2.92 | 4.31 | 5.55 |
| Local (M3) | turboquant | Sparse | 8258 | 12031 | 0.65 | 1.01 | 1.21 |
| Local (M3) | turboquant | Temporal | 8258 | 2067 | 3.39 | 6.39 | 11.02 |
| Remote (Ancalagon) | complex128 | Dense | 1583 | 958 | 8.08 | 11.24 | 16.82 |
| Remote (Ancalagon) | complex128 | Sparse | 1583 | 5972 | 1.12 | 1.64 | 2.02 |
| Remote (Ancalagon) | complex128 | Temporal | 1583 | 1876 | 3.90 | 5.86 | 7.29 |
| Remote (Ancalagon) | float32 | Dense | 875 | 614 | 12.10 | 21.18 | 39.08 |
| Remote (Ancalagon) | float32 | Sparse | 875 | 8049 | 0.99 | 1.41 | 1.59 |
| Remote (Ancalagon) | float32 | Temporal | 875 | 1697 | 4.28 | 7.85 | 10.04 |
| Remote (Ancalagon) | int8 | Dense | 2034 | 1069 | 6.73 | 12.53 | 22.15 |
| Remote (Ancalagon) | int8 | Sparse | 2034 | 7927 | 1.00 | 1.41 | 1.61 |
| Remote (Ancalagon) | int8 | Temporal | 2034 | 1113 | 6.22 | 12.70 | 18.10 |
| Remote (Ancalagon) | turboquant | Dense | 4950 | 1235 | 6.13 | 8.66 | 13.13 |
| Remote (Ancalagon) | turboquant | Sparse | 4950 | 8067 | 0.97 | 1.41 | 1.60 |
| Remote (Ancalagon) | turboquant | Temporal | 4950 | 1832 | 3.88 | 6.89 | 9.32 |


## Performance (dim=384, count=50000)
| Platform | Dtype | Search Mode | Ingest (vec/s) | QPS | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|
| Local (M3) | complex128 | Dense | 651 | 1027 | 7.66 | 10.50 | 11.84 |
| Local (M3) | complex128 | Sparse | 651 | 11696 | 0.68 | 1.05 | 1.24 |
| Local (M3) | complex128 | Temporal | 651 | 2577 | 2.92 | 4.42 | 6.12 |
| Local (M3) | float32 | Dense | 652 | 836 | 9.38 | 13.75 | 15.79 |
| Local (M3) | float32 | Sparse | 652 | 10362 | 0.70 | 1.29 | 1.69 |
| Local (M3) | float32 | Temporal | 652 | 2518 | 2.97 | 4.77 | 6.29 |
| Local (M3) | int8 | Dense | 1098 | 1396 | 5.56 | 8.40 | 9.88 |
| Local (M3) | int8 | Sparse | 1098 | 11129 | 0.68 | 1.11 | 1.57 |
| Local (M3) | int8 | Temporal | 1098 | 1396 | 4.89 | 10.52 | 13.26 |
| Local (M3) | turboquant | Dense | 9792 | 2251 | 3.36 | 5.29 | 9.33 |
| Local (M3) | turboquant | Sparse | 9792 | 11944 | 0.65 | 1.00 | 1.18 |
| Local (M3) | turboquant | Temporal | 9792 | 2443 | 2.99 | 5.32 | 7.12 |
| Remote (Ancalagon) | complex128 | Dense | 522 | 675 | 11.77 | 14.36 | 17.99 |
| Remote (Ancalagon) | complex128 | Sparse | 522 | 8003 | 0.97 | 1.52 | 1.75 |
| Remote (Ancalagon) | complex128 | Temporal | 522 | 2324 | 3.14 | 5.15 | 7.96 |
| Remote (Ancalagon) | float32 | Dense | 732 | 581 | 13.31 | 19.07 | 29.20 |
| Remote (Ancalagon) | float32 | Sparse | 732 | 7989 | 0.99 | 1.52 | 1.68 |
| Remote (Ancalagon) | float32 | Temporal | 732 | 2170 | 3.41 | 5.44 | 6.74 |
| Remote (Ancalagon) | int8 | Dense | 734 | 1032 | 7.22 | 12.83 | 19.24 |
| Remote (Ancalagon) | int8 | Sparse | 734 | 8122 | 0.98 | 1.34 | 1.50 |
| Remote (Ancalagon) | int8 | Temporal | 734 | 1374 | 4.96 | 10.50 | 14.89 |
| Remote (Ancalagon) | turboquant | Dense | 6473 | 1168 | 6.32 | 10.35 | 16.45 |
| Remote (Ancalagon) | turboquant | Sparse | 6473 | 8096 | 0.98 | 1.40 | 1.58 |
| Remote (Ancalagon) | turboquant | Temporal | 6473 | 2220 | 3.30 | 5.78 | 6.89 |


## Performance (dim=384, count=100000)
| Platform | Dtype | Search Mode | Ingest (vec/s) | QPS | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|
| Local (M3) | complex128 | Dense | 1017 | 795 | 9.31 | 16.76 | 24.17 |
| Local (M3) | complex128 | Sparse | 1017 | 11622 | 0.67 | 1.06 | 1.22 |
| Local (M3) | complex128 | Temporal | 1017 | 2224 | 3.33 | 5.41 | 6.90 |
| Local (M3) | float32 | Dense | 288 | 624 | 12.28 | 19.08 | 25.84 |
| Local (M3) | float32 | Sparse | 288 | 10412 | 0.75 | 1.23 | 1.52 |
| Local (M3) | float32 | Temporal | 288 | 2175 | 3.28 | 5.53 | 7.21 |
| Local (M3) | int8 | Dense | 1719 | 1280 | 5.92 | 9.61 | 15.63 |
| Local (M3) | int8 | Sparse | 1719 | 11568 | 0.68 | 1.02 | 1.36 |
| Local (M3) | int8 | Temporal | 1719 | 1065 | 6.65 | 11.98 | 14.80 |
| Local (M3) | turboquant | Dense | 6355 | 2127 | 3.50 | 5.68 | 11.01 |
| Local (M3) | turboquant | Sparse | 6355 | 11728 | 0.67 | 1.02 | 1.21 |
| Local (M3) | turboquant | Temporal | 6355 | 2208 | 3.32 | 5.51 | 6.98 |
| Remote (Ancalagon) | complex128 | Dense | 873 | 549 | 14.44 | 17.83 | 21.58 |
| Remote (Ancalagon) | complex128 | Sparse | 873 | 8066 | 1.00 | 1.35 | 1.51 |
| Remote (Ancalagon) | complex128 | Temporal | 873 | 1811 | 4.06 | 6.42 | 9.33 |
| Remote (Ancalagon) | float32 | Dense | 355 | 437 | 17.84 | 25.76 | 42.15 |
| Remote (Ancalagon) | float32 | Sparse | 355 | 7533 | 1.05 | 1.52 | 1.68 |
| Remote (Ancalagon) | float32 | Temporal | 355 | 1768 | 4.09 | 6.77 | 9.58 |
| Remote (Ancalagon) | int8 | Dense | 1201 | 942 | 8.04 | 12.15 | 21.04 |
| Remote (Ancalagon) | int8 | Sparse | 1201 | 7757 | 1.03 | 1.42 | 1.58 |
| Remote (Ancalagon) | int8 | Temporal | 1201 | 1161 | 6.01 | 10.43 | 18.23 |
| Remote (Ancalagon) | turboquant | Dense | 5906 | 1182 | 6.65 | 8.59 | 9.83 |
| Remote (Ancalagon) | turboquant | Sparse | 5906 | 7753 | 1.03 | 1.46 | 1.59 |
| Remote (Ancalagon) | turboquant | Temporal | 5906 | 1826 | 4.02 | 6.37 | 7.55 |


