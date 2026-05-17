## v0.2.1 Final Performance Validation (2026-05-16)

## Search Performance Summary (QPS)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32') |       1601.05 |        2380.77 |           2380.15 |               1244.44 |                 1223.25 |      2882.48 |                 1300.27 |           1317.67 |         2537.31 |                1734.8 |             1515.7 |         4380.02 |           3509.68 |

## Ingestion Performance (MB/s)

|                                  |   Throughput_MBs |
|:---------------------------------|-----------------:|
| ('perf', 'logs', 128, 'float32') |           514.19 |

## Search Latency Summary (P95 ms)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32') |          7.54 |           4.55 |              4.56 |                 19.08 |                   10.28 |         4.15 |                   10.01 |              9.56 |            4.69 |                   6.3 |               7.79 |            2.77 |              3.34 |

### Details: perf (logs)

| Host   | Mode   | Dataset                             | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:------------------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|---------:|
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | DoPut                 | 897374           |          438.171 |  0       |  0       |  0       |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | DoGet                 |      2.73066e+06 |         1333.33  |  0       |  0       |  0       |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Dense          |   2457.58        |            0     |  3.1835  |  4.23142 |  5.39842 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Hybrid         |   2525.09        |            0     |  3.08937 |  4.63804 |  5.56179 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Filtered       |   2399.48        |            0     |  3.24746 |  4.50921 |  5.59792 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_FilteredBool   |   1141.71        |            0     |  3.877   | 19.723   | 39.9337  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_FilteredString |   1313.53        |            0     |  5.87346 |  9.21525 | 11.4653  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Sparse         |   4517.94        |            0     |  1.72837 |  2.68625 |  3.88358 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_ByID           |   1644.9         |            0     |  4.7465  |  7.23537 | 10.0069  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_GraphRAG       |   1404.61        |            0     |  5.50533 |  8.53846 | 12.2238  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_GlobalGraphRAG |   1350.06        |            0     |  5.41642 |  9.68737 | 14.2927  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Recommend      |   1627.35        |            0     |  4.77154 |  7.09    |  9.0275  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Geo            |   3272.21        |            0     |  2.40046 |  3.45767 |  3.95054 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Temporal       |   3601.26        |            0     |  2.12017 |  3.24517 |  3.89829 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_LearnedIndex   |   1785.48        |            0     |  4.43363 |  5.8775  |  7.32525 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 |      1.20873e+06 |          590.202 |  0       |  0       |  0       |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 |      1.70325e+06 |          831.666 |  0       |  0       |  0       |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   2303.96        |            0     |  3.44612 |  4.87325 |  6.12938 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   2549.53        |            0     |  3.05738 |  4.73683 |  5.37312 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   2360.82        |            0     |  3.3705  |  4.60221 |  5.71054 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   1347.16        |            0     |  3.42971 | 18.4334  | 48.9161  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   1132.97        |            0     |  6.67208 | 11.3528  | 15.4937  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |   4242.09        |            0     |  1.83467 |  2.84625 |  3.38708 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |   1557.2         |            0     |  4.94958 |  7.84058 | 10.3948  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |   1230.73        |            0     |  6.02387 | 10.586   | 16.4136  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |   1250.48        |            0     |  5.92817 | 10.335   | 14.5945  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |   1404.05        |            0     |  5.52696 |  8.48729 | 10.8549  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   2492.76        |            0     |  2.57146 |  4.85217 | 14.1093  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   3418.09        |            0     |  2.23496 |  3.43879 |  4.10575 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |   1684.12        |            0     |  4.61492 |  6.72663 |  8.24812 |

# Longbow v0.2.1 Performance Matrix

## Search Performance Summary (QPS)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32') |       1601.05 |        2380.77 |           2380.15 |               1244.44 |                 1223.25 |      2882.48 |                 1300.27 |           1317.67 |         2537.31 |                1734.8 |             1515.7 |         4380.02 |           3509.68 |

## Ingestion Performance (MB/s)

|                                  |   Throughput_MBs |
|:---------------------------------|-----------------:|
| ('perf', 'logs', 128, 'float32') |           514.19 |

## Search Latency Summary (P95 ms)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32') |          7.54 |           4.55 |              4.56 |                 19.08 |                   10.28 |         4.15 |                   10.01 |              9.56 |            4.69 |                   6.3 |               7.79 |            2.77 |              3.34 |

### Details: perf (logs)

| Host   | Mode   | Dataset                             | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:------------------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|---------:|
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | DoPut                 | 897374           |          438.171 |  0       |  0       |  0       |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | DoGet                 |      2.73066e+06 |         1333.33  |  0       |  0       |  0       |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Dense          |   2457.58        |            0     |  3.1835  |  4.23142 |  5.39842 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Hybrid         |   2525.09        |            0     |  3.08937 |  4.63804 |  5.56179 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Filtered       |   2399.48        |            0     |  3.24746 |  4.50921 |  5.59792 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_FilteredBool   |   1141.71        |            0     |  3.877   | 19.723   | 39.9337  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_FilteredString |   1313.53        |            0     |  5.87346 |  9.21525 | 11.4653  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Sparse         |   4517.94        |            0     |  1.72837 |  2.68625 |  3.88358 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_ByID           |   1644.9         |            0     |  4.7465  |  7.23537 | 10.0069  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_GraphRAG       |   1404.61        |            0     |  5.50533 |  8.53846 | 12.2238  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_GlobalGraphRAG |   1350.06        |            0     |  5.41642 |  9.68737 | 14.2927  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Recommend      |   1627.35        |            0     |  4.77154 |  7.09    |  9.0275  |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Geo            |   3272.21        |            0     |  2.40046 |  3.45767 |  3.95054 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_Temporal       |   3601.26        |            0     |  2.12017 |  3.24517 |  3.89829 |
| perf   | logs   | result_cpu_float32_128_10000.json   | float32 |   128 |   10000 | Search_LearnedIndex   |   1785.48        |            0     |  4.43363 |  5.8775  |  7.32525 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 |      1.20873e+06 |          590.202 |  0       |  0       |  0       |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 |      1.70325e+06 |          831.666 |  0       |  0       |  0       |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   2303.96        |            0     |  3.44612 |  4.87325 |  6.12938 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   2549.53        |            0     |  3.05738 |  4.73683 |  5.37312 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   2360.82        |            0     |  3.3705  |  4.60221 |  5.71054 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   1347.16        |            0     |  3.42971 | 18.4334  | 48.9161  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   1132.97        |            0     |  6.67208 | 11.3528  | 15.4937  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |   4242.09        |            0     |  1.83467 |  2.84625 |  3.38708 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |   1557.2         |            0     |  4.94958 |  7.84058 | 10.3948  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |   1230.73        |            0     |  6.02387 | 10.586   | 16.4136  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |   1250.48        |            0     |  5.92817 | 10.335   | 14.5945  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |   1404.05        |            0     |  5.52696 |  8.48729 | 10.8549  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   2492.76        |            0     |  2.57146 |  4.85217 | 14.1093  |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   3418.09        |            0     |  2.23496 |  3.43879 |  4.10575 |
| perf   | logs   | result_metal_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |   1684.12        |            0     |  4.61492 |  6.72663 |  8.24812 |

