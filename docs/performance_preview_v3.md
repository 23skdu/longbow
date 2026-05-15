# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                     |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'host', 128, 'float32')   |      13086.7  |        9988.55 |          10748.8  |              10883.2  |                 9285.08 |      5149.83 |                11378.9  |          11256.5  |        10360.5  |               4447.77 |           13219.5  |        12133.5  |           6052.57 |
| ('local', 'host', 128, 'float64')   |      13174    |        6576.14 |           8807.41 |              11041.2  |                11403.1  |      5649.14 |                10605.8  |          11304.8  |         8691.74 |               4490.07 |           12613.9  |        12070.4  |           5796.75 |
| ('local', 'host', 384, 'float32')   |      13142.1  |        8124.99 |           8121.87 |               9061.88 |                 9108.45 |      5268.25 |                 8160    |           9223.89 |         9019.05 |               4571.32 |           12480.8  |        12236.9  |           6162.79 |
| ('local', 'host', 384, 'float64')   |      13421.4  |        8679.97 |           7428.89 |               9006.12 |                 9102.5  |      5529.51 |                 9085.4  |           8716.78 |         9436.83 |               4354.99 |           12105.3  |        12290.6  |           6072.47 |
| ('local', 'host', 768, 'float32')   |      12914    |        7479.61 |           6152.13 |               7832.37 |                 7798    |      5457.78 |                 7739.42 |           7732    |         6940.13 |               4386.01 |           12515.6  |        12163    |           6164.6  |
| ('local', 'host', 768, 'float64')   |      13490.6  |        7337.98 |           6250.76 |               7929.15 |                 7921.19 |      4419.8  |                 7758.74 |           7806.36 |         7806.94 |               4371.09 |           11853    |        12296    |           5758.03 |
| ('local', 'host', 1024, 'float32')  |      12497.8  |        6688.51 |           6827.26 |               6672.77 |                 6468.43 |      5155.36 |                 6802.57 |           6902.93 |         7067.2  |               4303.85 |           12079.3  |         9704.85 |           6147.8  |
| ('local', 'host', 1024, 'float64')  |      13556.4  |        6729.07 |           6782.02 |               6722.99 |                 6184.68 |      5470.8  |                 6829.52 |           6703.12 |         6793.56 |               4314.39 |           11663.7  |        11598.7  |           5944.39 |
| ('local', 'host', 3072, 'float32')  |      11417.1  |        3399.35 |           4206.21 |               4207.18 |                 4144.44 |      5441.61 |                 4310.35 |           4307.5  |         4291.72 |               3489.47 |           10665.5  |        11862.1  |           6198.33 |
| ('local', 'host', 3072, 'float64')  |      12225.3  |        4041.72 |           4097.1  |               3970.94 |                 3619.16 |      5303.63 |                 4163.89 |           4266.35 |         4257.26 |               3415.74 |            9920.32 |        10890.5  |           5985.88 |
| ('perf', 'logs', 128, 'float32')    |      13086.7  |        9988.55 |          10748.8  |              10883.2  |                 9285.08 |      5149.83 |                11378.9  |          11256.5  |        10360.5  |               4447.77 |           13219.5  |        12133.5  |           6052.57 |
| ('perf', 'logs', 128, 'float64')    |      13174    |        6576.14 |           8807.41 |              11041.2  |                11403.1  |      5649.14 |                10605.8  |          11304.8  |         8691.74 |               4490.07 |           12613.9  |        12070.4  |           5796.75 |
| ('perf', 'logs', 384, 'float32')    |      13142.1  |        8124.99 |           8121.87 |               9061.88 |                 9108.45 |      5268.25 |                 8160    |           9223.89 |         9019.05 |               4571.32 |           12480.8  |        12236.9  |           6162.79 |
| ('perf', 'logs', 384, 'float64')    |      13421.4  |        8679.97 |           7428.89 |               9006.12 |                 9102.5  |      5529.51 |                 9085.4  |           8716.78 |         9436.83 |               4354.99 |           12105.3  |        12290.6  |           6072.47 |
| ('perf', 'logs', 768, 'float32')    |      12914    |        7479.61 |           6152.13 |               7832.37 |                 7798    |      5457.78 |                 7739.42 |           7732    |         6940.13 |               4386.01 |           12515.6  |        12163    |           6164.6  |
| ('perf', 'logs', 768, 'float64')    |      13490.6  |        7337.98 |           6250.76 |               7929.15 |                 7921.19 |      4419.8  |                 7758.74 |           7806.36 |         7806.94 |               4371.09 |           11853    |        12296    |           5758.03 |
| ('perf', 'logs', 1024, 'float32')   |      12497.8  |        6688.51 |           6827.26 |               6672.77 |                 6468.43 |      5155.36 |                 6802.57 |           6902.93 |         7067.2  |               4303.85 |           12079.3  |         9704.85 |           6147.8  |
| ('perf', 'logs', 1024, 'float64')   |      13556.4  |        6729.07 |           6782.02 |               6722.99 |                 6184.68 |      5470.8  |                 6829.52 |           6703.12 |         6793.56 |               4314.39 |           11663.7  |        11598.7  |           5944.39 |
| ('perf', 'logs', 3072, 'float32')   |      11417.1  |        3399.35 |           4206.21 |               4207.18 |                 4144.44 |      5441.61 |                 4310.35 |           4307.5  |         4291.72 |               3489.47 |           10665.5  |        11862.1  |           6198.33 |
| ('perf', 'logs', 3072, 'float64')   |      12225.3  |        4041.72 |           4097.1  |               3970.94 |                 3619.16 |      5303.63 |                 4163.89 |           4266.35 |         4257.26 |               3415.74 |            9920.32 |        10890.5  |           5985.88 |
| ('remote', 'host', 128, 'float32')  |       6556.92 |        4985.78 |           5141.01 |               5482.43 |                 5598.69 |      1868.17 |                 5298.12 |           5369.49 |         2697.38 |               2721.47 |            6431.83 |         7243.1  |           2768.59 |
| ('remote', 'host', 128, 'float64')  |       6789.13 |        4936.2  |           5384.94 |               5372.72 |                 5490.63 |       673.85 |                 3963.48 |           4386.17 |         3434.71 |               2764.52 |            3764.31 |         7062.5  |           2707.03 |
| ('remote', 'host', 384, 'float32')  |       4650.58 |        4739.35 |           4786.55 |               4691.63 |                 5218.49 |      1677.5  |                 3511.46 |           3524.35 |         3582.97 |               2293.9  |            4911.35 |         5348.88 |           2760.5  |
| ('remote', 'host', 384, 'float64')  |       6882.9  |        5017.2  |           5048.54 |               4594.46 |                 5445.6  |      1076.23 |                 4898.06 |           4833.14 |         3349.16 |               2588.41 |            6173.97 |         7326.59 |           2574.22 |
| ('remote', 'host', 768, 'float32')  |       6343.21 |        4422.35 |           3971.57 |               3831.64 |                 3812.19 |      1565.1  |                 3066.11 |           3093.59 |         3865.95 |               2328.5  |            3619.56 |         6304.67 |           2984.79 |
| ('remote', 'host', 768, 'float64')  |       4589.08 |        2709.14 |           4070.76 |               4218.49 |                 4158.42 |      1547.03 |                 3194.05 |           3080.74 |         4167.91 |               2494.18 |            4990.66 |         5959.21 |           2960.44 |
| ('remote', 'host', 1024, 'float32') |       6248.12 |        3813    |           3595.66 |               3853.53 |                 3626.89 |       920    |                 2898.1  |           2718.59 |         3868.59 |               2467.2  |            4938.09 |         6481.1  |           2916.02 |
| ('remote', 'host', 1024, 'float64') |       5005.18 |        3760.48 |           3856.99 |               3770.95 |                 3009.85 |      1291.33 |                 2672.03 |           2321.91 |         4000.35 |               2632.9  |            4512.67 |         5168.34 |           2597.91 |
| ('remote', 'host', 3072, 'float32') |       5110.47 |        2624.17 |           1801.16 |               2113.1  |                 2035.9  |      1643.3  |                 2398.81 |           1883.74 |         2156.24 |               2755.85 |            5117.3  |         5812.92 |           3582.5  |
| ('remote', 'host', 3072, 'float64') |       6711.73 |        2657.6  |           2834.27 |               2846.64 |                 2926.36 |      1531.08 |                 1992.88 |           2407.17 |         3278.83 |               1765.96 |            3961.53 |         6825.5  |           2757.51 |

## Ingestion Performance (MB/s)

|                                     |   Throughput_MBs |
|:------------------------------------|-----------------:|
| ('local', 'host', 128, 'float32')   |           394.03 |
| ('local', 'host', 128, 'float64')   |           427.38 |
| ('local', 'host', 384, 'float32')   |           751.3  |
| ('local', 'host', 384, 'float64')   |           722.25 |
| ('local', 'host', 768, 'float32')   |           746.07 |
| ('local', 'host', 768, 'float64')   |           872.38 |
| ('local', 'host', 1024, 'float32')  |           817.78 |
| ('local', 'host', 1024, 'float64')  |           924.78 |
| ('local', 'host', 3072, 'float32')  |          1023.82 |
| ('local', 'host', 3072, 'float64')  |          1146.8  |
| ('perf', 'logs', 128, 'float32')    |           394.03 |
| ('perf', 'logs', 128, 'float64')    |           427.38 |
| ('perf', 'logs', 384, 'float32')    |           751.3  |
| ('perf', 'logs', 384, 'float64')    |           722.25 |
| ('perf', 'logs', 768, 'float32')    |           746.07 |
| ('perf', 'logs', 768, 'float64')    |           872.38 |
| ('perf', 'logs', 1024, 'float32')   |           817.78 |
| ('perf', 'logs', 1024, 'float64')   |           924.78 |
| ('perf', 'logs', 3072, 'float32')   |          1023.82 |
| ('perf', 'logs', 3072, 'float64')   |          1146.8  |
| ('remote', 'host', 128, 'float32')  |           158.35 |
| ('remote', 'host', 128, 'float64')  |           277.91 |
| ('remote', 'host', 384, 'float32')  |           315.26 |
| ('remote', 'host', 384, 'float64')  |           324    |
| ('remote', 'host', 768, 'float32')  |           342.28 |
| ('remote', 'host', 768, 'float64')  |           358.69 |
| ('remote', 'host', 1024, 'float32') |           337.25 |
| ('remote', 'host', 1024, 'float64') |           320.26 |
| ('remote', 'host', 3072, 'float32') |           328.51 |
| ('remote', 'host', 3072, 'float64') |           343.33 |

## Search Latency Summary (P95 ms)

|                                     |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'host', 128, 'float32')   |          0.76 |           1.03 |              0.84 |                  0.88 |                    1.29 |         2.17 |                    0.83 |              0.84 |            1.06 |                  2.66 |               0.79 |            1.04 |              1.87 |
| ('local', 'host', 128, 'float64')   |          0.8  |           2.34 |              1.44 |                  0.82 |                    0.82 |         1.95 |                    0.99 |              0.82 |            1.21 |                  2.64 |               0.8  |            1    |              1.99 |
| ('local', 'host', 384, 'float32')   |          0.77 |           1.59 |              1.44 |                  1    |                    0.99 |         2.06 |                    1.23 |              0.98 |            1.03 |                  2.54 |               0.79 |            0.98 |              1.82 |
| ('local', 'host', 384, 'float64')   |          0.78 |           1.02 |              1.03 |                  1    |                    0.97 |         1.87 |                    1.01 |              1.13 |            0.99 |                  2.55 |               0.82 |            1.02 |              1.85 |
| ('local', 'host', 768, 'float32')   |          0.78 |           1.19 |              1.58 |                  1.15 |                    1.16 |         2.14 |                    1.17 |              1.19 |            2.08 |                  2.56 |               0.79 |            1.01 |              1.8  |
| ('local', 'host', 768, 'float64')   |          0.76 |           1.24 |              1.56 |                  1.14 |                    1.14 |         3.46 |                    1.16 |              1.15 |            1.19 |                  2.57 |               0.84 |            0.98 |              1.97 |
| ('local', 'host', 1024, 'float32')  |          0.8  |           1.35 |              1.38 |                  1.47 |                    1.47 |         2.1  |                    1.41 |              1.35 |            1.34 |                  2.58 |               0.83 |            1.63 |              1.83 |
| ('local', 'host', 1024, 'float64')  |          0.77 |           1.36 |              1.32 |                  1.43 |                    1.46 |         1.83 |                    1.39 |              1.46 |            1.43 |                  2.51 |               0.83 |            1.03 |              1.89 |
| ('local', 'host', 3072, 'float32')  |          0.87 |           2.73 |              2.38 |                  2.37 |                    2.41 |         2.1  |                    2.31 |              2.33 |            2.31 |                  3.01 |               0.91 |            1.03 |              1.82 |
| ('local', 'host', 3072, 'float64')  |          0.84 |           2.43 |              2.41 |                  2.53 |                    3.74 |         1.95 |                    2.47 |              2.3  |            2.37 |                  3.04 |               1.02 |            1.1  |              1.87 |
| ('perf', 'logs', 128, 'float32')    |          0.76 |           1.03 |              0.84 |                  0.88 |                    1.29 |         2.17 |                    0.83 |              0.84 |            1.06 |                  2.66 |               0.79 |            1.04 |              1.87 |
| ('perf', 'logs', 128, 'float64')    |          0.8  |           2.34 |              1.44 |                  0.82 |                    0.82 |         1.95 |                    0.99 |              0.82 |            1.21 |                  2.64 |               0.8  |            1    |              1.99 |
| ('perf', 'logs', 384, 'float32')    |          0.77 |           1.59 |              1.44 |                  1    |                    0.99 |         2.06 |                    1.23 |              0.98 |            1.03 |                  2.54 |               0.79 |            0.98 |              1.82 |
| ('perf', 'logs', 384, 'float64')    |          0.78 |           1.02 |              1.03 |                  1    |                    0.97 |         1.87 |                    1.01 |              1.13 |            0.99 |                  2.55 |               0.82 |            1.02 |              1.85 |
| ('perf', 'logs', 768, 'float32')    |          0.78 |           1.19 |              1.58 |                  1.15 |                    1.16 |         2.14 |                    1.17 |              1.19 |            2.08 |                  2.56 |               0.79 |            1.01 |              1.8  |
| ('perf', 'logs', 768, 'float64')    |          0.76 |           1.24 |              1.56 |                  1.14 |                    1.14 |         3.46 |                    1.16 |              1.15 |            1.19 |                  2.57 |               0.84 |            0.98 |              1.97 |
| ('perf', 'logs', 1024, 'float32')   |          0.8  |           1.35 |              1.38 |                  1.47 |                    1.47 |         2.1  |                    1.41 |              1.35 |            1.34 |                  2.58 |               0.83 |            1.63 |              1.83 |
| ('perf', 'logs', 1024, 'float64')   |          0.77 |           1.36 |              1.32 |                  1.43 |                    1.46 |         1.83 |                    1.39 |              1.46 |            1.43 |                  2.51 |               0.83 |            1.03 |              1.89 |
| ('perf', 'logs', 3072, 'float32')   |          0.87 |           2.73 |              2.38 |                  2.37 |                    2.41 |         2.1  |                    2.31 |              2.33 |            2.31 |                  3.01 |               0.91 |            1.03 |              1.82 |
| ('perf', 'logs', 3072, 'float64')   |          0.84 |           2.43 |              2.41 |                  2.53 |                    3.74 |         1.95 |                    2.47 |              2.3  |            2.37 |                  3.04 |               1.02 |            1.1  |              1.87 |
| ('remote', 'host', 128, 'float32')  |          1.56 |           2.02 |              1.84 |                  1.69 |                    2.03 |        16.82 |                    1.76 |              1.71 |           16.38 |                  4.45 |               1.51 |            1.42 |              4.76 |
| ('remote', 'host', 128, 'float64')  |          1.46 |           2.35 |              1.72 |                  1.77 |                    1.67 |        30.23 |                    3.29 |              2.28 |            2.11 |                  4.18 |               3.29 |            1.48 |              4.64 |
| ('remote', 'host', 384, 'float32')  |          2.71 |           2.06 |              2.07 |                  2.07 |                    1.74 |         7.64 |                    3.37 |              3.21 |            2.03 |                  4.57 |               2.42 |            2.59 |              4.46 |
| ('remote', 'host', 384, 'float64')  |          1.43 |           1.81 |              1.83 |                  2.16 |                    1.63 |        21.12 |                    1.87 |              1.96 |            2.24 |                  4.69 |               1.53 |            1.47 |              5.44 |
| ('remote', 'host', 768, 'float32')  |          1.53 |           2.1  |              2.32 |                  2.39 |                    2.35 |         8.81 |                    3.59 |              3.47 |            2.75 |                  4.75 |               3.1  |            1.65 |              3.87 |
| ('remote', 'host', 768, 'float64')  |          3.47 |          11.78 |              2.25 |                  2.3  |                    2.3  |         8.44 |                    3.31 |              3.6  |            2.28 |                  4.56 |               2.41 |            2.18 |              3.97 |
| ('remote', 'host', 1024, 'float32') |          1.74 |           2.39 |              2.58 |                  2.41 |                    2.47 |        24.74 |                    4    |              3.78 |            2.55 |                  4.69 |               2.24 |            1.58 |              4.03 |
| ('remote', 'host', 1024, 'float64') |          2.63 |           2.43 |              2.4  |                  2.72 |                    3.71 |        16.32 |                    4.42 |              4.5  |            2.31 |                  4.38 |               2.5  |            2.55 |              4.71 |
| ('remote', 'host', 3072, 'float32') |          2.09 |           3.8  |              6.7  |                  5.44 |                    5.57 |         8.85 |                    4.81 |              6.76 |            5.65 |                  4.45 |               2.07 |            2.27 |              3.02 |
| ('remote', 'host', 3072, 'float64') |          1.46 |           3.67 |              3.62 |                  3.69 |                    3.53 |         7.97 |                    5.51 |              3.99 |            3.29 |                  6.61 |               2.9  |            1.57 |              4.09 |

### Details: local (host)

| Host   | Mode   | Dataset                           | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |    P99_ms |
|:-------|:-------|:----------------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|----------:|
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoPut                 |        806978    |          394.032 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoGet                 |        892758    |          435.917 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Dense          |          9988.55 |            0     | 0.710292 | 1.03254  |  2.31717  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Hybrid         |         10360.4  |            0     | 0.753791 | 1.06408  |  1.28933  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Filtered       |         10748.8  |            0     | 0.703042 | 0.842875 |  1.03667  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredBool   |         10883.2  |            0     | 0.709209 | 0.875875 |  1.20979  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredString |          9285.08 |            0     | 0.684416 | 1.29325  |  5.57496  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Sparse         |         12133.5  |            0     | 0.649042 | 1.03988  |  1.20258  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_ByID           |         13086.7  |            0     | 0.600084 | 0.765    |  0.845416 |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GraphRAG       |         11256.5  |            0     | 0.70225  | 0.838042 |  0.906125 |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GlobalGraphRAG |         11378.9  |            0     | 0.693875 | 0.833084 |  1.03838  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Recommend      |         13219.5  |            0     | 0.579375 | 0.78725  |  0.8785   |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Geo            |          5149.83 |            0     | 1.32483  | 2.17363  | 13.6505   |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Temporal       |          6052.57 |            0     | 1.31137  | 1.87333  |  2.08729  |
| local  | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_LearnedIndex   |          4447.77 |            0     | 1.78975  | 2.65521  |  2.97687  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoPut                 |        246528    |          722.25  | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoGet                 |        233615    |          684.418 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Dense          |          8679.97 |            0     | 0.864875 | 1.02396  |  2.01358  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Hybrid         |          9436.83 |            0     | 0.841125 | 0.991083 |  1.16904  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Filtered       |          7428.89 |            0     | 0.864958 | 1.02846  | 11.3649   |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredBool   |          9006.12 |            0     | 0.879209 | 1.00012  |  1.10254  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredString |          9102.5  |            0     | 0.864166 | 0.973458 |  1.63192  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Sparse         |         12290.6  |            0     | 0.642458 | 1.02167  |  1.18446  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_ByID           |         13421.4  |            0     | 0.588375 | 0.781334 |  0.898083 |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GraphRAG       |          8716.78 |            0     | 0.882583 | 1.12779  |  1.61196  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GlobalGraphRAG |          9085.4  |            0     | 0.87375  | 1.00562  |  1.08763  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Recommend      |         12105.3  |            0     | 0.644708 | 0.822292 |  0.910833 |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Geo            |          5529.51 |            0     | 1.37562  | 1.87008  |  2.31446  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Temporal       |          6072.47 |            0     | 1.28738  | 1.84917  |  2.11896  |
| local  | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_LearnedIndex   |          4354.99 |            0     | 1.73204  | 2.54804  |  3.29696  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoPut                 |        118372    |          924.781 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoGet                 |        117075    |          914.65  | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Dense          |          6729.07 |            0     | 1.13987  | 1.35888  |  2.02104  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Hybrid         |          6793.56 |            0     | 1.14296  | 1.43263  |  1.97758  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Filtered       |          6782.02 |            0     | 1.14142  | 1.318    |  1.45612  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredBool   |          6722.99 |            0     | 1.17362  | 1.431    |  1.65121  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredString |          6184.68 |            0     | 1.22158  | 1.46233  |  2.67217  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Sparse         |         11598.7  |            0     | 0.67425  | 1.02808  |  1.26525  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_ByID           |         13556.4  |            0     | 0.574917 | 0.766167 |  0.865666 |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GraphRAG       |          6703.12 |            0     | 1.18267  | 1.46412  |  1.62787  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GlobalGraphRAG |          6829.52 |            0     | 1.15212  | 1.38787  |  1.873    |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Recommend      |         11663.7  |            0     | 0.674208 | 0.833959 |  0.907791 |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Geo            |          5470.8  |            0     | 1.34987  | 1.83392  |  2.35358  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Temporal       |          5944.39 |            0     | 1.33979  | 1.891    |  2.08017  |
| local  | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_LearnedIndex   |          4314.39 |            0     | 1.84438  | 2.51162  |  2.81017  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoPut                 |        148886    |          872.381 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoGet                 |        116075    |          680.125 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Dense          |          7337.98 |            0     | 1.02479  | 1.239    |  2.31367  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Hybrid         |          7806.94 |            0     | 1.01767  | 1.18912  |  1.28767  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Filtered       |          6250.76 |            0     | 1.0245   | 1.55871  | 14.0167   |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredBool   |          7929.15 |            0     | 0.998875 | 1.14112  |  1.29192  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredString |          7921.19 |            0     | 0.999584 | 1.14346  |  1.28137  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Sparse         |         12296    |            0     | 0.62975  | 0.977834 |  1.2315   |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_ByID           |         13490.6  |            0     | 0.580666 | 0.763    |  0.867667 |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GraphRAG       |          7806.36 |            0     | 1.02212  | 1.15313  |  1.21883  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GlobalGraphRAG |          7758.74 |            0     | 1.02967  | 1.16237  |  1.25092  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Recommend      |         11853    |            0     | 0.646333 | 0.837375 |  1.22921  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Geo            |          4419.8  |            0     | 1.54433  | 3.46467  |  4.39262  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Temporal       |          5758.03 |            0     | 1.37433  | 1.97212  |  2.19983  |
| local  | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_LearnedIndex   |          4371.09 |            0     | 1.81471  | 2.57442  |  2.84333  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoPut                 |         87365.6  |         1023.82  | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoGet                 |         74504.5  |          873.1   | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Dense          |          3399.35 |            0     | 1.85263  | 2.73279  | 21.954    |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Hybrid         |          4291.72 |            0     | 1.85117  | 2.31129  |  2.48925  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Filtered       |          4206.21 |            0     | 1.86425  | 2.37879  |  2.67454  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredBool   |          4207.18 |            0     | 1.87142  | 2.37288  |  2.72388  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredString |          4144.44 |            0     | 1.87688  | 2.41154  |  2.68342  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Sparse         |         11862.1  |            0     | 0.657792 | 1.02921  |  1.24313  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_ByID           |         11417.1  |            0     | 0.682125 | 0.866875 |  0.928875 |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GraphRAG       |          4307.5  |            0     | 1.845    | 2.32587  |  2.69617  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GlobalGraphRAG |          4310.35 |            0     | 1.80663  | 2.31475  |  2.46637  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Recommend      |         10665.5  |            0     | 0.741042 | 0.905416 |  1.00558  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Geo            |          5441.61 |            0     | 1.381    | 2.09879  |  3.93825  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Temporal       |          6198.33 |            0     | 1.26442  | 1.82417  |  2.00875  |
| local  | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_LearnedIndex   |          3489.47 |            0     | 2.26433  | 3.01058  |  3.33175  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoPut                 |        209352    |          817.783 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoGet                 |        119768    |          467.844 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Dense          |          6688.51 |            0     | 1.12233  | 1.35154  |  2.53167  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Hybrid         |          7067.2  |            0     | 1.1255   | 1.34154  |  1.45317  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Filtered       |          6827.26 |            0     | 1.13275  | 1.38108  |  1.69579  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredBool   |          6672.77 |            0     | 1.1685   | 1.47437  |  1.83721  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredString |          6468.43 |            0     | 1.16467  | 1.46737  |  2.80542  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Sparse         |          9704.85 |            0     | 0.720292 | 1.62746  |  2.546    |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_ByID           |         12497.8  |            0     | 0.613459 | 0.800209 |  1.11017  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GraphRAG       |          6902.93 |            0     | 1.14325  | 1.34971  |  1.51596  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GlobalGraphRAG |          6802.57 |            0     | 1.14871  | 1.40996  |  1.80929  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Recommend      |         12079.3  |            0     | 0.643875 | 0.83075  |  0.918125 |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Geo            |          5155.36 |            0     | 1.32554  | 2.09971  | 12.7387   |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Temporal       |          6147.8  |            0     | 1.28908  | 1.82846  |  2.03121  |
| local  | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_LearnedIndex   |          4303.85 |            0     | 1.83942  | 2.58137  |  2.91525  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoPut                 |         48930.1  |         1146.8   | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoGet                 |         51510.7  |         1207.28  | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Dense          |          4041.72 |            0     | 1.86771  | 2.43037  |  5.4595   |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Hybrid         |          4257.26 |            0     | 1.86388  | 2.36912  |  2.61429  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Filtered       |          4097.1  |            0     | 1.9135   | 2.40942  |  2.76842  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredBool   |          3970.94 |            0     | 1.98992  | 2.52525  |  2.98404  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredString |          3619.16 |            0     | 2.06933  | 3.73679  |  5.46775  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Sparse         |         10890.5  |            0     | 0.72275  | 1.096    |  1.32729  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_ByID           |         12225.3  |            0     | 0.642042 | 0.836458 |  0.976792 |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GraphRAG       |          4266.35 |            0     | 1.86383  | 2.29921  |  2.54767  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GlobalGraphRAG |          4163.89 |            0     | 1.89088  | 2.46654  |  2.68721  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Recommend      |          9920.32 |            0     | 0.778625 | 1.01867  |  1.16346  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Geo            |          5303.63 |            0     | 1.33387  | 1.949    | 10.7855   |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Temporal       |          5985.88 |            0     | 1.32075  | 1.86504  |  2.11088  |
| local  | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_LearnedIndex   |          3415.74 |            0     | 2.309    | 3.04429  |  3.35992  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoPut                 |        437642    |          427.384 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoGet                 |        273526    |          267.115 | 0        | 0        |  0        |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Dense          |          6576.14 |            0     | 0.702792 | 2.33767  | 17.0246   |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Hybrid         |          8691.74 |            0     | 0.769    | 1.21304  |  5.48821  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Filtered       |          8807.41 |            0     | 0.711125 | 1.443    |  6.75933  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredBool   |         11041.2  |            0     | 0.697166 | 0.8185   |  1.05912  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredString |         11403.1  |            0     | 0.687667 | 0.822709 |  0.924    |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Sparse         |         12070.4  |            0     | 0.644    | 1.00438  |  1.25063  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_ByID           |         13174.1  |            0     | 0.592583 | 0.803958 |  0.922625 |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GraphRAG       |         11304.8  |            0     | 0.697667 | 0.815333 |  0.921    |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GlobalGraphRAG |         10605.8  |            0     | 0.715917 | 0.985125 |  1.60579  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Recommend      |         12613.9  |            0     | 0.618584 | 0.801084 |  0.896416 |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Geo            |          5649.14 |            0     | 1.33417  | 1.95158  |  3.51446  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Temporal       |          5796.75 |            0     | 1.35554  | 1.98804  |  2.35812  |
| local  | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_LearnedIndex   |          4490.07 |            0     | 1.76188  | 2.64017  |  3.02629  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoPut                 |        254658    |          746.069 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoGet                 |        237004    |          694.348 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Dense          |          7479.61 |            0     | 1.01942  | 1.19162  |  2.30037  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Hybrid         |          6940.13 |            0     | 1.03638  | 2.08125  |  2.85362  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Filtered       |          6152.13 |            0     | 1.04021  | 1.58092  | 13.1531   |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredBool   |          7832.37 |            0     | 1.00167  | 1.15442  |  1.3145   |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredString |          7798    |            0     | 1.01717  | 1.16225  |  1.28592  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Sparse         |         12163    |            0     | 0.64425  | 1.01492  |  1.15429  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_ByID           |         12914    |            0     | 0.6015   | 0.780125 |  0.841    |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GraphRAG       |          7732    |            0     | 1.02708  | 1.1925   |  1.30025  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GlobalGraphRAG |          7739.42 |            0     | 1.02546  | 1.17467  |  1.26738  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Recommend      |         12515.6  |            0     | 0.623333 | 0.793708 |  0.861625 |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Geo            |          5457.78 |            0     | 1.37204  | 2.14013  |  3.43138  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Temporal       |          6164.6  |            0     | 1.28146  | 1.79712  |  2.02279  |
| local  | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_LearnedIndex   |          4386.01 |            0     | 1.80754  | 2.56188  |  2.807    |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoPut                 |        512891    |          751.305 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoGet                 |        351796    |          515.326 | 0        | 0        |  0        |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Dense          |          8124.99 |            0     | 0.866125 | 1.59483  |  3.396    |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Hybrid         |          9019.05 |            0     | 0.861375 | 1.02571  |  1.23642  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Filtered       |          8121.87 |            0     | 0.872416 | 1.44254  |  4.12767  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredBool   |          9061.88 |            0     | 0.860625 | 1.00079  |  1.16242  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredString |          9108.45 |            0     | 0.870084 | 0.987584 |  1.09792  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Sparse         |         12236.9  |            0     | 0.63875  | 0.977458 |  1.14408  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_ByID           |         13142.1  |            0     | 0.596959 | 0.7725   |  0.844166 |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GraphRAG       |          9223.89 |            0     | 0.861625 | 0.980459 |  1.05542  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GlobalGraphRAG |          8160    |            0     | 0.865166 | 1.23342  |  1.971    |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Recommend      |         12480.8  |            0     | 0.628041 | 0.793    |  0.869125 |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Geo            |          5268.25 |            0     | 1.37575  | 2.06129  |  5.11604  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Temporal       |          6162.79 |            0     | 1.26579  | 1.81512  |  2.03404  |
| local  | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_LearnedIndex   |          4571.32 |            0     | 1.74221  | 2.54012  |  2.91904  |

### Details: perf (logs)

| Host   | Mode   | Dataset                           | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |    P99_ms |
|:-------|:-------|:----------------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|----------:|
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoPut                 |        806978    |          394.032 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoGet                 |        892758    |          435.917 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Dense          |          9988.55 |            0     | 0.710292 | 1.03254  |  2.31717  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Hybrid         |         10360.4  |            0     | 0.753791 | 1.06408  |  1.28933  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Filtered       |         10748.8  |            0     | 0.703042 | 0.842875 |  1.03667  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredBool   |         10883.2  |            0     | 0.709209 | 0.875875 |  1.20979  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredString |          9285.08 |            0     | 0.684416 | 1.29325  |  5.57496  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Sparse         |         12133.5  |            0     | 0.649042 | 1.03988  |  1.20258  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_ByID           |         13086.7  |            0     | 0.600084 | 0.765    |  0.845416 |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GraphRAG       |         11256.5  |            0     | 0.70225  | 0.838042 |  0.906125 |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GlobalGraphRAG |         11378.9  |            0     | 0.693875 | 0.833084 |  1.03838  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Recommend      |         13219.5  |            0     | 0.579375 | 0.78725  |  0.8785   |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Geo            |          5149.83 |            0     | 1.32483  | 2.17363  | 13.6505   |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Temporal       |          6052.57 |            0     | 1.31137  | 1.87333  |  2.08729  |
| perf   | logs   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_LearnedIndex   |          4447.77 |            0     | 1.78975  | 2.65521  |  2.97687  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoPut                 |        246528    |          722.25  | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoGet                 |        233615    |          684.418 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Dense          |          8679.97 |            0     | 0.864875 | 1.02396  |  2.01358  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Hybrid         |          9436.83 |            0     | 0.841125 | 0.991083 |  1.16904  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Filtered       |          7428.89 |            0     | 0.864958 | 1.02846  | 11.3649   |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredBool   |          9006.12 |            0     | 0.879209 | 1.00012  |  1.10254  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredString |          9102.5  |            0     | 0.864166 | 0.973458 |  1.63192  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Sparse         |         12290.6  |            0     | 0.642458 | 1.02167  |  1.18446  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_ByID           |         13421.4  |            0     | 0.588375 | 0.781334 |  0.898083 |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GraphRAG       |          8716.78 |            0     | 0.882583 | 1.12779  |  1.61196  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GlobalGraphRAG |          9085.4  |            0     | 0.87375  | 1.00562  |  1.08763  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Recommend      |         12105.3  |            0     | 0.644708 | 0.822292 |  0.910833 |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Geo            |          5529.51 |            0     | 1.37562  | 1.87008  |  2.31446  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Temporal       |          6072.47 |            0     | 1.28738  | 1.84917  |  2.11896  |
| perf   | logs   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_LearnedIndex   |          4354.99 |            0     | 1.73204  | 2.54804  |  3.29696  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoPut                 |        118372    |          924.781 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoGet                 |        117075    |          914.65  | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Dense          |          6729.07 |            0     | 1.13987  | 1.35888  |  2.02104  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Hybrid         |          6793.56 |            0     | 1.14296  | 1.43263  |  1.97758  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Filtered       |          6782.02 |            0     | 1.14142  | 1.318    |  1.45612  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredBool   |          6722.99 |            0     | 1.17362  | 1.431    |  1.65121  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredString |          6184.68 |            0     | 1.22158  | 1.46233  |  2.67217  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Sparse         |         11598.7  |            0     | 0.67425  | 1.02808  |  1.26525  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_ByID           |         13556.4  |            0     | 0.574917 | 0.766167 |  0.865666 |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GraphRAG       |          6703.12 |            0     | 1.18267  | 1.46412  |  1.62787  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GlobalGraphRAG |          6829.52 |            0     | 1.15212  | 1.38787  |  1.873    |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Recommend      |         11663.7  |            0     | 0.674208 | 0.833959 |  0.907791 |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Geo            |          5470.8  |            0     | 1.34987  | 1.83392  |  2.35358  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Temporal       |          5944.39 |            0     | 1.33979  | 1.891    |  2.08017  |
| perf   | logs   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_LearnedIndex   |          4314.39 |            0     | 1.84438  | 2.51162  |  2.81017  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoPut                 |        148886    |          872.381 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoGet                 |        116075    |          680.125 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Dense          |          7337.98 |            0     | 1.02479  | 1.239    |  2.31367  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Hybrid         |          7806.94 |            0     | 1.01767  | 1.18912  |  1.28767  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Filtered       |          6250.76 |            0     | 1.0245   | 1.55871  | 14.0167   |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredBool   |          7929.15 |            0     | 0.998875 | 1.14112  |  1.29192  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredString |          7921.19 |            0     | 0.999584 | 1.14346  |  1.28137  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Sparse         |         12296    |            0     | 0.62975  | 0.977834 |  1.2315   |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_ByID           |         13490.6  |            0     | 0.580666 | 0.763    |  0.867667 |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GraphRAG       |          7806.36 |            0     | 1.02212  | 1.15313  |  1.21883  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GlobalGraphRAG |          7758.74 |            0     | 1.02967  | 1.16237  |  1.25092  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Recommend      |         11853    |            0     | 0.646333 | 0.837375 |  1.22921  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Geo            |          4419.8  |            0     | 1.54433  | 3.46467  |  4.39262  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Temporal       |          5758.03 |            0     | 1.37433  | 1.97212  |  2.19983  |
| perf   | logs   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_LearnedIndex   |          4371.09 |            0     | 1.81471  | 2.57442  |  2.84333  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoPut                 |         87365.6  |         1023.82  | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoGet                 |         74504.5  |          873.1   | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Dense          |          3399.35 |            0     | 1.85263  | 2.73279  | 21.954    |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Hybrid         |          4291.72 |            0     | 1.85117  | 2.31129  |  2.48925  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Filtered       |          4206.21 |            0     | 1.86425  | 2.37879  |  2.67454  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredBool   |          4207.18 |            0     | 1.87142  | 2.37288  |  2.72388  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredString |          4144.44 |            0     | 1.87688  | 2.41154  |  2.68342  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Sparse         |         11862.1  |            0     | 0.657792 | 1.02921  |  1.24313  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_ByID           |         11417.1  |            0     | 0.682125 | 0.866875 |  0.928875 |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GraphRAG       |          4307.5  |            0     | 1.845    | 2.32587  |  2.69617  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GlobalGraphRAG |          4310.35 |            0     | 1.80663  | 2.31475  |  2.46637  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Recommend      |         10665.5  |            0     | 0.741042 | 0.905416 |  1.00558  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Geo            |          5441.61 |            0     | 1.381    | 2.09879  |  3.93825  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Temporal       |          6198.33 |            0     | 1.26442  | 1.82417  |  2.00875  |
| perf   | logs   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_LearnedIndex   |          3489.47 |            0     | 2.26433  | 3.01058  |  3.33175  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoPut                 |        209352    |          817.783 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoGet                 |        119768    |          467.844 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Dense          |          6688.51 |            0     | 1.12233  | 1.35154  |  2.53167  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Hybrid         |          7067.2  |            0     | 1.1255   | 1.34154  |  1.45317  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Filtered       |          6827.26 |            0     | 1.13275  | 1.38108  |  1.69579  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredBool   |          6672.77 |            0     | 1.1685   | 1.47437  |  1.83721  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredString |          6468.43 |            0     | 1.16467  | 1.46737  |  2.80542  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Sparse         |          9704.85 |            0     | 0.720292 | 1.62746  |  2.546    |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_ByID           |         12497.8  |            0     | 0.613459 | 0.800209 |  1.11017  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GraphRAG       |          6902.93 |            0     | 1.14325  | 1.34971  |  1.51596  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GlobalGraphRAG |          6802.57 |            0     | 1.14871  | 1.40996  |  1.80929  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Recommend      |         12079.3  |            0     | 0.643875 | 0.83075  |  0.918125 |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Geo            |          5155.36 |            0     | 1.32554  | 2.09971  | 12.7387   |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Temporal       |          6147.8  |            0     | 1.28908  | 1.82846  |  2.03121  |
| perf   | logs   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_LearnedIndex   |          4303.85 |            0     | 1.83942  | 2.58137  |  2.91525  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoPut                 |         48930.1  |         1146.8   | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoGet                 |         51510.7  |         1207.28  | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Dense          |          4041.72 |            0     | 1.86771  | 2.43037  |  5.4595   |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Hybrid         |          4257.26 |            0     | 1.86388  | 2.36912  |  2.61429  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Filtered       |          4097.1  |            0     | 1.9135   | 2.40942  |  2.76842  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredBool   |          3970.94 |            0     | 1.98992  | 2.52525  |  2.98404  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredString |          3619.16 |            0     | 2.06933  | 3.73679  |  5.46775  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Sparse         |         10890.5  |            0     | 0.72275  | 1.096    |  1.32729  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_ByID           |         12225.3  |            0     | 0.642042 | 0.836458 |  0.976792 |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GraphRAG       |          4266.35 |            0     | 1.86383  | 2.29921  |  2.54767  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GlobalGraphRAG |          4163.89 |            0     | 1.89088  | 2.46654  |  2.68721  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Recommend      |          9920.32 |            0     | 0.778625 | 1.01867  |  1.16346  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Geo            |          5303.63 |            0     | 1.33387  | 1.949    | 10.7855   |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Temporal       |          5985.88 |            0     | 1.32075  | 1.86504  |  2.11088  |
| perf   | logs   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_LearnedIndex   |          3415.74 |            0     | 2.309    | 3.04429  |  3.35992  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoPut                 |        437642    |          427.384 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoGet                 |        273526    |          267.115 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Dense          |          6576.14 |            0     | 0.702792 | 2.33767  | 17.0246   |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Hybrid         |          8691.74 |            0     | 0.769    | 1.21304  |  5.48821  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Filtered       |          8807.41 |            0     | 0.711125 | 1.443    |  6.75933  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredBool   |         11041.2  |            0     | 0.697166 | 0.8185   |  1.05912  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredString |         11403.1  |            0     | 0.687667 | 0.822709 |  0.924    |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Sparse         |         12070.4  |            0     | 0.644    | 1.00438  |  1.25063  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_ByID           |         13174.1  |            0     | 0.592583 | 0.803958 |  0.922625 |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GraphRAG       |         11304.8  |            0     | 0.697667 | 0.815333 |  0.921    |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GlobalGraphRAG |         10605.8  |            0     | 0.715917 | 0.985125 |  1.60579  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Recommend      |         12613.9  |            0     | 0.618584 | 0.801084 |  0.896416 |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Geo            |          5649.14 |            0     | 1.33417  | 1.95158  |  3.51446  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Temporal       |          5796.75 |            0     | 1.35554  | 1.98804  |  2.35812  |
| perf   | logs   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_LearnedIndex   |          4490.07 |            0     | 1.76188  | 2.64017  |  3.02629  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoPut                 |        254658    |          746.069 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoGet                 |        237004    |          694.348 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Dense          |          7479.61 |            0     | 1.01942  | 1.19162  |  2.30037  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Hybrid         |          6940.13 |            0     | 1.03638  | 2.08125  |  2.85362  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Filtered       |          6152.13 |            0     | 1.04021  | 1.58092  | 13.1531   |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredBool   |          7832.37 |            0     | 1.00167  | 1.15442  |  1.3145   |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredString |          7798    |            0     | 1.01717  | 1.16225  |  1.28592  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Sparse         |         12163    |            0     | 0.64425  | 1.01492  |  1.15429  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_ByID           |         12914    |            0     | 0.6015   | 0.780125 |  0.841    |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GraphRAG       |          7732    |            0     | 1.02708  | 1.1925   |  1.30025  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GlobalGraphRAG |          7739.42 |            0     | 1.02546  | 1.17467  |  1.26738  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Recommend      |         12515.6  |            0     | 0.623333 | 0.793708 |  0.861625 |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Geo            |          5457.78 |            0     | 1.37204  | 2.14013  |  3.43138  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Temporal       |          6164.6  |            0     | 1.28146  | 1.79712  |  2.02279  |
| perf   | logs   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_LearnedIndex   |          4386.01 |            0     | 1.80754  | 2.56188  |  2.807    |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoPut                 |        512891    |          751.305 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoGet                 |        351796    |          515.326 | 0        | 0        |  0        |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Dense          |          8124.99 |            0     | 0.866125 | 1.59483  |  3.396    |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Hybrid         |          9019.05 |            0     | 0.861375 | 1.02571  |  1.23642  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Filtered       |          8121.87 |            0     | 0.872416 | 1.44254  |  4.12767  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredBool   |          9061.88 |            0     | 0.860625 | 1.00079  |  1.16242  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredString |          9108.45 |            0     | 0.870084 | 0.987584 |  1.09792  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Sparse         |         12236.9  |            0     | 0.63875  | 0.977458 |  1.14408  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_ByID           |         13142.1  |            0     | 0.596959 | 0.7725   |  0.844166 |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GraphRAG       |          9223.89 |            0     | 0.861625 | 0.980459 |  1.05542  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GlobalGraphRAG |          8160    |            0     | 0.865166 | 1.23342  |  1.971    |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Recommend      |         12480.8  |            0     | 0.628041 | 0.793    |  0.869125 |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Geo            |          5268.25 |            0     | 1.37575  | 2.06129  |  5.11604  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Temporal       |          6162.79 |            0     | 1.26579  | 1.81512  |  2.03404  |
| perf   | logs   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_LearnedIndex   |          4571.32 |            0     | 1.74221  | 2.54012  |  2.91904  |

### Details: remote (host)

| Host   | Mode   | Dataset                           | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:----------------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|---------:|
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoPut                 |       324309     |          158.354 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | DoGet                 |       509166     |          248.616 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Dense          |         4985.78  |            0     |  1.51557 |  2.02317 |  2.92877 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Hybrid         |         2697.38  |            0     |  1.43625 | 16.3795  | 35.3737  |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Filtered       |         5141.01  |            0     |  1.51688 |  1.83933 |  2.53571 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredBool   |         5482.43  |            0     |  1.46052 |  1.68868 |  1.85254 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_FilteredString |         5598.69  |            0     |  1.31347 |  2.02858 |  2.52982 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Sparse         |         7243.1   |            0     |  1.1055  |  1.4223  |  1.57753 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_ByID           |         6556.92  |            0     |  1.20473 |  1.55825 |  1.89392 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GraphRAG       |         5369.49  |            0     |  1.48566 |  1.7093  |  1.79646 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_GlobalGraphRAG |         5298.12  |            0     |  1.49918 |  1.75519 |  1.90821 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Recommend      |         6431.83  |            0     |  1.2399  |  1.50859 |  1.59329 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Geo            |         1868.17  |            0     |  2.22703 | 16.8203  | 35.955   |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_Temporal       |         2768.59  |            0     |  2.65596 |  4.75737 |  6.74053 |
| remote | host   | result_cpu_float32_128_5000.json  | float32 |   128 |    5000 | Search_LearnedIndex   |         2721.47  |            0     |  2.74158 |  4.45043 |  7.60841 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoPut                 |       110593     |          324.004 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | DoGet                 |       122320     |          358.36  |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Dense          |         5017.2   |            0     |  1.55551 |  1.81114 |  2.61476 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Hybrid         |         3349.16  |            0     |  1.53005 |  2.24024 | 36.5287  |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Filtered       |         5048.54  |            0     |  1.55941 |  1.83076 |  2.06919 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredBool   |         4594.46  |            0     |  1.69826 |  2.16131 |  2.37177 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_FilteredString |         5445.6   |            0     |  1.46498 |  1.63163 |  1.69995 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Sparse         |         7326.59  |            0     |  1.08245 |  1.47004 |  1.67362 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_ByID           |         6882.9   |            0     |  1.14823 |  1.43285 |  1.58574 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GraphRAG       |         4833.14  |            0     |  1.62755 |  1.96393 |  2.312   |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_GlobalGraphRAG |         4898.06  |            0     |  1.62517 |  1.87287 |  1.97653 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Recommend      |         6173.97  |            0     |  1.29601 |  1.53296 |  1.64585 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Geo            |         1076.23  |            0     |  5.14557 | 21.1197  | 38.3949  |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_Temporal       |         2574.22  |            0     |  2.88835 |  5.443   |  7.22834 |
| remote | host   | result_cpu_float64_384_5000.json  | float64 |   384 |    5000 | Search_LearnedIndex   |         2588.41  |            0     |  2.88437 |  4.68744 |  6.00147 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoPut                 |        40993.1   |          320.259 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | DoGet                 |        58313.4   |          455.574 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Dense          |         3760.48  |            0     |  2.02101 |  2.43427 |  2.98701 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Hybrid         |         4000.35  |            0     |  1.99438 |  2.31116 |  2.46832 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Filtered       |         3856.99  |            0     |  2.05227 |  2.39754 |  2.73794 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredBool   |         3770.95  |            0     |  2.02459 |  2.72373 |  3.48953 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_FilteredString |         3009.85  |            0     |  2.50938 |  3.71025 |  5.02455 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Sparse         |         5168.34  |            0     |  1.44922 |  2.55264 |  3.35063 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_ByID           |         5005.18  |            0     |  1.47809 |  2.62671 |  3.50811 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GraphRAG       |         2321.91  |            0     |  2.93419 |  4.49709 | 10.2206  |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_GlobalGraphRAG |         2672.03  |            0     |  2.83298 |  4.42093 |  6.2587  |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Recommend      |         4512.67  |            0     |  1.6982  |  2.50375 |  3.13041 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Geo            |         1291.33  |            0     |  4.60574 | 16.3162  | 47.7821  |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_Temporal       |         2597.91  |            0     |  2.91075 |  4.70639 |  7.24188 |
| remote | host   | result_cpu_float64_1024_5000.json | float64 |  1024 |    5000 | Search_LearnedIndex   |         2632.9   |            0     |  2.92346 |  4.37825 |  5.03352 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoPut                 |        61216.4   |          358.69  |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | DoGet                 |        55979.1   |          328.002 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Dense          |         2709.14  |            0     |  1.83777 | 11.7776  | 37.2729  |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Hybrid         |         4167.91  |            0     |  1.90584 |  2.27843 |  2.5276  |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Filtered       |         4070.76  |            0     |  1.94914 |  2.24804 |  2.52441 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredBool   |         4218.49  |            0     |  1.87197 |  2.30355 |  2.6277  |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_FilteredString |         4158.42  |            0     |  1.89067 |  2.30243 |  2.83349 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Sparse         |         5959.21  |            0     |  1.25917 |  2.18491 |  2.95176 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_ByID           |         4589.08  |            0     |  1.49676 |  3.47299 |  4.38813 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GraphRAG       |         3080.74  |            0     |  2.47485 |  3.59868 |  4.79894 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_GlobalGraphRAG |         3194.05  |            0     |  2.33972 |  3.3119  |  4.93088 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Recommend      |         4990.66  |            0     |  1.50566 |  2.41054 |  4.02109 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Geo            |         1547.03  |            0     |  4.45732 |  8.43959 | 20.024   |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_Temporal       |         2960.44  |            0     |  2.61597 |  3.97375 |  4.78208 |
| remote | host   | result_cpu_float64_768_5000.json  | float64 |   768 |    5000 | Search_LearnedIndex   |         2494.18  |            0     |  3.06117 |  4.56078 |  5.98412 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoPut                 |        28033     |          328.512 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | DoGet                 |        36271.1   |          425.053 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Dense          |         2624.17  |            0     |  2.93482 |  3.79943 |  5.87161 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Hybrid         |         2156.24  |            0     |  3.55396 |  5.64715 |  8.11509 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Filtered       |         1801.16  |            0     |  4.1988  |  6.7029  |  9.24917 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredBool   |         2113.1   |            0     |  3.70131 |  5.44486 |  6.33744 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_FilteredString |         2035.9   |            0     |  3.7368  |  5.57466 |  8.11111 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Sparse         |         5812.92  |            0     |  1.30731 |  2.27297 |  2.86718 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_ByID           |         5110.47  |            0     |  1.54731 |  2.09302 |  2.46366 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GraphRAG       |         1883.74  |            0     |  3.69035 |  6.75761 |  9.34259 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_GlobalGraphRAG |         2398.81  |            0     |  3.30103 |  4.81416 |  5.58293 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Recommend      |         5117.3   |            0     |  1.47925 |  2.06902 |  4.04375 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Geo            |         1643.3   |            0     |  2.85615 |  8.85452 | 42.6427  |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_Temporal       |         3582.5   |            0     |  2.21608 |  3.02454 |  3.35428 |
| remote | host   | result_cpu_float32_3072_5000.json | float32 |  3072 |    5000 | Search_LearnedIndex   |         2755.85  |            0     |  2.73132 |  4.44621 |  5.83148 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoPut                 |        86335.5   |          337.248 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | DoGet                 |        88494.6   |          345.682 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Dense          |         3813     |            0     |  2.06217 |  2.39041 |  2.78749 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Hybrid         |         3868.59  |            0     |  2.03552 |  2.5528  |  2.89946 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Filtered       |         3595.66  |            0     |  2.19536 |  2.57778 |  2.94103 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredBool   |         3853.53  |            0     |  2.06662 |  2.40843 |  3.00125 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_FilteredString |         3626.89  |            0     |  2.1232  |  2.47322 |  2.62185 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Sparse         |         6481.1   |            0     |  1.22079 |  1.58296 |  2.10583 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_ByID           |         6248.12  |            0     |  1.22828 |  1.73999 |  3.82171 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GraphRAG       |         2718.59  |            0     |  2.18641 |  3.7832  | 44.1453  |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_GlobalGraphRAG |         2898.1   |            0     |  2.63173 |  3.99557 |  4.88012 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Recommend      |         4938.09  |            0     |  1.5894  |  2.23797 |  2.93142 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Geo            |          920.001 |            0     |  5.34621 | 24.7381  | 55.5691  |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_Temporal       |         2916.02  |            0     |  2.65032 |  4.02661 |  4.90489 |
| remote | host   | result_cpu_float32_1024_5000.json | float32 |  1024 |    5000 | Search_LearnedIndex   |         2467.2   |            0     |  3.12871 |  4.69109 |  6.04036 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoPut                 |        14648.8   |          343.331 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | DoGet                 |        17918.6   |          419.966 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Dense          |         2657.6   |            0     |  2.82987 |  3.67183 |  4.72389 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Hybrid         |         3278.83  |            0     |  2.3746  |  3.28881 |  3.70069 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Filtered       |         2834.27  |            0     |  2.84694 |  3.61842 |  4.10558 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredBool   |         2846.64  |            0     |  2.85736 |  3.68822 |  4.07692 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_FilteredString |         2926.36  |            0     |  2.75004 |  3.53155 |  3.88778 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Sparse         |         6825.5   |            0     |  1.17938 |  1.57267 |  1.79636 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_ByID           |         6711.73  |            0     |  1.18475 |  1.46284 |  1.58626 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GraphRAG       |         2407.17  |            0     |  3.02622 |  3.9904  |  5.51023 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_GlobalGraphRAG |         1992.88  |            0     |  3.94519 |  5.51227 |  7.03155 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Recommend      |         3961.53  |            0     |  1.9266  |  2.89857 |  3.38114 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Geo            |         1531.08  |            0     |  4.81304 |  7.97179 | 10.5482  |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_Temporal       |         2757.51  |            0     |  2.84554 |  4.09293 |  4.52767 |
| remote | host   | result_cpu_float64_3072_5000.json | float64 |  3072 |    5000 | Search_LearnedIndex   |         1765.96  |            0     |  4.39307 |  6.60888 |  8.16787 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoPut                 |       284581     |          277.911 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | DoGet                 |       291261     |          284.434 |  0       |  0       |  0       |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Dense          |         4936.2   |            0     |  1.44496 |  2.35284 |  3.06171 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Hybrid         |         3434.71  |            0     |  1.46883 |  2.10548 | 33.6891  |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Filtered       |         5384.94  |            0     |  1.4784  |  1.71542 |  1.87768 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredBool   |         5372.72  |            0     |  1.46473 |  1.77192 |  2.13939 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_FilteredString |         5490.63  |            0     |  1.44979 |  1.6704  |  1.75393 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Sparse         |         7062.5   |            0     |  1.13344 |  1.47927 |  1.63257 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_ByID           |         6789.13  |            0     |  1.17004 |  1.45599 |  1.62619 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GraphRAG       |         4386.17  |            0     |  1.54243 |  2.27837 |  4.66667 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_GlobalGraphRAG |         3963.48  |            0     |  1.82543 |  3.28504 |  4.7299  |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Recommend      |         3764.31  |            0     |  1.65924 |  3.28734 | 10.3059  |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Geo            |          673.85  |            0     |  7.55414 | 30.2251  | 60.3637  |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_Temporal       |         2707.03  |            0     |  2.80296 |  4.64352 |  5.87497 |
| remote | host   | result_cpu_float64_128_5000.json  | float64 |   128 |    5000 | Search_LearnedIndex   |         2764.52  |            0     |  2.5938  |  4.17883 | 11.8864  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoPut                 |       116832     |          342.28  |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | DoGet                 |       113969     |          333.892 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Dense          |         4422.35  |            0     |  1.7797  |  2.10226 |  3.03798 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Hybrid         |         3865.95  |            0     |  1.99296 |  2.74943 |  2.96266 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Filtered       |         3971.57  |            0     |  1.9793  |  2.32116 |  2.58432 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredBool   |         3831.64  |            0     |  2.07428 |  2.39468 |  2.6157  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_FilteredString |         3812.19  |            0     |  1.99644 |  2.3537  |  3.15943 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Sparse         |         6304.67  |            0     |  1.25169 |  1.65361 |  2.05929 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_ByID           |         6343.21  |            0     |  1.24932 |  1.53262 |  1.67581 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GraphRAG       |         3093.59  |            0     |  2.03906 |  3.46858 | 10.2544  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_GlobalGraphRAG |         3066.11  |            0     |  2.51232 |  3.58877 |  4.76351 |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Recommend      |         3619.56  |            0     |  1.64794 |  3.10487 | 16.4649  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Geo            |         1565.1   |            0     |  4.54572 |  8.8054  | 16.3978  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_Temporal       |         2984.79  |            0     |  2.59938 |  3.86926 |  4.3907  |
| remote | host   | result_cpu_float32_768_5000.json  | float32 |   768 |    5000 | Search_LearnedIndex   |         2328.5   |            0     |  3.0721  |  4.75464 |  8.04275 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoPut                 |       215220     |          315.264 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | DoGet                 |       172354     |          252.472 |  0       |  0       |  0       |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Dense          |         4739.35  |            0     |  1.57656 |  2.05858 |  4.06624 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Hybrid         |         3582.97  |            0     |  1.58534 |  2.03009 | 36.9401  |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Filtered       |         4786.55  |            0     |  1.60882 |  2.07211 |  4.05654 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredBool   |         4691.63  |            0     |  1.67969 |  2.07107 |  2.38287 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_FilteredString |         5218.49  |            0     |  1.51377 |  1.73675 |  2.25634 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Sparse         |         5348.88  |            0     |  1.36334 |  2.58828 |  4.07519 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_ByID           |         4650.58  |            0     |  1.61584 |  2.70917 |  3.42215 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GraphRAG       |         3524.35  |            0     |  2.1445  |  3.2105  |  4.60134 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_GlobalGraphRAG |         3511.46  |            0     |  2.11797 |  3.37203 |  4.78593 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Recommend      |         4911.35  |            0     |  1.55092 |  2.42209 |  3.48152 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Geo            |         1677.5   |            0     |  4.44494 |  7.64019 | 10.4141  |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_Temporal       |         2760.5   |            0     |  2.74734 |  4.45632 |  5.44006 |
| remote | host   | result_cpu_float32_384_5000.json  | float32 |   384 |    5000 | Search_LearnedIndex   |         2293.9   |            0     |  2.98193 |  4.56787 | 20.5175  |

