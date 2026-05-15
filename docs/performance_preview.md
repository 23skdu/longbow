# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                   |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32')  |       13086.7 |        9988.55 |          10748.8  |              10883.2  |                 9285.08 |      5149.83 |                11378.9  |          11256.5  |        10360.5  |               4447.77 |            13219.5 |        12133.5  |           6052.57 |
| ('perf', 'logs', 128, 'float64')  |       13174   |        6576.14 |           8807.41 |              11041.2  |                11403.1  |      5649.14 |                10605.8  |          11304.8  |         8691.74 |               4490.07 |            12613.9 |        12070.4  |           5796.75 |
| ('perf', 'logs', 384, 'float32')  |       13142.1 |        8124.99 |           8121.87 |               9061.88 |                 9108.45 |      5268.25 |                 8160    |           9223.89 |         9019.05 |               4571.32 |            12480.8 |        12236.9  |           6162.79 |
| ('perf', 'logs', 384, 'float64')  |       13421.4 |        8679.97 |           7428.89 |               9006.12 |                 9102.5  |      5529.51 |                 9085.4  |           8716.78 |         9436.83 |               4354.99 |            12105.3 |        12290.6  |           6072.47 |
| ('perf', 'logs', 768, 'float32')  |       12914   |        7479.61 |           6152.13 |               7832.37 |                 7798    |      5457.78 |                 7739.42 |           7732    |         6940.13 |               4386.01 |            12515.6 |        12163    |           6164.6  |
| ('perf', 'logs', 768, 'float64')  |       13490.6 |        7337.98 |           6250.76 |               7929.15 |                 7921.19 |      4419.8  |                 7758.74 |           7806.36 |         7806.94 |               4371.09 |            11853   |        12296    |           5758.03 |
| ('perf', 'logs', 1024, 'float32') |       12497.8 |        6688.51 |           6827.26 |               6672.77 |                 6468.43 |      5155.36 |                 6802.57 |           6902.93 |         7067.2  |               4303.85 |            12079.3 |         9704.85 |           6147.8  |
| ('perf', 'logs', 3072, 'float32') |       11417.1 |        3399.35 |           4206.21 |               4207.18 |                 4144.44 |      5441.61 |                 4310.35 |           4307.5  |         4291.72 |               3489.47 |            10665.5 |        11862.1  |           6198.33 |

## Ingestion Performance (MB/s)

|                                   |   Throughput_MBs |
|:----------------------------------|-----------------:|
| ('perf', 'logs', 128, 'float32')  |           394.03 |
| ('perf', 'logs', 128, 'float64')  |           427.38 |
| ('perf', 'logs', 384, 'float32')  |           751.3  |
| ('perf', 'logs', 384, 'float64')  |           722.25 |
| ('perf', 'logs', 768, 'float32')  |           746.07 |
| ('perf', 'logs', 768, 'float64')  |           872.38 |
| ('perf', 'logs', 1024, 'float32') |           817.78 |
| ('perf', 'logs', 3072, 'float32') |          1023.82 |

## Search Latency Summary (P95 ms)

|                                   |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('perf', 'logs', 128, 'float32')  |          0.76 |           1.03 |              0.84 |                  0.88 |                    1.29 |         2.17 |                    0.83 |              0.84 |            1.06 |                  2.66 |               0.79 |            1.04 |              1.87 |
| ('perf', 'logs', 128, 'float64')  |          0.8  |           2.34 |              1.44 |                  0.82 |                    0.82 |         1.95 |                    0.99 |              0.82 |            1.21 |                  2.64 |               0.8  |            1    |              1.99 |
| ('perf', 'logs', 384, 'float32')  |          0.77 |           1.59 |              1.44 |                  1    |                    0.99 |         2.06 |                    1.23 |              0.98 |            1.03 |                  2.54 |               0.79 |            0.98 |              1.82 |
| ('perf', 'logs', 384, 'float64')  |          0.78 |           1.02 |              1.03 |                  1    |                    0.97 |         1.87 |                    1.01 |              1.13 |            0.99 |                  2.55 |               0.82 |            1.02 |              1.85 |
| ('perf', 'logs', 768, 'float32')  |          0.78 |           1.19 |              1.58 |                  1.15 |                    1.16 |         2.14 |                    1.17 |              1.19 |            2.08 |                  2.56 |               0.79 |            1.01 |              1.8  |
| ('perf', 'logs', 768, 'float64')  |          0.76 |           1.24 |              1.56 |                  1.14 |                    1.14 |         3.46 |                    1.16 |              1.15 |            1.19 |                  2.57 |               0.84 |            0.98 |              1.97 |
| ('perf', 'logs', 1024, 'float32') |          0.8  |           1.35 |              1.38 |                  1.47 |                    1.47 |         2.1  |                    1.41 |              1.35 |            1.34 |                  2.58 |               0.83 |            1.63 |              1.83 |
| ('perf', 'logs', 3072, 'float32') |          0.87 |           2.73 |              2.38 |                  2.37 |                    2.41 |         2.1  |                    2.31 |              2.33 |            2.31 |                  3.01 |               0.91 |            1.03 |              1.82 |

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

