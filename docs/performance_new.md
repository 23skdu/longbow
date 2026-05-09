# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                        |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('bahamut', 'cpu', 128, 'complex128')  |       3043.32 |        1969.36 |           1986.68 |               1857.24 |                 1883.77 |     13035.4  |                 1953.26 |           1981.82 |         8161.06 |               1938.6  |           10028.4  |         32255.3 |            253.8  |
| ('bahamut', 'cpu', 128, 'complex64')   |      11340    |        1993.35 |           2006.65 |               1908.07 |                 1918.57 |     11667    |                 1997.51 |           2006.3  |         8455.9  |               1990.11 |           10495.1  |         32224.5 |            276.47 |
| ('bahamut', 'cpu', 128, 'float16')     |      27551.9  |        4883.9  |           4036.32 |               3938.09 |                 3980.1  |     11542.6  |                 4671.55 |           4462.29 |        19962    |               3322.43 |           24715.1  |         32361.5 |            892.33 |
| ('bahamut', 'cpu', 128, 'float32')     |      15684.5  |       11929    |          13064.3  |              10197.6  |                 8863.95 |      9216.09 |                13428.9  |          13493.8  |        12333.6  |               7038.11 |           15017.8  |         37584.2 |           2176.25 |
| ('bahamut', 'cpu', 128, 'float64')     |      15191.9  |        5865.13 |           5151.97 |               4825.42 |                 4533.48 |     12107.3  |                 4812.65 |           5328.09 |         9203.8  |               4319.32 |           13472.2  |         31777.9 |           1087.97 |
| ('bahamut', 'cpu', 128, 'int16')       |      13954.2  |         964.39 |            786.78 |                734.57 |                  827.62 |     13073.5  |                  817.06 |            819.63 |          895.16 |                807.5  |             981.08 |         30607.9 |            566.51 |
| ('bahamut', 'cpu', 128, 'int32')       |      14013    |        1643.85 |           1524.74 |               1296.84 |                 1251.2  |     13101.2  |                 1396.46 |           1497.97 |         2891.35 |               1617.28 |            2986.78 |         33309.5 |            482.59 |
| ('bahamut', 'cpu', 128, 'int64')       |      13405.7  |         847.42 |            784.21 |                738.85 |                  824.27 |     12510.2  |                  793.56 |            795.96 |          748.24 |                797.97 |             903.85 |         30799.6 |            411.15 |
| ('bahamut', 'cpu', 128, 'int8')        |      14875    |        3098.51 |           2766.68 |               2459.07 |                 2423.3  |     13767.5  |                 2411.31 |           2477.85 |         8711.49 |               2420.28 |            9581.6  |         31626.8 |            654.45 |
| ('bahamut', 'cpu', 128, 'turboquant2') |      39370.6  |        2046.76 |           2066.75 |               2053.61 |                 2066.72 |     11881.9  |                 2056.41 |           2072.32 |        23896.3  |               2047.17 |           32135.7  |         31490.8 |            236.18 |
| ('bahamut', 'cpu', 128, 'turboquant4') |      41421.8  |        2068.21 |           2061.08 |               2052.36 |                 2045.94 |     12852.6  |                 2064.95 |           2058.98 |        24650.3  |               2053.81 |           31956    |         32146   |            220.13 |
| ('bahamut', 'cpu', 128, 'turboquant8') |      38618    |        2045.27 |           2058.72 |               2070.84 |                 2062.28 |     12974.4  |                 2053.75 |           2062.31 |        23968.3  |               2066.45 |           33067.2  |         32259.6 |            208.09 |
| ('bahamut', 'cpu', 128, 'uint16')      |      13095.5  |         876.39 |            728.46 |                702.05 |                  769.45 |     11618    |                  733.8  |            745.42 |          937.8  |                725.68 |             884.92 |         30081.3 |            332.83 |
| ('bahamut', 'cpu', 128, 'uint32')      |      14362.2  |         812.54 |            629.72 |                672.49 |                  786.21 |     12961.2  |                  757.49 |            780.26 |          814.48 |                705.36 |             899.08 |         32711.1 |            312.96 |
| ('bahamut', 'cpu', 128, 'uint64')      |      15880.4  |         867.88 |            770.75 |                693.06 |                  806.66 |     12870.2  |                  825.86 |            799.37 |          907.27 |                816.72 |             976.05 |         31351.1 |            296.6  |
| ('bahamut', 'cpu', 128, 'uint8')       |      36031.3  |        2015.62 |           2045.17 |               2021.41 |                 2059.84 |     13480.7  |                 2053.76 |           2048.14 |        20507.7  |               2070.06 |           26141.7  |         33222.4 |            319.43 |

## Ingestion Performance (MB/s)

|                                        |   Throughput_MBs |
|:---------------------------------------|-----------------:|
| ('bahamut', 'cpu', 128, 'complex128')  |           718.73 |
| ('bahamut', 'cpu', 128, 'complex64')   |           646.55 |
| ('bahamut', 'cpu', 128, 'float16')     |           522.47 |
| ('bahamut', 'cpu', 128, 'float32')     |           605.82 |
| ('bahamut', 'cpu', 128, 'float64')     |           584.19 |
| ('bahamut', 'cpu', 128, 'int16')       |           521.18 |
| ('bahamut', 'cpu', 128, 'int32')       |           525.56 |
| ('bahamut', 'cpu', 128, 'int64')       |           947.98 |
| ('bahamut', 'cpu', 128, 'int8')        |           333.49 |
| ('bahamut', 'cpu', 128, 'turboquant2') |            33.52 |
| ('bahamut', 'cpu', 128, 'turboquant4') |            97.9  |
| ('bahamut', 'cpu', 128, 'turboquant8') |           152.86 |
| ('bahamut', 'cpu', 128, 'uint16')      |           486.22 |
| ('bahamut', 'cpu', 128, 'uint32')      |           484.89 |
| ('bahamut', 'cpu', 128, 'uint64')      |           701.95 |
| ('bahamut', 'cpu', 128, 'uint8')       |           334.16 |

### Details: bahamut (cpu)

| Host    | Mode   | Dataset                         | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |    P99_ms |
|:--------|:-------|:--------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|----------:|
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoPut                 | 662065           |         646.548  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoGet                 | 793231           |         774.64   |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Dense          |   1993.35        |           0      |  2.01558  |  2.34092  |  2.42988  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Hybrid         |   8455.9         |           0      |  0.465708 |  0.5175   |  0.576417 |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Filtered       |   2006.65        |           0      |  1.98325  |  2.31637  |  2.42671  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredBool   |   1908.07        |           0      |  2.10654  |  2.48717  |  2.58221  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredString |   1918.57        |           0      |  2.10312  |  2.50042  |  2.65921  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Sparse         |  32224.5         |           0      |  0.117583 |  0.169042 |  0.241958 |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_ByID           |  11340           |           0      |  0.343875 |  0.402583 |  0.49625  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GraphRAG       |   2006.3         |           0      |  2.02546  |  2.35283  |  2.42125  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GlobalGraphRAG |   1997.51        |           0      |  2.00367  |  2.33308  |  2.45725  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Recommend      |  10495.1         |           0      |  0.375042 |  0.437208 |  0.496333 |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Geo            |  11667           |           0      |  0.336167 |  0.425709 |  0.490125 |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Temporal       |    276.469       |           0      | 14.2238   | 16.7292   | 23.8098   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_LearnedIndex   |   1990.11        |           0      |  2.02242  |  2.32462  |  2.44433  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoPut                 | 993049           |         484.887  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoGet                 |      1.74912e+06 |         854.062  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Dense          |    812.539       |           0      |  4.82587  |  5.67425  |  6.18746  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Hybrid         |    814.477       |           0      |  4.8355   |  5.41992  |  6.16308  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Filtered       |    629.72        |           0      |  5.8215   |  8.02637  | 19.1666   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredBool   |    672.491       |           0      |  5.82929  |  6.71904  |  8.39233  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredString |    786.214       |           0      |  5.07396  |  5.62062  |  6.11925  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Sparse         |  32711.1         |           0      |  0.117083 |  0.171375 |  0.210125 |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_ByID           |  14362.2         |           0      |  0.269167 |  0.32125  |  0.41425  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GraphRAG       |    780.26        |           0      |  5.08038  |  5.77329  |  6.18883  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GlobalGraphRAG |    757.487       |           0      |  5.08946  |  6.38842  |  8.43238  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Recommend      |    899.082       |           0      |  4.43596  |  4.56313  |  4.70246  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Geo            |  12961.2         |           0      |  0.3015   |  0.382833 |  0.477916 |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Temporal       |    312.96        |           0      | 12.2595   | 15.9285   | 26.6773   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_LearnedIndex   |    705.358       |           0      |  5.35317  |  6.45008  | 13.4458   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoPut                 |      1.07635e+06 |         525.561  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoGet                 |      1.70285e+06 |         831.471  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Dense          |   1643.85        |           0      |  2.33504  |  3.02071  |  3.21592  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Hybrid         |   2891.35        |           0      |  1.37379  |  1.4665   |  1.52767  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Filtered       |   1524.74        |           0      |  2.65671  |  3.20458  |  3.2905   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredBool   |   1296.84        |           0      |  2.93771  |  4.26317  |  5.97179  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredString |   1251.2         |           0      |  2.78804  |  6.54896  | 13.2718   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Sparse         |  33309.5         |           0      |  0.113916 |  0.165417 |  0.229166 |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_ByID           |  14013           |           0      |  0.277709 |  0.330917 |  0.392708 |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GraphRAG       |   1497.97        |           0      |  2.72025  |  3.14646  |  3.36296  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GlobalGraphRAG |   1396.46        |           0      |  2.81708  |  3.81796  |  4.828    |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Recommend      |   2986.78        |           0      |  1.33413  |  1.40475  |  1.4535   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Geo            |  13101.2         |           0      |  0.29825  |  0.378833 |  0.462458 |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Temporal       |    482.594       |           0      |  8.27917  |  9.27629  | 10.3675   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_LearnedIndex   |   1617.28        |           0      |  2.44821  |  3.11763  |  3.29575  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoPut                 |      1.24072e+06 |         605.82   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoGet                 |      1.24926e+06 |         609.989  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Dense          |  11929           |           0      |  0.308583 |  0.355666 |  0.458042 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Hybrid         |  12333.6         |           0      |  0.316542 |  0.379542 |  0.437    |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Filtered       |  13064.3         |           0      |  0.293708 |  0.343417 |  0.409667 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredBool   |  10197.6         |           0      |  0.384875 |  0.435333 |  0.517667 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredString |   8863.95        |           0      |  0.443292 |  0.499583 |  0.593125 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Sparse         |  37584.2         |           0      |  0.097959 |  0.152375 |  0.263959 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_ByID           |  15684.5         |           0      |  0.248125 |  0.300125 |  0.382792 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GraphRAG       |  13493.8         |           0      |  0.288333 |  0.347167 |  0.412833 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GlobalGraphRAG |  13428.9         |           0      |  0.289458 |  0.345459 |  0.427834 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Recommend      |  15017.7         |           0      |  0.258708 |  0.308709 |  0.357833 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Geo            |   9216.09        |           0      |  0.395125 |  0.691916 |  0.782083 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Temporal       |   2176.25        |           0      |  1.76687  |  2.51554  |  3.09754  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_LearnedIndex   |   7038.11        |           0      |  0.563958 |  0.698292 |  0.777833 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoPut                 |      2.14003e+06 |         522.468  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoGet                 |      2.14896e+06 |         524.648  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Dense          |   4883.9         |           0      |  0.819375 |  0.953333 |  1.01129  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Hybrid         |  19962           |           0      |  0.194708 |  0.251042 |  0.342084 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Filtered       |   4036.32        |           0      |  0.976792 |  1.15138  |  1.18858  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredBool   |   3938.09        |           0      |  1.03337  |  1.19962  |  1.24054  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredString |   3980.1         |           0      |  1.03242  |  1.18475  |  1.25146  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Sparse         |  32361.5         |           0      |  0.118083 |  0.172667 |  0.225875 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_ByID           |  27551.9         |           0      |  0.138959 |  0.190708 |  0.256833 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GraphRAG       |   4462.29        |           0      |  1.09633  |  1.30233  |  1.35013  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GlobalGraphRAG |   4671.55        |           0      |  1.01696  |  1.32058  |  1.60417  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Recommend      |  24715.1         |           0      |  0.153292 |  0.215667 |  0.332084 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Geo            |  11542.6         |           0      |  0.317541 |  0.461542 |  0.564916 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Temporal       |    892.331       |           0      |  4.40417  |  5.194    |  6.35546  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_LearnedIndex   |   3322.43        |           0      |  1.22817  |  1.40367  |  1.46742  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoPut                 |      2.73193e+06 |         333.488  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoGet                 |      2.89324e+06 |         353.179  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Dense          |   3098.51        |           0      |  1.29283  |  1.57358  |  1.73362  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Hybrid         |   8711.49        |           0      |  0.451125 |  0.537458 |  0.585459 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Filtered       |   2766.68        |           0      |  1.44983  |  1.67283  |  1.80171  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredBool   |   2459.07        |           0      |  1.61983  |  1.96512  |  3.25471  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredString |   2423.3         |           0      |  1.66796  |  1.9805   |  2.09258  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Sparse         |  31626.8         |           0      |  0.116291 |  0.174291 |  0.260042 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_ByID           |  14875           |           0      |  0.260583 |  0.320375 |  0.392792 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GraphRAG       |   2477.85        |           0      |  1.62271  |  1.87521  |  1.95325  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GlobalGraphRAG |   2411.31        |           0      |  1.64825  |  1.99233  |  2.08562  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Recommend      |   9581.6         |           0      |  0.41475  |  0.464458 |  0.527708 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Geo            |  13767.5         |           0      |  0.278459 |  0.385125 |  0.475917 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Temporal       |    654.449       |           0      |  5.79213  |  7.01871  | 16.9855   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_LearnedIndex   |   2420.28        |           0      |  1.65671  |  1.93717  |  2.16458  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoPut                 |      1.60402e+06 |          97.9016 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoGet                 |      1.38825e+06 |         169.464  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Dense          |   2068.21        |           0      |  1.92942  |  2.24346  |  2.30212  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Hybrid         |  24650.4         |           0      |  0.154041 |  0.2105   |  0.295791 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Filtered       |   2061.08        |           0      |  1.98425  |  2.23371  |  2.29392  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredBool   |   2052.36        |           0      |  1.9235   |  2.25121  |  2.31146  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredString |   2045.94        |           0      |  1.91558  |  2.25308  |  2.56688  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Sparse         |  32146           |           0      |  0.119417 |  0.168083 |  0.203667 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_ByID           |  41421.8         |           0      |  0.089417 |  0.139666 |  0.204792 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GraphRAG       |   2058.98        |           0      |  1.98567  |  2.22587  |  2.40137  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GlobalGraphRAG |   2064.95        |           0      |  1.9755   |  2.21275  |  2.26946  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Recommend      |  31956           |           0      |  0.117958 |  0.176167 |  0.234083 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Geo            |  12852.6         |           0      |  0.306667 |  0.382792 |  0.468416 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Temporal       |    220.127       |           0      | 17.8745   | 20.8602   | 27.7745   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_LearnedIndex   |   2053.81        |           0      |  1.90604  |  2.25779  |  2.36279  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoPut                 |      1.99157e+06 |         486.223  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoGet                 |      1.69014e+06 |         412.632  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Dense          |    876.391       |           0      |  4.45017  |  5.41188  |  6.45271  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Hybrid         |    937.799       |           0      |  4.24304  |  4.48354  |  4.72342  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Filtered       |    728.463       |           0      |  5.21563  |  7.08329  |  8.47058  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredBool   |    702.049       |           0      |  5.59692  |  6.20154  |  6.45829  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredString |    769.452       |           0      |  5.16188  |  5.64075  |  5.94246  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Sparse         |  30081.3         |           0      |  0.125333 |  0.199    |  0.266792 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_ByID           |  13095.5         |           0      |  0.292416 |  0.38375  |  0.489875 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GraphRAG       |    745.416       |           0      |  5.17258  |  6.30471  |  7.93942  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GlobalGraphRAG |    733.802       |           0      |  5.14767  |  6.17779  | 12.6493   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Recommend      |    884.924       |           0      |  4.46596  |  5.01317  |  5.38633  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Geo            |  11618           |           0      |  0.324875 |  0.480667 |  0.563791 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Temporal       |    332.83        |           0      | 11.7202   | 13.7892   | 19.7843   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_LearnedIndex   |    725.678       |           0      |  5.26587  |  6.96608  |  9.30483  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoPut                 |      2.13474e+06 |         521.176  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoGet                 |      2.11308e+06 |         515.89   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Dense          |    964.389       |           0      |  4.07917  |  4.66121  |  5.11775  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Hybrid         |    895.164       |           0      |  4.36704  |  5.09117  |  5.4565   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Filtered       |    786.78        |           0      |  4.98942  |  5.64992  |  6.06537  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredBool   |    734.565       |           0      |  5.31583  |  6.13183  |  7.94025  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredString |    827.615       |           0      |  4.72883  |  5.34817  |  6.39996  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Sparse         |  30607.9         |           0      |  0.122042 |  0.180083 |  0.258333 |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_ByID           |  13954.2         |           0      |  0.273    |  0.347292 |  0.42725  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GraphRAG       |    819.633       |           0      |  4.78029  |  5.41504  |  5.791    |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GlobalGraphRAG |    817.057       |           0      |  4.786    |  5.47083  |  5.75237  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Recommend      |    981.079       |           0      |  4.05308  |  4.27008  |  4.40462  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Geo            |  13073.5         |           0      |  0.299625 |  0.383792 |  0.451625 |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Temporal       |    566.513       |           0      |  7.02337  |  7.99246  | 12.132    |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_LearnedIndex   |    807.504       |           0      |  4.82608  |  5.49083  |  5.95454  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoPut                 | 598211           |         584.191  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoGet                 | 762258           |         744.393  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Dense          |   5865.13        |           0      |  0.610333 |  1.18917  |  1.75133  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Hybrid         |   9203.8         |           0      |  0.388125 |  0.695917 |  1.00258  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Filtered       |   5151.97        |           0      |  0.770708 |  0.957875 |  1.11183  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredBool   |   4825.42        |           0      |  0.813792 |  1.00571  |  1.13538  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredString |   4533.48        |           0      |  0.877875 |  1.09533  |  1.1995   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Sparse         |  31777.9         |           0      |  0.121792 |  0.174792 |  0.219458 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_ByID           |  15191.9         |           0      |  0.255291 |  0.309625 |  0.394083 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GraphRAG       |   5328.09        |           0      |  0.79725  |  1.0195   |  1.08525  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GlobalGraphRAG |   4812.65        |           0      |  0.850375 |  1.05496  |  1.1375   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Recommend      |  13472.2         |           0      |  0.288916 |  0.345208 |  0.41175  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Geo            |  12107.3         |           0      |  0.320292 |  0.403792 |  0.500667 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Temporal       |   1087.97        |           0      |  3.67908  |  3.937    |  4.12333  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_LearnedIndex   |   4319.32        |           0      |  0.925792 |  1.146    |  1.23104  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoPut                 |      1.09827e+06 |          33.5165 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoGet                 |      1.23049e+06 |         150.206  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Dense          |   2046.76        |           0      |  1.98971  |  2.21787  |  2.46821  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Hybrid         |  23896.3         |           0      |  0.155083 |  0.235    |  0.322    |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Filtered       |   2066.75        |           0      |  1.98896  |  2.20487  |  2.28087  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredBool   |   2053.61        |           0      |  1.95967  |  2.23208  |  2.30217  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredString |   2066.72        |           0      |  1.97229  |  2.23058  |  2.27721  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Sparse         |  31490.8         |           0      |  0.119875 |  0.180167 |  0.218292 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_ByID           |  39370.6         |           0      |  0.095042 |  0.1445   |  0.220625 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GraphRAG       |   2072.32        |           0      |  1.92846  |  2.22608  |  2.27438  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GlobalGraphRAG |   2056.41        |           0      |  1.90996  |  2.24304  |  2.30233  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Recommend      |  32135.7         |           0      |  0.119291 |  0.177375 |  0.215458 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Geo            |  11881.9         |           0      |  0.331666 |  0.418709 |  0.503458 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Temporal       |    236.182       |           0      | 16.7888   | 18.5901   | 20.7923   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_LearnedIndex   |   2047.17        |           0      |  1.91288  |  2.25679  |  2.31212  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoPut                 | 718800           |         701.953  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoGet                 | 776222           |         758.029  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Dense          |    867.88        |           0      |  4.54625  |  5.26092  |  6.1205   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Hybrid         |    907.272       |           0      |  4.36363  |  4.66196  |  5.16308  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Filtered       |    770.748       |           0      |  5.08592  |  5.71871  |  6.05875  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredBool   |    693.062       |           0      |  5.66192  |  6.36183  |  6.92171  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredString |    806.663       |           0      |  4.91112  |  5.4325   |  5.66133  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Sparse         |  31351.1         |           0      |  0.121    |  0.183542 |  0.238042 |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_ByID           |  15880.4         |           0      |  0.243    |  0.301792 |  0.354    |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GraphRAG       |    799.368       |           0      |  4.86275  |  5.60917  |  6.70621  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GlobalGraphRAG |    825.86        |           0      |  4.76371  |  5.29846  |  5.5965   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Recommend      |    976.047       |           0      |  4.08646  |  4.19533  |  4.28021  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Geo            |  12870.2         |           0      |  0.295916 |  0.38     |  0.52225  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Temporal       |    296.599       |           0      | 12.695    | 16.4294   | 28.4788   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_LearnedIndex   |    816.725       |           0      |  4.82117  |  5.38975  |  5.85329  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoPut                 | 970732           |         947.981  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoGet                 |      1.42876e+06 |        1395.27   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Dense          |    847.418       |           0      |  4.52796  |  5.69025  |  8.1525   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Hybrid         |    748.236       |           0      |  4.24054  |  4.81737  | 17.4759   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Filtered       |    784.214       |           0      |  4.97012  |  5.83162  |  6.51067  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredBool   |    738.848       |           0      |  5.31858  |  5.93992  |  6.17279  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredString |    824.275       |           0      |  4.75762  |  5.26425  |  5.60979  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Sparse         |  30799.6         |           0      |  0.119    |  0.184875 |  0.321917 |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_ByID           |  13405.7         |           0      |  0.290041 |  0.344375 |  0.4515   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GraphRAG       |    795.955       |           0      |  4.96933  |  5.44371  |  5.69104  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GlobalGraphRAG |    793.562       |           0      |  4.95088  |  5.54404  |  6.45188  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Recommend      |    903.854       |           0      |  4.31254  |  4.89837  |  6.54812  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Geo            |  12510.2         |           0      |  0.307625 |  0.398041 |  0.491083 |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Temporal       |    411.148       |           0      |  9.39779  | 11.6428   | 17.2637   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_LearnedIndex   |    797.969       |           0      |  4.93817  |  5.528    |  5.79108  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoPut                 | 367990           |         718.73   |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoGet                 | 435265           |         850.127  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Dense          |   1969.36        |           0      |  2.01725  |  2.33546  |  2.44663  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Hybrid         |   8161.06        |           0      |  0.486541 |  0.539792 |  0.587375 |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Filtered       |   1986.68        |           0      |  2.00654  |  2.30187  |  2.43992  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredBool   |   1857.24        |           0      |  2.1905   |  2.53337  |  2.61846  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredString |   1883.77        |           0      |  2.15271  |  2.51304  |  2.66821  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Sparse         |  32255.3         |           0      |  0.119334 |  0.167958 |  0.212959 |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_ByID           |   3043.32        |           0      |  1.308    |  1.38767  |  1.44846  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GraphRAG       |   1981.82        |           0      |  2.01254  |  2.33729  |  2.42721  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GlobalGraphRAG |   1953.26        |           0      |  2.07213  |  2.37792  |  2.44325  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Recommend      |  10028.4         |           0      |  0.392583 |  0.452375 |  0.516    |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Geo            |  13035.4         |           0      |  0.299583 |  0.383917 |  0.46175  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Temporal       |    253.795       |           0      | 15.5742   | 17.7699   | 22.5275   |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_LearnedIndex   |   1938.6         |           0      |  2.07467  |  2.424    |  2.571    |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoPut                 |      2.73748e+06 |         334.165  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoGet                 |      2.38673e+06 |         291.349  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Dense          |   2015.62        |           0      |  1.94162  |  2.27579  |  2.58808  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Hybrid         |  20507.7         |           0      |  0.187209 |  0.2445   |  0.329375 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Filtered       |   2045.17        |           0      |  1.95779  |  2.22333  |  2.30317  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredBool   |   2021.41        |           0      |  1.99196  |  2.23396  |  2.29337  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredString |   2059.84        |           0      |  1.97446  |  2.25029  |  2.31796  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Sparse         |  33222.4         |           0      |  0.109416 |  0.181334 |  0.269709 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_ByID           |  36031.3         |           0      |  0.10425  |  0.160542 |  0.211458 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GraphRAG       |   2048.14        |           0      |  1.93675  |  2.24475  |  2.30525  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GlobalGraphRAG |   2053.76        |           0      |  1.94013  |  2.24467  |  2.32808  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Recommend      |  26141.7         |           0      |  0.148    |  0.2135   |  0.258542 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Geo            |  13480.7         |           0      |  0.279833 |  0.386292 |  0.476792 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Temporal       |    319.429       |           0      | 11.2742   | 19.5856   | 38.1942   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_LearnedIndex   |   2070.06        |           0      |  1.95667  |  2.24167  |  2.32392  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoPut                 |      1.25226e+06 |         152.863  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoGet                 |      1.3468e+06  |         164.404  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Dense          |   2045.27        |           0      |  1.92213  |  2.25146  |  2.32046  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Hybrid         |  23968.3         |           0      |  0.157    |  0.217167 |  0.367958 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Filtered       |   2058.72        |           0      |  1.99042  |  2.19863  |  2.32508  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredBool   |   2070.84        |           0      |  1.9775   |  2.191    |  2.23183  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredString |   2062.28        |           0      |  1.98983  |  2.17629  |  2.23596  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Sparse         |  32259.6         |           0      |  0.117459 |  0.174375 |  0.22625  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_ByID           |  38618           |           0      |  0.094959 |  0.147709 |  0.238083 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GraphRAG       |   2062.31        |           0      |  1.91046  |  2.22837  |  2.30233  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   2053.75        |           0      |  1.91308  |  2.23571  |  2.28971  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Recommend      |  33067.2         |           0      |  0.115167 |  0.162709 |  0.233833 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Geo            |  12974.4         |           0      |  0.299125 |  0.382125 |  0.437125 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Temporal       |    208.088       |           0      | 18.8469   | 22.6089   | 29.4669   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   2066.45        |           0      |  1.97796  |  2.22533  |  2.28604  |

