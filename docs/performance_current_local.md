# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                        |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('bahamut', 'cpu', 128, 'complex128')  |       1563.48 |        1903.33 |           1713.17 |               1984.9  |                 1765.21 |      7755.44 |                  767.21 |            668.99 |         3542.22 |                478.76 |            5836.82 |        31205.9  |          11145.2  |
| ('bahamut', 'cpu', 128, 'complex64')   |       5713.09 |        1740.01 |            434.19 |                446.01 |                  475.39 |      5423.32 |                  455.89 |            487.4  |         2294.84 |                507    |            5208.12 |        19365.9  |           6955.83 |
| ('bahamut', 'cpu', 128, 'float16')     |      10446.1  |         423.03 |            420.61 |                678.39 |                  424.99 |      9097.38 |                  875.4  |            957.22 |         1343    |                999.44 |            2372.78 |        22648.6  |           6760.19 |
| ('bahamut', 'cpu', 128, 'float32')     |       8058    |         554.22 |            593.08 |                589.93 |                  561.61 |      6744.79 |                 1780.54 |            655.14 |         4260.44 |                578.5  |            8587.63 |        11449.5  |          12345.1  |
| ('bahamut', 'cpu', 128, 'float64')     |       5712.62 |         946.54 |            971.05 |                560.69 |                  654.79 |     10450.8  |                  925.31 |           1028.95 |         4236.2  |                417.98 |            9984.86 |        27922.4  |          16376.9  |
| ('bahamut', 'cpu', 128, 'int16')       |       2714.9  |         452.75 |            277.22 |                304.25 |                  315.85 |      1976.33 |                  350.53 |            311.47 |          372.01 |                437.36 |             411.95 |          581.7  |           2278.09 |
| ('bahamut', 'cpu', 128, 'int32')       |       8068.08 |         736.96 |            733.52 |                536.78 |                  737.51 |      7438.78 |                  681.16 |            752.87 |         1218.37 |                824.98 |            2198.88 |        17374.8  |           7096.9  |
| ('bahamut', 'cpu', 128, 'int64')       |       6075.83 |         426.33 |            401.37 |                400.84 |                  469.11 |     10611.5  |                  453.97 |            512.34 |          515.26 |                654.3  |             641.66 |        12955.5  |           7610.58 |
| ('bahamut', 'cpu', 128, 'int8')        |       6107.79 |        1407.85 |            806.17 |                793.94 |                  787.63 |      5213.04 |                  722.63 |            691.6  |         3921.87 |               1731.52 |            4835.24 |        12128    |           4128.9  |
| ('bahamut', 'cpu', 128, 'turboquant2') |      12063.5  |         488.05 |            665.75 |                497.51 |                  517.59 |      6286.24 |                  586.83 |           1192.7  |         9375.24 |               1354.42 |           11394    |         4848.59 |          10425.2  |
| ('bahamut', 'cpu', 128, 'turboquant4') |       8732.65 |        2018.47 |           1517.65 |               1884.31 |                 2278.98 |     10966.9  |                 2678.08 |           1633.7  |        12828    |               1487.41 |           18582.7  |        22649.4  |          16419.8  |
| ('bahamut', 'cpu', 128, 'turboquant8') |      13818.9  |        1707.37 |           1475.71 |                636.79 |                  608.61 |      7904.24 |                  826.68 |            883.08 |        10760.9  |                526.02 |           15320.6  |         9176.81 |          11322.8  |
| ('bahamut', 'cpu', 128, 'uint16')      |       8103.77 |         312.82 |            261.68 |                323.83 |                  339.5  |      7044.75 |                  361.96 |            338.38 |          572.15 |                322.85 |             741.74 |        20625.7  |           5698.1  |
| ('bahamut', 'cpu', 128, 'uint32')      |       8427.42 |         356.92 |            303.9  |                257.57 |                  372.44 |      7686.08 |                  286.87 |            282.54 |          546.03 |                522.86 |             601.51 |         8406.55 |           4925.95 |
| ('bahamut', 'cpu', 128, 'uint64')      |       4734.8  |         752.46 |            294.63 |                250.75 |                  210.53 |      7958.18 |                  307.02 |            263.84 |          574.72 |                589.61 |             707.72 |          953.99 |           5400.72 |
| ('bahamut', 'cpu', 128, 'uint8')       |       9591.25 |         518.02 |           1063.38 |                419.83 |                  433.78 |      3181.93 |                  973.07 |            607    |         3268.09 |                292.09 |            4471.38 |         2844.85 |           2962    |
| ('bahamut', 'cpu', 384, 'float16')     |       4667.32 |         671.78 |            806.41 |               1223.07 |                 1307.59 |     10443.7  |                  723.27 |            949.39 |         1254.9  |                883.54 |            1760.33 |        14483.2  |           6506.93 |
| ('bahamut', 'cpu', 384, 'float32')     |      10492.1  |        7890.78 |           8282.59 |               3992.41 |                 5703.39 |      9943.55 |                 2595.66 |           2185.51 |         7573.63 |               1953.22 |            5553.72 |        31771.7  |          17097.1  |
| ('bahamut', 'cpu', 384, 'float64')     |       4536.13 |        1374.27 |           1628.3  |               1223.69 |                 1220.51 |      7886.31 |                  895.76 |            960.19 |         3512.62 |                593.13 |            5838.89 |        17023.3  |          11529.8  |
| ('bahamut', 'cpu', 384, 'int16')       |       2621    |         484.46 |            531.4  |                453.09 |                  413.08 |      2073.41 |                  485.6  |            352.27 |          493.61 |                376.04 |             464.2  |         6725.19 |           4509.67 |
| ('bahamut', 'cpu', 384, 'int32')       |       4259.74 |        1404.56 |           1258.47 |               1080.47 |                 1413.71 |      3066.47 |                  605.57 |            462.28 |         1725.85 |               1107.16 |            2401.38 |        20276.6  |           6432.17 |
| ('bahamut', 'cpu', 384, 'int64')       |       4692.43 |         530.62 |            495.47 |                453.23 |                  512.58 |      5164.39 |                  467.01 |            530.97 |          490.22 |                450.26 |             492.38 |         9513.9  |           2886.52 |
| ('bahamut', 'cpu', 384, 'int8')        |       5870.31 |        1073.49 |           1240.93 |               1131.64 |                  967.72 |      3419.3  |                  921.82 |            657.98 |         1777.21 |               1307.04 |            4323.92 |         2065.89 |           6741.75 |
| ('bahamut', 'cpu', 384, 'uint8')       |       8548.74 |        2573.33 |           2914.77 |               2315.24 |                 1943.2  |      4238.86 |                 1176.23 |           1008.4  |         4001.51 |                600.89 |            1779.13 |        30490.9  |           2715.18 |

## Ingestion Performance (MB/s)

|                                        |   Throughput_MBs |
|:---------------------------------------|-----------------:|
| ('bahamut', 'cpu', 128, 'complex128')  |           799.53 |
| ('bahamut', 'cpu', 128, 'complex64')   |           464.92 |
| ('bahamut', 'cpu', 128, 'float16')     |           467.32 |
| ('bahamut', 'cpu', 128, 'float32')     |           551.25 |
| ('bahamut', 'cpu', 128, 'float64')     |           541.8  |
| ('bahamut', 'cpu', 128, 'int16')       |           479.92 |
| ('bahamut', 'cpu', 128, 'int32')       |           645.76 |
| ('bahamut', 'cpu', 128, 'int64')       |           569.28 |
| ('bahamut', 'cpu', 128, 'int8')        |           321.71 |
| ('bahamut', 'cpu', 128, 'turboquant2') |            28.73 |
| ('bahamut', 'cpu', 128, 'turboquant4') |            85.06 |
| ('bahamut', 'cpu', 128, 'turboquant8') |            37.76 |
| ('bahamut', 'cpu', 128, 'uint16')      |           119.72 |
| ('bahamut', 'cpu', 128, 'uint32')      |           798.7  |
| ('bahamut', 'cpu', 128, 'uint64')      |           616.24 |
| ('bahamut', 'cpu', 128, 'uint8')       |           306.57 |
| ('bahamut', 'cpu', 384, 'float16')     |           150.69 |
| ('bahamut', 'cpu', 384, 'float32')     |           787.88 |
| ('bahamut', 'cpu', 384, 'float64')     |           996.18 |
| ('bahamut', 'cpu', 384, 'int16')       |            68.77 |
| ('bahamut', 'cpu', 384, 'int32')       |           179.3  |
| ('bahamut', 'cpu', 384, 'int64')       |           209.87 |
| ('bahamut', 'cpu', 384, 'int8')        |           534.6  |
| ('bahamut', 'cpu', 384, 'uint8')       |            46.51 |

### Details: bahamut (cpu)

| Host    | Mode   | Dataset                         | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |    P99_ms |
|:--------|:-------|:--------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|----------:|
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | DoPut                 | 127001           |          46.509  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | DoGet                 |      1.80709e+06 |         661.777  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Dense          |   2573.33        |           0      |  1.18687  |  2.69379  |  5.7255   |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Hybrid         |   4001.51        |           0      |  0.923583 |  1.29567  |  1.61458  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Filtered       |   2914.77        |           0      |  0.925166 |  2.09975  |  2.97742  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_FilteredBool   |   2315.24        |           0      |  1.40146  |  2.90679  |  7.00217  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_FilteredString |   1943.2         |           0      |  1.49638  |  3.14908  |  5.01575  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Sparse         |  30490.9         |           0      |  0.122375 |  0.208791 |  0.284667 |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_ByID           |   8548.74        |           0      |  0.431833 |  0.674833 |  0.86775  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_GraphRAG       |   1008.4         |           0      |  1.84604  |  8.01171  | 60.5902   |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_GlobalGraphRAG |   1176.23        |           0      |  2.03729  |  8.84675  | 13.4565   |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Recommend      |   1779.13        |           0      |  1.35175  |  3.89817  | 12.0566   |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Geo            |   4238.86        |           0      |  0.686667 |  2.435    |  4.28925  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Temporal       |   2715.18        |           0      |  1.09096  |  3.51292  |  6.30246  |
| bahamut | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_LearnedIndex   |    600.889       |           0      |  5.57383  |  9.68346  | 13.9003   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoPut                 | 476083           |         464.925  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoGet                 |      1.02049e+06 |         996.569  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Dense          |   1740.01        |           0      |  2.16213  |  3.24104  |  4.18371  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Hybrid         |   2294.84        |           0      |  0.995417 |  4.24883  |  4.47979  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Filtered       |    434.19        |           0      |  9.44758  | 11.3015   | 13.4048   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredBool   |    446.006       |           0      |  9.06246  | 11.9174   | 19.6537   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredString |    475.39        |           0      |  8.96646  | 11.491    | 15.394    |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Sparse         |  19365.9         |           0      |  0.188209 |  0.321583 |  0.394833 |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_ByID           |   5713.09        |           0      |  0.649333 |  1.06225  |  1.19625  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GraphRAG       |    487.397       |           0      |  8.21525  | 12.3918   | 18.1043   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GlobalGraphRAG |    455.891       |           0      |  8.00567  | 15.8714   | 31.6641   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Recommend      |   5208.12        |           0      |  0.708041 |  1.14079  |  1.49396  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Geo            |   5423.32        |           0      |  0.657375 |  1.15963  |  2.05942  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Temporal       |   6955.83        |           0      |  0.41475  |  1.0345   |  3.72667  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_LearnedIndex   |    506.998       |           0      |  8.60675  | 11.4518   | 16.1504   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoPut                 |      1.63575e+06 |         798.704  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoGet                 |      2.26377e+06 |        1105.35   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Dense          |    356.923       |           0      | 12.8439   | 15.8042   | 19.0495   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Hybrid         |    546.028       |           0      |  7.14275  |  9.62833  | 10.4468   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Filtered       |    303.899       |           0      | 13.4943   | 17.829    | 20.6551   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredBool   |    257.567       |           0      | 14.8462   | 20.4802   | 41.5047   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredString |    372.442       |           0      |  8.05154  | 18.0143   | 34.1614   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Sparse         |   8406.55        |           0      |  0.191458 |  3.73783  |  4.55171  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_ByID           |   8427.42        |           0      |  0.389917 |  0.811833 |  1.05446  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GraphRAG       |    282.544       |           0      | 14.2114   | 16.9733   | 19.4464   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GlobalGraphRAG |    286.868       |           0      | 14.1219   | 17.9205   | 24.5267   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Recommend      |    601.51        |           0      |  5.80821  | 10.6423   | 13.576    |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Geo            |   7686.08        |           0      |  0.506583 |  0.756    |  0.838833 |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Temporal       |   4925.95        |           0      |  0.524875 |  1.39775  |  3.33617  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_LearnedIndex   |    522.863       |           0      |  6.27046  | 15.1275   | 19.5738   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoPut                 |      1.32252e+06 |         645.761  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoGet                 |      1.38538e+06 |         676.454  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Dense          |    736.955       |           0      |  4.89787  |  9.80446  | 11.29     |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Hybrid         |   1218.37        |           0      |  2.71513  |  4.81412  |  8.77542  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Filtered       |    733.517       |           0      |  4.60146  | 10.3051   | 11.3024   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredBool   |    536.776       |           0      |  7.57292  | 11.2395   | 12.5087   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredString |    737.511       |           0      |  4.294    | 10.7302   | 12.7268   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Sparse         |  17374.7         |           0      |  0.20875  |  0.39575  |  0.575    |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_ByID           |   8068.08        |           0      |  0.414333 |  0.862459 |  1.00858  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GraphRAG       |    752.87        |           0      |  5.19946  |  7.62704  |  8.46704  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GlobalGraphRAG |    681.158       |           0      |  4.84533  | 10.711    | 19.9097   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Recommend      |   2198.88        |           0      |  1.71287  |  2.31121  |  2.94588  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Geo            |   7438.78        |           0      |  0.473042 |  0.936834 |  1.63871  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Temporal       |   7096.9         |           0      |  0.522375 |  0.819458 |  1.05137  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_LearnedIndex   |    824.975       |           0      |  3.82067  |  9.43513  | 12.671    |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoPut                 |      1.12895e+06 |         551.247  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoGet                 |      1.33684e+06 |         652.753  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Dense          |    554.215       |           0      |  7.35492  | 10.0223   | 10.9118   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Hybrid         |   4260.44        |           0      |  0.76225  |  2.05963  |  2.67767  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Filtered       |    593.078       |           0      |  6.72325  |  9.99904  | 11.7175   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredBool   |    589.926       |           0      |  6.76958  |  9.99629  | 11.0679   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredString |    561.607       |           0      |  7.14379  | 10.176    | 11.2284   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Sparse         |  11449.5         |           0      |  0.217333 |  0.932375 |  1.44983  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_ByID           |   8058           |           0      |  0.497083 |  0.659375 |  0.819291 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GraphRAG       |    655.142       |           0      |  6.25229  | 10.4155   | 11.2556   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GlobalGraphRAG |   1780.54        |           0      |  2.21629  |  2.89825  |  3.20063  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Recommend      |   8587.63        |           0      |  0.409417 |  0.769625 |  1.09754  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Geo            |   6744.79        |           0      |  0.532    |  0.986    |  1.37292  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Temporal       |  12345.1         |           0      |  0.249417 |  0.583542 |  0.765166 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_LearnedIndex   |    578.497       |           0      |  6.99167  |  9.78917  | 10.7617   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoPut                 |      1.91415e+06 |         467.322  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoGet                 |      2.19555e+06 |         536.023  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Dense          |    423.03        |           0      |  9.673    | 12.1292   | 15.8107   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Hybrid         |   1343           |           0      |  2.92442  |  4.70779  |  5.51471  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Filtered       |    420.615       |           0      |  9.7585   | 12.0267   | 13.0521   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredBool   |    678.387       |           0      |  4.32117  | 11.3059   | 12.6929   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredString |    424.986       |           0      | 10.2686   | 12.2261   | 13.7888   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Sparse         |  22648.6         |           0      |  0.136875 |  0.303541 |  0.9075   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_ByID           |  10446.1         |           0      |  0.340458 |  0.613417 |  0.719166 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GraphRAG       |    957.217       |           0      |  3.94612  |  6.56563  |  7.50217  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GlobalGraphRAG |    875.398       |           0      |  3.94762  |  8.46367  | 13.0183   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Recommend      |   2372.78        |           0      |  1.64292  |  2.04842  |  2.32462  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Geo            |   9097.38        |           0      |  0.416541 |  0.627625 |  0.724    |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Temporal       |   6760.19        |           0      |  0.516917 |  0.994334 |  1.11721  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_LearnedIndex   |    999.437       |           0      |  3.76437  |  5.74025  | 10.3516   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoPut                 |      2.63545e+06 |         321.71   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoGet                 |      3.65108e+06 |         445.688  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Dense          |   1407.85        |           0      |  2.85637  |  5.17912  |  5.83117  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Hybrid         |   3921.87        |           0      |  1.00717  |  1.49038  |  2.27079  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Filtered       |    806.166       |           0      |  5.00154  |  8.754    | 10.2011   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredBool   |    793.943       |           0      |  5.02375  |  9.0055   | 10.0573   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredString |    787.634       |           0      |  4.97792  |  9.03937  | 10.4231   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Sparse         |  12128           |           0      |  0.207959 |  1.22313  |  1.70654  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_ByID           |   6107.79        |           0      |  0.675041 |  0.913208 |  1.05279  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GraphRAG       |    691.597       |           0      |  5.28746  |  9.99579  | 18.0323   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GlobalGraphRAG |    722.627       |           0      |  4.89579  |  9.69771  | 13.5284   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Recommend      |   4835.24        |           0      |  0.817708 |  1.16496  |  1.55171  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Geo            |   5213.04        |           0      |  0.752834 |  1.09013  |  1.32346  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Temporal       |   4128.9         |           0      |  1.01487  |  1.42588  |  1.74446  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_LearnedIndex   |   1731.52        |           0      |  2.19937  |  3.32175  |  5.2955   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoPut                 |      1.39365e+06 |          85.0615 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoGet                 |      1.99857e+06 |         243.966  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Dense          |   2018.47        |           0      |  2.00496  |  2.2835   |  2.38388  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Hybrid         |  12827.9         |           0      |  0.284167 |  0.506875 |  0.64775  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Filtered       |   1517.65        |           0      |  2.15538  |  5.41842  |  6.53225  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredBool   |   1884.31        |           0      |  2.14592  |  2.77633  |  3.61754  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredString |   2278.98        |           0      |  1.92442  |  2.22217  |  2.35696  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Sparse         |  22649.4         |           0      |  0.126041 |  0.444917 |  0.6435   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_ByID           |   8732.65        |           0      |  0.436125 |  0.81775  |  1.14275  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GraphRAG       |   1633.7         |           0      |  2.01425  |  5.17692  |  7.00333  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GlobalGraphRAG |   2678.08        |           0      |  1.77675  |  2.22096  |  2.33717  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Recommend      |  18582.7         |           0      |  0.205416 |  0.28425  |  0.366042 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Geo            |  10966.9         |           0      |  0.351958 |  0.514125 |  0.603917 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Temporal       |  16419.8         |           0      |  0.226167 |  0.341209 |  0.416292 |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_LearnedIndex   |   1487.41        |           0      |  2.14708  |  5.20892  |  6.44971  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoPut                 | 490368           |         119.719  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoGet                 |      1.5612e+06  |         381.152  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Dense          |    312.821       |           0      | 12.9005   | 16.6135   | 20.5674   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Hybrid         |    572.146       |           0      |  6.40054  | 10.2872   | 14.3527   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Filtered       |    261.682       |           0      | 15.7163   | 23.2228   | 27.673    |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredBool   |    323.829       |           0      | 13.1633   | 17.1757   | 19.5328   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredString |    339.505       |           0      | 12.5508   | 15.8757   | 19.7508   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Sparse         |  20625.7         |           0      |  0.184084 |  0.278083 |  0.344959 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_ByID           |   8103.77        |           0      |  0.404792 |  0.770541 |  0.851416 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GraphRAG       |    338.382       |           0      | 12.6382   | 15.8031   | 17.5066   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GlobalGraphRAG |    361.956       |           0      | 11.5242   | 15.6418   | 18.6462   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Recommend      |    741.74        |           0      |  4.98525  |  7.42138  |  9.81317  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Geo            |   7044.75        |           0      |  0.555333 |  0.769709 |  0.85525  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Temporal       |   5698.1         |           0      |  0.55975  |  1.12271  |  1.31196  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_LearnedIndex   |    322.848       |           0      | 13.5685   | 16.3335   | 18.2854   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoPut                 |      1.96576e+06 |         479.923  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoGet                 |      2.27006e+06 |         554.214  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Dense          |    452.747       |           0      |  8.09779  | 13.8763   | 18.7801   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Hybrid         |    372.005       |           0      |  9.92575  | 17.5487   | 22.9503   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Filtered       |    277.216       |           0      | 12.8388   | 24.5246   | 33.5472   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredBool   |    304.252       |           0      | 12.5507   | 19.799    | 23.2966   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredString |    315.853       |           0      | 12.2443   | 19.6282   | 25.2338   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Sparse         |    581.7         |           0      |  6.437    | 15.069    | 28.5612   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_ByID           |   2714.9         |           0      |  1.17367  |  3.39642  |  4.90242  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GraphRAG       |    311.471       |           0      | 11.5877   | 20.4865   | 29.2961   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GlobalGraphRAG |    350.527       |           0      | 10.2995   | 19.2129   | 24.6559   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Recommend      |    411.949       |           0      |  8.59946  | 16.4313   | 20.364    |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Geo            |   1976.33        |           0      |  1.65654  |  4.33921  |  6.58171  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Temporal       |   2278.09        |           0      |  1.31496  |  3.86846  |  6.5145   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_LearnedIndex   |    437.362       |           0      |  8.34967  | 14.2939   | 20.7357   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | DoPut                 |  71634.4         |         209.866  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | DoGet                 |  39698           |         116.303  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Dense          |    530.619       |           0      |  6.68467  | 12.6835   | 15.1841   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Hybrid         |    490.217       |           0      |  6.63283  | 14.3124   | 19.6725   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Filtered       |    495.473       |           0      |  6.94367  | 14.0702   | 19.6438   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_FilteredBool   |    453.225       |           0      |  7.602    | 14.9014   | 23.1636   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_FilteredString |    512.582       |           0      |  6.98596  | 12.6462   | 15.0912   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Sparse         |   9513.9         |           0      |  0.401583 |  0.659375 |  0.887458 |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_ByID           |   4692.43        |           0      |  0.693    |  1.37646  |  1.62425  |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_GraphRAG       |    530.974       |           0      |  6.48517  | 13.272    | 16.2422   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_GlobalGraphRAG |    467.014       |           0      |  7.35396  | 15.1344   | 25.5983   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Recommend      |    492.382       |           0      |  6.67496  | 15.3382   | 23.1827   |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Geo            |   5164.39        |           0      |  0.565584 |  2.04333  |  2.76538  |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Temporal       |   2886.52        |           0      |  1.123    |  2.90121  |  5.11696  |
| bahamut | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_LearnedIndex   |    450.255       |           0      |  7.58087  | 14.9452   | 19.696    |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoPut                 | 340029           |         996.179  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoGet                 | 353343           |        1035.19   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Dense          |   1374.27        |           0      |  1.80371  |  6.66338  |  9.40762  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Hybrid         |   3512.62        |           0      |  1.03754  |  1.78729  |  2.30254  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Filtered       |   1628.3         |           0      |  1.82575  |  5.47538  |  6.691    |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredBool   |   1223.69        |           0      |  2.30042  |  6.72667  |  7.87325  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredString |   1220.51        |           0      |  2.64858  |  6.81117  | 10.231    |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Sparse         |  17023.3         |           0      |  0.193583 |  0.666833 |  0.864916 |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_ByID           |   4536.13        |           0      |  0.7705   |  1.72329  |  2.24621  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GraphRAG       |    960.19        |           0      |  3.47025  |  8.09575  |  9.14146  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GlobalGraphRAG |    895.755       |           0      |  3.84758  |  8.83279  | 11.3596   |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Recommend      |   5838.89        |           0      |  0.588459 |  1.1275   |  1.26208  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Geo            |   7886.31        |           0      |  0.498625 |  0.723459 |  0.816583 |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Temporal       |  11529.8         |           0      |  0.293167 |  0.573958 |  0.646584 |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_LearnedIndex   |    593.13        |           0      |  6.78396  | 10.345    | 11.6083   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoPut                 | 554806           |         541.802  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoGet                 | 232040           |         226.602  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Dense          |    946.536       |           0      |  4.12404  |  6.18667  |  7.19154  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Hybrid         |   4236.2         |           0      |  0.927667 |  1.37292  |  1.86379  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Filtered       |    971.049       |           0      |  3.4775   |  8.98558  | 10.3596   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredBool   |    560.692       |           0      |  7.33175  | 10.265    | 11.1828   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredString |    654.792       |           0      |  5.64546  | 10.2463   | 11.0341   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Sparse         |  27922.4         |           0      |  0.133667 |  0.219708 |  0.339916 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_ByID           |   5712.62        |           0      |  0.582333 |  1.54875  |  2.18592  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GraphRAG       |   1028.95        |           0      |  3.6545   |  6.1125   |  9.237    |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GlobalGraphRAG |    925.312       |           0      |  3.67437  |  8.44279  | 13.9043   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Recommend      |   9984.86        |           0      |  0.35925  |  0.651417 |  0.760375 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Geo            |  10450.8         |           0      |  0.359667 |  0.59325  |  0.704417 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Temporal       |  16376.9         |           0      |  0.222125 |  0.378084 |  0.490916 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_LearnedIndex   |    417.983       |           0      |  9.63679  | 11.0598   | 11.9294   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoPut                 | 941516           |          28.7328 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoGet                 |      1.87374e+06 |         228.728  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Dense          |    488.046       |           0      |  8.61937  | 10.9158   | 12.2593   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Hybrid         |   9375.24        |           0      |  0.326834 |  0.616584 |  3.32433  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Filtered       |    665.747       |           0      |  5.80121  | 10.3231   | 11.5436   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredBool   |    497.512       |           0      |  8.30962  | 10.5876   | 11.6403   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredString |    517.585       |           0      |  7.98925  | 10.8387   | 12.5153   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Sparse         |   4848.59        |           0      |  0.389958 |  2.23346  |  6.68033  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_ByID           |  12063.5         |           0      |  0.282042 |  0.530375 |  0.676292 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GraphRAG       |   1192.7         |           0      |  2.95604  |  5.64433  | 10.6811   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GlobalGraphRAG |    586.827       |           0      |  7.64375  | 10.5905   | 11.9083   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Recommend      |  11394           |           0      |  0.31275  |  0.564458 |  0.6765   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Geo            |   6286.24        |           0      |  0.596625 |  1.02333  |  1.32242  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Temporal       |  10425.2         |           0      |  0.3425   |  0.59525  |  0.671375 |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_LearnedIndex   |   1354.42        |           0      |  2.10683  |  9.16238  | 10.7081   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoPut                 | 631028           |         616.238  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoGet                 |      1.10149e+06 |        1075.68   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Dense          |    752.456       |           0      |  5.25321  |  5.84992  |  6.33746  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Hybrid         |    574.719       |           0      |  6.48179  | 10.0166   | 13.1869   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Filtered       |    294.632       |           0      | 13.6114   | 17.2399   | 18.8415   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredBool   |    250.749       |           0      | 17.4482   | 24.9755   | 30.3204   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredString |    210.528       |           0      | 17.9816   | 26.263    | 48.138    |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Sparse         |    953.99        |           0      |  3.95213  | 10.0634   | 11.927    |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_ByID           |   4734.8         |           0      |  0.790125 |  1.40687  |  2.04617  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GraphRAG       |    263.844       |           0      | 14.3344   | 24.292    | 34.3875   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GlobalGraphRAG |    307.021       |           0      | 13.4957   | 20.6294   | 25.8154   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Recommend      |    707.723       |           0      |  5.13854  |  8.36167  | 13.2471   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Geo            |   7958.18        |           0      |  0.404083 |  0.705625 |  0.886167 |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Temporal       |   5400.72        |           0      |  0.682291 |  1.07546  |  1.32438  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_LearnedIndex   |    589.613       |           0      |  6.24342  |  8.54221  | 17.6947   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoPut                 | 582943           |         569.28   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoGet                 | 962063           |         939.514  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Dense          |    426.326       |           0      |  9.10538  | 13.7385   | 15.2533   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Hybrid         |    515.256       |           0      |  6.82079  | 12.0281   | 22.2447   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Filtered       |    401.367       |           0      |  9.35704  | 14.5602   | 15.8054   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredBool   |    400.844       |           0      |  9.24358  | 14.7635   | 17.302    |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredString |    469.113       |           0      |  7.94071  | 13.1624   | 14.8215   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Sparse         |  12955.5         |           0      |  0.208291 |  0.792792 |  1.43892  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_ByID           |   6075.83        |           0      |  0.588    |  0.961    |  1.049    |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GraphRAG       |    512.344       |           0      |  7.06292  | 12.1645   | 14.6101   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GlobalGraphRAG |    453.974       |           0      |  7.95354  | 14.0454   | 15.9684   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Recommend      |    641.656       |           0      |  5.24508  | 10.6358   | 15.6674   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Geo            |  10611.5         |           0      |  0.361167 |  0.505584 |  0.644833 |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Temporal       |   7610.58        |           0      |  0.494417 |  0.705792 |  0.820709 |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_LearnedIndex   |    654.302       |           0      |  6.00154  |  7.15458  |  8.32179  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoPut                 | 409361           |         799.533  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoGet                 | 673174           |        1314.79   |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Dense          |   1903.33        |           0      |  2.08321  |  2.48908  |  2.81192  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Hybrid         |   3542.22        |           0      |  0.856333 |  2.06462  |  4.18767  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Filtered       |   1713.17        |           0      |  2.10888  |  4.79225  |  5.47446  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredBool   |   1984.9         |           0      |  2.05958  |  2.74067  |  3.32446  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredString |   1765.21        |           0      |  2.29275  |  2.93     |  3.15379  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Sparse         |  31205.9         |           0      |  0.120875 |  0.186083 |  0.257792 |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_ByID           |   1563.48        |           0      |  2.12829  |  4.50917  |  5.45613  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GraphRAG       |    668.986       |           0      |  4.76963  | 11.4548   | 12.3183   |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GlobalGraphRAG |    767.21        |           0      |  3.23054  | 11.9162   | 13.3896   |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Recommend      |   5836.82        |           0      |  0.595584 |  1.01629  |  1.18904  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Geo            |   7755.44        |           0      |  0.505959 |  0.766917 |  0.86375  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Temporal       |  11145.3         |           0      |  0.306583 |  0.583    |  0.712334 |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_LearnedIndex   |    478.76        |           0      |  8.62988  | 10.918    | 12.0243   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | DoPut                 |  93895.1         |          68.7708 |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | DoGet                 |      1.1048e+06  |         809.179  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Dense          |    484.465       |           0      |  7.08954  | 14.1027   | 26.4473   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Hybrid         |    493.614       |           0      |  6.9005   | 13.9657   | 23.3411   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Filtered       |    531.4         |           0      |  6.51912  | 12.8285   | 15.4286   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_FilteredBool   |    453.09        |           0      |  7.38325  | 14.387    | 21.4364   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_FilteredString |    413.077       |           0      |  7.33167  | 20.7328   | 36.6069   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Sparse         |   6725.19        |           0      |  0.495666 |  1.10479  |  4.51117  |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_ByID           |   2621           |           0      |  1.28475  |  2.85875  |  4.2975   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_GraphRAG       |    352.27        |           0      |  8.11013  | 26.3925   | 65.0237   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_GlobalGraphRAG |    485.601       |           0      |  7.40313  | 13.2375   | 16.9582   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Recommend      |    464.203       |           0      |  7.48221  | 14.1238   | 20.1313   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Geo            |   2073.41        |           0      |  0.785333 |  4.35958  | 34.5172   |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Temporal       |   4509.67        |           0      |  0.813292 |  1.41004  |  2.21992  |
| bahamut | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_LearnedIndex   |    376.041       |           0      |  9.62892  | 17.5607   | 24.7115   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | DoPut                 |      1.45982e+06 |         534.602  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | DoGet                 |      1.38506e+06 |         507.223  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Dense          |   1073.49        |           0      |  2.68875  |  8.66533  |  9.80542  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Hybrid         |   1777.21        |           0      |  1.68117  |  4.68204  |  7.14358  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Filtered       |   1240.93        |           0      |  2.34271  |  7.13483  |  8.8235   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_FilteredBool   |   1131.64        |           0      |  2.88346  |  7.46742  |  9.57596  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_FilteredString |    967.718       |           0      |  3.39204  |  8.69167  | 12.0527   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Sparse         |   2065.89        |           0      |  1.29621  |  7.78742  | 13.051    |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_ByID           |   5870.31        |           0      |  0.609    |  1.03687  |  1.10975  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_GraphRAG       |    657.985       |           0      |  3.9475   | 11.5144   | 20.1457   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_GlobalGraphRAG |    921.819       |           0      |  3.61446  |  8.52208  | 12.2446   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Recommend      |   4323.92        |           0      |  0.899208 |  1.01454  |  1.15279  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Geo            |   3419.3         |           0      |  0.508959 |  2.25417  | 14.4499   |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Temporal       |   6741.75        |           0      |  0.510125 |  1.11987  |  1.36388  |
| bahamut | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_LearnedIndex   |   1307.04        |           0      |  2.5705   |  5.27371  |  7.28304  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoPut                 | 205740           |         150.688  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoGet                 |      1.40445e+06 |        1028.65   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Dense          |    671.776       |           0      |  4.98212  | 10.7324   | 13.2872   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Hybrid         |   1254.9         |           0      |  2.60279  |  6.85375  | 10.5443   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Filtered       |    806.412       |           0      |  3.57292  | 10.2341   | 12.2116   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredBool   |   1223.07        |           0      |  2.88133  |  6.10383  |  8.15287  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredString |   1307.59        |           0      |  2.74054  |  5.18842  |  7.51788  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Sparse         |  14483.2         |           0      |  0.230375 |  0.523958 |  0.639375 |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_ByID           |   4667.32        |           0      |  0.774917 |  1.29175  |  1.73796  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GraphRAG       |    949.393       |           0      |  3.39183  |  8.54125  | 10.5248   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GlobalGraphRAG |    723.268       |           0      |  3.86863  | 10.7332   | 19.7215   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Recommend      |   1760.33        |           0      |  1.7405   |  4.46129  |  5.93487  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Geo            |  10443.7         |           0      |  0.362708 |  0.516625 |  0.6555   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Temporal       |   6506.93        |           0      |  0.511959 |  1.22596  |  1.72225  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_LearnedIndex   |    883.538       |           0      |  4.15042  |  7.44429  | 12.5683   |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoPut                 | 537859           |         787.879  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoGet                 | 960830           |        1407.47   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Dense          |   7890.78        |           0      |  0.441959 |  0.551    |  1.00621  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Hybrid         |   7573.63        |           0      |  0.481666 |  0.724917 |  1.00579  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Filtered       |   8282.59        |           0      |  0.457791 |  0.61625  |  0.754417 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredBool   |   3992.41        |           0      |  0.943958 |  1.65617  |  2.09067  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredString |   5703.39        |           0      |  0.662416 |  0.899541 |  1.22412  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Sparse         |  31771.7         |           0      |  0.114333 |  0.210292 |  0.284709 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_ByID           |  10492.1         |           0      |  0.365625 |  0.480125 |  0.570125 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GraphRAG       |   2185.51        |           0      |  1.12954  |  4.48937  | 10.4282   |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GlobalGraphRAG |   2595.66        |           0      |  1.22958  |  3.59063  |  7.04992  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Recommend      |   5553.72        |           0      |  0.543709 |  1.45833  |  2.09912  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Geo            |   9943.55        |           0      |  0.370875 |  0.598833 |  0.696208 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Temporal       |  17097.1         |           0      |  0.216792 |  0.333791 |  0.432083 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_LearnedIndex   |   1953.22        |           0      |  2.04533  |  2.41521  |  2.77017  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | DoPut                 | 122401           |         179.299  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | DoGet                 | 111783           |         163.745  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Dense          |   1404.56        |           0      |  2.33188  |  4.968    |  7.365    |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Hybrid         |   1725.85        |           0      |  2.08613  |  3.69258  |  4.26479  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Filtered       |   1258.47        |           0      |  2.54083  |  5.58408  |  7.07092  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_FilteredBool   |   1080.47        |           0      |  2.861    |  6.79704  | 15.1003   |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_FilteredString |   1413.71        |           0      |  2.36283  |  5.19113  |  6.65171  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Sparse         |  20276.6         |           0      |  0.188958 |  0.29375  |  0.406167 |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_ByID           |   4259.74        |           0      |  0.764875 |  1.73237  |  2.30629  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_GraphRAG       |    462.279       |           0      |  8.14158  | 14.8698   | 20.8765   |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_GlobalGraphRAG |    605.568       |           0      |  4.91183  | 12.8445   | 25.2207   |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Recommend      |   2401.38        |           0      |  1.59842  |  1.97104  |  2.22362  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Geo            |   3066.47        |           0      |  1.22183  |  2.21258  |  3.28242  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Temporal       |   6432.17        |           0      |  0.538042 |  1.0965   |  1.29917  |
| bahamut | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_LearnedIndex   |   1107.16        |           0      |  2.80192  |  6.74179  |  9.54883  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoPut                 |      2.51141e+06 |         306.568  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoGet                 |      5.16885e+06 |         630.963  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Dense          |    518.019       |           0      |  9.19658  | 11.1195   | 11.9772   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Hybrid         |   3268.09        |           0      |  0.70675  |  4.25158  |  4.37792  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Filtered       |   1063.38        |           0      |  2.25075  | 10.297    | 11.3254   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredBool   |    419.827       |           0      |  9.89754  | 11.4533   | 12.2242   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredString |    433.781       |           0      |  9.72842  | 11.529    | 12.4109   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Sparse         |   2844.85        |           0      |  1.28583  |  2.46896  |  2.74542  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_ByID           |   9591.25        |           0      |  0.383917 |  0.624917 |  0.688541 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GraphRAG       |    607.001       |           0      |  5.27217  | 12.4248   | 18.5282   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GlobalGraphRAG |    973.068       |           0      |  3.25567  |  8.42329  | 11.996    |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Recommend      |   4471.38        |           0      |  0.791    |  1.63525  |  2.08679  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Geo            |   3181.93        |           0      |  1.13608  |  2.32604  |  3.23708  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Temporal       |   2962           |           0      |  1.21604  |  2.43604  |  3.52637  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_LearnedIndex   |    292.087       |           0      | 13.5868   | 17.2622   | 20.6611   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoPut                 | 309360           |          37.7637 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoGet                 |      1.81741e+06 |         221.852  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Dense          |   1707.37        |           0      |  2.09042  |  6.639    |  9.71921  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Hybrid         |  10760.9         |           0      |  0.326917 |  0.543583 |  0.632083 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Filtered       |   1475.71        |           0      |  2.08321  |  8.79721  | 11.3515   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredBool   |    636.789       |           0      |  7.49192  | 10.5722   | 11.9938   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredString |    608.608       |           0      |  7.58717  | 10.3326   | 12.1347   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Sparse         |   9176.81        |           0      |  0.212042 |  1.39504  |  2.13958  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_ByID           |  13818.9         |           0      |  0.28675  |  0.4205   |  0.4895   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GraphRAG       |    883.081       |           0      |  5.1505   |  9.14638  | 10.5802   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |    826.683       |           0      |  5.45646  |  9.55229  | 10.6019   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Recommend      |  15320.6         |           0      |  0.234125 |  0.405375 |  0.482334 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Geo            |   7904.24        |           0      |  0.490208 |  0.748875 |  0.94725  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Temporal       |  11322.8         |           0      |  0.29425  |  0.596792 |  0.73425  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_LearnedIndex   |    526.022       |           0      |  7.99954  | 10.8209   | 11.8913   |

