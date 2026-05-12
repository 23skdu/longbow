# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                        |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('ancalagon', 'cpu', 128, 'float16')   |       3696.53 |         751.65 |            760.34 |                655.17 |                 1009.63 |      2550.78 |                  984.36 |            910.87 |          928.69 |                668.48 |            1341.76 |         7613.59 |           2458.15 |
| ('ancalagon', 'cpu', 128, 'float32')   |       4058.5  |        3994.91 |           3822.53 |               3061.71 |                 3015.44 |      2919.04 |                 1700.48 |           1578.11 |         4223.14 |               2301.21 |            3956.47 |         6924.15 |           4295.5  |
| ('ancalagon', 'cpu', 128, 'float64')   |       4643.45 |        3103.94 |           2838.56 |               2037.26 |                 1803.64 |      3174.12 |                 1391.96 |           1134.46 |         2948.07 |               1659.6  |            3882.76 |         7896.95 |           4604.58 |
| ('ancalagon', 'cpu', 128, 'int16')     |       4382.56 |         408.88 |            493    |                328.36 |                  350.24 |      2179.36 |                 1101.89 |            705.05 |          531.77 |               1100.79 |            6764.34 |         1427.17 |           4191.64 |
| ('ancalagon', 'cpu', 128, 'int32')     |       4725.06 |        1604.64 |           1474.42 |               1498.61 |                 1504.13 |      4208.11 |                  983.94 |            969.36 |         1581.02 |                888.69 |            1656.94 |         7057.2  |           4420.75 |
| ('ancalagon', 'cpu', 128, 'int64')     |       3721.73 |         524.97 |            553.59 |                489.17 |                  556    |      3317.83 |                  558.96 |            570.46 |          566.78 |                479.44 |             565.33 |         5951.46 |           4545.87 |
| ('ancalagon', 'cpu', 128, 'int8')      |       3142.26 |        2010.17 |           2234.81 |               1887.32 |                 1788.4  |      2546.4  |                 1162.06 |           1139.54 |         3434.55 |                824.64 |            3221.22 |         6519.98 |           3100.41 |
| ('ancalagon', 'cpu', 128, 'uint16')    |       4499.03 |         518.79 |            516.47 |                481.56 |                  533.23 |      3032.21 |                  555.44 |            548.7  |          554.38 |                479.01 |             569.97 |         7640.85 |           4612.4  |
| ('ancalagon', 'cpu', 128, 'uint32')    |       4756.66 |         548.77 |            545.07 |                482.44 |                  525.55 |      3065.54 |                  555.25 |            550.1  |          554.52 |                469.56 |             547.8  |         7842.76 |           4354.8  |
| ('ancalagon', 'cpu', 128, 'uint64')    |       4372.81 |         586.77 |            541.34 |                489.05 |                  527.66 |      3006.63 |                  559.38 |            554.37 |          559.5  |                479.71 |             564.68 |         7459.63 |           4564.98 |
| ('ancalagon', 'cpu', 128, 'uint8')     |       5318.32 |        1092.41 |           1105.9  |               1026.88 |                 1353.56 |      3228.83 |                 1595.5  |           1291.61 |         4326.84 |               1106.9  |            4652.61 |         7733.89 |           4580.85 |
| ('bahamut', 'cpu', 128, 'complex128')  |       1254.15 |        2275.51 |           2143.79 |               1920.01 |                 1695.63 |      2428.75 |                 1208.33 |           1144.39 |         2262.5  |                720.95 |            2736.58 |         5549.31 |           3634.77 |
| ('bahamut', 'cpu', 128, 'complex64')   |       2796.93 |         671.2  |           1257.5  |               1103.77 |                 1195.61 |      2643.9  |                  744.43 |            750.73 |         1250.11 |                483.61 |            2458.34 |         4813.01 |           3628.5  |
| ('bahamut', 'cpu', 128, 'float16')     |       5166.94 |        1425.15 |           1379.63 |               1221.35 |                 1289    |      4812.93 |                 1026.21 |           1001.47 |         1565.65 |               1120.36 |            1584.4  |         7925.81 |           3865.95 |
| ('bahamut', 'cpu', 128, 'float32')     |       5101.44 |        4083.48 |           4394.51 |               3581.26 |                 2686.97 |      5131.47 |                 1864.11 |           1679.23 |         4629.26 |               3044.64 |            5729.28 |         7754.45 |           6432.59 |
| ('bahamut', 'cpu', 128, 'float64')     |       4498.94 |        2971.89 |           3431.32 |               2622.32 |                 2261.96 |      4445.39 |                 1408.27 |           1509.05 |         3739.52 |               1928.9  |            4222.08 |         8981.83 |           5366.27 |
| ('bahamut', 'cpu', 128, 'int16')       |       4043    |         547.83 |            499.18 |                443.62 |                  452.77 |      3908.68 |                  500.68 |            527.41 |          541.83 |                443.62 |             560.48 |         6047.15 |           3770.53 |
| ('bahamut', 'cpu', 128, 'int32')       |       5203.21 |        1155.19 |           1583.75 |               1443.23 |                 1479.31 |      5754.39 |                 1077.79 |           1087.17 |         1475.47 |               1179.72 |            1901.95 |        10301.3  |           4591.76 |
| ('bahamut', 'cpu', 128, 'int64')       |       3987.43 |         502.49 |            544.25 |                474.02 |                  498.5  |      4926.1  |                  519.95 |            518.03 |          671.68 |                604.62 |             685.37 |         7768.14 |           4221.52 |
| ('bahamut', 'cpu', 128, 'int8')        |       4188.04 |        2235.69 |           3003.25 |               2121.16 |                 1771.69 |      4447.33 |                 1265.24 |           1256.11 |         2706.82 |                837.57 |            3432.13 |         6962.35 |           2385.99 |
| ('bahamut', 'cpu', 128, 'turboquant2') |       4443.63 |         725.14 |            749.96 |                682.87 |                  738.01 |      3102.18 |                 1639.19 |           1204.55 |         3427.35 |                725.37 |            3767.49 |         4550.54 |           3810.92 |
| ('bahamut', 'cpu', 128, 'turboquant4') |       4825.95 |         714.95 |            771.54 |               1036.56 |                 1038.17 |      2900.37 |                  709.5  |            970.05 |         3780    |                707.27 |            4324.95 |         5440.39 |           3071.15 |
| ('bahamut', 'cpu', 128, 'turboquant8') |       8429.7  |        1665.79 |           1546.68 |               1673.3  |                 1677.14 |      3194.27 |                  749.22 |           1689.44 |         7019.11 |                980.36 |            4569.54 |         9542.75 |           3639.52 |
| ('bahamut', 'cpu', 128, 'uint16')      |       5796.48 |         698.12 |            691.16 |                617.93 |                  625.35 |      4473.08 |                  549.8  |            571.62 |          698.82 |                609.8  |             654.3  |        10962.4  |           4330.46 |
| ('bahamut', 'cpu', 128, 'uint32')      |       5706.29 |         603.28 |            644.59 |                572.85 |                  624.85 |      2670.13 |                  697.41 |            674.49 |          649.59 |                283.57 |             540.37 |         7232.95 |           1896.13 |
| ('bahamut', 'cpu', 128, 'uint64')      |       2952.6  |         596.08 |            320.25 |                401.7  |                  509.31 |      3513.39 |                  327.57 |            350.29 |          351.9  |                344.17 |             469.09 |         1955.06 |           2375.79 |
| ('bahamut', 'cpu', 128, 'uint8')       |       4413.92 |        2306.71 |           2383.66 |               2524.1  |                 1653.73 |      4054.79 |                 1347.5  |           1303.47 |         2882.16 |               1132.67 |            3738.51 |         7094.37 |           3499.49 |
| ('bahamut', 'cpu', 384, 'float16')     |       1941.42 |        1177.48 |           1195.5  |                487.87 |                  552.77 |      2176.06 |                  480.11 |            371.56 |         1892.47 |                679.65 |             913.62 |         1180.28 |           2081.26 |
| ('bahamut', 'cpu', 384, 'float32')     |       5100.49 |        1611.84 |           1672.3  |               1786.79 |                 1694.47 |      3355.85 |                  966.81 |           1226.99 |         3919.74 |                726.25 |            2683    |         9572.29 |           3761.44 |
| ('bahamut', 'cpu', 384, 'float64')     |       2227.04 |         836.64 |            971.96 |                958.66 |                  936.37 |      3331.15 |                  757.86 |            741.13 |         2272.32 |                745.64 |            3189.22 |         5552.56 |           3599.48 |

## Ingestion Performance (MB/s)

|                                        |   Throughput_MBs |
|:---------------------------------------|-----------------:|
| ('ancalagon', 'cpu', 128, 'float16')   |           296.08 |
| ('ancalagon', 'cpu', 128, 'float32')   |           254.66 |
| ('ancalagon', 'cpu', 128, 'float64')   |           344.37 |
| ('ancalagon', 'cpu', 128, 'int16')     |           263.5  |
| ('ancalagon', 'cpu', 128, 'int32')     |           372.43 |
| ('ancalagon', 'cpu', 128, 'int64')     |           391.49 |
| ('ancalagon', 'cpu', 128, 'int8')      |           173.53 |
| ('ancalagon', 'cpu', 128, 'uint16')    |           254.29 |
| ('ancalagon', 'cpu', 128, 'uint32')    |           369.7  |
| ('ancalagon', 'cpu', 128, 'uint64')    |           418.73 |
| ('ancalagon', 'cpu', 128, 'uint8')     |           213.53 |
| ('bahamut', 'cpu', 128, 'complex128')  |           741.12 |
| ('bahamut', 'cpu', 128, 'complex64')   |           641.17 |
| ('bahamut', 'cpu', 128, 'float16')     |           581.59 |
| ('bahamut', 'cpu', 128, 'float32')     |           529.79 |
| ('bahamut', 'cpu', 128, 'float64')     |           983.62 |
| ('bahamut', 'cpu', 128, 'int16')       |           512.6  |
| ('bahamut', 'cpu', 128, 'int32')       |           521.15 |
| ('bahamut', 'cpu', 128, 'int64')       |           765.58 |
| ('bahamut', 'cpu', 128, 'int8')        |           317.32 |
| ('bahamut', 'cpu', 128, 'turboquant2') |            42.96 |
| ('bahamut', 'cpu', 128, 'turboquant4') |            94.83 |
| ('bahamut', 'cpu', 128, 'turboquant8') |           191.57 |
| ('bahamut', 'cpu', 128, 'uint16')      |           509.69 |
| ('bahamut', 'cpu', 128, 'uint32')      |           538.98 |
| ('bahamut', 'cpu', 128, 'uint64')      |           808.94 |
| ('bahamut', 'cpu', 128, 'uint8')       |           288.47 |
| ('bahamut', 'cpu', 384, 'float16')     |           846.54 |
| ('bahamut', 'cpu', 384, 'float32')     |           923.86 |
| ('bahamut', 'cpu', 384, 'float64')     |          1247.98 |

### Details: ancalagon (cpu)

| Host      | Mode   | Dataset                     | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |    P99_ms |
|:----------|:-------|:----------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|----------:|
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | DoPut                 | 757145           |         369.7    |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | DoGet                 | 234702           |         114.601  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Dense          |    548.772       |           0      |  6.88251  |  9.72896  | 11.3796   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Hybrid         |    554.521       |           0      |  6.6685   |  9.71821  | 14.5574   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Filtered       |    545.073       |           0      |  6.70296  | 10.0736   | 13.9899   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_FilteredBool   |    482.441       |           0      |  7.73785  | 10.9887   | 15.257    |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_FilteredString |    525.547       |           0      |  6.98888  | 10.0947   | 14.7028   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Sparse         |   7842.76        |           0      |  0.496822 |  0.79948  |  0.951748 |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_ByID           |   4756.66        |           0      |  0.820142 |  1.19944  |  1.43627  |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_GraphRAG       |    550.104       |           0      |  6.66383  | 10.0193   | 15.019    |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_GlobalGraphRAG |    555.253       |           0      |  6.63737  |  9.68042  | 10.361    |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Recommend      |    547.798       |           0      |  6.7061   | 10.0402   | 13.9942   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Geo            |   3065.54        |           0      |  0.904857 |  1.59687  |  8.6997   |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_Temporal       |   4354.8         |           0      |  0.867601 |  1.46356  |  1.84093  |
| ancalagon | cpu    | bench_uint32_128_5000.json  | uint32  |   128 |    5000 | Search_LearnedIndex   |    469.559       |           0      |  7.95992  | 11.2044   | 16.2678   |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | DoPut                 | 762746           |         372.435  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | DoGet                 | 263346           |         128.587  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Dense          |   1604.64        |           0      |  2.15664  |  3.27403  |  3.62842  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Hybrid         |   1581.02        |           0      |  2.2389   |  3.32329  |  3.62582  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Filtered       |   1474.42        |           0      |  2.20266  |  3.43154  |  8.0301   |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_FilteredBool   |   1498.61        |           0      |  2.42423  |  3.53896  |  3.77643  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_FilteredString |   1504.13        |           0      |  2.38965  |  3.52984  |  3.73016  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Sparse         |   7057.2         |           0      |  0.562203 |  0.853027 |  0.978266 |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_ByID           |   4725.06        |           0      |  0.790402 |  1.29543  |  1.5748   |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_GraphRAG       |    969.363       |           0      |  3.50476  |  5.91809  | 12.7009   |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_GlobalGraphRAG |    983.942       |           0      |  3.37405  |  8.54176  | 10.1622   |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Recommend      |   1656.94        |           0      |  2.13478  |  3.27663  |  3.47952  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Geo            |   4208.11        |           0      |  0.898114 |  1.23365  |  1.65366  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_Temporal       |   4420.75        |           0      |  0.84637  |  1.38422  |  1.63085  |
| ancalagon | cpu    | bench_int32_128_5000.json   | int32   |   128 |    5000 | Search_LearnedIndex   |    888.694       |           0      |  4.27785  |  6.1984   |  9.74578  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | DoPut                 | 496624           |         242.492  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | DoGet                 | 261037           |         127.46   |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Dense          |   4319.61        |           0      |  0.72374  |  1.41759  |  3.65957  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Hybrid         |   4027.7         |           0      |  0.835491 |  1.43623  |  1.98101  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Filtered       |   4772.56        |           0      |  0.742316 |  1.3486   |  1.74107  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_FilteredBool   |   3706.25        |           0      |  0.901587 |  1.50726  |  2.21206  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_FilteredString |   3806.58        |           0      |  0.950632 |  1.60047  |  1.7763   |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Sparse         |   7685.56        |           0      |  0.515048 |  0.800394 |  0.922232 |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_ByID           |   5241.29        |           0      |  0.693836 |  1.29425  |  1.59553  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_GraphRAG       |   1629.96        |           0      |  1.74316  |  7.01109  | 24.509    |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_GlobalGraphRAG |   1839.84        |           0      |  1.69111  |  4.81457  | 11.4248   |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Recommend      |   4766.34        |           0      |  0.757098 |  1.38676  |  1.64022  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Geo            |   3118.74        |           0      |  0.865295 |  1.5511   |  9.73985  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Temporal       |   5292.75        |           0      |  0.705541 |  1.27306  |  1.56366  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_LearnedIndex   |   3619.58        |           0      |  1.062    |  1.61146  |  1.86051  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | DoPut                 |      1.23282e+06 |         300.982  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | DoGet                 |      1.03772e+06 |         253.349  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Dense          |    681.287       |           0      |  5.51567  |  8.61225  | 12.2074   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Hybrid         |    895.051       |           0      |  3.76039  |  7.66945  | 12.1558   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Filtered       |    819.186       |           0      |  4.67656  |  7.53801  |  8.99596  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_FilteredBool   |    686.472       |           0      |  5.50091  |  9.33587  | 10.797    |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_FilteredString |   1089.76        |           0      |  3.6108   |  4.86124  |  5.46509  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Sparse         |   7680.59        |           0      |  0.516759 |  0.753752 |  0.879039 |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_ByID           |   3806.74        |           0      |  1.00956  |  1.49917  |  1.7091   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_GraphRAG       |    977.726       |           0      |  3.3058   |  7.82306  | 14.4637   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_GlobalGraphRAG |   1030.73        |           0      |  3.23324  |  6.25637  | 12.7009   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Recommend      |   1313.91        |           0      |  2.19829  |  6.25404  |  7.78013  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Geo            |   2009.04        |           0      |  1.71384  |  2.94325  |  6.61748  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Temporal       |   1870.67        |           0      |  2.07158  |  3.04207  |  3.57828  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_LearnedIndex   |    562.343       |           0      |  6.24077  | 10.7832   | 22.5598   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | DoPut                 |      1.503e+06   |         183.471  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | DoGet                 |      2.16651e+06 |         264.466  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Dense          |   1154.45        |           0      |  3.4899   |  4.82095  |  6.23226  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Hybrid         |   3888.49        |           0      |  0.992532 |  1.49736  |  1.72201  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Filtered       |   1239.94        |           0      |  3.39723  |  4.75537  |  7.45801  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_FilteredBool   |   1375.96        |           0      |  3.17355  |  4.60269  |  5.20298  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_FilteredString |   1388.18        |           0      |  2.85307  |  4.65327  |  6.09726  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Sparse         |   6421.88        |           0      |  0.538595 |  0.878855 |  2.67599  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_ByID           |   2884.59        |           0      |  1.35411  |  2.05389  |  2.30235  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_GraphRAG       |   1061.09        |           0      |  3.04483  |  7.4164   | 10.0202   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_GlobalGraphRAG |   1072.66        |           0      |  3.10272  |  6.64184  | 11.8228   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Recommend      |   2790.37        |           0      |  1.39683  |  2.07155  |  2.38479  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Geo            |   2663.54        |           0      |  1.3237   |  2.73218  |  4.07034  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Temporal       |   2917.53        |           0      |  1.3212   |  2.08021  |  2.35831  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_LearnedIndex   |    691.963       |           0      |  5.16506  |  9.24971  | 10.267    |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | DoPut                 |      1.04156e+06 |         254.288  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | DoGet                 | 270919           |          66.1422 |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Dense          |    518.787       |           0      |  7.24256  |  9.85365  | 11.9441   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Hybrid         |    554.378       |           0      |  6.59196  |  9.717    | 14.2935   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Filtered       |    516.473       |           0      |  7.25527  | 10.3177   | 14.053    |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_FilteredBool   |    481.558       |           0      |  7.68909  | 10.967    | 14.7974   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_FilteredString |    533.233       |           0      |  6.85016  |  9.96336  | 13.4339   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Sparse         |   7640.85        |           0      |  0.51695  |  0.793582 |  0.905075 |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_ByID           |   4499.03        |           0      |  0.872633 |  1.27968  |  1.51412  |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_GraphRAG       |    548.703       |           0      |  6.66692  |  9.99212  | 13.4329   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_GlobalGraphRAG |    555.44        |           0      |  6.60242  |  9.83185  | 13.217    |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Recommend      |    569.97        |           0      |  6.51493  |  9.62633  | 10.1797   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Geo            |   3032.21        |           0      |  0.913665 |  1.88783  | 11.0235   |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_Temporal       |   4612.4         |           0      |  0.805535 |  1.3209   |  1.60104  |
| ancalagon | cpu    | bench_uint16_128_5000.json  | uint16  |   128 |    5000 | Search_LearnedIndex   |    479.011       |           0      |  7.84993  | 11.0308   | 16.3883   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | DoPut                 |      1.07931e+06 |         263.504  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | DoGet                 |      1.38946e+06 |         339.223  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Dense          |    408.877       |           0      |  9.57218  | 14.5971   | 17.2197   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Hybrid         |    531.765       |           0      |  6.70586  | 11.0266   | 13.1574   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Filtered       |    492.999       |           0      |  7.59094  | 10.943    | 14.7305   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_FilteredBool   |    328.363       |           0      | 11.3017   | 19.4361   | 24.747    |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_FilteredString |    350.243       |           0      | 11.2561   | 16.166    | 17.7409   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Sparse         |   1427.17        |           0      |  0.619714 |  8.05011  |  8.82108  |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_ByID           |   4382.56        |           0      |  0.874985 |  1.35278  |  1.6409   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_GraphRAG       |    705.051       |           0      |  5.80281  |  9.28274  | 13.3451   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_GlobalGraphRAG |   1101.89        |           0      |  3.49372  |  4.93551  |  5.24418  |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Recommend      |   6764.34        |           0      |  0.574199 |  0.854231 |  0.954372 |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Geo            |   2179.36        |           0      |  1.34182  |  4.47659  |  8.7739   |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_Temporal       |   4191.64        |           0      |  0.895115 |  1.44898  |  1.78031  |
| ancalagon | cpu    | bench_int16_128_5000.json   | int16   |   128 |    5000 | Search_LearnedIndex   |   1100.79        |           0      |  3.48604  |  4.94301  |  5.37519  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | DoPut                 | 377221           |         368.38   |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | DoGet                 | 683821           |         667.794  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Dense          |   3865.42        |           0      |  0.903553 |  1.44598  |  1.89775  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Hybrid         |   3913.46        |           0      |  0.938469 |  1.44191  |  3.20655  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Filtered       |   4303.68        |           0      |  0.875615 |  1.33804  |  1.58584  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_FilteredBool   |   3364.58        |           0      |  1.1301   |  1.59688  |  1.86883  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_FilteredString |   2489.86        |           0      |  1.38743  |  1.98187  |  4.24057  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Sparse         |   7883.4         |           0      |  0.503529 |  0.762104 |  0.837215 |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_ByID           |   5019.2         |           0      |  0.765346 |  1.22179  |  1.46946  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_GraphRAG       |   1203.04        |           0      |  2.29977  |  9.24774  | 29.2876   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_GlobalGraphRAG |   1429.39        |           0      |  2.33875  |  3.99951  | 12.3298   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Recommend      |   3280.93        |           0      |  0.981469 |  2.33387  |  4.5689   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Geo            |   2960.4         |           0      |  1.07658  |  2.53772  |  4.1117   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Temporal       |   4022.62        |           0      |  0.836132 |  1.77903  |  5.37287  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_LearnedIndex   |   2219.21        |           0      |  1.73209  |  2.5028   |  3.40982  |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | DoPut                 | 428778           |         418.728  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | DoGet                 | 234653           |         229.153  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Dense          |    586.767       |           0      |  6.24416  |  9.53524  | 11.5793   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Hybrid         |    559.502       |           0      |  6.50501  |  9.87148  | 14.0295   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Filtered       |    541.335       |           0      |  6.73314  |  9.90918  | 14.1679   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_FilteredBool   |    489.053       |           0      |  7.56675  | 10.8083   | 16.0172   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_FilteredString |    527.656       |           0      |  7.11832  |  9.91585  | 10.6626   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Sparse         |   7459.63        |           0      |  0.533455 |  0.800969 |  0.905706 |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_ByID           |   4372.81        |           0      |  0.878351 |  1.31159  |  1.46244  |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_GraphRAG       |    554.367       |           0      |  6.60532  |  9.86508  | 14.3124   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_GlobalGraphRAG |    559.382       |           0      |  6.57891  |  9.98161  | 13.0899   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Recommend      |    564.678       |           0      |  6.59605  |  9.90181  | 10.5946   |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Geo            |   3006.63        |           0      |  0.910991 |  1.62381  |  6.65084  |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_Temporal       |   4564.98        |           0      |  0.802295 |  1.37307  |  1.64793  |
| ancalagon | cpu    | bench_uint64_128_5000.json  | uint64  |   128 |    5000 | Search_LearnedIndex   |    479.713       |           0      |  7.69734  | 11.0261   | 19.1791   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | DoPut                 | 400890           |         391.494  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | DoGet                 | 207438           |         202.576  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Dense          |    524.97        |           0      |  7.20628  | 10.1237   | 13.9981   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Hybrid         |    566.785       |           0      |  6.47231  |  9.55215  | 13.1366   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Filtered       |    553.594       |           0      |  6.69945  |  9.57016  | 13.6472   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_FilteredBool   |    489.171       |           0      |  7.59842  | 10.653    | 14.7311   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_FilteredString |    555.998       |           0      |  6.785    |  9.34672  |  9.98008  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Sparse         |   5951.46        |           0      |  0.58877  |  1.24212  |  1.90562  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_ByID           |   3721.73        |           0      |  0.948386 |  1.6836   |  4.96569  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_GraphRAG       |    570.461       |           0      |  6.48754  |  9.40779  |  9.81056  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_GlobalGraphRAG |    558.964       |           0      |  6.60919  |  9.57629  | 13.8519   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Recommend      |    565.33        |           0      |  6.5431   |  9.64594  | 13.7144   |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Geo            |   3317.83        |           0      |  0.882471 |  1.32409  |  5.79647  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_Temporal       |   4545.87        |           0      |  0.812054 |  1.37015  |  1.68053  |
| ancalagon | cpu    | bench_int64_128_5000.json   | int64   |   128 |    5000 | Search_LearnedIndex   |    479.438       |           0      |  7.80395  | 10.933    | 15.9577   |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | DoPut                 |      1.7492e+06  |         213.525  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | DoGet                 | 270792           |          33.0557 |  0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Dense          |   1092.41        |           0      |  3.52624  |  4.67327  |  5.1941   |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Hybrid         |   4326.84        |           0      |  0.892753 |  1.36542  |  1.6527   |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Filtered       |   1105.9         |           0      |  3.53907  |  4.64044  |  5.23821  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_FilteredBool   |   1026.88        |           0      |  3.58699  |  5.07371  | 10.6027   |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_FilteredString |   1353.56        |           0      |  3.23669  |  4.43943  |  5.16472  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Sparse         |   7733.89        |           0      |  0.519875 |  0.758514 |  0.86559  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_ByID           |   5318.32        |           0      |  0.707305 |  1.13112  |  1.3616   |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_GraphRAG       |   1291.61        |           0      |  2.42205  |  5.02726  |  8.39387  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_GlobalGraphRAG |   1595.5         |           0      |  1.9992   |  5.87405  |  9.04036  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Recommend      |   4652.61        |           0      |  0.810116 |  1.26228  |  1.57148  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Geo            |   3228.83        |           0      |  0.927781 |  3.208    |  7.59216  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_Temporal       |   4580.85        |           0      |  0.813575 |  1.36744  |  1.61526  |
| ancalagon | cpu    | bench_uint8_128_5000.json   | uint8   |   128 |    5000 | Search_LearnedIndex   |   1106.9         |           0      |  3.51218  |  4.76958  |  5.32968  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | DoPut                 | 546458           |         266.825  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | DoGet                 |      1.28188e+06 |         625.918  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Dense          |   3670.21        |           0      |  0.832055 |  1.46094  |  8.26979  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Hybrid         |   4418.57        |           0      |  0.850353 |  1.43515  |  1.66306  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Filtered       |   2872.5         |           0      |  1.22085  |  2.6945   |  4.80776  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_FilteredBool   |   2417.16        |           0      |  1.58253  |  2.57074  |  2.90569  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_FilteredString |   2224.29        |           0      |  1.71686  |  2.79558  |  3.21122  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Sparse         |   6162.74        |           0      |  0.624871 |  1.0161   |  1.21011  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_ByID           |   2875.71        |           0      |  1.30019  |  2.50843  |  2.94673  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_GraphRAG       |   1526.26        |           0      |  2.38084  |  4.18624  |  6.10026  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_GlobalGraphRAG |   1561.12        |           0      |  2.17105  |  3.99616  |  7.95394  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Recommend      |   3146.59        |           0      |  1.2314   |  2.15184  |  2.57308  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Geo            |   2719.34        |           0      |  1.32558  |  2.22919  |  4.18933  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_Temporal       |   3298.25        |           0      |  1.1469   |  2.00523  |  2.34761  |
| ancalagon | cpu    | bench_float32_128_5000.json | float32 |   128 |    5000 | Search_LearnedIndex   |    982.834       |           0      |  3.60987  |  6.47691  |  8.06537  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | DoPut                 |      1.19268e+06 |         291.181  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | DoGet                 |      1.27186e+06 |         310.512  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Dense          |    822.021       |           0      |  4.16563  |  9.34929  | 11.6806   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Hybrid         |    962.33        |           0      |  3.71672  |  5.84335  |  9.76522  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Filtered       |    701.49        |           0      |  5.30186  |  8.93504  | 11.451    |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_FilteredBool   |    623.875       |           0      |  6.2714   | 11.1152   | 12.7273   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_FilteredString |    929.506       |           0      |  4.23002  |  5.76457  |  6.45299  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Sparse         |   7546.6         |           0      |  0.531485 |  0.774258 |  0.864888 |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_ByID           |   3586.31        |           0      |  1.03225  |  1.58102  |  1.86284  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_GraphRAG       |    844.014       |           0      |  4.25472  |  8.73152  | 11.2431   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_GlobalGraphRAG |    937.984       |           0      |  3.38932  |  7.98582  | 23.2657   |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Recommend      |   1369.62        |           0      |  2.78135  |  3.96091  |  5.24236  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Geo            |   3092.52        |           0      |  1.14495  |  2.30127  |  3.41351  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_Temporal       |   3045.62        |           0      |  1.18427  |  2.18087  |  4.78989  |
| ancalagon | cpu    | bench_float16_128_5000.json | float16 |   128 |    5000 | Search_LearnedIndex   |    774.608       |           0      |  4.83741  |  7.63219  |  9.3992   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | DoPut                 |      1.34009e+06 |         163.585  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | DoGet                 |      1.33183e+06 |         162.576  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Dense          |   2865.9         |           0      |  1.23497  |  1.87806  |  6.46955  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Hybrid         |   2980.6         |           0      |  1.3142   |  1.89063  |  2.14714  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Filtered       |   3229.68        |           0      |  1.21267  |  1.7322   |  1.99722  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_FilteredBool   |   2398.68        |           0      |  1.51905  |  2.52565  |  3.94305  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_FilteredString |   2188.62        |           0      |  1.87968  |  2.40138  |  2.83164  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Sparse         |   6618.07        |           0      |  0.599832 |  0.86239  |  1.04995  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_ByID           |   3399.94        |           0      |  1.13013  |  1.72142  |  2.1531   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_GraphRAG       |   1217.99        |           0      |  2.74546  |  6.47762  | 10.1085   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_GlobalGraphRAG |   1251.45        |           0      |  2.7324   |  5.69821  | 11.1157   |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Recommend      |   3652.07        |           0      |  1.07992  |  1.56981  |  1.76976  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Geo            |   2429.25        |           0      |  1.18673  |  3.22721  |  7.44199  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_Temporal       |   3283.29        |           0      |  1.16838  |  1.83605  |  2.23025  |
| ancalagon | cpu    | bench_int8_128_5000.json    | int8    |   128 |    5000 | Search_LearnedIndex   |    957.326       |           0      |  3.98819  |  6.0618   |  7.85894  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | DoPut                 | 328056           |         320.367  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | DoGet                 | 491030           |         479.521  |  0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Dense          |   2342.46        |           0      |  1.46345  |  2.90204  |  4.49673  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Hybrid         |   1982.67        |           0      |  1.98868  |  2.69614  |  3.17084  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Filtered       |   1373.44        |           0      |  2.24475  |  5.57549  | 14.2413   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_FilteredBool   |    709.946       |           0      |  5.0381   |  8.67314  |  9.82681  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_FilteredString |   1117.41        |           0      |  3.4974   |  4.75467  |  6.1801   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Sparse         |   7910.5         |           0      |  0.496755 |  0.770792 |  0.840587 |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_ByID           |   4267.69        |           0      |  0.858179 |  1.51697  |  3.00028  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_GraphRAG       |   1065.87        |           0      |  3.25842  |  6.32769  | 11.139    |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_GlobalGraphRAG |   1354.53        |           0      |  2.34228  |  5.88829  | 13.3048   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Recommend      |   4484.6         |           0      |  0.874601 |  1.32071  |  1.5251   |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Geo            |   3387.83        |           0      |  0.930247 |  1.36805  |  6.92262  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_Temporal       |   5186.55        |           0      |  0.722752 |  1.21537  |  1.52419  |
| ancalagon | cpu    | bench_float64_128_5000.json | float64 |   128 |    5000 | Search_LearnedIndex   |   1100           |           0      |  3.5338   |  4.85157  |  5.26519  |

### Details: bahamut (cpu)

| Host    | Mode   | Dataset                         | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |    P99_ms |
|:--------|:-------|:--------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|----------:|
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoPut                 | 656563           |         641.175  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoGet                 |      1.06827e+06 |        1043.23   |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Dense          |    671.199       |           0      |  3.77771  | 13.0607   | 14.7253   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Hybrid         |   1250.11        |           0      |  2.6125   |  5.79417  |  6.32825  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Filtered       |   1257.5         |           0      |  1.69517  |  8.27613  | 10.0691   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredBool   |   1103.77        |           0      |  2.61262  |  7.392    |  8.83704  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredString |   1195.61        |           0      |  3.02942  |  6.02242  |  7.95262  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Sparse         |   4813.01        |           0      |  0.817917 |  1.24475  |  1.44267  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_ByID           |   2796.93        |           0      |  1.39125  |  2.02679  |  2.37054  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GraphRAG       |    750.728       |           0      |  4.41742  |  9.84133  | 13.0835   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GlobalGraphRAG |    744.431       |           0      |  4.79517  | 10.6582   | 14.2083   |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Recommend      |   2458.34        |           0      |  1.62196  |  2.11525  |  2.34879  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Geo            |   2643.9         |           0      |  1.50817  |  2.00862  |  2.36975  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Temporal       |   3628.5         |           0      |  1.09987  |  1.55937  |  1.74367  |
| bahamut | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_LearnedIndex   |    483.611       |           0      |  8.55038  | 13.9812   | 14.7795   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoPut                 |      1.10382e+06 |         538.977  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoGet                 |      2.1903e+06  |        1069.48   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Dense          |    603.278       |           0      |  6.01333  |  9.83617  | 20.7067   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Hybrid         |    649.587       |           0      |  5.65958  |  8.01046  | 15.707    |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Filtered       |    644.593       |           0      |  6.16308  |  6.81225  |  8.85033  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredBool   |    572.848       |           0      |  6.88125  |  7.67996  | 10.8646   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredString |    624.848       |           0      |  6.30483  |  7.07025  |  9.9025   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Sparse         |   7232.95        |           0      |  0.416042 |  1.42608  |  2.82683  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_ByID           |   5706.29        |           0      |  0.680208 |  0.934375 |  1.06479  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GraphRAG       |    674.486       |           0      |  5.69217  |  6.60738  | 11.8972   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GlobalGraphRAG |    697.413       |           0      |  5.62254  |  6.05854  |  7.62854  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Recommend      |    540.365       |           0      |  5.64538  | 15.6427   | 18.4218   |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Geo            |   2670.13        |           0      |  1.50883  |  1.86233  |  2.03533  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Temporal       |   1896.13        |           0      |  1.99321  |  2.75358  |  3.91108  |
| bahamut | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_LearnedIndex   |    283.571       |           0      | 13.9567   | 19.5807   | 22.1659   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoPut                 |      1.05434e+06 |         514.816  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoGet                 | 494631           |         241.519  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Dense          |   1894.32        |           0      |  2.03779  |  2.21225  |  3.71929  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Hybrid         |   1865.12        |           0      |  2.04537  |  2.28054  |  2.75342  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Filtered       |   1920.11        |           0      |  2.07546  |  2.21825  |  2.40442  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredBool   |   1730.46        |           0      |  2.22879  |  2.47283  |  3.2075   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredString |   1778.55        |           0      |  2.24067  |  2.45842  |  2.59504  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Sparse         |  11101.4         |           0      |  0.360125 |  0.466292 |  0.521583 |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_ByID           |   5771.28        |           0      |  0.682417 |  0.850042 |  0.9575   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GraphRAG       |   1219.33        |           0      |  2.96492  |  4.03608  | 12.252    |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GlobalGraphRAG |   1253.54        |           0      |  2.97729  |  4.61029  |  7.28029  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Recommend      |   1918.5         |           0      |  2.01404  |  2.39283  |  3.48967  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Geo            |   6239.55        |           0      |  0.632541 |  0.822791 |  0.91375  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Temporal       |   5002.76        |           0      |  0.7845   |  0.97     |  1.09088  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_LearnedIndex   |   1254.53        |           0      |  3.10962  |  3.84454  |  4.90171  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoPut                 | 937683           |         457.853  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoGet                 | 793289           |         387.348  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Dense          |   5536.77        |           0      |  0.63825  |  0.978625 |  2.49867  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Hybrid         |   5970.57        |           0      |  0.64175  |  0.813208 |  1.15729  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Filtered       |   6070.54        |           0      |  0.634625 |  0.884792 |  1.03046  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredBool   |   5169.73        |           0      |  0.758333 |  0.943    |  1.13963  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredString |   4579.66        |           0      |  0.862541 |  1.03671  |  1.19754  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Sparse         |  10772.3         |           0      |  0.364416 |  0.511667 |  0.849375 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_ByID           |   6675.86        |           0      |  0.584125 |  0.7845   |  0.929084 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GraphRAG       |   2686.79        |           0      |  1.36787  |  2.06025  |  5.16725  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2560.46        |           0      |  1.37783  |  2.72412  |  5.55633  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Recommend      |   6389.21        |           0      |  0.607166 |  0.83325  |  0.99625  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Geo            |   5963.09        |           0      |  0.627542 |  0.928292 |  1.82058  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Temporal       |   7026.05        |           0      |  0.557333 |  0.747917 |  0.914375 |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_LearnedIndex   |   4439.9         |           0      |  0.885917 |  1.10479  |  1.21021  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoPut                 |      3.05483e+06 |         745.809  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoGet                 |      1.77397e+06 |         433.097  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Dense          |   1969.71        |           0      |  1.92292  |  2.22954  |  6.03654  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Hybrid         |   2030.76        |           0      |  1.93429  |  2.14604  |  2.92667  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Filtered       |   2073.13        |           0      |  1.91779  |  2.09567  |  2.28108  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredBool   |   1749.85        |           0      |  2.22021  |  2.46042  |  4.43412  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredString |   1913.64        |           0      |  2.07688  |  2.27696  |  2.45629  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Sparse         |  10971.8         |           0      |  0.366209 |  0.4865   |  0.560125 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_ByID           |   6763.7         |           0      |  0.574083 |  0.803333 |  0.978    |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GraphRAG       |   1305.11        |           0      |  2.70804  |  5.29488  |  7.88771  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GlobalGraphRAG |   1356.92        |           0      |  2.75342  |  4.17958  |  6.70867  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Recommend      |   2037.35        |           0      |  1.91538  |  2.20337  |  3.24208  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Geo            |   6345.87        |           0      |  0.62125  |  0.810542 |  0.915875 |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Temporal       |   4997.86        |           0      |  0.78825  |  0.958709 |  1.12388  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_LearnedIndex   |   1580.64        |           0      |  2.50263  |  2.90087  |  3.15192  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoPut                 |      2.75349e+06 |         336.12   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoGet                 |      2.44613e+06 |         298.6    |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Dense          |   3776.88        |           0      |  0.961208 |  1.28396  |  2.67621  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Hybrid         |   4104.45        |           0      |  0.963333 |  1.12321  |  1.23654  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Filtered       |   4111.32        |           0      |  0.962833 |  1.14479  |  1.30804  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredBool   |   3176.75        |           0      |  1.24658  |  1.47637  |  1.61533  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredString |   2559.52        |           0      |  1.47558  |  1.71037  |  3.148    |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Sparse         |  10943.8         |           0      |  0.365292 |  0.480667 |  0.548291 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_ByID           |   5894.27        |           0      |  0.668708 |  0.818584 |  0.933334 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GraphRAG       |   1721.44        |           0      |  2.00821  |  4.24887  |  7.30063  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GlobalGraphRAG |   1684.11        |           0      |  2.03812  |  4.20546  |  9.50667  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Recommend      |   4597.12        |           0      |  0.862166 |  1.01654  |  1.16154  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Geo            |   6344.15        |           0      |  0.612125 |  0.818584 |  0.950917 |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Temporal       |   2724.91        |           0      |  0.967292 |  3.15029  | 11.1787   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_LearnedIndex   |   1219.33        |           0      |  2.19075  |  8.57671  | 27.2141   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoPut                 |      1.55364e+06 |          94.8266 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoGet                 |      2.13843e+06 |         261.039  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Dense          |    714.946       |           0      |  5.41288  |  9.31037  | 10.8103   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Hybrid         |   3780           |           0      |  0.938791 |  2.01304  |  2.9085   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Filtered       |    771.539       |           0      |  5.30354  |  9.02675  | 10.6615   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredBool   |   1036.56        |           0      |  3.67917  |  5.98004  |  7.90817  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredString |   1038.17        |           0      |  3.81329  |  5.44308  |  6.38704  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Sparse         |   5440.39        |           0      |  0.725625 |  1.06546  |  1.26933  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_ByID           |   4825.95        |           0      |  0.812292 |  1.15163  |  1.30604  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GraphRAG       |    970.054       |           0      |  4.22342  |  5.84871  |  6.48183  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GlobalGraphRAG |    709.505       |           0      |  5.28708  |  9.52471  | 10.696    |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Recommend      |   4324.95        |           0      |  0.911166 |  1.26296  |  1.44658  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Geo            |   2900.37        |           0      |  1.25567  |  2.22967  |  3.0405   |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Temporal       |   3071.15        |           0      |  1.22096  |  2.08308  |  2.49267  |
| bahamut | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_LearnedIndex   |    707.269       |           0      |  5.47067  |  9.49713  | 10.5762   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoPut                 |      2.08768e+06 |         509.688  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoGet                 |      1.42626e+06 |         348.209  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Dense          |    698.119       |           0      |  5.37725  |  7.59933  | 11.2318   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Hybrid         |    698.821       |           0      |  5.58521  |  6.04017  |  9.35871  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Filtered       |    691.156       |           0      |  5.63379  |  6.12725  |  9.776    |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredBool   |    617.929       |           0      |  6.28779  |  7.05196  | 10.2473   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredString |    625.35        |           0      |  5.76679  |  9.05488  | 16.5176   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Sparse         |  10962.4         |           0      |  0.365375 |  0.483375 |  0.581209 |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_ByID           |   5796.48        |           0      |  0.67475  |  0.870041 |  1.00225  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GraphRAG       |    571.62        |           0      |  5.70733  | 12.5274   | 28.8152   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GlobalGraphRAG |    549.804       |           0      |  5.84908  | 14.097    | 20.5649   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Recommend      |    654.302       |           0      |  5.69429  |  7.21633  | 13.3157   |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Geo            |   4473.08        |           0      |  0.614375 |  1.18383  |  6.15054  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Temporal       |   4330.46        |           0      |  0.845458 |  1.19275  |  2.80575  |
| bahamut | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_LearnedIndex   |    609.803       |           0      |  6.40525  |  7.32788  | 10.2086   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoPut                 |      2.25844e+06 |         551.377  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoGet                 |      2.02795e+06 |         495.105  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Dense          |    707.99        |           0      |  5.5285   |  6.45958  |  8.50221  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Hybrid         |    702.673       |           0      |  5.58604  |  5.95117  |  9.49362  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Filtered       |    665.075       |           0      |  5.72071  |  6.65817  |  9.18262  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredBool   |    584.543       |           0      |  6.34092  |  8.88875  | 15.4318   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredString |    646.784       |           0      |  5.93046  |  6.68233  | 10.4893   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Sparse         |  11179.3         |           0      |  0.357083 |  0.48175  |  0.52975  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_ByID           |   5563.54        |           0      |  0.701167 |  0.904167 |  1.03833  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GraphRAG       |    682.603       |           0      |  5.61879  |  6.45704  |  8.10679  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GlobalGraphRAG |    703.937       |           0      |  5.576    |  6.222    |  7.45342  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Recommend      |    704.998       |           0      |  5.55217  |  5.91817  |  9.56833  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Geo            |   4826.72        |           0      |  0.625625 |  1.87125  |  5.35758  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Temporal       |   5033.23        |           0      |  0.783208 |  0.9265   |  1.04225  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_LearnedIndex   |    627.106       |           0      |  6.22954  |  6.85379  |  7.62892  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoPut                 | 425978           |        1247.98   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoGet                 | 314866           |         922.46   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Dense          |    836.643       |           0      |  4.6125   |  8.27367  | 11.9137   |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Hybrid         |   2272.32        |           0      |  1.61813  |  2.62208  |  3.52608  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Filtered       |    971.964       |           0      |  4.03079  |  7.36375  |  8.807    |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredBool   |    958.661       |           0      |  3.82175  |  7.58167  |  9.86363  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredString |    936.373       |           0      |  3.75258  |  7.57088  |  8.65125  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Sparse         |   5552.56        |           0      |  0.706625 |  1.11538  |  1.34408  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_ByID           |   2227.04        |           0      |  1.78833  |  2.45508  |  2.71892  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GraphRAG       |    741.13        |           0      |  4.76717  |  9.14833  | 11.9438   |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GlobalGraphRAG |    757.863       |           0      |  4.93767  |  8.97979  | 12.2333   |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Recommend      |   3189.22        |           0      |  1.17508  |  1.86083  |  2.35225  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Geo            |   3331.15        |           0      |  1.1745   |  1.61304  |  1.95579  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Temporal       |   3599.48        |           0      |  1.04804  |  1.63762  |  2.16975  |
| bahamut | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_LearnedIndex   |    745.637       |           0      |  5.17754  |  8.978    | 10.0695   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoPut                 |      1.22389e+06 |        1195.21   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoGet                 |      1.12435e+06 |        1098      |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Dense          |   4539.06        |           0      |  0.767    |  0.983042 |  5.51308  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Hybrid         |   4976.82        |           0      |  0.747542 |  1.06958  |  1.89187  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Filtered       |   5153.77        |           0      |  0.7585   |  0.995666 |  1.14863  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredBool   |   3752.27        |           0      |  1.04583  |  1.29237  |  1.5765   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredString |   3099.16        |           0      |  1.25508  |  1.51229  |  2.27663  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Sparse         |  11512           |           0      |  0.349375 |  0.459625 |  0.547792 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_ByID           |   5675.37        |           0      |  0.695625 |  0.863042 |  0.984125 |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GraphRAG       |   1954.27        |           0      |  1.84871  |  3.50829  |  6.06492  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GlobalGraphRAG |   1823.79        |           0      |  1.89096  |  4.10475  |  8.97483  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Recommend      |   5406.72        |           0      |  0.730958 |  0.911083 |  1.04388  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Geo            |   5604.3         |           0      |  0.621875 |  0.936291 |  2.50387  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Temporal       |   6173.69        |           0      |  0.590333 |  1.09146  |  1.3635   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_LearnedIndex   |   3079.97        |           0      |  1.27838  |  1.5875   |  1.73592  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoPut                 |      1.40769e+06 |          42.9593 |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoGet                 |      1.89227e+06 |         230.99   |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Dense          |    725.135       |           0      |  5.29367  |  9.081    | 10.1911   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Hybrid         |   3427.35        |           0      |  1.05058  |  1.95188  |  2.71442  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Filtered       |    749.957       |           0      |  5.11096  |  9.09829  | 10.2918   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredBool   |    682.867       |           0      |  5.68346  |  9.38283  | 10.6765   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredString |    738.005       |           0      |  5.17     |  8.99292  | 10.0862   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Sparse         |   4550.54        |           0      |  0.773334 |  1.68479  |  2.54121  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_ByID           |   4443.63        |           0      |  0.893166 |  1.23792  |  1.4365   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GraphRAG       |   1204.55        |           0      |  2.65412  |  7.54183  | 10.1197   |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GlobalGraphRAG |   1639.19        |           0      |  2.49975  |  3.95904  |  5.08679  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Recommend      |   3767.49        |           0      |  1.043    |  1.42192  |  1.60996  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Geo            |   3102.18        |           0      |  1.27367  |  1.77358  |  1.99329  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Temporal       |   3810.92        |           0      |  1.04304  |  1.43312  |  1.68242  |
| bahamut | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_LearnedIndex   |    725.372       |           0      |  5.33554  |  9.21208  | 10.763    |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoPut                 | 828357           |         808.943  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoGet                 |      1.11791e+06 |        1091.71   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Dense          |    596.082       |           0      |  5.881    | 11.7473   | 18.0599   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Hybrid         |    351.896       |           0      | 10.3811   | 17.36     | 23.7257   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Filtered       |    320.253       |           0      | 11.1062   | 19.1757   | 21.4101   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredBool   |    401.698       |           0      |  7.42779  | 18.4638   | 20.7247   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredString |    509.307       |           0      |  6.55337  | 14.2006   | 18.0727   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Sparse         |   1955.06        |           0      |  2.08825  |  3.63042  |  4.16608  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_ByID           |   2952.6         |           0      |  1.31517  |  1.89783  |  2.16054  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GraphRAG       |    350.29        |           0      | 10.5358   | 16.5931   | 18.9707   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GlobalGraphRAG |    327.574       |           0      | 11.4613   | 18.8743   | 21.6955   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Recommend      |    469.094       |           0      |  7.64746  | 13.4569   | 16.3103   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Geo            |   3513.39        |           0      |  1.12417  |  1.75325  |  1.9285   |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Temporal       |   2375.79        |           0      |  1.58992  |  2.46392  |  3.21442  |
| bahamut | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_LearnedIndex   |    344.17        |           0      | 11.1851   | 17.938    | 20.8352   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoPut                 | 694814           |         678.53   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoGet                 | 699884           |         683.48   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Dense          |    690.075       |           0      |  5.73913  |  6.60454  |  8.75962  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Hybrid         |    662.047       |           0      |  5.78608  |  7.03279  | 11.5142   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Filtered       |    525.391       |           0      |  6.12733  | 15.0061   | 28.6684   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredBool   |    566.145       |           0      |  6.45675  | 10.59     | 17.2944   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredString |    664.247       |           0      |  5.90571  |  6.2185   |  9.14087  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Sparse         |  11147.5         |           0      |  0.363958 |  0.474208 |  0.53725  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_ByID           |   5733.95        |           0      |  0.676958 |  0.899    |  1.01342  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GraphRAG       |    694.217       |           0      |  5.68933  |  5.93596  |  7.40912  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GlobalGraphRAG |    685.577       |           0      |  5.70254  |  6.20333  |  7.88812  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Recommend      |    695.628       |           0      |  5.68017  |  5.96829  |  7.06346  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Geo            |   4669.66        |           0      |  0.618208 |  0.888917 |  6.34717  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Temporal       |   4932.15        |           0      |  0.79675  |  0.967333 |  1.08017  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_LearnedIndex   |    605.887       |           0      |  6.45971  |  7.14708  |  8.11508  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoPut                 | 379455           |         741.123  |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoGet                 | 731253           |        1428.23   |  0        |  0        |  0        |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Dense          |   2275.51        |           0      |  1.58104  |  2.317    | 12.2886   |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Hybrid         |   2262.5         |           0      |  1.67325  |  2.40025  |  2.791    |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Filtered       |   2143.79        |           0      |  1.73463  |  2.81479  |  3.71287  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredBool   |   1920.01        |           0      |  2.00996  |  2.87462  |  3.26446  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredString |   1695.63        |           0      |  2.25254  |  3.40688  |  4.18671  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Sparse         |   5549.31        |           0      |  0.714792 |  1.02575  |  1.19812  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_ByID           |   1254.15        |           0      |  2.90875  |  4.95033  |  5.98379  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GraphRAG       |   1144.39        |           0      |  3.07275  |  6.65396  |  8.12613  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GlobalGraphRAG |   1208.33        |           0      |  3.02446  |  5.77163  |  8.23662  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Recommend      |   2736.58        |           0      |  1.41733  |  2.06875  |  2.43921  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Geo            |   2428.75        |           0      |  1.46796  |  2.80888  |  3.51287  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Temporal       |   3634.77        |           0      |  1.06117  |  1.57258  |  1.92004  |
| bahamut | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_LearnedIndex   |    720.947       |           0      |  5.37142  |  9.15008  | 10.2688   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoPut                 |      1.15581e+06 |         846.543  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoGet                 |      1.10884e+06 |         812.14   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Dense          |   1177.48        |           0      |  3.14117  |  4.59529  |  7.24417  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Hybrid         |   1892.47        |           0      |  2.08017  |  2.33167  |  3.50083  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Filtered       |   1195.5         |           0      |  3.25888  |  3.94596  |  5.82137  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredBool   |    487.872       |           0      |  8.92762  | 13.3323   | 15.5564   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredString |    552.772       |           0      |  5.30692  | 13.8273   | 21.169    |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Sparse         |   1180.28        |           0      |  3.24792  |  5.56783  |  5.67671  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_ByID           |   1941.42        |           0      |  1.96529  |  2.81438  |  3.44867  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GraphRAG       |    371.557       |           0      |  8.55117  | 25.795    | 58.6716   |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GlobalGraphRAG |    480.105       |           0      |  8.22446  | 13.5903   | 17.103    |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Recommend      |    913.617       |           0      |  4.40283  |  5.99925  |  7.29517  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Geo            |   2176.06        |           0      |  1.66621  |  3.05642  |  3.52483  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Temporal       |   2081.26        |           0      |  1.75558  |  3.32454  |  5.64542  |
| bahamut | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_LearnedIndex   |    679.65        |           0      |  3.51196  | 12.6182   | 14.6217   |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoPut                 | 630689           |         923.861  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoGet                 |      1.01514e+06 |        1487.03   |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Dense          |   1611.84        |           0      |  2.48371  |  2.87187  |  3.06375  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Hybrid         |   3919.74        |           0      |  0.926708 |  1.34013  |  3.56483  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Filtered       |   1672.3         |           0      |  2.41275  |  3.87438  |  7.37688  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredBool   |   1786.79        |           0      |  2.38558  |  3.12254  |  4.65358  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredString |   1694.47        |           0      |  2.46825  |  3.08113  |  3.84258  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Sparse         |   9572.29        |           0      |  0.4125   |  0.579125 |  0.660583 |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_ByID           |   5100.49        |           0      |  0.771375 |  0.97575  |  1.12783  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GraphRAG       |   1226.99        |           0      |  2.87954  |  5.86083  |  7.79062  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GlobalGraphRAG |    966.815       |           0      |  3.29337  |  8.11229  |  9.72033  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Recommend      |   2683           |           0      |  1.42546  |  2.188    |  2.75575  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Geo            |   3355.85        |           0      |  1.17483  |  1.58158  |  1.77592  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Temporal       |   3761.44        |           0      |  1.04458  |  1.52337  |  1.76058  |
| bahamut | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_LearnedIndex   |    726.246       |           0      |  5.23729  |  9.35458  | 10.6988   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoPut                 |      1.97804e+06 |         241.46   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoGet                 |      2.33259e+06 |         284.74   |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Dense          |   4015.24        |           0      |  0.950416 |  1.14933  |  2.12467  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Hybrid         |   4200.79        |           0      |  0.949791 |  1.08004  |  1.22992  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Filtered       |   4093.63        |           0      |  0.96225  |  1.14013  |  1.31258  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredBool   |   3718.09        |           0      |  1.06342  |  1.26113  |  1.46704  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredString |   2745.74        |           0      |  1.34433  |  1.61517  |  2.18804  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Sparse         |  11078.6         |           0      |  0.359459 |  0.479625 |  0.546333 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_ByID           |   6922.01        |           0      |  0.557125 |  0.785667 |  0.977875 |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GraphRAG       |   1928.83        |           0      |  1.75808  |  3.46292  |  7.00742  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GlobalGraphRAG |   1970.58        |           0      |  1.83975  |  3.6895   |  5.03079  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Recommend      |   4888.87        |           0      |  0.810041 |  0.936625 |  1.06687  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Geo            |   5150.04        |           0      |  0.633833 |  1.58321  |  2.07688  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Temporal       |   4692.76        |           0      |  0.829042 |  1.06167  |  1.26113  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_LearnedIndex   |   1654.24        |           0      |  2.39254  |  2.81829  |  2.99613  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoPut                 |      1.56935e+06 |         191.57   |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoGet                 |      1.77544e+06 |         216.728  |  0        |  0        |  0        |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Dense          |   1665.79        |           0      |  2.40242  |  2.78662  |  2.94958  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Hybrid         |   7019.11        |           0      |  0.548333 |  0.729958 |  0.836125 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Filtered       |   1546.68        |           0      |  2.42358  |  3.97904  |  5.55387  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredBool   |   1673.3         |           0      |  2.38137  |  2.74642  |  2.92663  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredString |   1677.14        |           0      |  2.37817  |  2.77537  |  2.96446  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Sparse         |   9542.75        |           0      |  0.413625 |  0.581708 |  0.686708 |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_ByID           |   8429.7         |           0      |  0.469166 |  0.645708 |  0.72625  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GraphRAG       |   1689.44        |           0      |  2.35963  |  2.72587  |  2.83967  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |    749.219       |           0      |  5.01271  | 10.7529   | 12.3831   |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Recommend      |   4569.54        |           0      |  0.843208 |  1.16596  |  1.33342  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Geo            |   3194.27        |           0      |  1.23271  |  1.73371  |  2.04029  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Temporal       |   3639.52        |           0      |  1.07929  |  1.58262  |  1.78296  |
| bahamut | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_LearnedIndex   |    980.363       |           0      |  2.91296  | 10.3416   | 11.9281   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoPut                 |      1.08028e+06 |         527.482  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoGet                 |      1.9973e+06  |         975.246  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Dense          |    416.074       |           0      |  9.43371  | 14.0424   | 15.84     |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Hybrid         |   1085.82        |           0      |  2.51217  |  6.24354  |  6.72433  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Filtered       |   1247.39        |           0      |  3.16871  |  3.71183  |  3.999    |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredBool   |   1156           |           0      |  3.43046  |  4.06079  |  4.53379  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredString |   1180.06        |           0      |  3.31233  |  4.01504  |  4.34971  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Sparse         |   9501.18        |           0      |  0.414458 |  0.59875  |  0.658666 |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_ByID           |   4635.14        |           0      |  0.845416 |  1.08325  |  1.29562  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GraphRAG       |    955.018       |           0      |  3.95021  |  5.24854  |  9.09671  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GlobalGraphRAG |    902.035       |           0      |  4.03842  |  7.10612  | 10.8899   |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Recommend      |   1885.41        |           0      |  2.10433  |  2.36004  |  2.49812  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Geo            |   5269.22        |           0      |  0.732958 |  0.979667 |  1.10842  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Temporal       |   4180.75        |           0      |  0.935292 |  1.19612  |  1.39654  |
| bahamut | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_LearnedIndex   |   1104.91        |           0      |  3.37154  |  4.91963  |  9.41687  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoPut                 |      1.23235e+06 |         601.733  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoGet                 |      1.90709e+06 |         931.198  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Dense          |   2630.19        |           0      |  1.27146  |  1.91742  |  7.18117  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Hybrid         |   3287.95        |           0      |  1.15892  |  1.78429  |  2.07396  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Filtered       |   2718.47        |           0      |  1.44512  |  1.90621  |  2.16283  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredBool   |   1992.8         |           0      |  1.87658  |  2.56212  |  3.39938  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredString |    794.292       |           0      |  4.94058  |  8.85158  | 10.3953   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Sparse         |   4736.6         |           0      |  0.739667 |  1.63662  |  2.78437  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_ByID           |   3527.03        |           0      |  1.09008  |  1.61767  |  1.87146  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GraphRAG       |    671.672       |           0      |  5.90879  |  9.57008  | 11.2451   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GlobalGraphRAG |   1167.77        |           0      |  2.83471  |  7.05858  | 10.2058   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Recommend      |   5069.35        |           0      |  0.714708 |  1.34796  |  1.77692  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Geo            |   4299.85        |           0      |  0.807166 |  1.84067  |  2.5885   |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Temporal       |   5839.13        |           0      |  0.66825  |  0.962541 |  1.18633  |
| bahamut | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_LearnedIndex   |   1649.38        |           0      |  2.42517  |  2.83787  |  2.97204  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoPut                 |      1.70952e+06 |         417.364  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoGet                 |      3.50038e+06 |         854.585  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Dense          |    880.589       |           0      |  3.64804  |  8.41129  | 10.1038   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Hybrid         |   1100.55        |           0      |  3.32833  |  5.7495   |  6.89196  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Filtered       |    686.141       |           0      |  5.52033  |  9.27888  | 10.5392   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredBool   |    692.839       |           0      |  5.48575  |  9.14017  | 10.5149   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredString |    664.356       |           0      |  5.68854  |  9.63979  | 11.6504   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Sparse         |   4879.83        |           0      |  0.736459 |  1.43096  |  2.57071  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_ByID           |   3570.18        |           0      |  1.08908  |  1.57729  |  1.79092  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GraphRAG       |    697.832       |           0      |  5.1015   |  9.67788  | 12.3532   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GlobalGraphRAG |    695.5         |           0      |  5.03038  |  9.46525  | 11.1488   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Recommend      |   1131.44        |           0      |  3.48396  |  5.24667  |  6.31192  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Geo            |   3280           |           0      |  1.20171  |  1.63208  |  1.8485   |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Temporal       |   2734.03        |           0      |  1.3825   |  2.14487  |  2.65429  |
| bahamut | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_LearnedIndex   |    660.067       |           0      |  5.8      |  9.42533  | 10.8103   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoPut                 |      2.44554e+06 |         298.527  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoGet                 |      2.9393e+06  |         358.802  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Dense          |    694.489       |           0      |  2.758    | 13.1598   | 14.3975   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Hybrid         |   1309.19        |           0      |  2.40367  |  5.64642  |  5.77008  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Filtered       |   1895.18        |           0      |  2.17683  |  4.03337  |  6.22421  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredBool   |   1065.57        |           0      |  2.4725   |  8.90717  | 10.5744   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredString |    983.866       |           0      |  3.236    |  7.78538  |  9.30246  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Sparse         |   2980.87        |           0      |  0.857209 |  3.61504  |  4.51496  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_ByID           |   2481.81        |           0      |  1.57067  |  2.37267  |  2.65996  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GraphRAG       |    790.782       |           0      |  4.53462  |  9.12288  | 12.3019   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GlobalGraphRAG |    846.371       |           0      |  3.91467  |  9.25733  | 11.2586   |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Recommend      |   2267.14        |           0      |  1.65296  |  2.75729  |  3.47046  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Geo            |   2550.51        |           0      |  1.55896  |  2.05154  |  2.344    |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Temporal       |   2047.07        |           0      |  1.89821  |  2.58708  |  3.22146  |
| bahamut | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_LearnedIndex   |    455.814       |           0      |  8.30125  | 13.6237   | 14.6895   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoPut                 |      1.94081e+06 |         473.829  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoGet                 |      1.86625e+06 |         455.628  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Dense          |    387.663       |           0      | 10.1442   | 15.2629   | 17.7692   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Hybrid         |    380.992       |           0      |  9.23804  | 17.4323   | 22.5867   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Filtered       |    333.293       |           0      | 12.6399   | 18.6723   | 21.5964   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredBool   |    302.695       |           0      | 14.0715   | 19.4544   | 22.0149   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredString |    258.752       |           0      | 15.2829   | 19.8544   | 23.0371   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Sparse         |    914.999       |           0      |  4.82775  |  5.95558  |  6.26013  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_ByID           |   2522.45        |           0      |  1.55675  |  2.13921  |  2.33533  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GraphRAG       |    372.221       |           0      |  6.93833  | 19.3087   | 22.6817   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GlobalGraphRAG |    297.43        |           0      | 13.8501   | 18.6854   | 21.7431   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Recommend      |    415.97        |           0      |  7.88508  | 17.9391   | 24.2388   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Geo            |   2990.64        |           0      |  1.31283  |  1.84917  |  2.09375  |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Temporal       |   2507.83        |           0      |  1.53858  |  2.2855   |  2.8715   |
| bahamut | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_LearnedIndex   |    260.136       |           0      | 15.1418   | 21.2927   | 29.2772   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoPut                 | 790555           |         772.027  |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoGet                 |      1.52176e+06 |        1486.1    |  0        |  0        |  0        |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Dense          |   1404.72        |           0      |  2.62387  |  5.90783  |  8.54442  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Hybrid         |   2502.21        |           0      |  1.43429  |  2.62433  |  3.11092  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Filtered       |   1708.87        |           0      |  2.24554  |  4.70883  |  5.77779  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredBool   |   1492.36        |           0      |  2.62967  |  4.88783  |  6.37988  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredString |   1424.76        |           0      |  2.52813  |  4.87763  |  6.35804  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Sparse         |   6451.7         |           0      |  0.607709 |  0.959167 |  1.13896  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_ByID           |   3322.51        |           0      |  1.16875  |  1.76029  |  2.00454  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GraphRAG       |   1063.82        |           0      |  3.1335   |  6.97525  |  9.50883  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GlobalGraphRAG |    992.752       |           0      |  3.37892  |  7.47167  | 11.2876   |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Recommend      |   3037.44        |           0      |  1.25833  |  1.854    |  2.16296  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Geo            |   3286.48        |           0      |  1.19479  |  1.65771  |  1.96096  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Temporal       |   4558.85        |           0      |  0.832083 |  1.34579  |  1.62787  |
| bahamut | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_LearnedIndex   |    777.825       |           0      |  4.89046  |  8.76408  | 10.3715   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoPut                 | 873089           |         852.626  |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoGet                 |      1.26851e+06 |        1238.78   |  0        |  0        |  0        |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Dense          |    314.904       |           0      | 13.1191   | 18.6754   | 22.0372   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Hybrid         |    681.308       |           0      |  5.77767  |  6.17246  |  6.75854  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Filtered       |    563.114       |           0      |  6.83592  |  7.96004  | 13.1143   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredBool   |    381.889       |           0      |  8.38621  | 17.8574   | 21.1939   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredString |    332.763       |           0      | 11.4465   | 18.0312   | 20.2887   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Sparse         |   4388.75        |           0      |  0.624042 |  3.12142  |  4.33304  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_ByID           |   2240.91        |           0      |  1.75567  |  2.35704  |  2.61254  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GraphRAG       |    341.839       |           0      | 10.7933   | 17.3681   | 20.2839   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GlobalGraphRAG |    354.333       |           0      | 10.7012   | 19.9076   | 24.1613   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Recommend      |    675.103       |           0      |  5.71575  |  6.39379  | 11.2572   |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Geo            |   5182.54        |           0      |  0.708708 |  0.958833 |  2.86937  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Temporal       |   3510.88        |           0      |  0.919875 |  2.14617  |  2.94725  |
| bahamut | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_LearnedIndex   |    603.358       |           0      |  6.40517  |  7.38304  | 10.046    |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoPut                 |      2.7482e+06  |         335.473  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoGet                 |      3.84332e+06 |         469.155  |  0        |  0        |  0        |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Dense          |    598.185       |           0      |  6.81758  | 10.5437   | 11.9438   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Hybrid         |   1563.53        |           0      |  2.13621  |  5.04217  | 10.1742   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Filtered       |    673.684       |           0      |  6.00167  | 11.3414   | 19.3142   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredBool   |   1330.11        |           0      |  2.91317  |  3.79925  |  4.62887  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredString |    561.722       |           0      |  5.20988  | 16.1589   | 37.9285   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Sparse         |   3110.15        |           0      |  1.01142  |  2.89962  |  4.47121  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_ByID           |   1905.83        |           0      |  1.40596  |  6.34217  | 17.4507   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GraphRAG       |    678.109       |           0      |  5.65938  | 10.5205   | 13.835    |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GlobalGraphRAG |    724.414       |           0      |  5.04071  | 10.0597   | 11.9176   |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Recommend      |   2588.16        |           0      |  1.50642  |  2.10992  |  2.33188  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Geo            |   2959.54        |           0      |  1.32479  |  1.9075   |  2.28713  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Temporal       |   2306.22        |           0      |  1.70446  |  2.362    |  2.70029  |
| bahamut | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_LearnedIndex   |    611.111       |           0      |  6.18671  | 10.3298   | 11.8917   |

