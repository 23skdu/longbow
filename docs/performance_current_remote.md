# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                          |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:-----------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('ancalagon', 'cpu', 128, 'complex128')  |       1667.68 |        1269.26 |           1347.41 |               1346.55 |                 1214.51 |      6843.09 |                 1443.2  |           1435.27 |         4163.28 |               1171.87 |            4346.18 |         23184.5 |          11777.3  |
| ('ancalagon', 'cpu', 128, 'complex64')   |       7212.62 |        1146.79 |           3228.92 |               2601.79 |                 2306.3  |      6221    |                 1464.73 |           1388.24 |         4402.1  |               1185.58 |            6863.05 |         23884.8 |           8439.72 |
| ('ancalagon', 'cpu', 128, 'float16')     |       5835.82 |        1555.77 |           1714.96 |               1655.69 |                 1600.55 |      5878.8  |                 1034.84 |           1091.44 |         1765.14 |               1371.87 |            1807.96 |         24797.9 |           6003.36 |
| ('ancalagon', 'cpu', 128, 'float32')     |       9502.27 |        6107.41 |           6246.7  |               5971.01 |                 4869.85 |      4708.64 |                 1635.1  |           1676.86 |         5399.06 |               4600.86 |            7307.35 |         23487   |          12025.8  |
| ('ancalagon', 'cpu', 128, 'float64')     |       7671.89 |        4405.64 |           6644.96 |               4335.72 |                 2925.25 |      5772.19 |                 1283.11 |           1238.18 |         6028.08 |               2554.12 |            7409.1  |         23153.3 |          12196    |
| ('ancalagon', 'cpu', 128, 'int16')       |       7127.91 |         619.01 |            553.19 |                513.9  |                  565.82 |      4762.89 |                  564.46 |            578.13 |          557.59 |                409.76 |             587.99 |         24203.7 |           6194.61 |
| ('ancalagon', 'cpu', 128, 'int32')       |       6509.96 |        1593.4  |           1685.42 |               1587.42 |                 1629.55 |      5796.36 |                  968.73 |            972.84 |         1772.85 |                977.72 |            1686.08 |         24230.3 |           7014.9  |
| ('ancalagon', 'cpu', 128, 'int64')       |       7313.5  |         635.84 |            559.77 |                473.8  |                  563.34 |      4330.11 |                  566.17 |            556.28 |          559.68 |                490.59 |             565.78 |         23899.8 |           5266.05 |
| ('ancalagon', 'cpu', 128, 'int8')        |       8033.62 |        4635.61 |           6277.13 |               3893.49 |                 3235.79 |      4921.34 |                 1368.52 |           1376.84 |         6089.49 |               1466.53 |            7458.17 |         23901.8 |           7037.25 |
| ('ancalagon', 'cpu', 128, 'turboquant2') |      19547.6  |        1212.66 |           1199.72 |               1215.23 |                 1185.79 |      6937.63 |                 1208.66 |           1221.05 |        14451.8  |               1203.95 |           19124.7  |         23814.4 |          12047.3  |
| ('ancalagon', 'cpu', 128, 'turboquant4') |      12101.5  |        1265.34 |           1196.51 |               1254.63 |                 1254.38 |      6933.55 |                 1344.36 |           1245.2  |         9439.22 |               1189.67 |           11899.6  |         23177.5 |          12059.7  |
| ('ancalagon', 'cpu', 128, 'turboquant8') |      11330.3  |        1204.67 |           1199.33 |               1191.77 |                 1193.53 |      6899.11 |                 1204.81 |           1147.89 |         9387.97 |               1202.69 |           11248.6  |         24148.8 |          11956.7  |
| ('ancalagon', 'cpu', 128, 'uint16')      |       7381.9  |         591.15 |            517.05 |                475.07 |                  535.58 |      4217.52 |                  535.01 |            535.46 |          535.63 |                451.21 |             526.54 |         23982.6 |           5380.17 |
| ('ancalagon', 'cpu', 128, 'uint32')      |       6615.08 |         596.94 |            544.03 |                494.01 |                  538.85 |      4309.79 |                  537.44 |            551.91 |          541.43 |                459.27 |             562.34 |         23914.2 |           7042.44 |
| ('ancalagon', 'cpu', 128, 'uint64')      |       6964.76 |         563.39 |            534.45 |                491.63 |                  547.21 |      3767.57 |                  552.72 |            553.07 |          541.73 |                473.06 |             543.58 |         24034.1 |           6997.79 |
| ('ancalagon', 'cpu', 128, 'uint8')       |       9514.03 |        4075.19 |           7762.25 |               4931.41 |                 3912.09 |      6532.11 |                 2411.75 |           2327.75 |         5907.26 |               1170.82 |            8898.81 |         23774.6 |           7075.63 |
| ('ancalagon', 'cpu', 384, 'complex128')  |       1794.24 |        1361.1  |           1480.85 |               1129.96 |                  890.05 |      6454.62 |                 1125.52 |           1040.23 |         2453    |               1038.45 |            3649.09 |         23793.3 |          11653.8  |
| ('ancalagon', 'cpu', 384, 'complex64')   |       4949.96 |        1120.06 |           1190.05 |               1104.04 |                 1029.02 |      6592.76 |                 1428.21 |           1086.35 |         3931.65 |               1076.07 |            5623.52 |         23163.9 |          11579.1  |
| ('ancalagon', 'cpu', 384, 'float16')     |       2557.87 |        1729.11 |           1789.9  |               1639.49 |                 1523.58 |      4081.54 |                 1032.37 |           1129.75 |         1846.1  |               1002.26 |            1894.34 |         23895.7 |           6912.74 |
| ('ancalagon', 'cpu', 384, 'float32')     |       7949.3  |        1176.5  |           1187.84 |               1177.32 |                 2749.84 |      6767.51 |                 1844.97 |           2101.81 |         6338.93 |               1191.35 |            7098.73 |         23999   |          11655.9  |
| ('ancalagon', 'cpu', 384, 'float64')     |       6628.73 |        1264.2  |           1148.97 |               1187.01 |                 2230.66 |      6219.19 |                 1366.37 |           1360.85 |         4685.13 |               1159.22 |            6284.37 |         24026.9 |           7288.41 |
| ('ancalagon', 'cpu', 384, 'int16')       |       5070.68 |         515.14 |            524.96 |                477.35 |                  538.41 |      5644.53 |                  523.78 |            536.53 |          536.51 |                461.84 |             546.85 |         24251.7 |           7092.38 |
| ('ancalagon', 'cpu', 384, 'int32')       |       5230.09 |         964.33 |            969.71 |                844.8  |                  919.68 |      6627.6  |                  922.17 |            907.43 |         1671.85 |                971.84 |            1626.75 |         23698.2 |           7028.27 |
| ('ancalagon', 'cpu', 384, 'int64')       |       4836.58 |         527.49 |            481.03 |                508.87 |                  543.51 |      5881.97 |                  546.23 |            579.2  |          530.59 |                460.55 |             557.67 |         24458.7 |           7211.56 |
| ('ancalagon', 'cpu', 384, 'int8')        |       4889.76 |        1169    |           1182.3  |               1116.41 |                 1068.42 |      6360.01 |                 1419.35 |           1217.63 |         4106.63 |               1151.84 |            4453.92 |         23266.6 |           7088.1  |
| ('ancalagon', 'cpu', 384, 'turboquant2') |      15727.7  |        1245.66 |           1251.19 |               1252.7  |                 1611.68 |      6848.65 |                 2308.27 |           2084.07 |        10902    |               1189.11 |           10836.1  |         24380.5 |          11992.1  |
| ('ancalagon', 'cpu', 384, 'uint16')      |       4526.83 |         496.93 |            509.72 |                387.47 |                  522.49 |      5884.18 |                  518.49 |            544.56 |          528.34 |                460.49 |             541.53 |         23835.9 |           7002.49 |
| ('ancalagon', 'cpu', 384, 'uint32')      |       4665.92 |         500.22 |            480.07 |                437.67 |                  553.62 |      6158.66 |                  554.53 |            537.65 |          448.25 |                472.77 |             547.45 |         24093.9 |           4361.87 |
| ('ancalagon', 'cpu', 384, 'uint64')      |       4776.34 |         526.94 |            534.49 |                474.44 |                  542.56 |      3804.14 |                  553.46 |            531.9  |          528.21 |                479.16 |             541.51 |         25092   |           7064.38 |
| ('ancalagon', 'cpu', 384, 'uint8')       |       5739.57 |        1123.19 |           1140.06 |               1158.77 |                 1110.39 |      6482    |                 1554.15 |           1231.81 |         4026.03 |               1092.97 |            5494.79 |         23038.7 |           7117.71 |

## Ingestion Performance (MB/s)

|                                          |   Throughput_MBs |
|:-----------------------------------------|-----------------:|
| ('ancalagon', 'cpu', 128, 'complex128')  |           435.22 |
| ('ancalagon', 'cpu', 128, 'complex64')   |           332.68 |
| ('ancalagon', 'cpu', 128, 'float16')     |           284.43 |
| ('ancalagon', 'cpu', 128, 'float32')     |           298.57 |
| ('ancalagon', 'cpu', 128, 'float64')     |           430.63 |
| ('ancalagon', 'cpu', 128, 'int16')       |           263.55 |
| ('ancalagon', 'cpu', 128, 'int32')       |           373.5  |
| ('ancalagon', 'cpu', 128, 'int64')       |           431.96 |
| ('ancalagon', 'cpu', 128, 'int8')        |           205.55 |
| ('ancalagon', 'cpu', 128, 'turboquant2') |            24.28 |
| ('ancalagon', 'cpu', 128, 'turboquant4') |            44.62 |
| ('ancalagon', 'cpu', 128, 'turboquant8') |            85.75 |
| ('ancalagon', 'cpu', 128, 'uint16')      |           310.48 |
| ('ancalagon', 'cpu', 128, 'uint32')      |           309.43 |
| ('ancalagon', 'cpu', 128, 'uint64')      |           430.77 |
| ('ancalagon', 'cpu', 128, 'uint8')       |           172.73 |
| ('ancalagon', 'cpu', 384, 'complex128')  |           513.48 |
| ('ancalagon', 'cpu', 384, 'complex64')   |           432.06 |
| ('ancalagon', 'cpu', 384, 'float16')     |           398.78 |
| ('ancalagon', 'cpu', 384, 'float32')     |           455.88 |
| ('ancalagon', 'cpu', 384, 'float64')     |           470.8  |
| ('ancalagon', 'cpu', 384, 'int16')       |           384.86 |
| ('ancalagon', 'cpu', 384, 'int32')       |           447.91 |
| ('ancalagon', 'cpu', 384, 'int64')       |           447.03 |
| ('ancalagon', 'cpu', 384, 'int8')        |           295.48 |
| ('ancalagon', 'cpu', 384, 'turboquant2') |            26.06 |
| ('ancalagon', 'cpu', 384, 'uint16')      |           381.19 |
| ('ancalagon', 'cpu', 384, 'uint32')      |           394.01 |
| ('ancalagon', 'cpu', 384, 'uint64')      |           385.98 |
| ('ancalagon', 'cpu', 384, 'uint8')       |           364.31 |

### Details: ancalagon (cpu)

| Host      | Mode   | Dataset                         | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |    P95_ms |    P99_ms |
|:----------|:-------|:--------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|----------:|----------:|
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | DoPut                 | 994798           |         364.306  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | DoGet                 |      1.20019e+06 |         439.523  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Dense          |   1123.19        |           0      | 3.46305  |  4.51423  | 10.0209   |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Hybrid         |   4026.03        |           0      | 0.8287   |  1.25923  |  1.33581  |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Filtered       |   1140.06        |           0      | 3.37817  |  4.3343   |  4.87373  |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_FilteredBool   |   1158.77        |           0      | 3.42287  |  4.57562  |  5.04474  |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_FilteredString |   1110.39        |           0      | 3.64106  |  4.79556  |  5.33686  |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Sparse         |  23038.7         |           0      | 0.165559 |  0.277372 |  0.386852 |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_ByID           |   5739.57        |           0      | 0.657843 |  0.840122 |  0.975659 |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_GraphRAG       |   1231.81        |           0      | 2.42448  |  7.14105  | 12.4714   |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_GlobalGraphRAG |   1554.15        |           0      | 2.00619  |  3.905    | 10.6347   |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Recommend      |   5494.79        |           0      | 0.619685 |  0.984167 |  1.04782  |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Geo            |   6482           |           0      | 0.58615  |  0.735669 |  0.831309 |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_Temporal       |   7117.71        |           0      | 0.46942  |  0.836501 |  0.943434 |
| ancalagon | cpu    | bench_uint8_384_5000.json       | uint8       |   384 |    5000 | Search_LearnedIndex   |   1092.97        |           0      | 3.42588  |  4.80247  |  7.01581  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoPut                 | 340668           |         332.684  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | DoGet                 | 776508           |         758.308  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Dense          |   1146.79        |           0      | 3.37016  |  4.31046  | 10.9837   |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Hybrid         |   4402.1         |           0      | 0.616934 |  1.09268  |  3.483    |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Filtered       |   3228.92        |           0      | 0.83952  |  2.96066  |  3.42286  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredBool   |   2601.79        |           0      | 1.29511  |  3.24783  |  3.85272  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_FilteredString |   2306.3         |           0      | 1.55975  |  3.35441  |  4.29374  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Sparse         |  23884.8         |           0      | 0.158002 |  0.260109 |  0.361526 |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_ByID           |   7212.62        |           0      | 0.44849  |  0.823228 |  0.916218 |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GraphRAG       |   1388.24        |           0      | 2.29389  |  6.26109  | 12.3776   |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_GlobalGraphRAG |   1464.73        |           0      | 2.01907  |  6.19072  | 12.2509   |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Recommend      |   6863.05        |           0      | 0.478071 |  0.873234 |  0.955566 |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Geo            |   6221           |           0      | 0.554489 |  0.72909  |  0.90216  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_Temporal       |   8439.72        |           0      | 0.363286 |  0.9465   |  3.51351  |
| ancalagon | cpu    | bench_complex64_128_5000.json   | complex64   |   128 |    5000 | Search_LearnedIndex   |   1185.58        |           0      | 3.38626  |  4.22916  |  4.68753  |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoPut                 | 633707           |         309.427  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | DoGet                 | 971559           |         474.394  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Dense          |    596.937       |           0      | 6.37512  |  9.35106  | 10.7605   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Hybrid         |    541.431       |           0      | 6.78858  | 10.2006   | 14.8597   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Filtered       |    544.029       |           0      | 6.82594  | 10.0876   | 12.0602   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredBool   |    494.009       |           0      | 7.65215  | 10.8986   | 12.0124   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_FilteredString |    538.846       |           0      | 6.86958  |  9.90511  | 11.0208   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Sparse         |  23914.2         |           0      | 0.153657 |  0.266831 |  0.378239 |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_ByID           |   6615.08        |           0      | 0.568222 |  0.756131 |  0.848669 |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GraphRAG       |    551.909       |           0      | 6.75541  |  9.85933  | 10.808    |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_GlobalGraphRAG |    537.439       |           0      | 6.81448  | 10.1715   | 14.5784   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Recommend      |    562.342       |           0      | 6.634    |  9.83339  | 12.2808   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Geo            |   4309.79        |           0      | 0.575698 |  0.783624 | 14.7951   |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_Temporal       |   7042.44        |           0      | 0.477954 |  0.816297 |  0.905606 |
| ancalagon | cpu    | bench_uint32_128_5000.json      | uint32      |   128 |    5000 | Search_LearnedIndex   |    459.271       |           0      | 8.11087  | 11.163    | 12.5098   |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoPut                 | 764932           |         373.502  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | DoGet                 | 845189           |         412.69   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Dense          |   1593.4         |           0      | 1.9072   |  3.098    |  7.48181  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Hybrid         |   1772.85        |           0      | 1.96423  |  3.08644  |  3.21762  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Filtered       |   1685.42        |           0      | 2.0052   |  3.20016  |  4.65919  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredBool   |   1587.42        |           0      | 2.20326  |  3.38875  |  3.61423  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_FilteredString |   1629.55        |           0      | 2.15059  |  3.35627  |  3.51129  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Sparse         |  24230.3         |           0      | 0.156933 |  0.260415 |  0.333097 |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_ByID           |   6509.96        |           0      | 0.567985 |  0.762808 |  0.818055 |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GraphRAG       |    972.841       |           0      | 3.29623  |  6.68888  | 16.2706   |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_GlobalGraphRAG |    968.728       |           0      | 3.30679  |  7.96874  | 18.2772   |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Recommend      |   1686.08        |           0      | 1.9978   |  3.29651  |  4.10639  |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Geo            |   5796.36        |           0      | 0.588464 |  0.796742 |  3.0231   |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_Temporal       |   7014.9         |           0      | 0.471451 |  0.807672 |  0.958086 |
| ancalagon | cpu    | bench_int32_128_5000.json       | int32       |   128 |    5000 | Search_LearnedIndex   |    977.715       |           0      | 4.07971  |  5.45473  |  5.85227  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoPut                 | 611474           |         298.571  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | DoGet                 | 821887           |         401.312  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Dense          |   6107.41        |           0      | 0.389655 |  0.709158 |  6.05551  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Hybrid         |   5399.06        |           0      | 0.451761 |  0.805159 |  7.8958   |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Filtered       |   6246.7         |           0      | 0.410751 |  0.737429 |  6.72368  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredBool   |   5971.01        |           0      | 0.5232   |  0.933665 |  1.3966   |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_FilteredString |   4869.85        |           0      | 0.635591 |  1.13123  |  4.69914  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Sparse         |  23487           |           0      | 0.161523 |  0.258232 |  0.343811 |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_ByID           |   9502.27        |           0      | 0.342696 |  0.643644 |  0.709673 |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GraphRAG       |   1676.86        |           0      | 1.54523  |  8.79237  | 14.0791   |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_GlobalGraphRAG |   1635.1         |           0      | 1.53462  |  8.24086  | 16.1817   |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Recommend      |   7307.35        |           0      | 0.377069 |  0.702605 |  4.74349  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Geo            |   4708.64        |           0      | 0.556761 |  0.986783 |  7.55612  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_Temporal       |  12025.8         |           0      | 0.297003 |  0.464137 |  0.55327  |
| ancalagon | cpu    | bench_float32_128_5000.json     | float32     |   128 |    5000 | Search_LearnedIndex   |   4600.86        |           0      | 0.850519 |  1.26646  |  1.42857  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoPut                 |      1.16504e+06 |         284.433  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | DoGet                 |      1.25142e+06 |         305.521  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Dense          |   1555.77        |           0      | 2.36436  |  3.46376  |  4.46603  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Hybrid         |   1765.14        |           0      | 1.87644  |  3.00793  |  4.17939  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Filtered       |   1714.96        |           0      | 2.06666  |  3.22904  |  3.78957  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredBool   |   1655.69        |           0      | 2.21457  |  3.32382  |  3.68859  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_FilteredString |   1600.55        |           0      | 2.16773  |  3.35942  |  7.8037   |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Sparse         |  24797.9         |           0      | 0.154003 |  0.250083 |  0.329358 |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_ByID           |   5835.82        |           0      | 0.586694 |  0.95434  |  1.00493  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GraphRAG       |   1091.44        |           0      | 2.99532  |  8.00399  | 14.1153   |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_GlobalGraphRAG |   1034.84        |           0      | 3.0066   |  7.75883  | 19.5353   |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Recommend      |   1807.96        |           0      | 1.85342  |  2.95304  |  7.21689  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Geo            |   5878.8         |           0      | 0.555799 |  0.708832 |  1.06026  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_Temporal       |   6003.36        |           0      | 0.488494 |  0.857564 |  4.57347  |
| ancalagon | cpu    | bench_float16_128_5000.json     | float16     |   128 |    5000 | Search_LearnedIndex   |   1371.87        |           0      | 2.84655  |  3.8887   |  4.42084  |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoPut                 |      1.68385e+06 |         205.548  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | DoGet                 |      2.10531e+06 |         256.996  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Dense          |   4635.61        |           0      | 0.5577   |  0.859676 | 10.1308   |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Hybrid         |   6089.49        |           0      | 0.586111 |  0.86428  |  0.992663 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Filtered       |   6277.13        |           0      | 0.556628 |  0.843749 |  0.938687 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredBool   |   3893.49        |           0      | 0.839755 |  1.23926  |  1.41035  |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_FilteredString |   3235.79        |           0      | 0.999999 |  1.48834  |  4.08221  |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Sparse         |  23901.8         |           0      | 0.163268 |  0.247114 |  0.313445 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_ByID           |   8033.62        |           0      | 0.468445 |  0.623233 |  0.729888 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GraphRAG       |   1376.84        |           0      | 2.14727  |  6.89103  | 15.0589   |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_GlobalGraphRAG |   1368.52        |           0      | 2.16156  |  6.946    | 18.2324   |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Recommend      |   7458.17        |           0      | 0.474251 |  0.708885 |  0.760497 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Geo            |   4921.34        |           0      | 0.573977 |  0.843788 |  7.28276  |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_Temporal       |   7037.25        |           0      | 0.472005 |  0.821669 |  0.942689 |
| ancalagon | cpu    | bench_int8_128_5000.json        | int8        |   128 |    5000 | Search_LearnedIndex   |   1466.53        |           0      | 2.73937  |  3.43907  |  3.84319  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoPut                 | 731050           |          44.6197 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | DoGet                 | 863325           |         105.386  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Dense          |   1265.34        |           0      | 3.2742   |  4.28136  |  4.74511  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Hybrid         |   9439.22        |           0      | 0.384583 |  0.579771 |  0.668561 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Filtered       |   1196.51        |           0      | 3.32733  |  4.30649  |  4.87067  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredBool   |   1254.63        |           0      | 3.27323  |  4.26645  |  4.73223  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_FilteredString |   1254.38        |           0      | 3.27351  |  4.42928  |  5.84322  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Sparse         |  23177.5         |           0      | 0.163644 |  0.258951 |  0.374633 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_ByID           |  12101.5         |           0      | 0.294395 |  0.46208  |  0.530541 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GraphRAG       |   1245.2         |           0      | 3.28085  |  4.33839  |  4.62738  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_GlobalGraphRAG |   1344.36        |           0      | 3.19914  |  4.11186  |  4.60938  |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Recommend      |  11899.6         |           0      | 0.300874 |  0.479158 |  0.567005 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Geo            |   6933.55        |           0      | 0.557725 |  0.714779 |  0.809976 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_Temporal       |  12059.7         |           0      | 0.296798 |  0.46286  |  0.574968 |
| ancalagon | cpu    | bench_turboquant4_128_5000.json | turboquant4 |   128 |    5000 | Search_LearnedIndex   |   1189.67        |           0      | 3.39191  |  4.24281  |  4.58931  |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoPut                 |      1.27173e+06 |         310.48   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | DoGet                 |      1.51653e+06 |         370.248  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Dense          |    591.149       |           0      | 6.12157  |  9.50399  | 13.7586   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Hybrid         |    535.627       |           0      | 6.93135  | 10.2673   | 11.1973   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Filtered       |    517.048       |           0      | 7.12372  | 10.5905   | 12.4831   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredBool   |    475.067       |           0      | 7.91838  | 11.5092   | 15.9343   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_FilteredString |    535.578       |           0      | 6.94001  | 10.2443   | 12.3818   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Sparse         |  23982.6         |           0      | 0.157088 |  0.264999 |  0.371024 |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_ByID           |   7381.9         |           0      | 0.513723 |  0.680715 |  0.797059 |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GraphRAG       |    535.459       |           0      | 6.86246  | 10.3294   | 12.3898   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_GlobalGraphRAG |    535.005       |           0      | 6.94193  | 10.1963   | 12.2739   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Recommend      |    526.536       |           0      | 7.05311  | 10.3775   | 12.2193   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Geo            |   4217.52        |           0      | 0.599496 |  0.862085 | 13.1831   |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_Temporal       |   5380.17        |           0      | 0.551955 |  1.30767  |  4.60907  |
| ancalagon | cpu    | bench_uint16_128_5000.json      | uint16      |   128 |    5000 | Search_LearnedIndex   |    451.207       |           0      | 8.50168  | 11.555    | 14.1647   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoPut                 |      1.07948e+06 |         263.546  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | DoGet                 |      1.29654e+06 |         316.539  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Dense          |    619.014       |           0      | 5.88928  |  9.16756  | 11.665    |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Hybrid         |    557.592       |           0      | 6.49884  |  9.79359  | 13.649    |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Filtered       |    553.191       |           0      | 6.70293  | 10.1363   | 12.2896   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredBool   |    513.9         |           0      | 7.27543  | 10.6669   | 12.3233   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_FilteredString |    565.816       |           0      | 6.56958  |  9.67884  | 12.0149   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Sparse         |  24203.7         |           0      | 0.158386 |  0.248904 |  0.335872 |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_ByID           |   7127.91        |           0      | 0.513697 |  0.728949 |  0.794091 |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GraphRAG       |    578.131       |           0      | 6.40076  |  9.63529  | 10.7886   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_GlobalGraphRAG |    564.459       |           0      | 6.38389  |  9.57452  | 13.0391   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Recommend      |    587.991       |           0      | 6.34924  |  9.44772  | 10.6317   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Geo            |   4762.89        |           0      | 0.584459 |  0.860429 | 10.5946   |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_Temporal       |   6194.61        |           0      | 0.494458 |  1.35436  |  2.79344  |
| ancalagon | cpu    | bench_int16_128_5000.json       | int16       |   128 |    5000 | Search_LearnedIndex   |    409.755       |           0      | 8.13721  | 18.6446   | 25.2198   |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | DoPut                 |  87633.5         |         513.477  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | DoGet                 | 100045           |         586.198  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Dense          |   1361.1         |           0      | 2.76034  |  4.48469  |  5.46616  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Hybrid         |   2453           |           0      | 1.5012   |  2.13113  |  2.31876  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Filtered       |   1480.85        |           0      | 2.58254  |  4.39332  |  4.85922  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_FilteredBool   |   1129.96        |           0      | 3.33483  |  5.4255   |  6.27766  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_FilteredString |    890.054       |           0      | 4.29843  |  6.07676  |  6.76131  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Sparse         |  23793.3         |           0      | 0.161093 |  0.242284 |  0.336497 |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_ByID           |   1794.24        |           0      | 1.93079  |  3.06215  |  3.16188  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_GraphRAG       |   1040.23        |           0      | 3.04467  |  7.67128  | 13.1269   |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_GlobalGraphRAG |   1125.52        |           0      | 2.96019  |  8.11293  | 13.4111   |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Recommend      |   3649.09        |           0      | 0.951002 |  1.42284  |  1.55326  |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Geo            |   6454.62        |           0      | 0.594531 |  0.766887 |  0.945571 |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_Temporal       |  11653.8         |           0      | 0.310797 |  0.48216  |  0.607264 |
| ancalagon | cpu    | bench_complex128_384_5000.json  | complex128  |   384 |    5000 | Search_LearnedIndex   |   1038.45        |           0      | 3.91019  |  5.08542  |  5.46878  |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | DoPut                 | 131749           |         385.985  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | DoGet                 | 207634           |         608.303  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Dense          |    526.937       |           0      | 7.26808  | 10.0287   | 11.3421   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Hybrid         |    528.207       |           0      | 6.94658  | 10.3971   | 16.9045   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Filtered       |    534.493       |           0      | 7.09644  | 10.1583   | 11.0828   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_FilteredBool   |    474.445       |           0      | 7.7313   | 11.5604   | 17.1893   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_FilteredString |    542.564       |           0      | 6.93677  | 10.002    | 10.6594   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Sparse         |  25092           |           0      | 0.150959 |  0.251649 |  0.387095 |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_ByID           |   4776.34        |           0      | 0.725295 |  1.1436   |  1.22592  |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_GraphRAG       |    531.896       |           0      | 6.9146   | 10.3541   | 15.7305   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_GlobalGraphRAG |    553.463       |           0      | 6.74802  |  9.87448  | 10.3952   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Recommend      |    541.513       |           0      | 6.66481  | 10.0911   | 16.4428   |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Geo            |   3804.14        |           0      | 0.558155 |  0.707861 |  5.84814  |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_Temporal       |   7064.38        |           0      | 0.495119 |  0.83159  |  0.944573 |
| ancalagon | cpu    | bench_uint64_384_5000.json      | uint64      |   384 |    5000 | Search_LearnedIndex   |    479.163       |           0      | 8.00576  | 11.0256   | 11.9031   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | DoPut                 | 152587           |         447.034  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | DoGet                 | 106328           |         311.506  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Dense          |    527.493       |           0      | 6.90184  | 11.1521   | 16.6193   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Hybrid         |    530.588       |           0      | 6.694    | 10.6522   | 16.3757   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Filtered       |    481.027       |           0      | 7.0752   | 13.5608   | 16.0245   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_FilteredBool   |    508.867       |           0      | 7.44382  | 10.6896   | 11.3527   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_FilteredString |    543.512       |           0      | 6.69008  | 10.0438   | 15.1768   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Sparse         |  24458.7         |           0      | 0.154474 |  0.241378 |  0.394713 |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_ByID           |   4836.58        |           0      | 0.768082 |  1.01768  |  1.11773  |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_GraphRAG       |    579.201       |           0      | 6.36838  |  9.57043  | 10.0697   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_GlobalGraphRAG |    546.232       |           0      | 6.61587  |  9.86895  | 14.5671   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Recommend      |    557.668       |           0      | 6.57042  |  9.78287  | 15.0382   |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Geo            |   5881.97        |           0      | 0.566181 |  0.721791 |  1.12865  |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_Temporal       |   7211.56        |           0      | 0.466883 |  0.830437 |  0.966396 |
| ancalagon | cpu    | bench_int64_384_5000.json       | int64       |   384 |    5000 | Search_LearnedIndex   |    460.553       |           0      | 7.97455  | 11.0427   | 15.2814   |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | DoPut                 | 284592           |          26.0552 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | DoGet                 | 448832           |         164.367  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Dense          |   1245.66        |           0      | 3.26532  |  4.28899  |  4.56262  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Hybrid         |  10902           |           0      | 0.348669 |  0.515857 |  0.576395 |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Filtered       |   1251.19        |           0      | 3.27029  |  4.26872  |  4.786    |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_FilteredBool   |   1252.7         |           0      | 3.33307  |  4.33512  |  4.69947  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_FilteredString |   1611.68        |           0      | 2.85007  |  4.33907  |  5.21819  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Sparse         |  24380.5         |           0      | 0.154544 |  0.261376 |  0.336021 |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_ByID           |  15727.7         |           0      | 0.240118 |  0.356599 |  0.430384 |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_GraphRAG       |   2084.07        |           0      | 1.87846  |  3.92198  |  4.37418  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_GlobalGraphRAG |   2308.27        |           0      | 0.516031 |  3.64504  |  4.18755  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Recommend      |  10836.1         |           0      | 0.328533 |  0.507423 |  0.565212 |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Geo            |   6848.65        |           0      | 0.574038 |  0.7183   |  0.820376 |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_Temporal       |  11992.1         |           0      | 0.304994 |  0.469615 |  0.55611  |
| ancalagon | cpu    | bench_turboquant2_384_5000.json | turboquant2 |   384 |    5000 | Search_LearnedIndex   |   1189.11        |           0      | 3.33337  |  4.3263   |  4.75096  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoPut                 | 160699           |         470.798  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | DoGet                 | 216787           |         635.117  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Dense          |   1264.2         |           0      | 3.343    |  4.27596  |  4.62388  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Hybrid         |   4685.13        |           0      | 0.799886 |  1.13868  |  1.22579  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Filtered       |   1148.97        |           0      | 3.41402  |  4.34848  |  4.84294  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredBool   |   1187.01        |           0      | 3.35057  |  4.59736  |  7.08971  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_FilteredString |   2230.66        |           0      | 1.54194  |  2.7324   |  3.3826   |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Sparse         |  24026.9         |           0      | 0.158106 |  0.254782 |  0.318176 |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_ByID           |   6628.73        |           0      | 0.531477 |  0.814515 |  0.870739 |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GraphRAG       |   1360.85        |           0      | 2.28586  |  6.23268  | 13.6643   |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_GlobalGraphRAG |   1366.37        |           0      | 2.21112  |  4.48139  | 14.8592   |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Recommend      |   6284.37        |           0      | 0.567321 |  0.847605 |  0.911252 |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Geo            |   6219.19        |           0      | 0.570665 |  0.731821 |  0.879302 |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_Temporal       |   7288.41        |           0      | 0.372192 |  1.68932  |  4.25193  |
| ancalagon | cpu    | bench_float64_384_5000.json     | float64     |   384 |    5000 | Search_LearnedIndex   |   1159.22        |           0      | 3.42752  |  4.35806  |  4.87699  |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoPut                 | 440967           |         430.631  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | DoGet                 | 665520           |         649.922  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Dense          |   4405.64        |           0      | 0.571305 |  0.966109 | 13.5815   |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Hybrid         |   6028.08        |           0      | 0.589077 |  0.832694 |  1.02593  |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Filtered       |   6644.96        |           0      | 0.55167  |  0.777888 |  0.934971 |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredBool   |   4335.72        |           0      | 0.855354 |  1.19585  |  1.28903  |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_FilteredString |   2925.25        |           0      | 1.1239   |  1.64065  |  8.32641  |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Sparse         |  23153.3         |           0      | 0.162489 |  0.274491 |  0.344556 |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_ByID           |   7671.89        |           0      | 0.467954 |  0.742322 |  0.942103 |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GraphRAG       |   1238.18        |           0      | 2.23349  |  9.12053  | 19.1378   |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_GlobalGraphRAG |   1283.11        |           0      | 2.11287  |  8.79365  | 20.9503   |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Recommend      |   7409.1         |           0      | 0.475423 |  0.656414 |  0.760462 |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Geo            |   5772.19        |           0      | 0.553521 |  0.77355  |  5.57424  |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_Temporal       |  12196.1         |           0      | 0.296855 |  0.458963 |  0.549013 |
| ancalagon | cpu    | bench_float64_128_5000.json     | float64     |   128 |    5000 | Search_LearnedIndex   |   2554.12        |           0      | 1.55559  |  2.11063  |  3.01527  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoPut                 | 795522           |          24.2774 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | DoGet                 |      1.2053e+06  |         147.131  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Dense          |   1212.66        |           0      | 3.27375  |  4.22934  |  4.57426  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Hybrid         |  14451.8         |           0      | 0.267536 |  0.390053 |  0.515575 |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Filtered       |   1199.72        |           0      | 3.3221   |  4.19794  |  4.70618  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredBool   |   1215.23        |           0      | 3.27618  |  4.18411  |  4.5253   |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_FilteredString |   1185.79        |           0      | 3.37445  |  4.46722  |  5.21678  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Sparse         |  23814.4         |           0      | 0.158556 |  0.268988 |  0.333853 |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_ByID           |  19547.6         |           0      | 0.199245 |  0.292936 |  0.379731 |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GraphRAG       |   1221.05        |           0      | 3.2959   |  4.27365  |  4.69511  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_GlobalGraphRAG |   1208.66        |           0      | 3.26866  |  4.29175  |  4.6776   |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Recommend      |  19124.7         |           0      | 0.205579 |  0.299016 |  0.396739 |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Geo            |   6937.63        |           0      | 0.559382 |  0.7215   |  0.82199  |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_Temporal       |  12047.3         |           0      | 0.296062 |  0.46416  |  0.537981 |
| ancalagon | cpu    | bench_turboquant2_128_5000.json | turboquant2 |   128 |    5000 | Search_LearnedIndex   |   1203.95        |           0      | 3.29982  |  4.32679  |  4.74897  |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoPut                 | 441108           |         430.77   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | DoGet                 | 741181           |         723.81   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Dense          |    563.387       |           0      | 6.4922   |  9.51645  | 13.1243   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Hybrid         |    541.732       |           0      | 6.81682  | 10.2453   | 15.3209   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Filtered       |    534.452       |           0      | 6.93276  | 10.1219   | 11.1183   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredBool   |    491.633       |           0      | 7.65342  | 10.9769   | 12.7462   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_FilteredString |    547.209       |           0      | 6.75507  |  9.97663  | 11.0225   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Sparse         |  24034.1         |           0      | 0.156626 |  0.244592 |  0.328726 |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_ByID           |   6964.76        |           0      | 0.522129 |  0.747806 |  0.830377 |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GraphRAG       |    553.069       |           0      | 6.71777  | 10.0431   | 12.1679   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_GlobalGraphRAG |    552.718       |           0      | 6.7477   |  9.84878  | 10.8425   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Recommend      |    543.577       |           0      | 6.76898  | 10.0006   | 13.2863   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Geo            |   3767.57        |           0      | 0.59984  |  1.71257  | 12.8081   |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_Temporal       |   6997.79        |           0      | 0.477019 |  0.814147 |  0.942412 |
| ancalagon | cpu    | bench_uint64_128_5000.json      | uint64      |   128 |    5000 | Search_LearnedIndex   |    473.06        |           0      | 8.08096  | 11.0308   | 13.1745   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoPut                 | 442328           |         431.961  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | DoGet                 | 184344           |         180.023  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Dense          |    635.835       |           0      | 5.85431  |  8.81972  | 10.2289   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Hybrid         |    559.678       |           0      | 6.54039  | 10.0515   | 14.046    |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Filtered       |    559.774       |           0      | 6.53389  |  9.94385  | 10.5057   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredBool   |    473.801       |           0      | 8.09755  | 10.9748   | 13.1888   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_FilteredString |    563.336       |           0      | 6.58651  |  9.77643  | 11.5342   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Sparse         |  23899.8         |           0      | 0.157985 |  0.271432 |  0.36326  |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_ByID           |   7313.5         |           0      | 0.515888 |  0.674808 |  0.791496 |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GraphRAG       |    556.276       |           0      | 6.5619   | 10.0822   | 12.8226   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_GlobalGraphRAG |    566.166       |           0      | 6.66245  |  9.88889  | 11.14     |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Recommend      |    565.776       |           0      | 6.54144  |  9.64565  | 10.8196   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Geo            |   4330.11        |           0      | 0.575033 |  0.76661  | 11.9745   |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_Temporal       |   5266.05        |           0      | 0.523038 |  1.4789   |  3.36328  |
| ancalagon | cpu    | bench_int64_128_5000.json       | int64       |   128 |    5000 | Search_LearnedIndex   |    490.595       |           0      | 7.84621  | 10.82     | 13.3331   |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoPut                 | 222832           |         435.22   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | DoGet                 | 279788           |         546.461  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Dense          |   1269.26        |           0      | 3.32933  |  4.21909  |  4.65884  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Hybrid         |   4163.28        |           0      | 0.782208 |  1.12664  |  1.27946  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Filtered       |   1347.41        |           0      | 3.23091  |  4.1956   |  4.59576  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredBool   |   1346.55        |           0      | 3.14149  |  4.43428  |  4.82875  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_FilteredString |   1214.51        |           0      | 3.3924   |  4.72775  |  5.20947  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Sparse         |  23184.5         |           0      | 0.162433 |  0.273767 |  0.351816 |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_ByID           |   1667.68        |           0      | 2.04457  |  3.2812   |  5.99417  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GraphRAG       |   1435.27        |           0      | 2.22589  |  4.8299   | 12.8481   |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_GlobalGraphRAG |   1443.2         |           0      | 2.19052  |  3.39169  | 18.5469   |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Recommend      |   4346.18        |           0      | 0.701182 |  1.45312  |  4.31239  |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Geo            |   6843.09        |           0      | 0.570567 |  0.726374 |  0.886162 |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_Temporal       |  11777.3         |           0      | 0.310218 |  0.474332 |  0.602595 |
| ancalagon | cpu    | bench_complex128_128_5000.json  | complex128  |   128 |    5000 | Search_LearnedIndex   |   1171.87        |           0      | 3.4221   |  4.35623  |  4.74389  |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | DoPut                 | 520453           |         381.191  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | DoGet                 | 772641           |         565.899  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Dense          |    496.928       |           0      | 7.55896  | 12.2891   | 15.7045   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Hybrid         |    528.342       |           0      | 6.96605  | 10.4354   | 17.2252   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Filtered       |    509.718       |           0      | 7.1822   | 10.6724   | 15.0759   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_FilteredBool   |    387.472       |           0      | 8.23933  | 20.7066   | 23.7848   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_FilteredString |    522.487       |           0      | 6.98965  | 10.4196   | 14.2041   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Sparse         |  23835.9         |           0      | 0.157053 |  0.281565 |  0.432902 |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_ByID           |   4526.83        |           0      | 0.826748 |  1.07217  |  1.14955  |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_GraphRAG       |    544.558       |           0      | 6.93719  | 10.0002   | 10.8325   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_GlobalGraphRAG |    518.486       |           0      | 6.97654  | 10.5272   | 15.6382   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Recommend      |    541.534       |           0      | 6.71866  | 10.1725   | 15.8138   |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Geo            |   5884.18        |           0      | 0.57199  |  0.725039 |  0.929883 |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_Temporal       |   7002.49        |           0      | 0.466917 |  0.81794  |  0.936963 |
| ancalagon | cpu    | bench_uint16_384_5000.json      | uint16      |   384 |    5000 | Search_LearnedIndex   |    460.493       |           0      | 8.17365  | 11.0599   | 17.1787   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | DoPut                 | 525459           |         384.858  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | DoGet                 | 618844           |         453.255  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Dense          |    515.144       |           0      | 7.04605  | 11.3442   | 15.3479   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Hybrid         |    536.514       |           0      | 6.86691  | 10.2605   | 16.1546   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Filtered       |    524.957       |           0      | 6.95061  | 10.3292   | 15.9628   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_FilteredBool   |    477.351       |           0      | 7.83147  | 11.4826   | 16.8014   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_FilteredString |    538.407       |           0      | 6.93779  | 10.1408   | 10.6596   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Sparse         |  24251.7         |           0      | 0.15268  |  0.262128 |  0.408431 |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_ByID           |   5070.68        |           0      | 0.687583 |  1.0863   |  1.14711  |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_GraphRAG       |    536.526       |           0      | 6.78997  | 10.1969   | 15.5704   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_GlobalGraphRAG |    523.785       |           0      | 6.87048  | 10.2273   | 16.066    |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Recommend      |    546.853       |           0      | 6.7679   | 10.0319   | 16.2727   |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Geo            |   5644.53        |           0      | 0.571381 |  0.746392 |  2.50444  |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_Temporal       |   7092.38        |           0      | 0.475641 |  0.798002 |  0.882358 |
| ancalagon | cpu    | bench_int16_384_5000.json       | int16       |   384 |    5000 | Search_LearnedIndex   |    461.837       |           0      | 8.22985  | 11.344    | 17.3281   |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | DoPut                 | 806863           |         295.482  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | DoGet                 | 907272           |         332.253  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Dense          |   1169           |           0      | 3.41112  |  4.37296  |  4.81678  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Hybrid         |   4106.63        |           0      | 0.879124 |  1.35116  |  1.43985  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Filtered       |   1182.3         |           0      | 3.377    |  4.32549  |  4.94934  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_FilteredBool   |   1116.41        |           0      | 3.54323  |  4.71707  |  5.16645  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_FilteredString |   1068.42        |           0      | 3.7161   |  5.02379  |  5.63928  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Sparse         |  23266.6         |           0      | 0.160236 |  0.261985 |  0.356242 |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_ByID           |   4889.76        |           0      | 0.762424 |  0.979446 |  1.13584  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_GraphRAG       |   1217.63        |           0      | 2.49     |  6.26949  | 16.4789   |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_GlobalGraphRAG |   1419.35        |           0      | 2.18408  |  6.89384  | 11.9367   |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Recommend      |   4453.92        |           0      | 0.773602 |  1.24379  |  1.33266  |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Geo            |   6360.01        |           0      | 0.570721 |  0.732416 |  0.957187 |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_Temporal       |   7088.1         |           0      | 0.470869 |  0.807128 |  0.916531 |
| ancalagon | cpu    | bench_int8_384_5000.json        | int8        |   384 |    5000 | Search_LearnedIndex   |   1151.84        |           0      | 3.38003  |  4.50287  |  5.08343  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoPut                 | 544468           |         398.78   | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | DoGet                 | 195923           |         143.498  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Dense          |   1729.11        |           0      | 1.87582  |  3.04094  |  8.91643  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Hybrid         |   1846.1         |           0      | 1.91036  |  2.97379  |  3.15901  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Filtered       |   1789.9         |           0      | 1.9756   |  3.06164  |  3.29406  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredBool   |   1639.49        |           0      | 2.18325  |  3.33629  |  3.57744  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_FilteredString |   1523.58        |           0      | 2.19118  |  3.6748   |  7.1744   |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Sparse         |  23895.7         |           0      | 0.15472  |  0.252777 |  0.3772   |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_ByID           |   2557.87        |           0      | 1.26292  |  2.40414  |  2.53938  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GraphRAG       |   1129.75        |           0      | 3.0677   |  4.62682  | 11.195    |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_GlobalGraphRAG |   1032.37        |           0      | 3.08229  |  7.68943  | 14.772    |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Recommend      |   1894.34        |           0      | 1.84901  |  2.9263   |  3.1224   |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Geo            |   4081.54        |           0      | 0.594554 |  3.41266  |  8.38902  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_Temporal       |   6912.74        |           0      | 0.484955 |  0.883438 |  1.17465  |
| ancalagon | cpu    | bench_float16_384_5000.json     | float16     |   384 |    5000 | Search_LearnedIndex   |   1002.26        |           0      | 3.98992  |  5.41657  |  6.142    |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoPut                 | 311216           |         455.882  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | DoGet                 | 431543           |         632.144  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Dense          |   1176.5         |           0      | 3.40706  |  4.35546  |  4.80106  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Hybrid         |   6338.93        |           0      | 0.562922 |  0.936799 |  1.00881  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Filtered       |   1187.84        |           0      | 3.39469  |  4.26428  |  4.76043  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredBool   |   1177.32        |           0      | 3.3763   |  4.4635   |  4.93756  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_FilteredString |   2749.84        |           0      | 0.873291 |  2.99899  |  4.87997  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Sparse         |  23999           |           0      | 0.159412 |  0.239251 |  0.323141 |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_ByID           |   7949.3         |           0      | 0.414268 |  0.741923 |  0.817062 |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GraphRAG       |   2101.81        |           0      | 1.58278  |  3.01911  |  8.31995  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_GlobalGraphRAG |   1844.97        |           0      | 1.54533  |  4.82345  | 12.3693   |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Recommend      |   7098.73        |           0      | 0.455342 |  0.837097 |  1.11284  |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Geo            |   6767.51        |           0      | 0.566101 |  0.735817 |  0.855522 |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_Temporal       |  11655.9         |           0      | 0.306203 |  0.468424 |  0.589023 |
| ancalagon | cpu    | bench_float32_384_5000.json     | float32     |   384 |    5000 | Search_LearnedIndex   |   1191.35        |           0      | 3.37476  |  4.23937  |  4.81198  |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | DoPut                 | 268975           |         394.007  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | DoGet                 | 322354           |         472.198  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Dense          |    500.22        |           0      | 7.34127  | 11.6456   | 15.7888   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Hybrid         |    448.25        |           0      | 7.19576  | 16.529    | 19.941    |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Filtered       |    480.065       |           0      | 7.71979  | 11.3425   | 15.7298   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_FilteredBool   |    437.669       |           0      | 8.58073  | 12.2299   | 17.0975   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_FilteredString |    553.624       |           0      | 6.77701  |  9.83458  | 10.5407   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Sparse         |  24093.9         |           0      | 0.156923 |  0.250544 |  0.372528 |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_ByID           |   4665.92        |           0      | 0.800764 |  1.04322  |  1.1419   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_GraphRAG       |    537.647       |           0      | 6.79269  | 10.2732   | 15.2894   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_GlobalGraphRAG |    554.526       |           0      | 6.68304  |  9.82495  | 10.4751   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Recommend      |    547.446       |           0      | 6.69837  |  9.96055  | 14.6505   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Geo            |   6158.66        |           0      | 0.560426 |  0.711163 |  1.23699  |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_Temporal       |   4361.87        |           0      | 0.601067 |  3.19706  |  4.8366   |
| ancalagon | cpu    | bench_uint32_384_5000.json      | uint32      |   384 |    5000 | Search_LearnedIndex   |    472.773       |           0      | 8.12766  | 11.0762   | 11.9948   |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | DoPut                 | 305772           |         447.908  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | DoGet                 | 324832           |         475.828  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Dense          |    964.328       |           0      | 3.91331  |  5.6286   |  7.72994  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Hybrid         |   1671.85        |           0      | 2.08247  |  3.2002   |  3.36799  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Filtered       |    969.706       |           0      | 4.01506  |  5.58116  |  6.04839  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_FilteredBool   |    844.796       |           0      | 4.26018  |  5.81178  |  6.68783  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_FilteredString |    919.677       |           0      | 4.14696  |  7.33649  |  8.6626   |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Sparse         |  23698.2         |           0      | 0.157063 |  0.263383 |  0.371628 |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_ByID           |   5230.09        |           0      | 0.713831 |  0.936933 |  1.00435  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_GraphRAG       |    907.432       |           0      | 3.67554  |  9.2719   | 15.3052   |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_GlobalGraphRAG |    922.169       |           0      | 3.50793  |  5.4786   | 17.8948   |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Recommend      |   1626.75        |           0      | 2.04935  |  3.20361  |  4.95229  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Geo            |   6627.6         |           0      | 0.565976 |  0.737266 |  1.06118  |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_Temporal       |   7028.27        |           0      | 0.48609  |  0.825065 |  0.945088 |
| ancalagon | cpu    | bench_int32_384_5000.json       | int32       |   384 |    5000 | Search_LearnedIndex   |    971.841       |           0      | 4.0964   |  5.59636  |  6.19101  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | DoPut                 | 147475           |         432.056  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | DoGet                 | 261114           |         764.981  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Dense          |   1120.06        |           0      | 3.50532  |  4.42305  |  5.09998  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Hybrid         |   3931.65        |           0      | 0.900855 |  1.43579  |  1.55922  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Filtered       |   1190.05        |           0      | 3.42303  |  4.32835  |  4.68392  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_FilteredBool   |   1104.04        |           0      | 3.61108  |  4.89591  |  5.30715  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_FilteredString |   1029.02        |           0      | 3.90882  |  5.18357  |  5.7251   |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Sparse         |  23163.9         |           0      | 0.164409 |  0.25597  |  0.358045 |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_ByID           |   4949.96        |           0      | 0.677215 |  1.1902   |  1.25708  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_GraphRAG       |   1086.35        |           0      | 2.40212  |  6.08127  | 33.0242   |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_GlobalGraphRAG |   1428.21        |           0      | 2.23135  |  3.85064  | 10.1905   |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Recommend      |   5623.52        |           0      | 0.586738 |  1.01403  |  1.08365  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Geo            |   6592.76        |           0      | 0.577008 |  0.736042 |  0.844318 |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_Temporal       |  11579.2         |           0      | 0.309738 |  0.486184 |  0.61419  |
| ancalagon | cpu    | bench_complex64_384_5000.json   | complex64   |   384 |    5000 | Search_LearnedIndex   |   1076.07        |           0      | 3.60923  |  5.08777  |  7.93725  |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoPut                 |      1.41504e+06 |         172.735  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | DoGet                 |      2.59582e+06 |         316.872  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Dense          |   4075.19        |           0      | 0.512306 |  1.18394  | 19.481    |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Hybrid         |   5907.26        |           0      | 0.4895   |  0.700809 |  0.771755 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Filtered       |   7762.25        |           0      | 0.458703 |  0.671105 |  0.767571 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredBool   |   4931.41        |           0      | 0.71572  |  1.02363  |  1.1207   |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_FilteredString |   3912.09        |           0      | 0.890949 |  1.30273  |  1.39096  |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Sparse         |  23774.6         |           0      | 0.159637 |  0.25613  |  0.348241 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_ByID           |   9514.03        |           0      | 0.414099 |  0.521834 |  0.61026  |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GraphRAG       |   2327.75        |           0      | 1.32348  |  2.82034  |  7.3478   |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_GlobalGraphRAG |   2411.75        |           0      | 1.32169  |  3.67032  |  6.95349  |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Recommend      |   8898.81        |           0      | 0.411613 |  0.601241 |  0.665436 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Geo            |   6532.11        |           0      | 0.581402 |  0.734376 |  0.849353 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_Temporal       |   7075.63        |           0      | 0.464114 |  0.811595 |  0.880227 |
| ancalagon | cpu    | bench_uint8_128_5000.json       | uint8       |   128 |    5000 | Search_LearnedIndex   |   1170.82        |           0      | 3.29071  |  4.34011  |  4.72308  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoPut                 | 702504           |          85.7549 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | DoGet                 |      1.20676e+06 |         147.309  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Dense          |   1204.67        |           0      | 3.30152  |  4.24363  |  4.58842  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Hybrid         |   9387.97        |           0      | 0.388108 |  0.591088 |  0.675468 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Filtered       |   1199.33        |           0      | 3.3415   |  4.18525  |  4.61522  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredBool   |   1191.77        |           0      | 3.38361  |  4.13983  |  4.72543  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_FilteredString |   1193.53        |           0      | 3.35251  |  4.2945   |  4.64074  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Sparse         |  24148.8         |           0      | 0.157841 |  0.251977 |  0.335562 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_ByID           |  11330.3         |           0      | 0.304842 |  0.490312 |  0.563444 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GraphRAG       |   1147.89        |           0      | 3.38795  |  4.32087  |  4.76473  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   1204.81        |           0      | 3.3071   |  4.1404   |  4.55913  |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Recommend      |  11248.6         |           0      | 0.317685 |  0.499025 |  0.623827 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Geo            |   6899.11        |           0      | 0.563076 |  0.718904 |  0.827584 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_Temporal       |  11956.7         |           0      | 0.300061 |  0.471786 |  0.574119 |
| ancalagon | cpu    | bench_turboquant8_128_5000.json | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   1202.69        |           0      | 3.31333  |  4.33     |  4.63424  |

