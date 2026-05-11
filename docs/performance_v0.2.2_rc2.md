# Longbow v0.2.2-rc2 Performance Matrix

## Search Performance Summary (QPS)

|                                       |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:--------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('ancalagon', 'cpu', 128, 'float16')  |       2954.4  |        3209.12 |           3014.03 |               2793.62 |                 2667.92 |      2761.52 |                 2150.68 |           1989.1  |         3118.62 |               1904.45 |            1764.93 |         19153.3 |           2995.55 |
| ('ancalagon', 'cpu', 128, 'float32')  |       1833.16 |        2623.51 |           2170.99 |               2057.65 |                 2021.78 |      2622.84 |                 1558.71 |           1705.29 |         2140.11 |               1196.7  |            1327.63 |         15550.9 |           6064.57 |
| ('ancalagon', 'cpu', 128, 'float64')  |       2747.23 |        2084.83 |           2421.36 |               2293.42 |                 2248.62 |      2170.57 |                 1777.7  |           1710.45 |         2451.09 |               1196.72 |            1329.22 |         19913.8 |           5641.05 |
| ('ancalagon', 'cuda', 128, 'float32') |        682.86 |        1036.2  |           1116.19 |               1074.96 |                 1127.95 |      1521.04 |                  771.89 |            820.3  |         1085.36 |                573.09 |             810.57 |         12469.7 |           5527.65 |
| ('ancalagon', 'cuda', 128, 'float64') |       1988.05 |        1424.52 |           1589.59 |               1451.88 |                 1568.49 |      1091.07 |                 1229.76 |           1246.21 |         1588.13 |                592.4  |             768.19 |         22135   |           5412.85 |
| ('bahamut', 'cpu', 128, 'float16')    |      10122.6  |        5583.16 |           6810.61 |               6652.4  |                 5511.37 |      7357.15 |                 2751.05 |           3490.25 |         6219.62 |               3063.29 |            3311.71 |         33087.6 |           4559.35 |
| ('bahamut', 'cpu', 128, 'float32')    |       5843.1  |        5891.57 |           6031.01 |               5937.84 |                 5655.24 |      7236.18 |                 3565.19 |           3750.39 |         6141.39 |               4354.78 |            3578.8  |         29287.5 |           8228.12 |
| ('bahamut', 'cpu', 128, 'float64')    |       9073.32 |        3455.95 |           6271.86 |               6522.61 |                 6157.52 |      5499.37 |                 4221.48 |           5442.49 |         6135.98 |               4250.3  |            3806.72 |         34440.5 |           8327.42 |
| ('bahamut', 'cpu', 128, 'int8')       |       9493.54 |        4396.78 |           6931.62 |               6696.74 |                 6708.72 |      6060.86 |                 4256.68 |           4907.96 |         6860.58 |               3006.13 |            3742.55 |         31633.1 |           4243.49 |

## Ingestion Performance (MB/s)

|                                       |   Throughput_MBs |
|:--------------------------------------|-----------------:|
| ('ancalagon', 'cpu', 128, 'float16')  |           280.58 |
| ('ancalagon', 'cpu', 128, 'float32')  |           369.39 |
| ('ancalagon', 'cpu', 128, 'float64')  |           428.77 |
| ('ancalagon', 'cuda', 128, 'float32') |           387.36 |
| ('ancalagon', 'cuda', 128, 'float64') |           391.43 |
| ('bahamut', 'cpu', 128, 'float16')    |           501.56 |
| ('bahamut', 'cpu', 128, 'float32')    |           900.71 |
| ('bahamut', 'cpu', 128, 'float64')    |           659.35 |
| ('bahamut', 'cpu', 128, 'int8')       |           449.39 |

### Details: ancalagon (cpu)

| Host      | Mode   | Dataset                      | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |    P95_ms |    P99_ms |
|:----------|:-------|:-----------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|----------:|----------:|
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoPut                 | 465421           |          454.512 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoGet                 | 436268           |          426.043 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Dense          |   1269.4         |            0     | 2.42284  |  4.28603  | 27.7542   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Hybrid         |   1582.94        |            0     | 2.45804  |  3.52897  |  4.17657  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Filtered       |   1579.66        |            0     | 2.42688  |  3.57289  |  4.16445  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredBool   |   1434.86        |            0     | 2.55946  |  4.16892  |  6.12475  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredString |   1577.11        |            0     | 2.47371  |  3.70183  |  4.15654  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Sparse         |  20836.3         |            0     | 0.171144 |  0.351345 |  0.427239 |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_ByID           |   1922.71        |            0     | 2.01248  |  2.99701  |  3.53099  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GraphRAG       |   1300.64        |            0     | 2.85052  |  5.09079  |  6.32613  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GlobalGraphRAG |   1246.66        |            0     | 2.89802  |  5.5313   |  7.89555  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Recommend      |    749.104       |            0     | 4.8331   |  9.06213  | 11.2453   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Geo            |   1087.85        |            0     | 3.31977  |  6.02539  |  6.75445  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Temporal       |   5584.73        |            0     | 0.622101 |  0.985812 |  1.08745  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_LearnedIndex   |    596.738       |            0     | 5.88588  | 11.2394   | 12.5108   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 | 700322           |          341.954 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 | 709183           |          346.281 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   1157.83        |            0     | 3.23902  |  5.18086  |  5.67663  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   1070.62        |            0     | 3.52419  |  5.32523  |  6.31459  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   1104.37        |            0     | 3.46597  |  5.15608  |  6.57857  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   1052.19        |            0     | 3.45587  |  5.51946  |  7.36476  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   1173.91        |            0     | 3.34591  |  5.10788  |  6.16581  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |   9991.37        |            0     | 0.359756 |  0.766994 |  1.37218  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |    656.348       |            0     | 5.13024  | 11.9413   | 16.074    |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |    836.778       |            0     | 4.44557  |  8.39898  |  9.40625  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |    799.564       |            0     | 4.59045  |  8.56244  | 10.8148   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |    824.052       |            0     | 4.71933  |  6.96322  |  7.82314  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   1592.38        |            0     | 2.47985  |  3.07948  |  3.43997  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   5330.19        |            0     | 0.654838 |  0.985265 |  1.26079  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |    590.751       |            0     | 6.14341  | 10.9856   | 13.0136   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoPut                 | 435406           |          425.201 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoGet                 | 555546           |          542.525 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Dense          |   1814.46        |            0     | 0.971798 | 11.5069   | 30.2649   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Hybrid         |   3121.5         |            0     | 1.2279   |  1.71196  |  2.10338  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Filtered       |   3065.82        |            0     | 1.2311   |  1.7562   |  2.97316  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredBool   |   2920.16        |            0     | 1.35724  |  1.88168  |  2.33665  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredString |   2877.54        |            0     | 1.35131  |  1.92802  |  2.3158   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Sparse         |  20173.6         |            0     | 0.186384 |  0.259279 |  0.424581 |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_ByID           |   3372.78        |            0     | 1.13925  |  1.5858   |  2.00201  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GraphRAG       |   2106.09        |            0     | 1.70764  |  3.02876  |  3.6208   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GlobalGraphRAG |   2138.62        |            0     | 1.75899  |  2.74481  |  3.62362  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Recommend      |   1523.66        |            0     | 2.13419  |  3.22461  | 28.2509   |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Geo            |   2693.74        |            0     | 1.20798  |  3.03484  |  6.25486  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Temporal       |   5923.32        |            0     | 0.653811 |  0.958238 |  1.27618  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_LearnedIndex   |   1024.97        |            0     | 4.02597  |  5.14607  |  5.84977  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 | 692155           |          337.966 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 |      1.00063e+06 |          488.589 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   3355.61        |            0     | 0.596129 |  0.966513 | 23.7958   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   2892.13        |            0     | 0.98988  |  2.03072  |  6.06763  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   2818.82        |            0     | 1.34591  |  2.06384  |  2.4355   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   2719.73        |            0     | 1.4404   |  2.01726  |  2.58845  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   2599.83        |            0     | 1.52103  |  2.11521  |  2.38029  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |  18087.3         |            0     | 0.207893 |  0.347544 |  0.544043 |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |   2625.3         |            0     | 1.49518  |  2.04862  |  2.30794  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |   2143.45        |            0     | 1.75516  |  2.88497  |  3.47831  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |   1857.14        |            0     | 1.93336  |  3.52233  |  4.98264  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |   1678.32        |            0     | 2.28382  |  3.35817  |  3.93359  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   3336.76        |            0     | 1.14234  |  1.50318  |  2.4827   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   6738.52        |            0     | 0.51008  |  0.817075 |  1.1217   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |    985.464       |            0     | 4.14425  |  5.258    |  5.61136  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoPut                 | 416344           |          406.586 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoGet                 | 679131           |          663.213 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Dense          |   3170.63        |            0     | 1.17839  |  1.75839  |  2.14904  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Hybrid         |   2648.84        |            0     | 1.46203  |  2.09215  |  2.48005  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Filtered       |   2618.6         |            0     | 1.4886   |  2.07821  |  2.78938  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredBool   |   2525.25        |            0     | 1.54524  |  2.23242  |  2.62623  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredString |   2291.21        |            0     | 1.66003  |  2.43203  |  4.61504  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Sparse         |  18731.6         |            0     | 0.201343 |  0.313693 |  0.371826 |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_ByID           |   2946.2         |            0     | 1.32115  |  1.82147  |  2.23821  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GraphRAG       |   1724.6         |            0     | 2.20134  |  3.83306  |  4.70413  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GlobalGraphRAG |   1947.82        |            0     | 1.89664  |  3.40999  |  3.96949  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Recommend      |   1714.9         |            0     | 2.24185  |  3.2952   |  3.70221  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Geo            |   2730.13        |            0     | 1.28159  |  2.51754  |  5.22783  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Temporal       |   5415.1         |            0     | 0.748695 |  0.944918 |  1.35188  |
| ancalagon | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_LearnedIndex   |   1968.46        |            0     | 2.00574  |  2.7599   |  3.10364  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | DoPut                 |      1.14924e+06 |          280.575 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | DoGet                 |      2.00082e+06 |          488.48  | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Dense          |   3209.12        |            0     | 1.02235  |  1.457    |  9.31305  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Hybrid         |   3118.62        |            0     | 1.25069  |  1.71992  |  1.91359  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Filtered       |   3014.03        |            0     | 1.2905   |  1.83237  |  2.31791  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_FilteredBool   |   2793.62        |            0     | 1.41433  |  1.99427  |  2.38824  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_FilteredString |   2667.92        |            0     | 1.40023  |  2.11582  |  3.42451  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Sparse         |  19153.3         |            0     | 0.187399 |  0.338843 |  0.517817 |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_ByID           |   2954.4         |            0     | 1.30923  |  1.80474  |  2.32048  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_GraphRAG       |   1989.1         |            0     | 1.8293   |  3.28084  |  3.90839  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_GlobalGraphRAG |   2150.68        |            0     | 1.72846  |  2.66426  |  3.28849  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Recommend      |   1764.93        |            0     | 2.14945  |  2.95212  |  3.52044  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Geo            |   2761.52        |            0     | 1.17366  |  3.04275  |  4.51289  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Temporal       |   2995.55        |            0     | 1.17466  |  1.88506  |  2.90188  |
| ancalagon | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_LearnedIndex   |   1904.45        |            0     | 2.10353  |  2.94154  |  3.55449  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 | 877027           |          428.236 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 | 715775           |          349.499 | 0        |  0        |  0        |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   3357.1         |            0     | 1.13639  |  1.73061  |  2.26886  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   2457.58        |            0     | 1.43437  |  2.24588  |  8.45516  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   2589.78        |            0     | 1.49862  |  2.0412   |  2.8448   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   2401.04        |            0     | 1.62074  |  2.24098  |  2.7166   |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   2291.6         |            0     | 1.69008  |  2.37675  |  3.07919  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |  18574.1         |            0     | 0.200553 |  0.305784 |  0.437497 |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |   2217.83        |            0     | 1.71063  |  2.59946  |  3.35762  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |   2135.64        |            0     | 1.78631  |  2.79903  |  3.43332  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |   2019.41        |            0     | 1.92316  |  2.63753  |  3.08447  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |   1480.51        |            0     | 2.57332  |  3.67238  |  4.166    |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   2939.39        |            0     | 1.19059  |  2.29997  |  4.23075  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   6125.01        |            0     | 0.600611 |  0.851551 |  1.25927  |
| ancalagon | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |   2013.88        |            0     | 1.94084  |  2.66613  |  2.90784  |

### Details: ancalagon (cuda)

| Host      | Mode   | Dataset                      | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |    P95_ms |    P99_ms |
|:----------|:-------|:-----------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|----------:|----------:|
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | DoPut                 |       400828     |          391.434 | 0        |  0        |  0        |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | DoGet                 |       251909     |          246.005 | 0        |  0        |  0        |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Dense          |         1424.52  |            0     | 2.47712  |  4.05584  |  6.51578  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Hybrid         |         1588.13  |            0     | 2.43185  |  3.47803  |  3.93433  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Filtered       |         1589.59  |            0     | 2.44085  |  3.62022  |  4.01311  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredBool   |         1451.88  |            0     | 2.57583  |  4.3644   |  5.09548  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredString |         1568.49  |            0     | 2.47632  |  3.60927  |  4.072    |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Sparse         |        22135     |            0     | 0.166699 |  0.299643 |  0.362941 |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_ByID           |         1988.05  |            0     | 1.94706  |  2.89766  |  3.47176  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GraphRAG       |         1246.21  |            0     | 2.87739  |  5.30983  |  6.79173  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GlobalGraphRAG |         1229.76  |            0     | 2.91919  |  5.4357   |  6.79896  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Recommend      |          768.195 |            0     | 4.89037  |  8.30059  | 10.3171   |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Geo            |         1091.07  |            0     | 3.25449  |  5.94669  |  6.83147  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Temporal       |         5412.85  |            0     | 0.658209 |  1.04339  |  1.26577  |
| ancalagon | cuda   | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_LearnedIndex   |          592.4   |            0     | 5.9857   | 11.1996   | 12.5258   |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 |       793305     |          387.356 | 0        |  0        |  0        |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 |       907734     |          443.229 | 0        |  0        |  0        |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |         1036.2   |            0     | 3.40389  |  5.66362  | 17.6668   |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |         1085.36  |            0     | 3.55262  |  5.42506  |  6.58866  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |         1116.19  |            0     | 3.45114  |  5.35467  |  6.23673  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |         1074.96  |            0     | 3.547    |  5.39456  |  7.03667  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |         1127.95  |            0     | 3.41582  |  5.26125  |  6.09209  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |        12469.7   |            0     | 0.278199 |  0.606101 |  0.834292 |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |          682.856 |            0     | 5.27257  | 11.9797   | 14.2509   |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |          820.301 |            0     | 4.38958  |  8.48687  | 10.5054   |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |          771.891 |            0     | 4.52579  |  8.41796  | 10.2231   |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |          810.566 |            0     | 4.73889  |  6.95543  |  7.82878  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |         1521.04  |            0     | 2.49533  |  3.26466  |  6.13022  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |         5527.65  |            0     | 0.655762 |  1.00442  |  1.28659  |
| ancalagon | cuda   | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |          573.092 |            0     | 6.36673  | 11.7032   | 13.4556   |

### Details: bahamut (cpu)

| Host    | Mode   | Dataset                      | DType   |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |    P99_ms |
|:--------|:-------|:-----------------------------|:--------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|----------:|
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoPut                 | 675176           |          659.351 | 0        | 0        |  0        |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | DoGet                 |      1.8477e+06  |         1804.4   | 0        | 0        |  0        |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Dense          |   3455.95        |            0     | 0.562458 | 0.822084 | 24.6056   |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Hybrid         |   6135.98        |            0     | 0.641416 | 0.818459 |  0.91675  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Filtered       |   6271.86        |            0     | 0.626709 | 0.826334 |  1.04704  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredBool   |   6522.61        |            0     | 0.59775  | 0.777042 |  1.07362  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_FilteredString |   6157.52        |            0     | 0.620792 | 0.931    |  1.34308  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Sparse         |  34440.5         |            0     | 0.111959 | 0.150042 |  0.174708 |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_ByID           |   9073.32        |            0     | 0.432708 | 0.589791 |  0.667125 |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GraphRAG       |   5442.49        |            0     | 0.7205   | 0.98325  |  1.12392  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_GlobalGraphRAG |   4221.48        |            0     | 0.90425  | 1.35775  |  1.64017  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Recommend      |   3806.72        |            0     | 1.03233  | 1.38271  |  1.57037  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Geo            |   5499.37        |            0     | 0.611875 | 1.125    |  2.47917  |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_Temporal       |   8327.42        |            0     | 0.448875 | 0.689375 |  0.861917 |
| bahamut | cpu    | bench_float64_128_10000.json | float64 |   128 |   10000 | Search_LearnedIndex   |   4250.3         |            0     | 0.938875 | 1.17942  |  1.33058  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | DoPut                 |      3.68138e+06 |          449.387 | 0        | 0        |  0        |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | DoGet                 |      3.14614e+06 |          384.05  | 0        | 0        |  0        |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Dense          |   4396.78        |            0     | 0.304042 | 0.727375 | 16.145    |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Hybrid         |   6860.58        |            0     | 0.580083 | 0.750167 |  0.9005   |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Filtered       |   6931.62        |            0     | 0.562334 | 0.764125 |  1.05833  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_FilteredBool   |   6696.74        |            0     | 0.587625 | 0.784666 |  0.98825  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_FilteredString |   6708.72        |            0     | 0.571625 | 0.814583 |  0.905875 |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Sparse         |  31633.1         |            0     | 0.121667 | 0.184    |  0.255042 |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_ByID           |   9493.54        |            0     | 0.403708 | 0.599    |  0.717291 |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_GraphRAG       |   4907.96        |            0     | 0.769083 | 1.21383  |  1.55246  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_GlobalGraphRAG |   4256.68        |            0     | 0.874542 | 1.40542  |  1.8535   |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Recommend      |   3742.55        |            0     | 1.03887  | 1.47342  |  1.68983  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Geo            |   6060.86        |            0     | 0.582875 | 0.926916 |  1.97983  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_Temporal       |   4243.49        |            0     | 0.876208 | 1.28083  |  1.83487  |
| bahamut | cpu    | bench_int8_128_10000.json    | int8    |   128 |   10000 | Search_LearnedIndex   |   3006.13        |            0     | 1.32929  | 1.58221  |  2.024    |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | DoPut                 |      2.05439e+06 |          501.56  | 0        | 0        |  0        |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | DoGet                 |      4.18264e+06 |         1021.15  | 0        | 0        |  0        |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Dense          |   5583.16        |            0     | 0.530459 | 0.780167 |  9.60617  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Hybrid         |   6219.62        |            0     | 0.635417 | 0.802083 |  0.900459 |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Filtered       |   6810.61        |            0     | 0.572208 | 0.759667 |  1.07996  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_FilteredBool   |   6652.4         |            0     | 0.584917 | 0.82275  |  1.01604  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_FilteredString |   5511.37        |            0     | 0.687084 | 1.02675  |  1.568    |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Sparse         |  33087.6         |            0     | 0.11375  | 0.159041 |  0.282416 |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_ByID           |  10122.6         |            0     | 0.383334 | 0.542041 |  0.601458 |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_GraphRAG       |   3490.25        |            0     | 1.06421  | 1.79612  |  2.03154  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_GlobalGraphRAG |   2751.05        |            0     | 1.26504  | 2.37921  |  5.73925  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Recommend      |   3311.71        |            0     | 1.15508  | 1.63046  |  2.04404  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Geo            |   7357.15        |            0     | 0.531792 | 0.684417 |  0.815292 |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_Temporal       |   4559.35        |            0     | 0.840833 | 0.988542 |  1.28742  |
| bahamut | cpu    | bench_float16_128_10000.json | float16 |   128 |   10000 | Search_LearnedIndex   |   3063.29        |            0     | 1.29779  | 1.5995   |  2.15088  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoPut                 |      1.84465e+06 |          900.708 | 0        | 0        |  0        |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | DoGet                 |      2.83437e+06 |         1383.97  | 0        | 0        |  0        |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Dense          |   5891.57        |            0     | 0.599208 | 1.05675  |  1.60854  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Hybrid         |   6141.39        |            0     | 0.63825  | 0.835125 |  1.01583  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Filtered       |   6031.01        |            0     | 0.641916 | 0.898792 |  1.44408  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredBool   |   5937.84        |            0     | 0.649375 | 0.885084 |  1.13575  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_FilteredString |   5655.24        |            0     | 0.674625 | 1.01375  |  1.42788  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Sparse         |  29287.5         |            0     | 0.128334 | 0.192666 |  0.318083 |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_ByID           |   5843.1         |            0     | 0.658291 | 0.975292 |  1.25875  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GraphRAG       |   3750.39        |            0     | 1.011    | 1.59987  |  1.72558  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_GlobalGraphRAG |   3565.19        |            0     | 1.04754  | 1.70788  |  1.87208  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Recommend      |   3578.8         |            0     | 1.11354  | 1.45525  |  1.69308  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Geo            |   7236.18        |            0     | 0.539583 | 0.711584 |  0.907792 |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_Temporal       |   8228.12        |            0     | 0.451125 | 0.69725  |  1.38158  |
| bahamut | cpu    | bench_float32_128_10000.json | float32 |   128 |   10000 | Search_LearnedIndex   |   4354.78        |            0     | 0.899959 | 1.17167  |  1.55758  |

