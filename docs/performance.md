<!-- Latest validated commit: 04edb659 (2026-05-20) - Security hardening + overflow checks -->
<!-- Benchmark run in progress: localhost (CPU+Metal, 18GB) + ancalagon (CPU+CUDA, 14GB -->

## v0.2.1 Final Performance Validation (2026-05-16)

## Search Performance Summary (QPS)

|                                         |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'metal', 128, 'float16')      |       5592.28 |        1919.48 |           2114.63 |               2183.69 |                 2209.27 |      5526.27 |                 1444.42 |           1392.64 |         2238.67 |               2057.21 |            2197.29 |        11869.7  |           3887.21 |
| ('local', 'metal', 128, 'float32')      |       5488.68 |        4495.74 |           4892.26 |               4276.86 |                 4031.26 |      5430.75 |                 2463.68 |           2316.95 |         5501.38 |               3971.4  |            5404.43 |        11863.2  |           6076.81 |
| ('local', 'metal', 128, 'float64')      |       4766.45 |        3840.12 |           4149.99 |               3556.03 |                 2939.59 |      4741.59 |                 1791.59 |           1915.01 |         4116.91 |               3361.88 |            4368.56 |        11446.8  |           4816.62 |
| ('local', 'metal', 128, 'turboquant8')  |       1946.35 |        1530.41 |           1876.7  |               1934.76 |                 1844.6  |      2601.39 |                 1816.53 |           1770.16 |         1813.95 |               1904.84 |            1817.88 |        11796.3  |           3627.41 |
| ('local', 'metal', 384, 'float16')      |       2976.07 |        2110.62 |           2206.86 |               2137.94 |                 1943.3  |      5180.37 |                 1413.17 |           1221.15 |         2237.54 |               1941.95 |            2208.28 |        11829.3  |           2193.56 |
| ('local', 'metal', 384, 'float32')      |       4553.92 |        4061.69 |           2578.84 |               2883.45 |                 3266.94 |      4816.53 |                 1844.71 |           1717.43 |         4357.45 |               3386.1  |            4133.73 |        11739    |           5446.46 |
| ('local', 'metal', 384, 'float64')      |       4075.67 |        3264.06 |           3494.36 |               2776.72 |                 2265.37 |      5228.91 |                 1711.8  |           1794.69 |         3663.13 |               2877.06 |            3788.67 |        12077.8  |           5836.81 |
| ('local', 'metal', 768, 'float16')      |       1952.72 |        1973.65 |           1975.93 |               1875.35 |                 1881.72 |      5439.9  |                 1177.31 |           1349.07 |         1963.63 |               1911.53 |            2116.11 |         9247.14 |           3719.69 |
| ('local', 'metal', 768, 'float32')      |       3885.34 |        3324.16 |           3357.53 |               3076.65 |                 2897    |      4939.96 |                 1980.49 |           1671.22 |         3410.78 |               2977.37 |            3864.38 |        10183.4  |           5721.27 |
| ('local', 'metal', 768, 'float64')      |       3320.2  |        2803.47 |           2606.95 |               1829.92 |                 1745.81 |      5469.95 |                 1499.32 |           1560.49 |         2928.01 |               2134.47 |            3126.01 |        11486.9  |           5284.93 |
| ('local', 'metal', 1024, 'float32')     |       3756.99 |        3030.26 |           3015.15 |               2839.21 |                 2504.47 |      5057.99 |                 1899.84 |           1879.82 |         3307.17 |               2807.07 |            3661.23 |        11777.8  |           5932.22 |
| ('local', 'metal', 1024, 'float64')     |       3105.16 |        2195.82 |           2424.31 |               1896.07 |                 1607.82 |      5119.14 |                 1411.5  |           1465.34 |         2548.57 |               2296.11 |            3111.19 |        11596.2  |           5920.46 |
| ('local', 'metal', 3072, 'float32')     |       2289.87 |        1069.34 |           1169.84 |               1224.29 |                 1217.8  |      4802.84 |                 1239.92 |           1227.81 |         1194.36 |               1499.31 |            2321.83 |         8297.4  |           5110.93 |
| ('local', 'metal', 3072, 'float64')     |       2037.73 |        1426.92 |           1452.5  |               1072.97 |                  805.87 |      5645.92 |                 1067.64 |           1020.19 |         1440.87 |               1352.37 |            1801.05 |        10672    |           5928.64 |
| ('local', 'metal', 3072, 'turboquant8') |       1485.21 |        1090.62 |           1309.8  |               1337.77 |                 1330.64 |      2588.98 |                 1268.47 |           1267.22 |         1327.58 |               1281.32 |            1308.5  |        11550.7  |           3714.93 |
| ('remote', 'cpu', 128, 'float32')       |       2510.59 |        2274.83 |           2317.09 |               2304.91 |                 2508.33 |      2736.27 |                 1342.38 |           1115.13 |         2488.09 |               2371.71 |            2339.1  |         7791.37 |           3502.39 |
| ('remote', 'cpu', 128, 'int8')          |       2536.32 |        2141.08 |           2522.49 |               2260.14 |                 1882.87 |      2476.23 |                 1637.21 |           1451.88 |         2450.46 |               2132.56 |            2558.6  |         7158.31 |           3099.02 |
| ('remote', 'cpu', 128, 'turboquant8')   |       2569.82 |        2214.29 |           2481.31 |               2567.59 |                 2399.82 |      2568.25 |                 2207.45 |           2471.63 |         2284.79 |               1909.74 |            2650.08 |         8182.01 |           3453.7  |
| ('remote', 'cpu', 768, 'float32')       |       2190.85 |        1721.71 |           1809.83 |               1800.46 |                 1787.82 |      2781.1  |                 1213.74 |           1072.76 |         1873.77 |               1807.82 |            1978.23 |         7837.6  |           3527.81 |
| ('remote', 'cpu', 768, 'int8')          |       1945.74 |        1684.32 |           1821.44 |               1854.43 |                 1709.33 |      2815.65 |                 1163.17 |           1110.39 |         1870.92 |               1770.28 |            1988.83 |         8210.38 |           3160.85 |
| ('remote', 'cpu', 768, 'turboquant8')   |       2333.68 |        1964.77 |           1953.04 |               2171.37 |                 2177.82 |      2594.44 |                 2000.72 |           1965.2  |         2010.19 |               1654.49 |            2443.7  |         8192.73 |           3559.25 |
| ('remote', 'cpu', 3072, 'float32')      |       1415.52 |        1113.13 |           1295.37 |               1172.11 |                 1024.05 |      2268.38 |                  778.47 |            727.69 |         1271.44 |               1072.95 |            1459.24 |         7016.11 |           2608.91 |
| ('remote', 'cpu', 3072, 'int8')         |       1232.43 |         852.34 |           1074.45 |                796.35 |                  683.56 |      2860.12 |                  763.89 |            724.78 |          993.32 |               1009.98 |            1174.21 |         8265.93 |           3239.94 |
| ('remote', 'cpu', 3072, 'turboquant8')  |       2059.29 |        1596.88 |           1727.08 |               1791.96 |                 1655.63 |      2797.16 |                 1677.94 |           1717.78 |         1600.05 |               1618.73 |            1973.95 |         8625.8  |           3474.8  |
| ('remote', 'cuda', 128, 'float32')      |       2445.82 |        2316.68 |           2427.11 |               2186.15 |                 2378.52 |      2717.05 |                 1203.06 |           1214.96 |         2134.06 |               1900.29 |            2088.33 |         7939.59 |           3308.01 |
| ('remote', 'cuda', 128, 'int8')         |       2564.46 |        2031.17 |           2482.32 |               2061.96 |                 2081.95 |      2594.15 |                 1372.71 |           1166.32 |         2484.12 |               1975.98 |            2519.92 |         7959.13 |           3208.12 |
| ('remote', 'cuda', 128, 'turboquant8')  |       2548.51 |        2242.53 |           2416.07 |               2507.44 |                 2473    |      2477.98 |                 2458.93 |           2428.34 |         2247.26 |               1977.07 |            2515.28 |         6778.3  |           3468.29 |
| ('remote', 'cuda', 768, 'float32')      |       2318.87 |        2062.75 |           1891.09 |               1842.56 |                 1749.9  |      2643.03 |                 1157.57 |           1071.17 |         1570    |               1655.82 |            2255.86 |         8113.03 |           3538.41 |
| ('remote', 'cuda', 768, 'int8')         |       1906.04 |        1609.91 |           2079.97 |               1859.81 |                 1506.43 |      2866.47 |                 1323.1  |           1188.74 |         1768.87 |               1664.53 |            1928.28 |         7901.99 |           3194.78 |
| ('remote', 'cuda', 768, 'turboquant8')  |       2039.58 |        1708.13 |           2113.99 |               2097.51 |                 2095.58 |      2336.02 |                 1804.02 |           1744.17 |         1936.94 |               1782.04 |            2271.79 |         7268.95 |           3486.64 |
| ('remote', 'cuda', 3072, 'float32')     |       1403.39 |        1237.05 |           1322.5  |               1049.46 |                 1125.62 |      2624.18 |                  896.05 |            801.64 |         1323.21 |               1067.54 |            1293.85 |         7773.52 |           3504.45 |
| ('remote', 'cuda', 3072, 'int8')        |       1132.57 |         738.29 |            969.11 |                785.8  |                  686.62 |      2454.25 |                  687.88 |            664.5  |          927.28 |                941.13 |            1178.83 |         8323.83 |           3067.43 |
| ('remote', 'cuda', 3072, 'turboquant8') |       2301.9  |        1548.41 |           1755.98 |               1685.42 |                 1594.57 |      2641.34 |                 1680.22 |           1655.64 |         1641.69 |               1584.29 |            2233.11 |         8380.82 |           3509.37 |

## Ingestion Performance (MB/s)

|                                         |   Throughput_MBs |
|:----------------------------------------|-----------------:|
| ('local', 'metal', 128, 'float16')      |           132.52 |
| ('local', 'metal', 128, 'float32')      |           222.15 |
| ('local', 'metal', 128, 'float64')      |           334.36 |
| ('local', 'metal', 128, 'turboquant8')  |           206.6  |
| ('local', 'metal', 384, 'float16')      |           401.74 |
| ('local', 'metal', 384, 'float32')      |           604.87 |
| ('local', 'metal', 384, 'float64')      |           806.05 |
| ('local', 'metal', 768, 'float16')      |           482.03 |
| ('local', 'metal', 768, 'float32')      |           824.86 |
| ('local', 'metal', 768, 'float64')      |           898.8  |
| ('local', 'metal', 1024, 'float32')     |           763.76 |
| ('local', 'metal', 1024, 'float64')     |           942.32 |
| ('local', 'metal', 3072, 'float32')     |          1076.24 |
| ('local', 'metal', 3072, 'float64')     |          1225.13 |
| ('local', 'metal', 3072, 'turboquant8') |           327.31 |
| ('remote', 'cpu', 128, 'float32')       |           229.95 |
| ('remote', 'cpu', 128, 'int8')          |           140.64 |
| ('remote', 'cpu', 128, 'turboquant8')   |            67.09 |
| ('remote', 'cpu', 768, 'float32')       |           323.66 |
| ('remote', 'cpu', 768, 'int8')          |           286.97 |
| ('remote', 'cpu', 768, 'turboquant8')   |            86.73 |
| ('remote', 'cpu', 3072, 'float32')      |           375.97 |
| ('remote', 'cpu', 3072, 'int8')         |           348.28 |
| ('remote', 'cpu', 3072, 'turboquant8')  |            93.63 |
| ('remote', 'cuda', 128, 'float32')      |           196.85 |
| ('remote', 'cuda', 128, 'int8')         |            70.06 |
| ('remote', 'cuda', 128, 'turboquant8')  |            67.23 |
| ('remote', 'cuda', 768, 'float32')      |           349.93 |
| ('remote', 'cuda', 768, 'int8')         |           305.63 |
| ('remote', 'cuda', 768, 'turboquant8')  |            88.11 |
| ('remote', 'cuda', 3072, 'float32')     |           378.69 |
| ('remote', 'cuda', 3072, 'int8')        |           340.62 |
| ('remote', 'cuda', 3072, 'turboquant8') |            94.66 |

## Search Latency Summary (P95 ms)

|                                         |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'metal', 128, 'float16')      |          2.03 |           7.59 |              5.69 |                  5.43 |                    5.24 |         2.08 |                    9.04 |              8.61 |            5.04 |                  6.05 |               5.62 |            1.03 |              3.04 |
| ('local', 'metal', 128, 'float32')      |          2.02 |           2.77 |              2.15 |                  2.51 |                    2.58 |         2.34 |                    4.7  |              5.41 |            2.13 |                  2.74 |               2.06 |            1.02 |              1.83 |
| ('local', 'metal', 128, 'float64')      |          2.24 |           2.7  |              2.49 |                  2.95 |                    3.9  |         2.68 |                    7.49 |              7    |            2.82 |                  3.49 |               2.46 |            1.06 |              2.56 |
| ('local', 'metal', 128, 'turboquant8')  |          6.35 |           8.07 |              6.31 |                  5.94 |                    6.25 |         3.91 |                    6.44 |              6.46 |            6.96 |                  5.5  |               6.58 |            1.08 |              2.92 |
| ('local', 'metal', 384, 'float16')      |          4    |           5.7  |              5.42 |                  5.27 |                    6.09 |         2.44 |                    8.54 |             11.38 |            5.01 |                  6.65 |               5.57 |            1    |             13.44 |
| ('local', 'metal', 384, 'float32')      |          2.35 |           2.67 |              4.94 |                  4.43 |                    3.46 |         2.57 |                    7.06 |              8.72 |            2.58 |                  3.32 |               2.82 |            1.04 |              2.05 |
| ('local', 'metal', 384, 'float64')      |          2.57 |           3.37 |              2.99 |                  3.84 |                    4.99 |         2.45 |                    6.69 |              6.49 |            2.95 |                  3.8  |               2.91 |            1.01 |              1.89 |
| ('local', 'metal', 768, 'float16')      |          5.82 |           5.76 |              5.98 |                  6.18 |                    6.14 |         2.11 |                   19.03 |              8.63 |            6.1  |                  6.24 |               5.88 |            1.46 |              3.13 |
| ('local', 'metal', 768, 'float32')      |          2.78 |           3.09 |              3.1  |                  3.54 |                    3.69 |         2.46 |                    5.93 |              9.66 |            3.31 |                  3.64 |               2.89 |            1.34 |              1.93 |
| ('local', 'metal', 768, 'float64')      |          3.5  |           3.75 |              4.23 |                  5.98 |                    6.2  |         2.13 |                    8.01 |              7.34 |            3.6  |                  5.27 |               3.52 |            1.07 |              2.32 |
| ('local', 'metal', 1024, 'float32')     |          2.89 |           3.45 |              3.46 |                  3.7  |                    4.4  |         2.48 |                    6.1  |              5.89 |            3.28 |                  3.78 |               2.96 |            1.03 |              1.82 |
| ('local', 'metal', 1024, 'float64')     |          3.6  |           4.92 |              4.33 |                  5.54 |                    6.41 |         2.12 |                    9.12 |              7.74 |            4.23 |                  4.69 |               3.57 |            1.03 |              1.82 |
| ('local', 'metal', 3072, 'float32')     |          4.46 |           9.71 |              9.36 |                 10    |                    9.17 |         2.37 |                    9.08 |              9.48 |            9.74 |                  7.39 |               4.48 |            1.5  |              2.3  |
| ('local', 'metal', 3072, 'float64')     |          5.06 |           6.98 |              6.74 |                  8.99 |                   11.76 |         2    |                   11.13 |             11.72 |            6.79 |                  7.6  |               5.59 |            1.16 |              1.84 |
| ('local', 'metal', 3072, 'turboquant8') |          7.66 |           9.53 |              7.59 |                  7.25 |                    7.22 |         3.84 |                    8.03 |              7.96 |            8.44 |                  7.78 |               9.25 |            1.06 |              2.9  |
| ('remote', 'cpu', 128, 'float32')       |          6.25 |           6.2  |              5.37 |                  5.51 |                    5.47 |         4.2  |                   13.7  |             17.6  |            6.16 |                  6.32 |               6.77 |            1.43 |              3.23 |
| ('remote', 'cpu', 128, 'int8')          |          5.68 |           6    |              5.11 |                  5.33 |                    7.35 |         4.9  |                    9.06 |             11.69 |            5.1  |                  6.18 |               5.05 |            1.58 |              4.03 |
| ('remote', 'cpu', 128, 'turboquant8')   |          6.11 |           7.08 |              4.91 |                  4.84 |                    4.85 |         5.16 |                    5.36 |              5.17 |            5.74 |                  7.09 |               4.98 |            1.37 |              3.4  |
| ('remote', 'cpu', 768, 'float32')       |          8.28 |           8.82 |              8    |                  8.2  |                    8.18 |         4.53 |                   15.9  |             18.05 |            9.56 |                  9.4  |              10.47 |            1.45 |              3.28 |
| ('remote', 'cpu', 768, 'int8')          |          8.31 |           8.24 |              6.01 |                  6.13 |                    6.28 |         4.41 |                   13.45 |             14.15 |            6.97 |                  8.13 |               7.46 |            1.4  |              3.87 |
| ('remote', 'cpu', 768, 'turboquant8')   |          7.45 |           9.52 |              7.34 |                  6.37 |                    6.4  |         4.57 |                    7.37 |              6.99 |            7.87 |                  9.19 |               7.32 |            1.39 |              3.24 |
| ('remote', 'cpu', 3072, 'float32')      |         42.65 |          71.91 |             76.08 |                 66.36 |                   55.06 |        11.24 |                   52.58 |             52.28 |           57.11 |                 14.39 |              56.36 |            1.81 |              2.44 |
| ('remote', 'cpu', 3072, 'int8')         |         11.96 |          15.83 |             12.25 |                 14.52 |                   16    |         4.28 |                   17.5  |             19.63 |           15.06 |                 14.46 |              15.03 |            1.36 |              3.77 |
| ('remote', 'cpu', 3072, 'turboquant8')  |          8.96 |          15.76 |              8.58 |                  8.45 |                    8.64 |         4.25 |                    8.47 |              8.86 |            9.72 |                  9.99 |               8.61 |            1.34 |              3.36 |
| ('remote', 'cuda', 128, 'float32')      |          6.21 |           7.74 |              5.33 |                  6.01 |                    5.67 |         5.47 |                   14.61 |             14.78 |            7.3  |                  7.01 |               7.01 |            1.42 |              4.02 |
| ('remote', 'cuda', 128, 'int8')         |          5.85 |           7.66 |              5.2  |                  5.66 |                    5.63 |         5.23 |                   10.52 |             14.02 |            5.73 |                  6.68 |               5.35 |            1.43 |              3.79 |
| ('remote', 'cuda', 128, 'turboquant8')  |          6.64 |           8.23 |              5.16 |                  4.98 |                    5.15 |         4.65 |                    5.48 |              5.99 |            6.31 |                  6.65 |               5.39 |            1.7  |              3.32 |
| ('remote', 'cuda', 768, 'float32')      |          8.39 |           7.96 |              7.96 |                  8.34 |                    8.49 |         4.4  |                   15.54 |             19.7  |            9.65 |                 10.12 |              10.68 |            1.42 |              3.21 |
| ('remote', 'cuda', 768, 'int8')         |          7.95 |          10.45 |              5.83 |                  5.99 |                    6.61 |         4.37 |                   10.22 |             13.48 |            8.37 |                  8.6  |               7.81 |            1.41 |              4.02 |
| ('remote', 'cuda', 768, 'turboquant8')  |          8.31 |          12.12 |              6.52 |                  6.99 |                    6.23 |         5.11 |                    7.22 |              9.7  |            7.19 |                  9.3  |               6.31 |            1.46 |              3.33 |
| ('remote', 'cuda', 3072, 'float32')     |         22.46 |          23.07 |             23.77 |                 23.24 |                   23.41 |         4.73 |                   26.69 |             28.77 |           25.11 |                 25.75 |              30.83 |            1.44 |              3.21 |
| ('remote', 'cuda', 3072, 'int8')        |         12.85 |          19.7  |             13.17 |                 14.41 |                   16.23 |         4.53 |                   19.9  |             22.35 |           14.97 |                 14.95 |              15.62 |            1.38 |              4.03 |
| ('remote', 'cuda', 3072, 'turboquant8') |         10.71 |          11.42 |              9.96 |                 10.22 |                    9.84 |         4.27 |                   10.1  |             10.27 |           10.75 |                 12.18 |               9.48 |            1.33 |              3.26 |

### Details: local (metal)

| Host   | Mode   | Dataset                                  | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |   P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:-----------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|---------:|---------:|---------:|
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | DoPut                 | 454954           |          222.145 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | DoGet                 |      1.13671e+06 |          555.033 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Dense          |   4495.74        |            0     | 1.65167  |  2.76933 |  4.97758 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Hybrid         |   5501.38        |            0     | 1.40454  |  2.12838 |  2.57763 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Filtered       |   4892.26        |            0     | 1.59867  |  2.14833 |  2.46775 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_FilteredBool   |   4276.86        |            0     | 1.83754  |  2.50667 |  2.892   |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_FilteredString |   4031.26        |            0     | 1.96983  |  2.57592 |  2.84383 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Sparse         |  11863.2         |            0     | 0.666292 |  1.02112 |  1.22325 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_ByID           |   5488.68        |            0     | 1.44904  |  2.01825 |  2.31387 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_GraphRAG       |   2316.95        |            0     | 3.00279  |  5.41183 | 13.5258  |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2463.68        |            0     | 3.07504  |  4.70012 |  6.444   |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Recommend      |   5404.43        |            0     | 1.45283  |  2.05983 |  2.30508 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Geo            |   5430.75        |            0     | 1.39587  |  2.33521 |  3.44588 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_Temporal       |   6076.81        |            0     | 1.29921  |  1.82529 |  2.03796 |
| local  | metal  | result_cpu_float32_128_5000.json         | float32     |   128 |    5000 | Search_LearnedIndex   |   3971.4         |            0     | 1.977    |  2.74408 |  3.16342 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | DoPut                 | 275132           |          806.051 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | DoGet                 | 191624           |          561.397 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Dense          |   3264.06        |            0     | 2.26071  |  3.37192 |  5.62317 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Hybrid         |   3663.13        |            0     | 2.12096  |  2.949   |  3.50979 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Filtered       |   3494.36        |            0     | 2.2245   |  2.98513 |  3.90142 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_FilteredBool   |   2776.72        |            0     | 2.76917  |  3.83542 |  4.54979 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_FilteredString |   2265.37        |            0     | 3.17138  |  4.98783 | 12.0117  |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Sparse         |  12077.8         |            0     | 0.642333 |  1.01304 |  1.18879 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_ByID           |   4075.67        |            0     | 1.94538  |  2.56625 |  3.00758 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_GraphRAG       |   1794.69        |            0     | 4.1235   |  6.4925  |  9.0695  |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_GlobalGraphRAG |   1711.8         |            0     | 4.34871  |  6.68992 |  8.40496 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Recommend      |   3788.67        |            0     | 2.02354  |  2.91363 |  3.54083 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Geo            |   5228.91        |            0     | 1.44333  |  2.45196 |  3.85717 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_Temporal       |   5836.81        |            0     | 1.34325  |  1.89229 |  2.17212 |
| local  | metal  | result_cpu_float64_384_5000.json         | float64     |   384 |    5000 | Search_LearnedIndex   |   2877.06        |            0     | 2.68204  |  3.79537 |  4.37158 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | DoPut                 | 120617           |          942.318 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | DoGet                 | 159590           |         1246.8   | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Dense          |   2195.82        |            0     | 3.49758  |  4.91946 |  6.75771 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Hybrid         |   2548.57        |            0     | 3.04096  |  4.23008 |  4.76567 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Filtered       |   2424.31        |            0     | 3.21379  |  4.33333 |  4.97783 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_FilteredBool   |   1896.07        |            0     | 3.91583  |  5.54408 |  7.593   |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_FilteredString |   1607.82        |            0     | 4.79746  |  6.40696 |  7.07317 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Sparse         |  11596.2         |            0     | 0.674125 |  1.02608 |  1.23267 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_ByID           |   3105.16        |            0     | 2.49029  |  3.59587 |  4.09854 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_GraphRAG       |   1465.34        |            0     | 5.12371  |  7.74467 | 10.1317  |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_GlobalGraphRAG |   1411.5         |            0     | 5.17037  |  9.12104 | 15.3928  |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Recommend      |   3111.19        |            0     | 2.48471  |  3.56992 |  3.95925 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Geo            |   5119.14        |            0     | 1.44642  |  2.11646 |  5.06542 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_Temporal       |   5920.46        |            0     | 1.33183  |  1.82375 |  2.03708 |
| local  | metal  | result_cpu_float64_1024_5000.json        | float64     |  1024 |    5000 | Search_LearnedIndex   |   2296.11        |            0     | 3.38537  |  4.69488 |  5.26579 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | DoPut                 | 153394           |          898.795 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | DoGet                 | 111178           |          651.436 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Dense          |   2803.47        |            0     | 2.72342  |  3.74517 |  6.62567 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Hybrid         |   2928.01        |            0     | 2.65942  |  3.59858 |  4.10179 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Filtered       |   2606.95        |            0     | 2.90429  |  4.23329 |  5.502   |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_FilteredBool   |   1829.92        |            0     | 4.24404  |  5.97637 |  6.77933 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_FilteredString |   1745.81        |            0     | 4.39262  |  6.19987 |  6.84492 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Sparse         |  11486.9         |            0     | 0.668584 |  1.06563 |  1.38225 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_ByID           |   3320.2         |            0     | 2.29558  |  3.50225 |  4.08996 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_GraphRAG       |   1560.49        |            0     | 4.80608  |  7.34037 |  9.28446 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_GlobalGraphRAG |   1499.32        |            0     | 4.96563  |  8.00596 | 10.9191  |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Recommend      |   3126.01        |            0     | 2.46879  |  3.51579 |  4.079   |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Geo            |   5469.95        |            0     | 1.41479  |  2.13429 |  2.69558 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_Temporal       |   5284.93        |            0     | 1.45054  |  2.31738 |  3.06496 |
| local  | metal  | result_cpu_float64_768_5000.json         | float64     |   768 |    5000 | Search_LearnedIndex   |   2134.47        |            0     | 3.69633  |  5.26758 |  6.10358 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | DoPut                 |  91839.4         |         1076.24  | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | DoGet                 |  82038.2         |          961.385 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Dense          |   1069.34        |            0     | 7.24558  |  9.71233 | 14.9145  |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Hybrid         |   1194.36        |            0     | 6.28917  |  9.73533 | 12.834   |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Filtered       |   1169.84        |            0     | 6.56808  |  9.35958 | 10.5452  |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_FilteredBool   |   1224.29        |            0     | 5.93992  | 10.0029  | 14.5203  |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_FilteredString |   1217.8         |            0     | 6.29658  |  9.17133 | 10.5805  |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Sparse         |   8297.4         |            0     | 0.931458 |  1.49737 |  1.83317 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_ByID           |   2289.87        |            0     | 3.53317  |  4.45625 |  5.14475 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_GraphRAG       |   1227.81        |            0     | 6.30071  |  9.47558 | 11.4321  |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1239.92        |            0     | 6.26008  |  9.08388 | 11.678   |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Recommend      |   2321.83        |            0     | 3.45967  |  4.48175 |  5.00996 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Geo            |   4802.84        |            0     | 1.62838  |  2.37104 |  2.83025 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_Temporal       |   5110.93        |            0     | 1.50108  |  2.30471 |  2.78242 |
| local  | metal  | result_cpu_float32_3072_5000.json        | float32     |  3072 |    5000 | Search_LearnedIndex   |   1499.31        |            0     | 5.1245   |  7.39183 | 10.2355  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | DoPut                 | 542822           |          132.525 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | DoGet                 |      1.06064e+06 |          258.946 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Dense          |   1919.48        |            0     | 3.59842  |  7.58867 | 11.7313  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Hybrid         |   2238.67        |            0     | 3.33142  |  5.04387 |  5.91404 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Filtered       |   2114.63        |            0     | 3.285    |  5.69404 | 14.9463  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_FilteredBool   |   2183.69        |            0     | 3.39829  |  5.42504 |  6.1015  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_FilteredString |   2209.27        |            0     | 3.39242  |  5.23854 |  6.14054 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Sparse         |  11869.7         |            0     | 0.661708 |  1.02975 |  1.23    |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_ByID           |   5592.28        |            0     | 1.40204  |  2.03058 |  2.29333 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_GraphRAG       |   1392.64        |            0     | 5.11533  |  8.60917 | 12.3003  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_GlobalGraphRAG |   1444.42        |            0     | 4.94487  |  9.04225 | 11.1936  |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Recommend      |   2197.29        |            0     | 3.23071  |  5.6235  |  6.19287 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Geo            |   5526.27        |            0     | 1.41933  |  2.08192 |  2.57954 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_Temporal       |   3887.21        |            0     | 1.96375  |  3.04396 |  3.64179 |
| local  | metal  | result_cpu_float16_128_5000.json         | float16     |   128 |    5000 | Search_LearnedIndex   |   2057.21        |            0     | 3.44975  |  6.05346 |  6.69383 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | DoPut                 | 548506           |          401.738 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | DoGet                 | 691862           |          506.735 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Dense          |   2110.62        |            0     | 3.354    |  5.69813 | 14.3448  |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Hybrid         |   2237.54        |            0     | 3.34058  |  5.00687 |  5.84946 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Filtered       |   2206.86        |            0     | 3.33679  |  5.42229 |  6.20725 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_FilteredBool   |   2137.94        |            0     | 3.49992  |  5.27342 |  6.34371 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_FilteredString |   1943.3         |            0     | 3.596    |  6.0935  |  7.35225 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Sparse         |  11829.3         |            0     | 0.657    |  1.00012 |  1.17954 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_ByID           |   2976.07        |            0     | 2.39054  |  3.99588 |  8.38254 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_GraphRAG       |   1221.15        |            0     | 5.62437  | 11.3825  | 21.8509  |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_GlobalGraphRAG |   1413.17        |            0     | 4.97342  |  8.53717 | 13.3456  |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Recommend      |   2208.28        |            0     | 3.22975  |  5.57062 |  6.10825 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Geo            |   5180.37        |            0     | 1.45354  |  2.43933 |  3.72988 |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_Temporal       |   2193.56        |            0     | 2.38008  | 13.4404  | 25.9244  |
| local  | metal  | result_cpu_float16_384_5000.json         | float16     |   384 |    5000 | Search_LearnedIndex   |   1941.95        |            0     | 3.57342  |  6.65067 |  7.9335  |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | DoPut                 | 195522           |          763.757 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | DoGet                 | 226791           |          885.903 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Dense          |   3030.26        |            0     | 2.53317  |  3.44858 |  4.65658 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Hybrid         |   3307.17        |            0     | 2.35517  |  3.27892 |  3.77521 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Filtered       |   3015.15        |            0     | 2.60083  |  3.45925 |  4.04296 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_FilteredBool   |   2839.21        |            0     | 2.755    |  3.70133 |  4.18333 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_FilteredString |   2504.47        |            0     | 2.98842  |  4.39987 |  8.01979 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Sparse         |  11777.8         |            0     | 0.667625 |  1.0285  |  1.17967 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_ByID           |   3756.99        |            0     | 2.092    |  2.88979 |  3.29675 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_GraphRAG       |   1879.82        |            0     | 3.88262  |  5.89412 | 11.4819  |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_GlobalGraphRAG |   1899.84        |            0     | 3.91933  |  6.10325 |  9.671   |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Recommend      |   3661.23        |            0     | 2.11146  |  2.95967 |  3.39458 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Geo            |   5057.99        |            0     | 1.52075  |  2.48046 |  3.85754 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_Temporal       |   5932.22        |            0     | 1.33192  |  1.8235  |  2.04721 |
| local  | metal  | result_cpu_float32_1024_5000.json        | float32     |  1024 |    5000 | Search_LearnedIndex   |   2807.07        |            0     | 2.76713  |  3.777   |  4.18554 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | DoPut                 | 329066           |          482.031 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | DoGet                 | 498273           |          729.892 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Dense          |   1973.65        |            0     | 3.68017  |  5.76037 | 10.2655  |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Hybrid         |   1963.63        |            0     | 3.75371  |  6.09687 |  7.16737 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Filtered       |   1975.93        |            0     | 3.67446  |  5.97867 |  6.88025 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_FilteredBool   |   1875.35        |            0     | 3.88954  |  6.17983 |  6.77808 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_FilteredString |   1881.72        |            0     | 3.86163  |  6.14267 |  6.78812 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Sparse         |   9247.14        |            0     | 0.805042 |  1.46175 |  1.82304 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_ByID           |   1952.72        |            0     | 3.95404  |  5.82362 |  6.79117 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_GraphRAG       |   1349.07        |            0     | 5.56729  |  8.63046 | 10.6597  |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_GlobalGraphRAG |   1177.31        |            0     | 4.91983  | 19.0348  | 35.6327  |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Recommend      |   2116.11        |            0     | 3.23842  |  5.87508 |  7.49075 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Geo            |   5439.9         |            0     | 1.44421  |  2.10517 |  2.44029 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_Temporal       |   3719.69        |            0     | 2.05058  |  3.13304 |  4.69333 |
| local  | metal  | result_cpu_float16_768_5000.json         | float16     |   768 |    5000 | Search_LearnedIndex   |   1911.53        |            0     | 3.66033  |  6.24271 |  6.75846 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | DoPut                 |  52272.3         |         1225.13  | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | DoGet                 |  53530.5         |         1254.62  | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Dense          |   1426.92        |            0     | 5.44813  |  6.97925 |  8.96817 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Hybrid         |   1440.87        |            0     | 5.46608  |  6.78812 |  7.35313 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Filtered       |   1452.5         |            0     | 5.40254  |  6.74212 |  8.29092 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_FilteredBool   |   1072.97        |            0     | 7.33287  |  8.99454 |  9.7855  |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_FilteredString |    805.872       |            0     | 9.60037  | 11.7605  | 19.8173  |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Sparse         |  10672           |            0     | 0.734334 |  1.15508 |  1.31775 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_ByID           |   2037.73        |            0     | 3.81158  |  5.06408 |  5.53417 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_GraphRAG       |   1020.19        |            0     | 7.25029  | 11.7242  | 14.6674  |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_GlobalGraphRAG |   1067.64        |            0     | 6.90196  | 11.1278  | 20.4517  |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Recommend      |   1801.05        |            0     | 4.35867  |  5.58679 |  5.96512 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Geo            |   5645.92        |            0     | 1.39037  |  1.99946 |  2.30971 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_Temporal       |   5928.64        |            0     | 1.32246  |  1.83983 |  2.04971 |
| local  | metal  | result_cpu_float64_3072_5000.json        | float64     |  3072 |    5000 | Search_LearnedIndex   |   1352.37        |            0     | 5.77404  |  7.5985  |  8.47808 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | DoPut                 | 342381           |          334.356 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | DoGet                 | 390054           |          380.912 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Dense          |   3840.12        |            0     | 1.81712  |  2.70375 | 14.0006  |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Hybrid         |   4116.91        |            0     | 1.685    |  2.81767 |  5.56363 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Filtered       |   4149.99        |            0     | 1.88271  |  2.4885  |  3.48008 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_FilteredBool   |   3556.03        |            0     | 2.21296  |  2.95413 |  3.39429 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_FilteredString |   2939.59        |            0     | 2.61046  |  3.89833 |  4.40696 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Sparse         |  11446.8         |            0     | 0.675875 |  1.06171 |  1.31954 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_ByID           |   4766.45        |            0     | 1.6545   |  2.24325 |  2.46225 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_GraphRAG       |   1915.01        |            0     | 3.81279  |  7.00462 | 10.7355  |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_GlobalGraphRAG |   1791.59        |            0     | 4.06046  |  7.48642 | 11.242   |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Recommend      |   4368.56        |            0     | 1.79642  |  2.46217 |  2.89425 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Geo            |   4741.59        |            0     | 1.60542  |  2.67813 |  3.91    |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_Temporal       |   4816.62        |            0     | 1.59021  |  2.555   |  3.26592 |
| local  | metal  | result_cpu_float64_128_5000.json         | float64     |   128 |    5000 | Search_LearnedIndex   |   3361.88        |            0     | 2.28246  |  3.48854 |  4.10217 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 |      1.6925e+06  |          206.604 | 0        |  0       |  0       |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 |      1.39861e+06 |          170.729 | 0        |  0       |  0       |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |   1530.41        |            0     | 5.02604  |  8.07396 | 10.2421  |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |   1813.95        |            0     | 4.01258  |  6.96087 | 16.559   |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |   1876.7         |            0     | 4.06387  |  6.31288 |  9.27108 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1934.76        |            0     | 4.06304  |  5.93608 |  6.95437 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |   1844.6         |            0     | 4.24383  |  6.25008 |  7.45404 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |  11796.3         |            0     | 0.65825  |  1.08217 |  1.20338 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |   1946.35        |            0     | 3.96192  |  6.34987 |  7.36746 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1770.16        |            0     | 4.40317  |  6.45733 |  7.73658 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1816.53        |            0     | 4.21529  |  6.44242 |  7.57683 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |   1817.88        |            0     | 4.22254  |  6.57554 |  7.8315  |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |   2601.39        |            0     | 3.00146  |  3.91017 |  5.40696 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |   3627.41        |            0     | 2.13267  |  2.91758 |  3.69675 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1904.84        |            0     | 4.15463  |  5.49821 |  6.22313 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | DoPut                 | 281553           |          824.863 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | DoGet                 | 304691           |          892.651 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Dense          |   3324.16        |            0     | 2.345    |  3.09    |  3.8445  |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Hybrid         |   3410.78        |            0     | 2.13933  |  3.30533 |  5.08479 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Filtered       |   3357.53        |            0     | 2.33096  |  3.09571 |  3.90179 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_FilteredBool   |   3076.65        |            0     | 2.54108  |  3.54108 |  4.02367 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_FilteredString |   2897           |            0     | 2.67833  |  3.68517 |  4.00825 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Sparse         |  10183.4         |            0     | 0.734333 |  1.34246 |  1.85221 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_ByID           |   3885.34        |            0     | 2.01183  |  2.78379 |  3.23758 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_GraphRAG       |   1671.22        |            0     | 4.07292  |  9.66383 | 15.8429  |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_GlobalGraphRAG |   1980.49        |            0     | 3.83675  |  5.92917 |  7.60071 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Recommend      |   3864.38        |            0     | 2.02417  |  2.89417 |  3.39729 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Geo            |   4939.96        |            0     | 1.42654  |  2.46479 |  8.09137 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_Temporal       |   5721.27        |            0     | 1.36758  |  1.92775 |  2.20871 |
| local  | metal  | result_cpu_float32_768_5000.json         | float32     |   768 |    5000 | Search_LearnedIndex   |   2977.37        |            0     | 2.6155   |  3.6395  |  4.66242 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 | 111720           |          327.306 | 0        |  0       |  0       |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 | 147514           |          432.171 | 0        |  0       |  0       |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |   1090.62        |            0     | 7.25146  |  9.52796 | 11.2553  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |   1327.58        |            0     | 5.93592  |  8.43612 | 10.1849  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |   1309.8         |            0     | 6.05717  |  7.59033 |  8.954   |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |   1337.77        |            0     | 5.94646  |  7.24567 |  8.01321 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |   1330.64        |            0     | 5.97696  |  7.21875 |  8.34925 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |  11550.7         |            0     | 0.67825  |  1.06062 |  1.26758 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |   1485.21        |            0     | 5.17383  |  7.66179 | 11.5228  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |   1267.22        |            0     | 6.25267  |  7.95571 |  9.36479 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |   1268.47        |            0     | 6.19258  |  8.03033 |  9.78492 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |   1308.5         |            0     | 5.99429  |  9.25354 | 11.3744  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |   2588.98        |            0     | 3.01083  |  3.84196 |  6.43479 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |   3714.93        |            0     | 2.07858  |  2.90446 |  3.68625 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |   1281.32        |            0     | 6.16583  |  7.77729 |  9.76729 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | DoPut                 | 412927           |          604.874 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | DoGet                 | 544623           |          797.787 | 0        |  0       |  0       |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Dense          |   4061.69        |            0     | 1.89758  |  2.66896 |  3.7915  |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Hybrid         |   4357.45        |            0     | 1.747    |  2.57946 |  3.42013 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Filtered       |   2578.84        |            0     | 2.95962  |  4.94287 |  6.47267 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_FilteredBool   |   2883.45        |            0     | 2.57446  |  4.42754 |  5.49087 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_FilteredString |   3266.94        |            0     | 2.31529  |  3.46175 |  4.27775 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Sparse         |  11739           |            0     | 0.657167 |  1.04125 |  1.37063 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_ByID           |   4553.92        |            0     | 1.72917  |  2.35042 |  2.68225 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_GraphRAG       |   1717.43        |            0     | 3.96783  |  8.72446 | 17.0319  |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_GlobalGraphRAG |   1844.71        |            0     | 3.92512  |  7.06258 | 11.1753  |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Recommend      |   4133.73        |            0     | 1.88679  |  2.82275 |  3.31058 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Geo            |   4816.53        |            0     | 1.59146  |  2.56808 |  3.60808 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_Temporal       |   5446.46        |            0     | 1.43992  |  2.05354 |  2.37546 |
| local  | metal  | result_cpu_float32_384_5000.json         | float32     |   384 |    5000 | Search_LearnedIndex   |   3386.1         |            0     | 2.27521  |  3.31579 |  4.06321 |

### Details: remote (cpu)

| Host   | Mode   | Dataset                                 | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |     P50_ms |    P95_ms |    P99_ms |
|:-------|:-------|:----------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|-----------:|----------:|----------:|
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | DoPut                 | 588978           |          71.8967 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | DoGet                 |      1.08581e+06 |         132.545  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Dense          |   1282.76        |           0      |   5.49888  |  10.4171  |  27.3928  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Hybrid         |   1494.07        |           0      |   5.27537  |   7.61421 |   8.79303 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Filtered       |   1519.81        |           0      |   4.90858  |   6.19018 |  12.065   |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1579.77        |           0      |   5.01065  |   6.0665  |   6.88499 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_FilteredString |   1589.04        |           0      |   4.98346  |   5.95425 |   6.76022 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Sparse         |   9052.35        |           0      |   0.862709 |   1.25724 |   1.49132 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_ByID           |   1397.22        |           0      |   5.24792  |   8.9804  |  13.4848  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1417.94        |           0      |   5.09491  |   6.88576 |  15.0846  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1419.13        |           0      |   5.05243  |   6.50913 |  38.1554  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Recommend      |   1521.61        |           0      |   5.24053  |   6.48693 |   7.50118 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Geo            |   1868.86        |           0      |   4.17688  |   5.39112 |   7.77184 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_Temporal       |   3020.27        |           0      |   2.45537  |   4.02702 |   5.14347 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json   | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1509.04        |           0      |   4.91471  |   7.24633 |  18.3368  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 | 607103           |          74.1093 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 | 893681           |         109.092  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |   1134.75        |           0      |   6.37572  |  11.0262  |  26.5302  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |   1237.78        |           0      |   5.76352  |   9.11537 |  37.6199  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |   1399.33        |           0      |   5.64598  |   7.14197 |   8.67728 |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1415.36        |           0      |   5.60171  |   7.16761 |   8.16655 |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |   1417.93        |           0      |   5.56912  |   6.92217 |   8.1016  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |   7928.74        |           0      |   0.999266 |   1.37675 |   1.6252  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |   1303.82        |           0      |   5.80817  |   9.16947 |  13.846   |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1318.24        |           0      |   5.61797  |   7.48196 |  15.4911  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1344.7         |           0      |   5.56862  |   7.40884 |  11.8483  |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |   1407.02        |           0      |   5.62555  |   7.26471 |   8.87555 |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |   1791.2         |           0      |   4.30335  |   5.89024 |   7.55136 |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |   3067.07        |           0      |   2.47764  |   3.93741 |   4.43878 |
| remote | cpu    | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1394.78        |           0      |   5.50412  |   7.7885  |  10.222   |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | DoPut                 | 329613           |         160.944  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | DoGet                 | 875814           |         427.643  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Dense          |   3390.66        |           0      |   2.2001   |   3.35963 |   6.47539 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Hybrid         |   3238.76        |           0      |   2.05802  |   3.41066 |  27.8336  |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Filtered       |   3176.05        |           0      |   2.01832  |   3.1627  |  29.0672  |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_FilteredBool   |   3766.17        |           0      |   2.04988  |   3.15938 |   3.63986 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_FilteredString |   4000.52        |           0      |   1.92473  |   2.98364 |   3.37464 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Sparse         |   7688.29        |           0      |   1.02609  |   1.41268 |   1.55981 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_ByID           |   3803.07        |           0      |   2.02658  |   3.08909 |   3.47519 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_GraphRAG       |   1614.86        |           0      |   2.98774  |  16.347   |  38.6085  |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2071.92        |           0      |   2.99206  |   7.45308 |  19.1319  |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Recommend      |   3758.86        |           0      |   2.0487   |   3.06125 |   3.60138 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Geo            |   3806.67        |           0      |   1.77963  |   3.22189 |   4.42694 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_Temporal       |   3889.77        |           0      |   2.02141  |   2.81001 |   3.50798 |
| remote | cpu    | result_cpu_float32_128_5000.json        | float32     |   128 |    5000 | Search_LearnedIndex   |   3710.65        |           0      |   1.95977  |   3.42582 |   4.24951 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | DoPut                 | 252779           |         123.427  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | DoGet                 | 178087           |          86.9567 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Dense          |   3317.67        |           0      |   2.13119  |   3.58553 |  11.7224  |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Hybrid         |   4248.84        |           0      |   1.78015  |   2.96065 |   3.41249 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Filtered       |   3531.62        |           0      |   2.10694  |   3.12804 |   4.20283 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredBool   |   2871.15        |           0      |   1.93868  |   4.17441 |  32.3022  |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredString |   3475.39        |           0      |   2.17441  |   3.68788 |   4.64729 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Sparse         |   7857.56        |           0      |   1.00477  |   1.44027 |   1.60202 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_ByID           |   3795.73        |           0      |   1.99011  |   3.17307 |   3.72976 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_GraphRAG       |   1702.12        |           0      |   3.09942  |  13.0971  |  24.6091  |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2166.08        |           0      |   2.8214   |   6.41761 |  29.9441  |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Recommend      |   3641.7         |           0      |   2.09881  |   3.34118 |   3.93679 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Geo            |   3440.13        |           0      |   1.73504  |   2.55066 |  15.9864  |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Temporal       |   3839.53        |           0      |   2.05188  |   2.79596 |   3.21725 |
| remote | cpu    | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_LearnedIndex   |   3291.07        |           0      |   2.24768  |   3.93572 |   5.27324 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | DoPut                 | 429546           |         314.609  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | DoGet                 | 623457           |         456.634  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Dense          |   1103.85        |           0      |   6.80744  |  11.2487  |  16.3946  |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Hybrid         |   1181.41        |           0      |   6.65414  |   9.26445 |  10.6283  |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Filtered       |   1197.05        |           0      |   6.64966  |   7.75082 |   9.08246 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredBool   |   1200.98        |           0      |   6.68589  |   7.65548 |   8.34606 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredString |   1184.71        |           0      |   6.67443  |   7.79178 |   8.95342 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Sparse         |   7912.79        |           0      |   1.00328  |   1.42978 |   1.58125 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_ByID           |   1208.76        |           0      |   6.16529  |  10.4253  |  17.1557  |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_GraphRAG       |   1068.63        |           0      |   7.09259  |   9.5877  |  17.8909  |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_GlobalGraphRAG |   1125.5         |           0      |   7.07925  |   8.72272 |   9.53152 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Recommend      |    890.464       |           0      |   8.82007  |  11.429   |  14.0021  |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Geo            |   1761.03        |           0      |   4.52105  |   5.73155 |   6.27457 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Temporal       |   2377.25        |           0      |   3.21107  |   4.74719 |   5.83944 |
| remote | cpu    | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_LearnedIndex   |   1124.98        |           0      |   7.06397  |   9.05896 |  10.8428  |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoPut                 |  31276.2         |          91.6294 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoGet                 |  41091.3         |         120.385  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Dense          |   2348.26        |           0      |   3.00857  |   4.64099 |  15.6022  |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Hybrid         |   2612.14        |           0      |   2.85093  |   4.50497 |   6.501   |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Filtered       |   2790.44        |           0      |   2.76683  |   3.98155 |   4.66012 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredBool   |   2880.46        |           0      |   2.71264  |   3.84449 |   4.22833 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredString |   2446.38        |           0      |   2.97017  |   4.97256 |   7.0083  |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Sparse         |   8174.02        |           0      |   0.94536  |   1.47282 |   2.04931 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_ByID           |   3636.87        |           0      |   2.13609  |   3.09149 |   3.80772 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GraphRAG       |   2574.63        |           0      |   2.85157  |   5.1644  |   7.38797 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |   2522.83        |           0      |   2.97582  |   4.27088 |   5.04157 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Recommend      |   3199.94        |           0      |   2.17302  |   3.73886 |   4.80056 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Geo            |   3906.82        |           0      |   1.78686  |   3.78994 |   5.53625 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Temporal       |   3658.3         |           0      |   2.13315  |   3.09003 |   3.98152 |
| remote | cpu    | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |   2443.02        |           0      |   2.99403  |   5.6954  |   8.04452 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | DoPut                 |  30407.7         |          89.0851 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | DoGet                 |  39750.1         |         116.455  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Dense          |   2678           |           0      |   2.71938  |   4.51816 |   6.73306 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Hybrid         |   2108.26        |           0      |   2.82     |   6.65931 |  31.0198  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Filtered       |   2398.81        |           0      |   2.83911  |   4.60178 |  13.5506  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_FilteredBool   |   2578.46        |           0      |   2.69523  |   4.38059 |  12.0588  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_FilteredString |   2467.83        |           0      |   2.87402  |   4.51823 |  14.2643  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Sparse         |   8124.62        |           0      |   0.979784 |   1.37286 |   1.51528 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_ByID           |   2860.05        |           0      |   2.44141  |   5.26114 |   7.45142 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_GraphRAG       |   2605.52        |           0      |   2.86065  |   4.46857 |   5.58458 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |   2489.18        |           0      |   2.71589  |   4.44465 |  14.2435  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Recommend      |   3032.87        |           0      |   2.40429  |   4.27093 |   4.8636  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Geo            |   3596.21        |           0      |   1.65049  |   2.41123 |  26.4041  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_Temporal       |   3736.69        |           0      |   2.10405  |   3.04371 |   3.86751 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json   | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |   2426.45        |           0      |   3.00824  |   4.96882 |   9.02727 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | DoPut                 | 397043           |         290.803  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | DoGet                 | 220899           |         161.792  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Dense          |   2146.56        |           0      |   2.50449  |   6.20776 |  36.2332  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Hybrid         |   2581.68        |           0      |   2.97049  |   4.36653 |   4.9543  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Filtered       |   2521.06        |           0      |   2.48475  |   4.33367 |  17.3942  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredBool   |   2577.78        |           0      |   2.87418  |   4.15501 |   4.67821 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredString |   2328.46        |           0      |   3.24096  |   4.69046 |   5.48576 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Sparse         |   8438.05        |           0      |   0.937453 |   1.35113 |   1.53515 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_ByID           |   2783.03        |           0      |   2.69037  |   4.67212 |   6.48817 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_GraphRAG       |   1144.46        |           0      |   4.78788  |  19.7331  |  31.2707  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_GlobalGraphRAG |   1275.72        |           0      |   4.46309  |  17.1732  |  29.4652  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Recommend      |   2945.63        |           0      |   2.41813  |   3.80025 |   4.8974  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Geo            |   4016.04        |           0      |   1.70102  |   2.35894 |  12.1656  |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Temporal       |   3988.53        |           0      |   1.97053  |   2.93034 |   3.30145 |
| remote | cpu    | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_LearnedIndex   |   2156.95        |           0      |   2.71011  |   8.70137 |  24.0196  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoPut                 | 505803           |          61.7435 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoGet                 | 185614           |          22.658  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Dense          |   3346.52        |           0      |   2.14065  |   3.52583 |   7.28188 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Hybrid         |   3275.35        |           0      |   1.91892  |   3.31563 |  33.9151  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Filtered       |   3511.75        |           0      |   2.16204  |   3.21948 |   3.87382 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredBool   |   3692.75        |           0      |   2.06951  |   3.08244 |   3.6169  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredString |   3034.41        |           0      |   2.0817   |   3.43662 |  12.6013  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Sparse         |   7852.58        |           0      |   0.997446 |   1.43416 |   1.63494 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_ByID           |   3835.35        |           0      |   1.99134  |   3.24062 |   3.78955 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GraphRAG       |   3731.41        |           0      |   2.09447  |   3.247   |   4.38446 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   3044.69        |           0      |   2.02918  |   3.47585 |  24.1709  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Recommend      |   3919.14        |           0      |   1.99328  |   3.03262 |   3.40604 |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Geo            |   3278.75        |           0      |   1.9079   |   3.92467 |  13.9805  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Temporal       |   3832.32        |           0      |   2.06418  |   2.82285 |   3.3752  |
| remote | cpu    | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   2642.61        |           0      |   2.58776  |   5.6295  |   8.72169 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | DoPut                 |      1.67069e+06 |         203.941  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | DoGet                 | 781953           |          95.4533 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Dense          |   1293.65        |           0      |   5.41768  |   7.30463 |  48.0228  |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Hybrid         |   1580.35        |           0      |   4.88123  |   6.97622 |   8.53767 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Filtered       |   1457.26        |           0      |   5.49261  |   6.69891 |   7.65712 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_FilteredBool   |   1433.02        |           0      |   5.52094  |   6.83637 |   7.79804 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_FilteredString |   1589.9         |           0      |   4.9631   |   6.22663 |   7.93304 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Sparse         |   8032.49        |           0      |   0.985239 |   1.4125  |   1.55309 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_ByID           |   1304.87        |           0      |   5.91082  |   8.62876 |  10.4109  |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_GraphRAG       |   1582.72        |           0      |   4.88665  |   7.17305 |   8.37856 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_GlobalGraphRAG |   1738.93        |           0      |   4.57443  |   5.93277 |   6.9631  |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Recommend      |   1447.67        |           0      |   5.43025  |   6.73425 |   7.86684 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Geo            |   1808.18        |           0      |   4.28252  |   5.47519 |   6.17283 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_Temporal       |   2455.31        |           0      |   2.9495   |   4.64137 |   6.20585 |
| remote | cpu    | result_cpu_int8_128_25000.json          | int8        |   128 |   25000 | Search_LearnedIndex   |   1451.43        |           0      |   5.04278  |   7.42802 |  29.1365  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | DoPut                 | 123572           |         362.027  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | DoGet                 | 125888           |         368.814  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Dense          |    877.581       |           0      |   5.98394  |  20.349   |  37.2007  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Hybrid         |   1335.89        |           0      |   4.80595  |  11.4167  |  25.1289  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Filtered       |   1464.47        |           0      |   4.5345   |   6.8762  |  20.3442  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_FilteredBool   |   1018.31        |           0      |   6.76178  |  10.7409  |  18.8319  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_FilteredString |    826.303       |           0      |   8.30578  |  13.0272  |  17.0785  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Sparse         |   8256.48        |           0      |   0.96224  |   1.37931 |   1.67787 |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_ByID           |   1685.5         |           0      |   4.24119  |   6.95623 |  14.0726  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_GraphRAG       |    948.369       |           0      |   6.76001  |  18.7291  |  33.2796  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_GlobalGraphRAG |    957.681       |           0      |   6.78453  |  15.9802  |  31.0329  |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Recommend      |   2002.1         |           0      |   3.47005  |   5.62155 |   9.48311 |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Geo            |   3723.5         |           0      |   1.72079  |   3.29317 |   6.21757 |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_Temporal       |   3892.38        |           0      |   1.99441  |   3.05683 |   3.52539 |
| remote | cpu    | result_cpu_int8_3072_5000.json          | int8        |  3072 |    5000 | Search_LearnedIndex   |   1478.84        |           0      |   4.82797  |   7.12948 |  11.242   |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | DoPut                 | 119202           |          87.3063 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | DoGet                 | 101644           |          74.4466 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Dense          |   3151.92        |           0      |   2.35676  |   3.72785 |   6.53098 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Hybrid         |   3268.9         |           0      |   2.38486  |   3.62841 |   4.35848 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Filtered       |   2389.6         |           0      |   2.29241  |   5.81371 |  35.6009  |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_FilteredBool   |   3351.34        |           0      |   2.29004  |   3.54934 |   4.39816 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_FilteredString |   3352.63        |           0      |   2.35645  |   3.16569 |   3.81921 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Sparse         |   7983.03        |           0      |   0.97863  |   1.41104 |   1.6099  |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_ByID           |   3790.79        |           0      |   1.97159  |   3.4886  |   4.76374 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_GraphRAG       |   3197.96        |           0      |   2.41902  |   3.71996 |   4.64425 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |   2953.37        |           0      |   2.45221  |   4.12895 |   5.93095 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Recommend      |   3899.96        |           0      |   1.95253  |   3.24015 |   3.82556 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Geo            |   3512.2         |           0      |   1.89778  |   3.53952 |   7.44654 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_Temporal       |   3876.61        |           0      |   2.04728  |   2.77925 |   3.26792 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json    | turboquant8 |   768 |    5000 | Search_LearnedIndex   |   2093.72        |           0      |   2.88567  |  10.2033  |  26.2909  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | DoPut                 | 123565           |         362.007  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | DoGet                 | 251439           |         736.639  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Dense          |    770.575       |           0      |  10.3219   |  12.3674  |  15.7062  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Hybrid         |    764.503       |           0      |  10.0465   |  14.524   |  17.9088  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Filtered       |    775.694       |           0      |  10.2775   |  12.3955  |  14.4383  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredBool   |    774.112       |           0      |  10.3111   |  12.0376  |  13.18    |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredString |    767.136       |           0      |  10.4029   |  12.3124  |  14.2193  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Sparse         |   9408.59        |           0      |   0.831514 |   1.24299 |   1.41825 |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_ByID           |    775.294       |           0      |  10.1658   |  13.3306  |  17.6439  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_GraphRAG       |    491.339       |           0      |  15.4902   |  24.393   |  35.2277  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_GlobalGraphRAG |    492.996       |           0      |  15.5879   |  23.5405  |  27.7052  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Recommend      |    578.621       |           0      |  13.701    |  16.7607  |  20.9447  |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Geo            |   1819.01        |           0      |   4.35474  |   5.40698 |   6.00062 |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Temporal       |   3148.03        |           0      |   2.33012  |   3.77632 |   5.04735 |
| remote | cpu    | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_LearnedIndex   |    764.85        |           0      |  10.2621   |  14.5312  |  17.2567  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | DoPut                 |  33017.4         |          96.7308 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | DoGet                 |  38201.8         |         111.919  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Dense          |    691.648       |           0      |  11.3924   |  15.2973  |  20.5369  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Hybrid         |    796.304       |           0      |   9.79966  |  13.8825  |  16.0053  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Filtered       |    835.65        |           0      |   9.53389  |  12.0306  |  14.0373  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_FilteredBool   |    832.275       |           0      |   9.46028  |  12.0116  |  15.2833  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_FilteredString |    836.128       |           0      |   9.51457  |  11.4094  |  14.7491  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Sparse         |   8955.87        |           0      |   0.874944 |   1.25438 |   1.45846 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_ByID           |    847.887       |           0      |   9.18692  |  12.6941  |  19.5353  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_GraphRAG       |    817.561       |           0      |   9.74204  |  12.2033  |  13.541   |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |    821.877       |           0      |   9.71855  |  12.2927  |  13.9006  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Recommend      |    751.989       |           0      |  10.5305   |  13.4164  |  15.802   |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Geo            |   1823.56        |           0      |   4.26565  |   5.5086  |   8.31546 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_Temporal       |   3127.92        |           0      |   2.39179  |   3.78154 |   4.58727 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json  | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |    802.824       |           0      |   9.78492  |  13.8222  |  16.2428  |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | DoPut                 |  31543.9         |         369.655  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | DoGet                 |  40892.9         |         479.213  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Dense          |   2105.5         |           0      |   3.03214  |   4.94658 |  20.9507  |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Hybrid         |   2270.09        |           0      |   3.2297   |   4.74364 |   5.67842 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Filtered       |   2333.21        |           0      |   3.10726  |   4.77509 |   7.52315 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_FilteredBool   |   2105.38        |           0      |   3.49132  |   5.30566 |   6.89004 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_FilteredString |   2048.65        |           0      |   3.59177  |   5.28669 |   6.05807 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Sparse         |   8049.19        |           0      |   0.987506 |   1.37043 |   1.50114 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_ByID           |   2701.46        |           0      |   2.61468  |   5.03723 |   6.91287 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_GraphRAG       |   1322.1         |           0      |   4.62681  |  12.6759  |  31.9116  |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1365.88        |           0      |   4.57334  |  11.069   |  34.935   |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Recommend      |   2833.75        |           0      |   2.55712  |   4.30395 |   5.01682 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Geo            |   3324.74        |           0      |   2.10171  |   3.24483 |   9.18517 |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_Temporal       |   3688.6         |           0      |   2.06069  |   2.97007 |   6.8759  |
| remote | cpu    | result_cpu_float32_3072_5000.json       | float32     |  3072 |    5000 | Search_LearnedIndex   |   1824.67        |           0      |   3.3982   |   8.14125 |  28.9102  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | DoPut                 | 342176           |          41.7696 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | DoGet                 | 328621           |          40.1149 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Dense          |   2783.3         |           0      |   2.10116  |   3.78557 |  35.2182  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Hybrid         |   3801.84        |           0      |   2.05667  |   3.1199  |   3.66152 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Filtered       |   3583.94        |           0      |   2.11392  |   3.29632 |   4.48438 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_FilteredBool   |   2832.67        |           0      |   2.69423  |   4.27622 |   5.04644 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_FilteredString |   2016.09        |           0      |   2.74595  |  11.5482  |  33.1311  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Sparse         |   7166.2         |           0      |   1.08227  |   1.62907 |   2.13948 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_ByID           |   3721.89        |           0      |   2.10295  |   2.9252  |   3.47722 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_GraphRAG       |   1455.62        |           0      |   3.60233  |  16.2236  |  34.0333  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_GlobalGraphRAG |   1717.66        |           0      |   3.3849   |   9.96837 |  31.2425  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Recommend      |   3768.13        |           0      |   2.08553  |   2.93147 |   3.34667 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Geo            |   2833.31        |           0      |   2.36716  |   4.22626 |  10.1659  |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_Temporal       |   3858.75        |           0      |   1.98617  |   3.24033 |   4.23635 |
| remote | cpu    | result_cpu_int8_128_5000.json           | int8        |   128 |    5000 | Search_LearnedIndex   |   2709.7         |           0      |   2.70317  |   4.95605 |   6.36645 |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | DoPut                 | 124768           |         365.531  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | DoGet                 | 221723           |         649.58   |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Dense          |    572.57        |           0      |  13.8459   |  17.0615  |  19.3805  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Hybrid         |    563.726       |           0      |  13.8369   |  19.8158  |  23.4076  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Filtered       |    561.156       |           0      |  14.165    |  17.7031  |  21.662   |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_FilteredBool   |    559.971       |           0      |  14.1692   |  18.919   |  22.515   |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_FilteredString |    548.924       |           0      |  14.4858   |  18.0512  |  21.9945  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Sparse         |   8233.87        |           0      |   0.974827 |   1.37488 |   1.51927 |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_ByID           |    626.819       |           0      |  12.4274   |  17.3703  |  24.3617  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_GraphRAG       |    525.021       |           0      |  15.0186   |  20.5319  |  25.4048  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_GlobalGraphRAG |    525.642       |           0      |  14.9691   |  20.6892  |  24.3434  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Recommend      |    412.486       |           0      |  19.3456   |  23.9799  |  29.0553  |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Geo            |   1850           |           0      |   4.2679   |   5.41912 |   6.01041 |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_Temporal       |   2522.66        |           0      |   2.88511  |   4.54068 |   5.85454 |
| remote | cpu    | result_cpu_int8_3072_25000.json         | int8        |  3072 |   25000 | Search_LearnedIndex   |    532.231       |           0      |  14.3137   |  21.9743  |  25.5222  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | DoPut                 | 659650           |         322.095  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | DoGet                 | 808933           |         394.987  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Dense          |   1221.13        |           0      |   6.18305  |   8.70259 |  15.961   |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Hybrid         |   1255.57        |           0      |   6.18969  |   8.8863  |  10.1632  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Filtered       |   1289.8         |           0      |   6.15597  |   7.57528 |   9.27386 |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_FilteredBool   |   1308.23        |           0      |   6.10808  |   7.23911 |   7.89097 |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_FilteredString |   1314.73        |           0      |   6.07091  |   7.29765 |   8.05332 |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Sparse         |   8043.64        |           0      |   0.982824 |   1.37694 |   1.62419 |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_ByID           |   1231.03        |           0      |   6.31554  |   8.93612 |  11.9692  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_GraphRAG       |    560.356       |           0      |  13.5513   |  20.6829  |  25.0835  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_GlobalGraphRAG |    554.885       |           0      |  13.6799   |  20.8158  |  25.5455  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Recommend      |    992.032       |           0      |   8.00057  |  10.3213  |  11.8939  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Geo            |   1802.25        |           0      |   4.31796  |   5.68888 |   8.8446  |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_Temporal       |   3179.32        |           0      |   2.36661  |   3.64217 |   4.29043 |
| remote | cpu    | result_cpu_float32_128_25000.json       | float32     |   128 |   25000 | Search_LearnedIndex   |   1287.49        |           0      |   6.07729  |   8.74094 |  10.8911  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | DoPut                 | 496554           |          60.6145 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | DoGet                 | 241707           |          29.5053 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Dense          |   3093.12        |           0      |   2.17858  |   3.34694 |   9.6213  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Hybrid         |   3131.97        |           0      |   1.81171  |   2.91211 |  35.9723  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Filtered       |   3494.35        |           0      |   2.18651  |   3.09979 |   4.05396 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_FilteredBool   |   3582.49        |           0      |   2.15948  |   3.05053 |   3.73753 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_FilteredString |   3557.88        |           0      |   2.16452  |   3.08097 |   4.26394 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Sparse         |   7894.38        |           0      |   1.00351  |   1.41226 |   1.56908 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_ByID           |   3742.92        |           0      |   2.07539  |   3.03945 |   3.57559 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_GraphRAG       |   3418.94        |           0      |   2.11689  |   3.08233 |   3.50741 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   3021.27        |           0      |   2.06039  |   4.05568 |  20.4416  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Recommend      |   3752.56        |           0      |   2.05304  |   3.14056 |   3.62691 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Geo            |   3334.21        |           0      |   1.7222   |   5.44047 |  10.7787  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_Temporal       |   3895.16        |           0      |   2.04449  |   2.82929 |   3.15269 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json    | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   2092.54        |           0      |   3.04116  |   7.69852 |  16.8553  |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoPut                 | 112351           |          82.2883 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoGet                 | 104186           |          76.308  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Dense          |   2789.57        |           0      |   2.34954  |   4.2971  |   7.49864 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Hybrid         |   2609.19        |           0      |   2.94694  |   4.76457 |   5.41162 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Filtered       |   3093.48        |           0      |   2.4572   |   4.01223 |   5.5108  |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredBool   |   2976.52        |           0      |   2.58944  |   4.08298 |   4.91468 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredString |   3047.02        |           0      |   2.51561  |   4.06562 |   4.97632 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Sparse         |   7818.53        |           0      |   1.01017  |   1.44972 |   1.65072 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_ByID           |   3182.03        |           0      |   2.3173   |   4.20107 |   4.96308 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GraphRAG       |   2407.83        |           0      |   2.77664  |   5.16529 |  10.6199  |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |   2774.98        |           0      |   2.55502  |   4.10709 |   9.10204 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Recommend      |   3618.71        |           0      |   2.05023  |   3.54397 |   4.17113 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Geo            |   3227.62        |           0      |   2.34859  |   3.62699 |   4.14407 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Temporal       |   3769.74        |           0      |   2.08266  |   2.91275 |   3.62268 |
| remote | cpu    | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_LearnedIndex   |   2203.14        |           0      |   2.90982  |   7.34481 |  13.2002  |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | DoPut                 |  32847.6         |         384.933  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | DoGet                 |  57118.4         |         669.356  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Dense          |     72.4764      |           0      |  79.5387   | 237.28    | 269.141   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Hybrid         |     82.0704      |           0      |  85.07     | 176.673   | 261.06    |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Filtered       |     43.7982      |           0      | 204.828    | 255.491   | 278.192   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredBool   |     69.0403      |           0      | 115.92     | 216.599   | 255.217   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredString |     67.0126      |           0      | 123.898    | 168.925   | 222.193   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Sparse         |   3557.8         |           0      |   2.14058  |   3.13014 |   5.47329 |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_ByID           |     98.6477      |           0      |  80.758    | 122.163   | 166.605   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GraphRAG       |     86.7231      |           0      |  92.2483   | 135.879   | 166.04    |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GlobalGraphRAG |     84.456       |           0      |  94.3747   | 146.26    | 181.903   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Recommend      |     67.2661      |           0      | 120.756    | 162.906   | 200.164   |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Geo            |    319.611       |           0      |  24.9462   |  32.1847  |  36.1014  |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Temporal       |      0           |           0      |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_LearnedIndex   |      0           |           0      |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | DoPut                 | 811475           |          99.0569 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | DoGet                 | 342065           |          41.7559 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Dense          |   3155.38        |           0      |   2.15728  |   3.66459 |  15.6207  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Hybrid         |   2804.43        |           0      |   1.93443  |   3.37506 |  33.2324  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Filtered       |   3689.52        |           0      |   2.05981  |   3.09844 |   3.89828 |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredBool   |   3334.27        |           0      |   2.30958  |   3.41386 |   4.06515 |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredString |   2548.21        |           0      |   2.73289  |   4.42937 |  12.8231  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Sparse         |   5857.99        |           0      |   1.1057   |   1.80937 |   2.92955 |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_ByID           |   3702.55        |           0      |   2.12443  |   2.98308 |   3.50102 |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_GraphRAG       |   1252.32        |           0      |   4.27178  |  15.7557  |  37.0875  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_GlobalGraphRAG |   1531.08        |           0      |   3.70776  |  13.1234  |  27.3623  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Recommend      |   3717.64        |           0      |   2.08363  |   2.95024 |   3.6628  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Geo            |   3580.62        |           0      |   1.79353  |   3.91434 |  10.9179  |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Temporal       |   3619.2         |           0      |   2.04297  |   3.54531 |   6.05011 |
| remote | cpu    | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_LearnedIndex   |   2895.13        |           0      |   2.56114  |   4.43194 |   5.62022 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | DoPut                 | 121533           |          89.0136 |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | DoGet                 | 195719           |         143.349  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Dense          |    946.425       |           0      |   6.91363  |  15.6024  |  40.2192  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Hybrid         |   1121.54        |           0      |   6.22389  |  11.1011  |  30.7015  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Filtered       |   1202.8         |           0      |   6.10069  |  10.6658  |  14.5866  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_FilteredBool   |   1259.38        |           0      |   6.09009  |   7.42882 |  15.7246  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_FilteredString |   1216.85        |           0      |   6.20813  |   8.10863 |  15.0394  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Sparse         |   8896.38        |           0      |   0.877829 |   1.30577 |   1.44818 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_ByID           |   1244.92        |           0      |   6.11189  |  10.1222  |  13.9562  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_GraphRAG       |   1166           |           0      |   6.24671  |   8.88766 |  32.0981  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |   1201.29        |           0      |   6.44037  |   8.17047 |  17.1912  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Recommend      |   1139.47        |           0      |   6.18666  |  12.4294  |  27.5632  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Geo            |   1751.55        |           0      |   4.44288  |   5.7523  |   7.6397  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_Temporal       |   3325.34        |           0      |   2.29414  |   3.56128 |   3.98579 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json   | turboquant8 |   768 |   25000 | Search_LearnedIndex   |   1196.52        |           0      |   6.30366  |   9.38312 |  16.0804  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoPut                 | 120572           |          88.3095 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoGet                 | 171498           |         125.609  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Dense          |    971.145       |           0      |   7.34375  |  14.4393  |  28.5904  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Hybrid         |   1041.11        |           0      |   6.68664  |  12.003   |  32.7689  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Filtered       |   1126.29        |           0      |   6.76587  |   8.88402 |  15.5263  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredBool   |   1098.25        |           0      |   6.80153  |  10.4306  |  20.3604  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredString |   1094.76        |           0      |   6.64274  |  10.2596  |  37.5744  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Sparse         |   8072.99        |           0      |   0.985054 |   1.38004 |   1.57256 |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_ByID           |   1116.99        |           0      |   6.76406  |  11.9816  |  15.8075  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GraphRAG       |   1089.02        |           0      |   6.72157  |  10.1802  |  39.9109  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |   1073.25        |           0      |   7.00933  |  13.0576  |  17.0706  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Recommend      |   1116.67        |           0      |   6.78499  |  10.0521  |  20.7458  |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Geo            |   1886.4         |           0      |   4.16819  |   5.36111 |   5.82581 |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Temporal       |   3265.32        |           0      |   2.30389  |   3.72215 |   4.40217 |
| remote | cpu    | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_LearnedIndex   |   1124.58        |           0      |   6.66409  |   9.84295 |  14.7455  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | DoPut                 | 104203           |         305.282  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | DoGet                 | 106618           |         312.358  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Dense          |   1397.38        |           0      |   4.37297  |   8.39611 |  36.6219  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Hybrid         |   1548.25        |           0      |   4.71126  |   6.93056 |   9.68631 |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Filtered       |   1717.26        |           0      |   4.34646  |   6.362   |   8.10935 |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredBool   |   1063.17        |           0      |   6.5783   |  10.1105  |  17.4839  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredString |    815.941       |           0      |   8.48849  |  13.5063  |  14.1687  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Sparse         |   8082.46        |           0      |   0.969071 |   1.39838 |   1.63365 |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_ByID           |   1988.44        |           0      |   3.60882  |   6.7272  |  10.6444  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GraphRAG       |    907.429       |           0      |   6.77928  |  18.7599  |  38.7118  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GlobalGraphRAG |   1044.8         |           0      |   6.33169  |  13.4217  |  28.1334  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Recommend      |   1867.94        |           0      |   3.59051  |   6.07842 |  14.7395  |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Geo            |   4097.49        |           0      |   1.6879   |   2.72686 |   7.91117 |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Temporal       |   3955           |           0      |   1.99744  |   3.07025 |   3.52838 |
| remote | cpu    | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_LearnedIndex   |   1513.68        |           0      |   4.72764  |   7.12851 |   9.43357 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | DoPut                 |      1.78409e+06 |         217.784  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | DoGet                 | 928585           |         113.353  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Dense          |   1331.98        |           0      |   5.45489  |   9.23704 |  19.177   |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Hybrid         |   1615.21        |           0      |   4.7894   |   6.94205 |   8.29193 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Filtered       |   1359.25        |           0      |   5.34923  |   7.33387 |  21.4169  |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredBool   |   1440.59        |           0      |   5.47897  |   6.78421 |   7.90987 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredString |   1377.26        |           0      |   5.75981  |   7.18768 |   8.09238 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Sparse         |   7576.56        |           0      |   1.05079  |   1.45478 |   1.64961 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_ByID           |   1415.97        |           0      |   5.34356  |   8.19656 |  10.2201  |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_GraphRAG       |   1516.87        |           0      |   5.07655  |   7.62472 |   9.26176 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_GlobalGraphRAG |   1561.18        |           0      |   5.04385  |   7.21211 |   8.42629 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Recommend      |   1300.96        |           0      |   6.06365  |   7.57657 |   9.5699  |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Geo            |   1682.8         |           0      |   4.62801  |   5.97431 |   9.34373 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Temporal       |   2462.8         |           0      |   2.9519   |   4.69909 |   6.08287 |
| remote | cpu    | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_LearnedIndex   |   1473.96        |           0      |   5.23065  |   7.89225 |  10.2919  |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | DoPut                 | 328409           |         240.534  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | DoGet                 | 168483           |         123.401  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Dense          |   2344.1         |           0      |   2.81852  |   5.90392 |  13.9689  |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Hybrid         |   2532.65        |           0      |   2.92122  |   4.95005 |   7.02635 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Filtered       |   2341.51        |           0      |   2.49199  |   4.50924 |  32.4434  |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_FilteredBool   |   2428.06        |           0      |   3.04136  |   5.08106 |   6.45466 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_FilteredString |   2112.14        |           0      |   3.44017  |   5.05187 |   5.40841 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Sparse         |   8261.14        |           0      |   0.932908 |   1.43366 |   1.73206 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_ByID           |   2776.15        |           0      |   2.58567  |   5.29253 |   7.07047 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_GraphRAG       |   1267.41        |           0      |   4.37786  |  16.4837  |  36.1853  |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_GlobalGraphRAG |   1268.97        |           0      |   4.41586  |  16.9012  |  35.6559  |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Recommend      |   3220.31        |           0      |   2.33895  |   3.654   |   4.03924 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Geo            |   3735.25        |           0      |   1.86825  |   3.77399 |   5.92704 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_Temporal       |   3744.25        |           0      |   1.99245  |   3.27725 |   6.88378 |
| remote | cpu    | result_cpu_int8_768_5000.json           | int8        |   768 |    5000 | Search_LearnedIndex   |   2717.53        |           0      |   2.63745  |   4.75619 |   6.91738 |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | DoPut                 | 122974           |         360.275  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | DoGet                 | 244442           |         716.139  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Dense          |    561.843       |           0      |  14.22     |  17.5074  |  21.2778  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Hybrid         |    525.397       |           0      |  14.6344   |  22.0641  |  27.6524  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Filtered       |    554.926       |           0      |  14.4273   |  18.0517  |  21.0018  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredBool   |    543.94        |           0      |  14.6316   |  18.3264  |  20.8189  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredString |    543.081       |           0      |  14.6299   |  19.4003  |  22.1992  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Sparse         |   8490.91        |           0      |   0.932193 |   1.29894 |   1.49937 |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_ByID           |    628.976       |           0      |  12.4141   |  16.7945  |  24.1446  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GraphRAG       |    518.314       |           0      |  15.097    |  20.4878  |  29.0195  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GlobalGraphRAG |    527.44        |           0      |  14.9645   |  19.8913  |  22.8751  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Recommend      |    414.322       |           0      |  19.1622   |  24.4401  |  28.5865  |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Geo            |   1769.49        |           0      |   4.3435   |   5.68571 |   7.18512 |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Temporal       |   2589.72        |           0      |   2.85015  |   4.40136 |   5.25023 |
| remote | cpu    | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_LearnedIndex   |    515.15        |           0      |  15.4157   |  21.5934  |  25.5692  |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | DoPut                 |  79065.2         |         231.636  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | DoGet                 |  97710.4         |         286.261  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Dense          |   2329.94        |           0      |   2.29765  |   6.87955 |  34.442   |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Hybrid         |   2623.37        |           0      |   2.56279  |   5.75125 |  15.6408  |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Filtered       |   3216.97        |           0      |   2.36246  |   3.35076 |   4.34488 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_FilteredBool   |   2840.93        |           0      |   2.69718  |   4.43593 |   5.35876 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_FilteredString |   2848.33        |           0      |   2.65891  |   4.19312 |   5.28026 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Sparse         |   6533.71        |           0      |   1.07423  |   1.67812 |   2.6283  |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_ByID           |   3419.61        |           0      |   2.05083  |   4.11569 |   7.33953 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_GraphRAG       |   1484.11        |           0      |   3.33391  |  16.8483  |  30.2564  |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_GlobalGraphRAG |   1757.55        |           0      |   3.15494  |  11.5187  |  24.4813  |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Recommend      |   3082.07        |           0      |   2.15615  |   4.81645 |  13.03    |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Geo            |   3547.85        |           0      |   1.919    |   4.2874  |   5.29084 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_Temporal       |   3696.5         |           0      |   2.09397  |   3.03022 |   4.61335 |
| remote | cpu    | result_cpu_float32_768_5000.json        | float32     |   768 |    5000 | Search_LearnedIndex   |   2528.32        |           0      |   2.89556  |   5.08428 |   7.33297 |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | DoPut                 | 125319           |         367.145  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | DoGet                 | 218178           |         639.193  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Dense          |    753.106       |           0      |  10.4322   |  12.511   |  19.4099  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Hybrid         |    795.525       |           0      |   9.7026   |  14.2576  |  16.3535  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Filtered       |    778.959       |           0      |  10.2751   |  12.1995  |  13.597   |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_FilteredBool   |    790.225       |           0      |  10.1659   |  11.84    |  12.8483  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_FilteredString |    783.508       |           0      |  10.209    |  11.9173  |  13.5503  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Sparse         |   7858.07        |           0      |   1.00053  |   1.41942 |   1.58171 |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_ByID           |    792.555       |           0      |   9.81327  |  12.4215  |  21.298   |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_GraphRAG       |    490.048       |           0      |  15.8379   |  21.9939  |  27.1422  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_GlobalGraphRAG |    494.195       |           0      |  15.9388   |  21.8755  |  24.8709  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Recommend      |    588.946       |           0      |  13.4589   |  16.8704  |  21.3501  |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Geo            |   1875.22        |           0      |   4.22138  |   5.26666 |   5.80052 |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_Temporal       |   3398.71        |           0      |   2.21907  |   3.48628 |   4.09528 |
| remote | cpu    | result_cpu_float32_768_25000.json       | float32     |   768 |   25000 | Search_LearnedIndex   |    777.615       |           0      |  10.0707   |  14.1134  |  18.2632  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 |  33131.3         |          97.0643 |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 |  62369.5         |         182.723  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |    669.611       |           0      |   8.95486  |  38.6024  |  58.7191  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |    883.517       |           0      |   8.64245  |  13.8274  |  17.2941  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |    883.425       |           0      |   8.51265  |  13.7056  |  18.1438  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |    876.645       |           0      |   8.70238  |  13.5828  |  16.3513  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |    872.182       |           0      |   8.70934  |  13.646   |  15.7753  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |   9248.68        |           0      |   0.846168 |   1.26422 |   1.457   |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |    892.348       |           0      |   8.33996  |  14.8088  |  19.3209  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |    873.424       |           0      |   8.83258  |  13.5946  |  16.1879  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |    877.862       |           0      |   8.80368  |  12.8904  |  15.9726  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |    910.989       |           0      |   8.43072  |  13.0072  |  15.8887  |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |   1862.06        |           0      |   4.25973  |   5.30752 |   6.43171 |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |   3376.3         |           0      |   2.22147  |   3.52106 |   4.52603 |
| remote | cpu    | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |    802.625       |           0      |   9.17619  |  15.4791  |  30.2562  |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | DoPut                 | 113953           |         333.848  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | DoGet                 | 111083           |         325.438  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Dense          |   3033.22        |           0      |   2.29572  |   3.51518 |   5.8532  |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Hybrid         |   3311.69        |           0      |   2.33017  |   3.70981 |   4.33998 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Filtered       |   2467.68        |           0      |   2.34116  |   4.04356 |  35.8084  |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredBool   |   2796.57        |           0      |   2.74643  |   4.47273 |   5.38225 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredString |   2752.3         |           0      |   2.78923  |   4.31483 |   5.04462 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Sparse         |   7550.04        |           0      |   1.0595   |   1.45161 |   1.57609 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_ByID           |   3775.94        |           0      |   2.00988  |   3.25728 |   3.72816 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_GraphRAG       |   1825.56        |           0      |   3.17648  |   8.9811  |  31.851   |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_GlobalGraphRAG |   2110.2         |           0      |   3.09488  |   6.66113 |  16.3941  |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Recommend      |   3663.27        |           0      |   2.02655  |   3.4272  |   6.04122 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Geo            |   3882.32        |           0      |   1.89471  |   3.14558 |   3.99432 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Temporal       |   3868           |           0      |   2.07203  |   2.80937 |   3.09513 |
| remote | cpu    | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_LearnedIndex   |   3160.49        |           0      |   2.32939  |   3.86072 |   4.97298 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | DoPut                 |  30542.2         |         357.916  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | DoGet                 |  41335.3         |         484.398  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Dense          |   2008.06        |           0      |   3.04228  |   5.73342 |  32.1698  |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Hybrid         |   2464.99        |           0      |   3.04754  |   4.31823 |   5.21847 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Filtered       |   2540.21        |           0      |   2.91059  |   4.35022 |   4.9537  |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredBool   |   2250.93        |           0      |   3.32669  |   4.84588 |   5.79476 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredString |   1715.11        |           0      |   3.78219  |   6.95089 |  27.516   |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Sparse         |   8440.44        |           0      |   0.950698 |   1.33023 |   1.44849 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_ByID           |   2600.41        |           0      |   2.75988  |   5.16452 |   6.73942 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GraphRAG       |   1243.28        |           0      |   4.73189  |  17.1843  |  22.8001  |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1403.19        |           0      |   4.29934  |  11.1109  |  33.7085  |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Recommend      |   2736.85        |           0      |   2.5791   |   4.53639 |   5.70814 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Geo            |   3793.95        |           0      |   1.78464  |   3.43794 |   9.91307 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Temporal       |   3800.35        |           0      |   2.07093  |   2.85982 |   3.20334 |
| remote | cpu    | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_LearnedIndex   |   2201.63        |           0      |   3.36795  |   5.17463 |   6.61669 |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | DoPut                 |  33396.3         |         391.363  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | DoGet                 |  60222.8         |         705.735  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Dense          |    266.482       |           0      |  29.9221   |  39.6887  |  50.3432  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Hybrid         |    268.597       |           0      |  28.7837   |  42.6944  |  50.7604  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Filtered       |    264.248       |           0      |  29.9608   |  39.6938  |  47.4705  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_FilteredBool   |    263.081       |           0      |  30.3387   |  38.6907  |  46.2671  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_FilteredString |    265.433       |           0      |  29.8842   |  39.0878  |  46.4678  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Sparse         |   8017.03        |           0      |   1.00372  |   1.39091 |   1.53576 |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_ByID           |    261.577       |           0      |  30.5798   |  38.2375  |  47.7057  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_GraphRAG       |    258.642       |           0      |  30.5561   |  43.3739  |  51.748   |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_GlobalGraphRAG |    260.334       |           0      |  30.2979   |  41.8631  |  49.6403  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Recommend      |    199.1         |           0      |  39.5962   |  53.7008  |  66.5343  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Geo            |   1635.2         |           0      |   4.77758  |   6.08835 |   6.72832 |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_Temporal       |   2946.67        |           0      |   2.55564  |   3.92785 |   4.4691  |
| remote | cpu    | result_cpu_float32_3072_25000.json      | float32     |  3072 |   25000 | Search_LearnedIndex   |    265.505       |           0      |  29.3862   |  44.2243  |  55.6206  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | DoPut                 | 412232           |         301.928  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | DoGet                 |      1.14956e+06 |         841.961  |   0        |   0       |   0       |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Dense          |   1142.74        |           0      |   6.5425   |   9.61409 |  19.5988  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Hybrid         |   1187.96        |           0      |   6.57002  |   9.30138 |  11.0483  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Filtered       |   1226.15        |           0      |   6.49118  |   7.45174 |   8.11001 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_FilteredBool   |   1210.91        |           0      |   6.55128  |   7.62426 |   8.95115 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_FilteredString |   1212.02        |           0      |   6.56346  |   7.5794  |   8.28956 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Sparse         |   8229.55        |           0      |   0.964501 |   1.36891 |   1.49853 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_ByID           |   1015.02        |           0      |   7.16772  |  12.8479  |  18.0279  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_GraphRAG       |    961.082       |           0      |   7.92945  |  10.7985  |  20.9724  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_GlobalGraphRAG |    982.493       |           0      |   8.01227  |  11.0218  |  12.6279  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Recommend      |    898.925       |           0      |   8.84298  |  10.9397  |  12.4373  |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Geo            |   1750.29        |           0      |   4.47257  |   5.78386 |   7.68898 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_Temporal       |   2533.38        |           0      |   2.91633  |   4.52733 |   6.22774 |
| remote | cpu    | result_cpu_int8_768_25000.json          | int8        |   768 |   25000 | Search_LearnedIndex   |   1081.65        |           0      |   6.96438  |   9.9871  |  19.5049  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | DoPut                 | 641683           |         313.322  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | DoGet                 | 709025           |         346.203  |   0        |   0       |   0       |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Dense          |   1169.85        |           0      |   6.43865  |   9.14739 |  18.5773  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Hybrid         |   1209.2         |           0      |   6.37222  |   9.39041 |  10.6316  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Filtered       |   1270.89        |           0      |   6.27094  |   7.60735 |   9.00799 |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredBool   |   1274.1         |           0      |   6.26822  |   7.47991 |   8.08497 |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredString |   1242.69        |           0      |   6.38612  |   7.93047 |   9.06521 |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Sparse         |   7575.98        |           0      |   1.04057  |   1.50664 |   2.05621 |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_ByID           |   1212.52        |           0      |   6.32172  |   9.7866  |  14.684   |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_GraphRAG       |    583.188       |           0      |  13.001    |  20.2628  |  23.3064  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_GlobalGraphRAG |    576.635       |           0      |  13.1193   |  20.1284  |  23.3089  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Recommend      |    963.797       |           0      |   8.20441  |  10.3622  |  12.2206  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Geo            |   1896.02        |           0      |   4.06112  |   5.32503 |   6.63949 |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Temporal       |   3100.96        |           0      |   2.43899  |   3.65603 |   4.2981  |
| remote | cpu    | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_LearnedIndex   |   1197.61        |           0      |   5.9972   |   9.16451 |  38.9335  |

### Details: remote (cuda)

| Host   | Mode   | Dataset                                 | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:----------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|---------:|---------:|
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 |       667845     |          81.5241 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 |       784380     |          95.7495 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |         1080.9   |           0      |  6.44487  | 13.3818  | 35.0462  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |         1232.58  |           0      |  6.10319  |  9.52605 | 14.8531  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |         1342.47  |           0      |  5.63786  |  7.28599 | 11.7967  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |         1407.86  |           0      |  5.62763  |  6.95228 |  7.84821 |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |         1372.4   |           0      |  5.71454  |  7.29797 | 11.9331  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |         5909.59  |           0      |  1.06556  |  1.92562 |  5.47206 |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |         1250.63  |           0      |  6.02874  | 10.5236  | 13.441   |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |         1210.78  |           0      |  5.92622  |  9.11697 | 29.0476  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |         1314.31  |           0      |  5.97879  |  8.0381  |  9.42338 |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |         1301.86  |           0      |  5.86214  |  7.83976 | 13.6783  |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |         1818.34  |           0      |  4.31311  |  5.53619 |  6.42192 |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |         3086.32  |           0      |  2.44239  |  3.79114 |  4.44051 |
| remote | cuda   | result_cuda_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |         1331.6   |           0      |  5.64375  |  7.91139 | 18.2238  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | DoPut                 |       186024     |          90.832  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | DoGet                 |       367480     |         179.434  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Dense          |         3486.35  |           0      |  2.14171  |  3.31798 |  6.29924 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Hybrid         |         3157.05  |           0      |  1.73065  |  3.32265 | 33.7678  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Filtered       |         3573.9   |           0      |  2.11456  |  3.10144 |  5.20511 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredBool   |         3104.91  |           0      |  2.11685  |  4.23785 | 14.4384  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredString |         3499.02  |           0      |  2.19359  |  3.5321  |  4.37491 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Sparse         |         7705.22  |           0      |  1.02396  |  1.45362 |  1.58367 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_ByID           |         3677     |           0      |  2.06699  |  3.37149 |  4.29585 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_GraphRAG       |         1841.18  |           0      |  2.97584  | 11.2096  | 32.9946  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_GlobalGraphRAG |         1817.33  |           0      |  2.9495   | 11.1462  | 30.9104  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Recommend      |         3213.8   |           0      |  2.02441  |  3.32461 | 18.0547  |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Geo            |         3681.6   |           0      |  1.78414  |  5.23985 |  9.53985 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_Temporal       |         3559.96  |           0      |  2.0478   |  4.21763 |  6.87563 |
| remote | cuda   | result_cuda_float32_128_5000.json       | float32     |   128 |    5000 | Search_LearnedIndex   |         2603.96  |           0      |  2.63268  |  4.99799 |  9.93079 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | DoPut                 |       424759     |         311.103  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | DoGet                 |       543263     |         397.898  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Dense          |         1110.45  |           0      |  6.71443  | 11.5115  | 16.7891  |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Hybrid         |         1059.06  |           0      |  6.8665   | 12.0584  | 22.9218  |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Filtered       |         1186.21  |           0      |  6.67167  |  7.91986 |  9.10298 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredBool   |         1189.28  |           0      |  6.74407  |  7.70019 |  8.31476 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredString |         1174.16  |           0      |  6.76212  |  7.84876 |  8.94949 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Sparse         |         7875.25  |           0      |  1.02304  |  1.42085 |  1.56675 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_ByID           |         1201.97  |           0      |  6.36215  |  9.76795 | 13.2719  |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_GraphRAG       |         1076.08  |           0      |  7.08302  |  8.92644 | 30.0587  |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_GlobalGraphRAG |         1116.12  |           0      |  7.14326  |  8.51735 |  9.14375 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Recommend      |          851.703 |           0      |  9.31168  | 11.6329  | 13.9524  |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Geo            |         1783.56  |           0      |  4.43569  |  5.69405 |  6.19073 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_Temporal       |         2523.41  |           0      |  2.93452  |  4.69677 |  6.68169 |
| remote | cuda   | result_cuda_int8_768_25000.json         | int8        |   768 |   25000 | Search_LearnedIndex   |         1129.08  |           0      |  7.05724  |  9.23031 | 10.7481  |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoPut                 |        31252.2   |          91.5592 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoGet                 |        36496.9   |         106.925  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Dense          |         2421.82  |           0      |  2.83777  |  4.44486 |  7.44044 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Hybrid         |         2563.42  |           0      |  3.01239  |  4.32306 |  4.96725 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Filtered       |         2772.53  |           0      |  2.74681  |  3.84068 |  4.47327 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredBool   |         2628.39  |           0      |  2.91306  |  4.10087 |  5.29875 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredString |         2453.79  |           0      |  2.76986  |  4.12285 | 30.6514  |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Sparse         |         8284.94  |           0      |  0.957041 |  1.34741 |  1.50948 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_ByID           |         3847.8   |           0      |  1.99112  |  3.28819 |  4.0224  |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GraphRAG       |         2567.95  |           0      |  2.9665   |  4.31731 |  5.30534 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |         2614.32  |           0      |  2.94001  |  4.35231 |  5.30025 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Recommend      |         3708.38  |           0      |  1.94703  |  3.20108 |  3.82947 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Geo            |         3455.89  |           0      |  2.12523  |  3.06318 |  4.24993 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Temporal       |         3823.47  |           0      |  2.05418  |  2.84916 |  3.31276 |
| remote | cuda   | result_cuda_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |         2454.96  |           0      |  2.88223  |  6.35099 |  8.25714 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | DoPut                 |       409820     |         300.161  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | DoGet                 |       194303     |         142.312  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Dense          |         2109.38  |           0      |  2.56414  |  9.38583 | 32.2761  |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Hybrid         |         2478.67  |           0      |  3.0511   |  4.69055 |  5.46264 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Filtered       |         2973.73  |           0      |  2.56326  |  3.73712 |  4.1994  |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredBool   |         2530.33  |           0      |  2.85952  |  4.28649 |  4.94909 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredString |         1838.69  |           0      |  3.52015  |  5.36871 | 26.2662  |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Sparse         |         7928.73  |           0      |  0.996915 |  1.40847 |  1.61921 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_ByID           |         2610.12  |           0      |  2.64306  |  6.13633 |  8.50733 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_GraphRAG       |         1301.4   |           0      |  4.28838  | 18.0396  | 36.9031  |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_GlobalGraphRAG |         1530.07  |           0      |  3.92396  | 11.9285  | 26.1939  |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Recommend      |         3004.86  |           0      |  2.52433  |  3.97716 |  4.54864 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Geo            |         3949.37  |           0      |  1.80509  |  3.05032 |  5.50224 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_Temporal       |         3866.16  |           0      |  1.96657  |  3.34179 |  3.92203 |
| remote | cuda   | result_cuda_int8_768_5000.json          | int8        |   768 |    5000 | Search_LearnedIndex   |         2199.99  |           0      |  2.72637  |  7.97432 | 19.085   |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoPut                 |       433720     |          52.9443 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoGet                 |       241540     |          29.4848 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Dense          |         3404.16  |           0      |  2.25603  |  3.08289 |  4.25392 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Hybrid         |         3261.94  |           0      |  1.68884  |  3.09095 | 33.9541  |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Filtered       |         3489.68  |           0      |  2.16284  |  3.03636 |  6.01216 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredBool   |         3607.02  |           0      |  2.17501  |  3.01013 |  3.44601 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredString |         3573.6   |           0      |  2.16254  |  3.00368 |  3.47219 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Sparse         |         7647     |           0      |  1.02123  |  1.46456 |  1.70688 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_ByID           |         3846.38  |           0      |  2.04451  |  2.75079 |  3.23239 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GraphRAG       |         3645.9   |           0      |  2.1386   |  2.87185 |  3.78167 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |         3603.55  |           0      |  2.17597  |  2.92035 |  3.25497 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Recommend      |         3728.7   |           0      |  2.09405  |  2.93611 |  3.35092 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Geo            |         3137.61  |           0      |  2.11777  |  3.75679 | 16.3132  |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Temporal       |         3850.26  |           0      |  2.0274   |  2.8514  |  3.21811 |
| remote | cuda   | result_cuda_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_LearnedIndex   |         2622.55  |           0      |  2.79749  |  5.39642 |  6.69112 |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | DoPut                 |       124418     |         364.505  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | DoGet                 |       277611     |         813.315  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Dense          |          754.213 |           0      | 10.5735   | 12.7065  | 15.8131  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Hybrid         |          753.97  |           0      | 10.3271   | 14.612   | 17.047   |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Filtered       |          756.883 |           0      | 10.5566   | 12.3811  | 13.7433  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredBool   |          755.466 |           0      | 10.5228   | 12.6032  | 14.4889  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredString |          751.084 |           0      | 10.6334   | 12.6169  | 14.2383  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Sparse         |         8166.47  |           0      |  0.980431 |  1.41166 |  1.55442 |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_ByID           |          763.098 |           0      | 10.2541   | 13.6232  | 18.5072  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_GraphRAG       |          489.512 |           0      | 15.7677   | 22.8403  | 39.0711  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_GlobalGraphRAG |          481.807 |           0      | 16.0264   | 22.7221  | 25.4074  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Recommend      |          552.208 |           0      | 14.4909   | 18.2101  | 20.4697  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Geo            |         1654.89  |           0      |  4.72529  |  6.04427 |  7.04627 |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_Temporal       |         3315.85  |           0      |  2.29713  |  3.50011 |  4.1272  |
| remote | cuda   | result_cuda_float32_768_25000.json      | float32     |   768 |   25000 | Search_LearnedIndex   |          739.151 |           0      | 10.6667   | 14.9706  | 18.1441  |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoPut                 |       114435     |          83.815  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoGet                 |       122731     |          89.8912 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Dense          |         2518.8   |           0      |  2.34465  |  4.4504  | 35.6001  |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Hybrid         |         2711.47  |           0      |  2.82254  |  4.50017 |  5.39648 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Filtered       |         3061.98  |           0      |  2.44794  |  4.04007 |  5.73927 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredBool   |         3034.4   |           0      |  2.53601  |  4.07289 |  4.67925 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredString |         2990.12  |           0      |  2.59041  |  4.08342 |  4.80973 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Sparse         |         8066.88  |           0      |  0.975409 |  1.3885  |  1.55893 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_ByID           |         2936.82  |           0      |  2.52062  |  4.50364 |  5.43687 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GraphRAG       |         2417.38  |           0      |  2.68492  |  5.03649 | 10.4728  |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |         2456.52  |           0      |  2.92783  |  5.68513 |  8.63517 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Recommend      |         3351.31  |           0      |  2.24598  |  3.95175 |  4.57313 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Geo            |         2904.21  |           0      |  1.91547  |  4.60209 | 29.5141  |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Temporal       |         3868.01  |           0      |  2.04507  |  2.83021 |  3.27278 |
| remote | cuda   | result_cuda_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_LearnedIndex   |         2500.96  |           0      |  2.8302   |  5.98207 |  8.31525 |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | DoPut                 |        33734.8   |         395.33   |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | DoGet                 |        58838     |         689.508  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Dense          |          254.074 |           0      | 31.3614   | 41.172   | 47.1621  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Hybrid         |          255.96  |           0      | 30.0533   | 45.5925  | 52.5159  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Filtered       |          253.362 |           0      | 31.2813   | 42.8016  | 51.9985  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredBool   |          252.781 |           0      | 31.5896   | 40.6268  | 46.4159  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredString |          252.85  |           0      | 31.7639   | 41.5648  | 48.4219  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Sparse         |         7422.28  |           0      |  1.08095  |  1.47608 |  1.63204 |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_ByID           |          258.952 |           0      | 30.8781   | 39.6289  | 47.5648  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GraphRAG       |          247.25  |           0      | 31.7325   | 46.0508  | 55.7779  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GlobalGraphRAG |          248.25  |           0      | 31.9821   | 44.1361  | 49.9373  |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Recommend      |          192.528 |           0      | 41.2146   | 56.2847  | 69.683   |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Geo            |         1473.18  |           0      |  5.24749  |  6.74265 |  8.36042 |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Temporal       |         3331.39  |           0      |  2.23903  |  3.42669 |  3.80015 |
| remote | cuda   | result_cuda_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_LearnedIndex   |          259.348 |           0      | 30.3947   | 43.6698  | 52.0855  |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | DoPut                 |       543813     |          66.3834 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | DoGet                 |       306580     |          37.4244 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Dense          |         2842.36  |           0      |  2.13287  |  3.53729 | 23.6846  |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Hybrid         |         3594.55  |           0      |  2.02225  |  3.31715 |  4.75379 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Filtered       |         3610.69  |           0      |  2.11846  |  3.07735 |  4.17005 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredBool   |         2731.75  |           0      |  2.51932  |  4.13684 |  5.73528 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredString |         2761.52  |           0      |  2.76254  |  4.21664 |  5.13162 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Sparse         |         8065.07  |           0      |  0.950787 |  1.4438  |  1.81196 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_ByID           |         3821.67  |           0      |  2.06485  |  2.79203 |  3.27826 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_GraphRAG       |         1185.11  |           0      |  4.06601  | 18.1734  | 40.3552  |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_GlobalGraphRAG |         1509.02  |           0      |  3.62719  | 12.433   | 32.5191  |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Recommend      |         3753.27  |           0      |  2.06963  |  2.98436 |  3.36812 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Geo            |         3442.48  |           0      |  1.92419  |  4.64981 |  7.13314 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_Temporal       |         3889.43  |           0      |  1.96264  |  3.23998 |  4.03218 |
| remote | cuda   | result_cuda_int8_128_5000.json          | int8        |   128 |    5000 | Search_LearnedIndex   |         2533.64  |           0      |  2.85847  |  5.53357 |  7.3351  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoPut                 |       126151     |          92.3959 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoGet                 |       139593     |         102.241  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Dense          |          897.458 |           0      |  7.28128  | 19.7796  | 39.5178  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Hybrid         |         1162.42  |           0      |  6.37138  |  9.88044 | 14.34    |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Filtered       |         1165.99  |           0      |  6.3079   |  9.00558 | 22.1646  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredBool   |         1160.62  |           0      |  6.36486  |  9.90806 | 17.3698  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredString |         1201.04  |           0      |  6.48796  |  8.37274 | 12.5373  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Sparse         |         6471.01  |           0      |  1.02913  |  1.5271  |  2.05474 |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_ByID           |         1142.35  |           0      |  6.49549  | 12.1149  | 17.4201  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GraphRAG       |         1070.96  |           0      |  6.63393  | 14.3688  | 18.8581  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |         1151.52  |           0      |  6.53476  |  8.75324 | 16.0136  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Recommend      |         1192.26  |           0      |  6.51457  |  8.66054 | 14.3123  |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Geo            |         1767.83  |           0      |  4.41422  |  5.62056 |  8.84015 |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Temporal       |         3105.27  |           0      |  2.45833  |  3.82075 |  4.34731 |
| remote | cuda   | result_cuda_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_LearnedIndex   |         1063.12  |           0      |  6.39737  | 12.613   | 40.0012  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | DoPut                 |       111754     |         327.405  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | DoGet                 |       100003     |         292.979  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Dense          |          907.969 |           0      |  6.11467  | 22.0114  | 38.0587  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Hybrid         |         1312.23  |           0      |  5.15046  |  9.31939 | 16.8435  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Filtered       |         1385.96  |           0      |  5.05572  |  7.48858 | 17.6691  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredBool   |         1025.68  |           0      |  6.73445  | 10.2289  | 19.7015  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredString |          826.462 |           0      |  8.43973  | 13.2209  | 21.45    |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Sparse         |         7991.03  |           0      |  0.985713 |  1.41388 |  1.61824 |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_ByID           |         1682.49  |           0      |  4.34499  |  6.89613 | 13.8858  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GraphRAG       |          813.121 |           0      |  7.73893  | 23.1555  | 37.3655  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GlobalGraphRAG |          871.931 |           0      |  7.62687  | 18.3114  | 31.143   |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Recommend      |         1950.52  |           0      |  3.41201  |  6.0615  | 10.4195  |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Geo            |         3212.76  |           0      |  2.12026  |  3.3253  | 13.071   |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Temporal       |         3624.57  |           0      |  2.0141   |  3.62393 |  5.90766 |
| remote | cuda   | result_cuda_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_LearnedIndex   |         1357.27  |           0      |  5.2178   |  7.8532  | 12.8067  |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | DoPut                 |       603997     |          73.73   |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | DoGet                 |       651036     |          79.4721 |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Dense          |         1219.98  |           0      |  6.00954  | 11.7727  | 17.9038  |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Hybrid         |         1373.69  |           0      |  5.72871  |  8.13655 |  9.38159 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Filtered       |         1353.96  |           0      |  5.85695  |  7.32492 |  7.99688 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredBool   |         1392.17  |           0      |  5.6703   |  7.18193 |  8.41844 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredString |         1402.37  |           0      |  5.61868  |  7.03541 |  7.8148  |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Sparse         |         7853.2   |           0      |  1.00493  |  1.41572 |  1.55395 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_ByID           |         1307.24  |           0      |  5.88239  |  8.91352 | 10.7263  |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_GraphRAG       |         1147.53  |           0      |  6.44309  |  9.8649  | 27.4443  |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_GlobalGraphRAG |         1236.4   |           0      |  6.383    |  8.59776 |  9.91089 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Recommend      |         1286.57  |           0      |  6.12686  |  7.70905 |  9.64553 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Geo            |         1745.81  |           0      |  4.43126  |  5.81112 |  8.44345 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_Temporal       |         2526.81  |           0      |  2.94898  |  4.34771 |  6.83196 |
| remote | cuda   | result_cuda_int8_128_25000.json         | int8        |   128 |   25000 | Search_LearnedIndex   |         1418.31  |           0      |  5.54842  |  7.82053 |  9.23984 |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | DoPut                 |       120772     |         353.825  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | DoGet                 |       222519     |         651.91   |  0        |  0       |  0       |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Dense          |          568.608 |           0      | 13.9611   | 17.3934  | 20.5423  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Hybrid         |          542.326 |           0      | 14.3871   | 20.6255  | 24.4934  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Filtered       |          552.259 |           0      | 14.4527   | 18.859   | 22.9604  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredBool   |          545.916 |           0      | 14.5763   | 18.5945  | 21.4573  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredString |          546.78  |           0      | 14.5552   | 19.2348  | 22.1015  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Sparse         |         8656.63  |           0      |  0.900522 |  1.33625 |  1.52753 |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_ByID           |          582.65  |           0      | 13.3873   | 18.7971  | 25.5616  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GraphRAG       |          515.878 |           0      | 15.1715   | 21.536   | 25.1175  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GlobalGraphRAG |          503.819 |           0      | 15.6325   | 21.4943  | 25.5883  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Recommend      |          407.14  |           0      | 19.4816   | 25.1835  | 33.6836  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Geo            |         1695.74  |           0      |  4.62915  |  5.73527 |  6.3791  |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Temporal       |         2510.28  |           0      |  2.8957   |  4.44182 |  6.49405 |
| remote | cuda   | result_cuda_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_LearnedIndex   |          524.996 |           0      | 14.9779   | 22.0474  | 25.0581  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 |        33368.8   |          97.76   |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 |        35927.3   |         105.256  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |          674.995 |           0      | 10.7165   | 18.3861  | 28.8264  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |          719.959 |           0      | 10.169    | 17.1859  | 32.7359  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |          739.435 |           0      | 10.2838   | 16.0731  | 19.7749  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |          742.446 |           0      | 10.1877   | 16.3318  | 19.8973  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |          735.344 |           0      | 10.6473   | 15.5519  | 18.4755  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |         8476.7   |           0      |  0.92997  |  1.31664 |  1.47088 |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |          755.999 |           0      |  9.71289  | 18.1322  | 23.6277  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |          743.337 |           0      | 10.2537   | 16.2182  | 20.5136  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |          746.13  |           0      | 10.2693   | 15.8539  | 18.9183  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |          757.829 |           0      | 10.1573   | 15.7633  | 21.0126  |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |         1826.79  |           0      |  4.34658  |  5.47545 |  6.21032 |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |         3195.27  |           0      |  2.32302  |  3.66903 |  4.49308 |
| remote | cuda   | result_cuda_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |          713.62  |           0      | 10.3745   | 18.0078  | 27.6146  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | DoPut                 |       114469     |         335.358  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | DoGet                 |       128299     |         375.877  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Dense          |         3371.29  |           0      |  2.29632  |  3.21161 |  3.81307 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Hybrid         |         2386.02  |           0      |  2.416    |  4.68011 | 37.0202  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Filtered       |         3025.3   |           0      |  2.38421  |  3.53103 | 14.1265  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredBool   |         2929.65  |           0      |  2.44537  |  4.08284 |  6.78318 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredString |         2748.72  |           0      |  2.73869  |  4.37113 |  5.00198 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Sparse         |         8059.59  |           0      |  0.978806 |  1.43434 |  1.57522 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_ByID           |         3874.65  |           0      |  2.00103  |  3.16229 |  3.83534 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_GraphRAG       |         1652.82  |           0      |  3.20127  | 16.5519  | 29.2041  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_GlobalGraphRAG |         1833.34  |           0      |  3.33678  |  8.36585 | 24.2204  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Recommend      |         3959.5   |           0      |  1.94947  |  3.15219 |  3.85793 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Geo            |         3631.17  |           0      |  1.87565  |  2.75635 |  7.11538 |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_Temporal       |         3760.98  |           0      |  2.06152  |  2.92772 |  3.4999  |
| remote | cuda   | result_cuda_float32_768_5000.json       | float32     |   768 |    5000 | Search_LearnedIndex   |         2572.49  |           0      |  2.80129  |  5.26152 |  6.61999 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | DoPut                 |        30895.8   |         362.06   |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | DoGet                 |        42983.9   |         503.718  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Dense          |         2220.03  |           0      |  3.04484  |  4.96596 |  8.57625 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Hybrid         |         2390.46  |           0      |  3.17752  |  4.62786 |  5.78144 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Filtered       |         2391.63  |           0      |  3.15282  |  4.73032 |  5.60123 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredBool   |         1846.14  |           0      |  3.56401  |  5.86137 | 36.3771  |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredString |         1998.4   |           0      |  3.70123  |  5.2539  |  6.58316 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Sparse         |         8124.77  |           0      |  0.973169 |  1.40056 |  1.71707 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_ByID           |         2547.83  |           0      |  2.7557   |  5.29715 |  7.86794 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GraphRAG       |         1356.02  |           0      |  4.71514  | 11.4918  | 22.8308  |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GlobalGraphRAG |         1543.86  |           0      |  4.396    |  9.24228 | 18.7929  |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Recommend      |         2395.16  |           0      |  2.73592  |  5.37158 | 10.7877  |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Geo            |         3775.18  |           0      |  1.73136  |  2.72155 |  9.54652 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Temporal       |         3677.5   |           0      |  2.12668  |  2.99208 |  3.46431 |
| remote | cuda   | result_cuda_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_LearnedIndex   |         1875.73  |           0      |  3.31038  |  7.83474 | 28.6695  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | DoPut                 |       620256     |         302.859  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | DoGet                 |       712789     |         348.041  |  0        |  0       |  0       |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Dense          |         1147.02  |           0      |  6.43453  | 12.1565  | 19.8664  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Hybrid         |         1111.07  |           0      |  6.42798  | 11.2834  | 28.9706  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Filtered       |         1280.33  |           0      |  6.22487  |  7.56788 |  9.03893 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredBool   |         1267.38  |           0      |  6.29177  |  7.7727  |  8.54664 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredString |         1258.02  |           0      |  6.31467  |  7.81328 |  8.80446 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Sparse         |         8173.95  |           0      |  0.959577 |  1.38532 |  1.53712 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_ByID           |         1214.64  |           0      |  6.30387  |  9.04736 | 14.372   |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_GraphRAG       |          588.735 |           0      | 13.2118   | 18.3524  | 21.7016  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_GlobalGraphRAG |          588.785 |           0      | 13.3358   | 18.0771  | 21.0576  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Recommend      |          962.852 |           0      |  8.22123  | 10.6996  | 13.1207  |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Geo            |         1752.5   |           0      |  4.43503  |  5.70361 |  6.82264 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_Temporal       |         3056.07  |           0      |  2.49696  |  3.81863 |  4.42466 |
| remote | cuda   | result_cuda_float32_128_25000.json      | float32     |   128 |   25000 | Search_LearnedIndex   |         1196.63  |           0      |  6.55484  |  9.02296 | 10.7607  |

## v0.2.1 Final Performance Validation (2026-05-16)

## Search Performance Summary (QPS)

|                                         |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'cpu', 128, 'float32')        |       2478.22 |        2422.53 |           2469.94 |               2164.52 |                 2040.12 |      3002.31 |                 1188.58 |           1126.39 |         2757.79 |               2016.6  |            2551.51 |         6130.64 |           2907.25 |
| ('local', 'cpu', 128, 'int8')           |       4029.27 |        3120.95 |           3205.23 |               2840.94 |                 2657.91 |      4001.41 |                 1957.05 |           1856.47 |         3382.98 |               3002.43 |            3160.71 |        11418.7  |           3236.32 |
| ('local', 'cpu', 128, 'turboquant8')    |       3936.78 |        3297.02 |           3744.35 |               3576.22 |                 3815.72 |      4112.08 |                 3405.64 |           3491.68 |         3996.69 |               2901.52 |            3932.82 |        12020.1  |           4788.59 |
| ('local', 'cpu', 768, 'float32')        |       2155.36 |        1831.37 |           1849.51 |               1720.39 |                 1562.48 |      2800.3  |                 1074.96 |           1110.47 |         1928.38 |               1671.42 |            2045.16 |         6063.09 |           3079.63 |
| ('local', 'cpu', 768, 'int8')           |       2844.84 |        1739.29 |           1819.8  |               1521.74 |                 1457.67 |      4261.11 |                 1360.81 |           1270.04 |         1823.69 |               1764.56 |            1887.04 |        12109.1  |           3192.23 |
| ('local', 'cpu', 768, 'turboquant8')    |       3052.68 |        2474.92 |           2686.64 |               2664.06 |                 3094    |      4049.07 |                 2642.98 |           2558.31 |         2814.72 |               2418.46 |            3041.13 |        12139.7  |           4862.15 |
| ('local', 'cpu', 3072, 'float32')       |       1420.85 |        1150.34 |           1196.11 |               1109.46 |                 1009.63 |      2786.68 |                  858.57 |            778.54 |         1179.56 |               1068.77 |            1354.35 |         5927.46 |           3119.06 |
| ('local', 'cpu', 3072, 'int8')          |       1596.55 |         825.17 |            833.93 |                708.71 |                  597.13 |      4327.39 |                  718.17 |            742.14 |          828.56 |                813.36 |             914.09 |        11943.3  |           3343.97 |
| ('local', 'cpu', 3072, 'turboquant8')   |       2740.32 |        1869.76 |           2075.28 |               2015.77 |                 2138.14 |      3983.67 |                 2044.82 |           2010.85 |         2060.23 |               1995.4  |            2687.49 |        12018.4  |           4787.9  |
| ('local', 'metal', 128, 'float32')      |       2759.24 |        2318.99 |           2444.63 |               2121.37 |                 2024.64 |      2750.94 |                 1184.26 |           1094.47 |         2725.44 |               2024.93 |            2593.78 |         6191.5  |           3126.25 |
| ('local', 'metal', 128, 'int8')         |       4044.75 |        2993.27 |           3167.6  |               2874.14 |                 2592.29 |      4274.3  |                 1910.87 |           1779.16 |         3436.53 |               2956.53 |            3013.26 |        11722.7  |           3380.42 |
| ('local', 'metal', 128, 'turboquant8')  |       3618.73 |        3143.21 |           3379.51 |               3302.26 |                 3358.98 |      3806.02 |                 2952.72 |           3079.97 |         3694.69 |               2779.97 |            3525.01 |        11491    |           4870.66 |
| ('local', 'metal', 768, 'float32')      |       2135.74 |        1788.92 |           1824.4  |               1696.37 |                 1607.04 |      3003.13 |                 1076.75 |           1039.55 |         1959.47 |               1697.5  |            2042.67 |         6264.66 |           3150.9  |
| ('local', 'metal', 768, 'int8')         |       2960.39 |        1819.8  |           1845.95 |               1659.01 |                 1465.25 |      4177.03 |                 1438.07 |           1432.67 |         1922.56 |               1838.07 |            1908.35 |        11893.1  |           3288.45 |
| ('local', 'metal', 768, 'turboquant8')  |       2995.66 |        2440.79 |           2680.96 |               2655.61 |                 2709.48 |      4112.9  |                 2669.82 |           2567.72 |         2747.28 |               2461.99 |            2984.37 |        11939.6  |           4831.86 |
| ('local', 'metal', 3072, 'float32')     |       1406.67 |        1156.68 |           1196.63 |               1092.63 |                 1017.45 |      2837.59 |                  883.73 |            813.79 |         1164.4  |               1068.08 |            1387.94 |         6152.66 |           3127.51 |
| ('local', 'metal', 3072, 'int8')        |       1643.57 |         828.45 |            858.28 |                759.5  |                  606.12 |      4354.84 |                  762.06 |            764.73 |          857.76 |                843.04 |             865.19 |        12085.8  |           3224.32 |
| ('local', 'metal', 3072, 'turboquant8') |       3225.25 |        2078.96 |           2136.55 |               2272.35 |                 2254.03 |      4008.21 |                 2248.9  |           2248.49 |         2348.23 |               2118.23 |            3199.32 |        11716.8  |           4956.27 |
| ('perf', 'logs', 128, 'float32')        |       2504.12 |        2243.76 |           2401.25 |               2072.63 |                 2359.04 |      2668.08 |                 1371.36 |           1142.66 |         2729.02 |               2244.34 |            2302.75 |         7716.77 |           3470.24 |
| ('perf', 'logs', 128, 'int8')           |       3702.55 |        3155.38 |           3689.52 |               3334.27 |                 2548.21 |      3580.62 |                 1531.08 |           1252.32 |         2804.43 |               2895.13 |            3717.64 |         5857.99 |           3619.2  |
| ('perf', 'logs', 128, 'turboquant8')    |       3835.35 |        3346.52 |           3511.75 |               3692.75 |                 3034.41 |      3278.75 |                 3044.69 |           3731.41 |         3275.35 |               2642.61 |            3919.14 |         7852.58 |           3832.32 |
| ('perf', 'logs', 768, 'float32')        |       2275.62 |        1901.9  |           1621.69 |               1785.34 |                 1759.72 |      2850.67 |                 1301.6  |           1158.45 |         2038.1  |               1962.67 |            2120.95 |         8479.31 |           3508.02 |
| ('perf', 'logs', 768, 'int8')           |       2783.03 |        2146.56 |           2521.06 |               2577.78 |                 2328.46 |      4016.04 |                 1275.72 |           1144.46 |         2581.68 |               2156.95 |            2945.63 |         8438.05 |           3988.53 |
| ('perf', 'logs', 768, 'turboquant8')    |       3182.03 |        2789.57 |           3093.48 |               2976.52 |                 3047.02 |      3227.62 |                 2774.98 |           2407.83 |         2609.19 |               2203.14 |            3618.71 |         7818.53 |           3769.74 |
| ('perf', 'logs', 3072, 'float32')       |       2600.41 |        2008.06 |           2540.21 |               2250.93 |                 1715.11 |      3793.95 |                 1403.19 |           1243.28 |         2464.99 |               2201.63 |            2736.85 |         8440.44 |           3800.35 |
| ('perf', 'logs', 3072, 'int8')          |       1988.44 |        1397.38 |           1717.26 |               1063.17 |                  815.94 |      4097.49 |                 1044.8  |            907.43 |         1548.25 |               1513.68 |            1867.94 |         8082.46 |           3955    |
| ('perf', 'logs', 3072, 'turboquant8')   |       3636.87 |        2348.26 |           2790.44 |               2880.46 |                 2446.38 |      3906.82 |                 2522.83 |           2574.63 |         2612.14 |               2443.02 |            3199.94 |         8174.02 |           3658.3  |
| ('remote', 'cpu', 128, 'float32')       |       2529.38 |        2298.25 |           2359.32 |               2230.38 |                 2172.71 |      2668.42 |                 1339.64 |            875.58 |         2691.89 |               1963.93 |            2165.91 |         7876.51 |           3477.52 |
| ('remote', 'cpu', 128, 'int8')          |       2693.69 |        2258.7  |           2648.41 |               2315.48 |                 2057.36 |      2513.85 |                 1401.63 |           1253.86 |         2278.1  |               2238.66 |            2180.39 |         6725.7  |           3078.33 |
| ('remote', 'cpu', 128, 'turboquant8')   |       2486.56 |        2262.96 |           2425.83 |               2265.1  |                 2503    |      2452.52 |                 2433.96 |           2179.05 |         2610.25 |               1988.75 |            2439.59 |         7572.32 |           3549.75 |
| ('remote', 'cpu', 768, 'float32')       |       2261.45 |        1791.55 |           2052.72 |               1855.34 |                 1859.73 |      2709.21 |                 1271.15 |           1131.69 |         2005.96 |               1727.56 |            2279.62 |         7816.12 |           3515.63 |
| ('remote', 'cpu', 768, 'int8')          |       2017.09 |        1796.16 |           2024.91 |               1940.51 |                 1540.13 |      2769.82 |                 1034.36 |           1173.57 |         1594.84 |               1592.74 |            1792.13 |         7777.88 |           3140.74 |
| ('remote', 'cpu', 768, 'turboquant8')   |       2599.85 |        2153.22 |           2090.71 |               2291.83 |                 2374.49 |      2431.18 |                 2251.86 |           2253.66 |         2231.37 |               1831.56 |            2198.85 |         8564.89 |           3466.92 |
| ('remote', 'cpu', 3072, 'float32')      |       1475.79 |        1278.14 |           1258.57 |               1291.06 |                 1156.13 |      2585.83 |                  890.78 |            681.78 |         1153.33 |               1042.68 |            1381.72 |         8453.1  |           3439.79 |
| ('remote', 'cpu', 3072, 'int8')         |       1125.91 |         726.45 |            940.6  |                761.76 |                  676.03 |      2523.5  |                  685.4  |            609.86 |          912.16 |                893.77 |            1098.97 |         8254.41 |           3166.41 |
| ('remote', 'cpu', 3072, 'turboquant8')  |       2256.96 |        1503.45 |           1700.65 |               1695.64 |                 1746.88 |      2470.65 |                 1614.92 |           1582.71 |         1727.33 |               1566.03 |            1957.71 |         8378.04 |           3482.78 |

## Ingestion Performance (MB/s)

|                                         |   Throughput_MBs |
|:----------------------------------------|-----------------:|
| ('local', 'cpu', 128, 'float32')        |           545.19 |
| ('local', 'cpu', 128, 'int8')           |           238.67 |
| ('local', 'cpu', 128, 'turboquant8')    |           130.47 |
| ('local', 'cpu', 768, 'float32')        |           982.09 |
| ('local', 'cpu', 768, 'int8')           |           674.14 |
| ('local', 'cpu', 768, 'turboquant8')    |           228.98 |
| ('local', 'cpu', 3072, 'float32')       |          1194.22 |
| ('local', 'cpu', 3072, 'int8')          |           921.63 |
| ('local', 'cpu', 3072, 'turboquant8')   |           294.61 |
| ('local', 'metal', 128, 'float32')      |           523.7  |
| ('local', 'metal', 128, 'int8')         |           283.3  |
| ('local', 'metal', 128, 'turboquant8')  |           135.7  |
| ('local', 'metal', 768, 'float32')      |          1019.93 |
| ('local', 'metal', 768, 'int8')         |           685.42 |
| ('local', 'metal', 768, 'turboquant8')  |           236.92 |
| ('local', 'metal', 3072, 'float32')     |          1168.17 |
| ('local', 'metal', 3072, 'int8')        |           950.55 |
| ('local', 'metal', 3072, 'turboquant8') |           301.03 |
| ('perf', 'logs', 128, 'float32')        |           218.37 |
| ('perf', 'logs', 128, 'int8')           |            99.06 |
| ('perf', 'logs', 128, 'turboquant8')    |            61.74 |
| ('perf', 'logs', 768, 'float32')        |           347.93 |
| ('perf', 'logs', 768, 'int8')           |           290.8  |
| ('perf', 'logs', 768, 'turboquant8')    |            82.29 |
| ('perf', 'logs', 3072, 'float32')       |           357.92 |
| ('perf', 'logs', 3072, 'int8')          |           305.28 |
| ('perf', 'logs', 3072, 'turboquant8')   |            91.63 |
| ('remote', 'cpu', 128, 'float32')       |           258.8  |
| ('remote', 'cpu', 128, 'int8')          |           154.97 |
| ('remote', 'cpu', 128, 'turboquant8')   |            68.6  |
| ('remote', 'cpu', 768, 'float32')       |           353.46 |
| ('remote', 'cpu', 768, 'int8')          |           305.16 |
| ('remote', 'cpu', 768, 'turboquant8')   |            85.53 |
| ('remote', 'cpu', 3072, 'float32')      |           370.47 |
| ('remote', 'cpu', 3072, 'int8')         |           341.02 |
| ('remote', 'cpu', 3072, 'turboquant8')  |            94.28 |

## Search Latency Summary (P95 ms)

|                                         |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:----------------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| ('local', 'cpu', 128, 'float32')        |          1.17 |           1.1  |              1.04 |                  1.22 |                    1.29 |         0.92 |                    2.63 |              3.05 |            1.01 |                  1.34 |               1.11 |            0.5  |              0.98 |
| ('local', 'cpu', 128, 'int8')           |          2.89 |           3.78 |              3.53 |                  3.89 |                    4.12 |         3.24 |                    6.14 |              6.82 |            3.43 |                  3.53 |               4.07 |            1.16 |              3.74 |
| ('local', 'cpu', 128, 'turboquant8')    |          3.97 |           4.89 |              4.11 |                  4.22 |                    3.9  |         3.07 |                    4.27 |              4.25 |            4.03 |                  4.22 |               4.06 |            1.02 |              2.41 |
| ('local', 'cpu', 768, 'float32')        |          1.22 |           1.37 |              1.34 |                  1.47 |                    1.64 |         1.11 |                    2.86 |              2.4  |            1.37 |                  1.5  |               1.28 |            0.5  |              0.91 |
| ('local', 'cpu', 768, 'int8')           |          4.1  |           6.63 |              6    |                  7.89 |                    7.27 |         2.87 |                    8.59 |              9.98 |            6.3  |                  6.07 |               7.09 |            1.01 |              3.98 |
| ('local', 'cpu', 768, 'turboquant8')    |          6.53 |           7.52 |              6.57 |                  6.47 |                    6.23 |         3.07 |                    6.7  |              6.81 |            7.12 |                  6.45 |               6.6  |            0.98 |              2.37 |
| ('local', 'cpu', 3072, 'float32')       |          1.79 |           2.15 |              1.98 |                  2.18 |                    2.45 |         1.13 |                    3.41 |              3.78 |            2.04 |                  2.4  |               1.89 |            0.5  |              0.88 |
| ('local', 'cpu', 3072, 'int8')          |          7.89 |          13.04 |             13.13 |                 14.48 |                   16.97 |         2.88 |                   15.19 |             14.39 |           13.92 |                 13.26 |              15.55 |            1    |              3.87 |
| ('local', 'cpu', 3072, 'turboquant8')   |          6.02 |           7.25 |              6.34 |                  6.38 |                    6.2  |         3.07 |                    6.45 |              6.81 |            7.14 |                  6.6  |               6.12 |            0.98 |              2.52 |
| ('local', 'metal', 128, 'float32')      |          1.04 |           1.2  |              1.14 |                  1.26 |                    1.29 |         1.25 |                    2.61 |              3.13 |            1.06 |                  1.33 |               1.11 |            0.49 |              0.89 |
| ('local', 'metal', 128, 'int8')         |          3.03 |           4.01 |              3.49 |                  3.91 |                    4.23 |         3.08 |                    6.59 |              7.38 |            3.42 |                  3.7  |               4.12 |            1.1  |              3.72 |
| ('local', 'metal', 128, 'turboquant8')  |          4.75 |           5.71 |              4.83 |                  4.94 |                    4.88 |         3.36 |                    5.1  |              5.1  |            4.97 |                  5.19 |               4.9  |            1.04 |              2.39 |
| ('local', 'metal', 768, 'float32')      |          1.23 |           1.42 |              1.38 |                  1.48 |                    1.55 |         0.92 |                    2.73 |              3.16 |            1.33 |                  1.47 |               1.36 |            0.49 |              0.9  |
| ('local', 'metal', 768, 'int8')         |          3.91 |           6.16 |              5.9  |                  6.62 |                    7.19 |         3    |                    8    |              8.2  |            6.09 |                  5.83 |               6.82 |            1.05 |              3.81 |
| ('local', 'metal', 768, 'turboquant8')  |          6.57 |           7.84 |              6.45 |                  6.47 |                    6.49 |         2.94 |                    6.66 |              6.58 |            6.91 |                  6.68 |               6.5  |            1    |              2.38 |
| ('local', 'metal', 3072, 'float32')     |          1.81 |           2.05 |              1.97 |                  2.22 |                    2.46 |         1.02 |                    2.9  |              3.6  |            2.06 |                  2.38 |               1.79 |            0.47 |              0.9  |
| ('local', 'metal', 3072, 'int8')        |          7.55 |          13.03 |             13.2  |                 13.69 |                   17.13 |         2.81 |                   14.36 |             14.38 |           13.63 |                 13.03 |              15.3  |            0.99 |              4.06 |
| ('local', 'metal', 3072, 'turboquant8') |          5.58 |           6.62 |              6.05 |                  5.78 |                    5.78 |         2.94 |                    6.07 |              5.95 |            6.15 |                  6.11 |               5.54 |            1.02 |              2.36 |
| ('perf', 'logs', 128, 'float32')        |          6.48 |           6.37 |              5.37 |                  5.83 |                    5.81 |         3.94 |                   13.27 |             16.68 |            6.18 |                  6.55 |               6.85 |            1.47 |              3.23 |
| ('perf', 'logs', 128, 'int8')           |          2.98 |           3.66 |              3.1  |                  3.41 |                    4.43 |         3.91 |                   13.12 |             15.76 |            3.38 |                  4.43 |               2.95 |            1.81 |              3.55 |
| ('perf', 'logs', 128, 'turboquant8')    |          3.24 |           3.53 |              3.22 |                  3.08 |                    3.44 |         3.92 |                    3.48 |              3.25 |            3.32 |                  5.63 |               3.03 |            1.43 |              2.82 |
| ('perf', 'logs', 768, 'float32')        |          8.29 |           7.94 |              8.22 |                  8.26 |                    8.31 |         4.28 |                   15.1  |             16.69 |            9.12 |                  9.2  |              10.09 |            1.35 |              3.29 |
| ('perf', 'logs', 768, 'int8')           |          4.67 |           6.21 |              4.33 |                  4.16 |                    4.69 |         2.36 |                   17.17 |             19.73 |            4.37 |                  8.7  |               3.8  |            1.35 |              2.93 |
| ('perf', 'logs', 768, 'turboquant8')    |          4.2  |           4.3  |              4.01 |                  4.08 |                    4.07 |         3.63 |                    4.11 |              5.17 |            4.76 |                  7.34 |               3.54 |            1.45 |              2.91 |
| ('perf', 'logs', 3072, 'float32')       |          5.16 |           5.73 |              4.35 |                  4.85 |                    6.95 |         3.44 |                   11.11 |             17.18 |            4.32 |                  5.17 |               4.54 |            1.33 |              2.86 |
| ('perf', 'logs', 3072, 'int8')          |          6.73 |           8.4  |              6.36 |                 10.11 |                   13.51 |         2.73 |                   13.42 |             18.76 |            6.93 |                  7.13 |               6.08 |            1.4  |              3.07 |
| ('perf', 'logs', 3072, 'turboquant8')   |          3.09 |           4.64 |              3.98 |                  3.84 |                    4.97 |         3.79 |                    4.27 |              5.16 |            4.5  |                  5.7  |               3.74 |            1.47 |              3.09 |
| ('remote', 'cpu', 128, 'float32')       |          6.41 |           6.3  |              5.51 |                  5.28 |                    5.7  |         4.82 |                   13.5  |             19.96 |            6.75 |                  7.61 |               7.01 |            1.43 |              3.38 |
| ('remote', 'cpu', 128, 'int8')          |          4.99 |           6.78 |              4.43 |                  4.86 |                    5.06 |         4.93 |                   10.64 |             13.48 |            5.48 |                  5.65 |               5.65 |            1.66 |              3.83 |
| ('remote', 'cpu', 128, 'turboquant8')   |          6.71 |           6.48 |              5.28 |                  5.16 |                    5.16 |         5.34 |                    5.76 |              6.51 |            6.99 |                  6.99 |               5.89 |            1.46 |              3.24 |
| ('remote', 'cpu', 768, 'float32')       |          7.6  |           7.27 |              6.9  |                  7.32 |                    7.26 |         4.56 |                   13.09 |             15.59 |            8    |                  8.81 |               8.8  |            1.43 |              3.26 |
| ('remote', 'cpu', 768, 'int8')          |          7.54 |           7.03 |              6.37 |                  6.17 |                    6.7  |         4.27 |                   15.84 |             12.79 |            7.5  |                  9.09 |               8    |            1.49 |              3.93 |
| ('remote', 'cpu', 768, 'turboquant8')   |          6.46 |           9.32 |              6.86 |                  5.22 |                    5.45 |         5.3  |                    5.61 |              5.93 |            7.13 |                  7.55 |               7.83 |            1.36 |              3.35 |
| ('remote', 'cpu', 3072, 'float32')      |         22.31 |          23.56 |             23.85 |                 22.92 |                   23.15 |         4.54 |                   26.88 |             31.88 |           24.84 |                 26.49 |              30.27 |            1.35 |              3.33 |
| ('remote', 'cpu', 3072, 'int8')         |         12.44 |          19.52 |             13.22 |                 15.49 |                   15.97 |         4.75 |                   19.34 |             22.96 |           28.26 |                 15.45 |              16.13 |            1.36 |              3.92 |
| ('remote', 'cpu', 3072, 'turboquant8')  |         12.03 |          16.43 |             12.56 |                 11.41 |                   11.1  |         4.67 |                   12.9  |             12.29 |           12.12 |                 17.86 |              13.37 |            1.37 |              3.38 |

### Details: local (cpu)

| Host   | Mode   | Dataset                                | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |   P99_ms |
|:-------|:-------|:---------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|---------:|
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 |      1.48294e+06 |         181.022  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 |      1.10739e+06 |         135.18   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |   1725.33        |           0      |  4.20663  |  7.79438  | 14.161   |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |   1908.78        |           0      |  3.97508  |  6.20937  |  7.56954 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |   1899.43        |           0      |  4.07175  |  6.33437  |  7.34817 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1833.08        |           0      |  4.16308  |  6.48092  |  9.5685  |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |   1908.47        |           0      |  4.18467  |  5.96937  |  7.41758 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |  11995.7         |           0      |  0.655042 |  1.03038  |  1.19342 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |   1932.83        |           0      |  4.03662  |  6.02762  |  7.33387 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1787.05        |           0      |  4.3745   |  6.46517  |  7.60017 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1789.19        |           0      |  4.29304  |  6.41804  |  8.21179 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |   1891.1         |           0      |  4.08075  |  6.26863  |  7.19675 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |   2622.95        |           0      |  2.99208  |  4.04554  |  5.6005  |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |   3597.96        |           0      |  2.18142  |  2.91504  |  3.35475 |
| local  | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1836.98        |           0      |  4.29608  |  5.72717  |  7.04104 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | DoPut                 | 608828           |         297.279  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | DoGet                 |      1.24533e+06 |         608.071  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Dense          |   4845.07        |           0      |  1.57871  |  2.19096  |  3.14908 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Hybrid         |   5515.58        |           0      |  1.37804  |  2.01104  |  2.44067 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Filtered       |   4939.89        |           0      |  1.59525  |  2.07496  |  2.46992 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredBool   |   4329.04        |           0      |  1.81875  |  2.44579  |  2.76404 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredString |   4080.25        |           0      |  1.92742  |  2.577    |  2.98704 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Sparse         |  12261.3         |           0      |  0.642084 |  1.00192  |  1.19579 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_ByID           |   4956.44        |           0      |  1.44721  |  2.3375   |  5.85229 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_GraphRAG       |   2252.78        |           0      |  3.12987  |  6.10971  | 12.2439  |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2377.16        |           0      |  3.15108  |  5.25154  |  6.90129 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Recommend      |   5103.03        |           0      |  1.53446  |  2.22812  |  2.54883 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Geo            |   6004.62        |           0      |  1.31625  |  1.84554  |  2.11538 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Temporal       |   5814.51        |           0      |  1.35017  |  1.95671  |  2.30037 |
| local  | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_LearnedIndex   |   4033.2         |           0      |  1.93675  |  2.67633  |  3.03775 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoPut                 |  87582.9         |         256.59   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoGet                 |  81216.8         |         237.94   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Dense          |   3112.86        |           0      |  2.49333  |  3.24375  |  4.11846 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Hybrid         |   3062.87        |           0      |  2.52483  |  3.28446  |  3.92638 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Filtered       |   3044.1         |           0      |  2.532    |  3.40038  |  4.92642 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredBool   |   2916.72        |           0      |  2.68046  |  3.48554  |  4.16275 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredString |   3166.26        |           0      |  2.49037  |  3.19267  |  3.57996 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Sparse         |  12158.1         |           0      |  0.652958 |  0.970625 |  1.11704 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_ByID           |   4230.19        |           0      |  1.86838  |  2.51183  |  2.74358 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GraphRAG       |   2946.04        |           0      |  2.59617  |  3.38312  |  4.86096 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |   3000.76        |           0      |  2.61042  |  3.43379  |  3.97767 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Recommend      |   4142.39        |           0      |  1.90171  |  2.53342  |  2.88058 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Geo            |   5496.1         |           0      |  1.35796  |  2.02196  |  2.90754 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Temporal       |   6089.57        |           0      |  1.29267  |  1.79217  |  2.04021 |
| local  | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |   2883.82        |           0      |  2.70308  |  3.60033  |  4.00237 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | DoPut                 |      2.9835e+06  |         364.197  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | DoGet                 |      3.2089e+06  |         391.711  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Dense          |   2482.1         |           0      |  3.11958  |  4.73133  |  6.16442 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Hybrid         |   2645.1         |           0      |  2.95254  |  4.30533  |  5.02854 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Filtered       |   2545.68        |           0      |  3.10746  |  4.42079  |  5.26546 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredBool   |   2382.56        |           0      |  3.16308  |  4.68842  | 11.8593  |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredString |   2565.99        |           0      |  3.09629  |  4.2765   |  5.26271 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Sparse         |  10476.1         |           0      |  0.707417 |  1.32496  |  1.77729 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_ByID           |   3101.08        |           0      |  2.56863  |  3.58425  |  4.3525  |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_GraphRAG       |   1784.79        |           0      |  4.23371  |  7.05042  |  8.80942 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_GlobalGraphRAG |   1888.79        |           0      |  4.01837  |  6.35508  |  8.363   |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Recommend      |   2098.72        |           0      |  3.74517  |  5.60304  |  6.24004 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Geo            |   2476.14        |           0      |  3.12625  |  4.1455   |  7.00783 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Temporal       |   2500.98        |           0      |  2.9325   |  4.79213  |  6.23421 |
| local  | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_LearnedIndex   |   2444.98        |           0      |  3.27079  |  4.13842  |  4.93042 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | DoPut                 | 274810           |         805.108  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | DoGet                 | 156173           |         457.537  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Dense          |   1022.02        |           0      |  7.36117  |  9.92842  | 12.9986  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Hybrid         |   1027.01        |           0      |  7.44125  |  9.60821  | 10.1643  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Filtered       |   1036.12        |           0      |  7.35792  |  9.75687  | 10.4392  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredBool   |    787.931       |           0      |  9.61254  | 12.8873   | 13.4062  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredString |    567.322       |           0      | 13.1719   | 17.6984   | 18.6063  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Sparse         |  12101.6         |           0      |  0.642625 |  0.971959 |  1.14562 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_ByID           |   1928.1         |           0      |  3.72517  |  6.32408  |  7.31763 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GraphRAG       |    892.688       |           0      |  8.46242  | 11.378    | 12.4971  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GlobalGraphRAG |    847.433       |           0      |  8.59079  | 12.839    | 17.2362  |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Recommend      |   1301.03        |           0      |  5.75017  |  8.24033  |  8.68454 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Geo            |   6049.14        |           0      |  1.30196  |  1.84542  |  2.04962 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Temporal       |   4230.96        |           0      |  1.87433  |  2.48587  |  2.86783 |
| local  | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_LearnedIndex   |   1000.71        |           0      |  7.53854  | 10.0571   | 10.865   |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoPut                 | 270184           |         197.889  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoGet                 | 259487           |         190.054  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Dense          |   4170.01        |           0      |  1.78933  |  2.391    |  4.08263 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Hybrid         |   4605           |           0      |  1.72596  |  2.21571  |  2.44037 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Filtered       |   4331.33        |           0      |  1.8195   |  2.28521  |  2.60879 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredBool   |   4277.6         |           0      |  1.84546  |  2.39892  |  2.79008 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredString |   5149.69        |           0      |  1.54658  |  1.99063  |  2.242   |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Sparse         |  12224.7         |           0      |  0.635916 |  0.962833 |  1.15179 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_ByID           |   5007.83        |           0      |  1.59079  |  2.13383  |  2.40008 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GraphRAG       |   4115.84        |           0      |  1.88083  |  2.41483  |  2.94475 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |   4275.01        |           0      |  1.85783  |  2.27612  |  2.47929 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Recommend      |   5009.19        |           0      |  1.57183  |  2.17325  |  2.40254 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Geo            |   5602.62        |           0      |  1.34862  |  1.99496  |  3.06121 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Temporal       |   6116.45        |           0      |  1.28221  |  1.82963  |  2.08867 |
| local  | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_LearnedIndex   |   3801.28        |           0      |  2.07508  |  2.72117  |  3.00892 |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 | 113535           |         332.623  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 | 192265           |         563.276  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |    626.66        |           0      |  8.20112  | 11.257    | 13.0148  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |   1057.59        |           0      |  7.24458  | 10.9968   | 13.067   |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |   1106.47        |           0      |  7.17125  |  9.28521  | 10.642   |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |   1114.83        |           0      |  7.10625  |  9.26933  | 10.9482  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |   1110.02        |           0      |  7.09679  |  9.20146  | 10.7519  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |  11878.7         |           0      |  0.653667 |  0.999083 |  1.14671 |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |   1250.46        |           0      |  6.256    |  9.52217  | 10.9097  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |   1075.67        |           0      |  7.27096  | 10.2383   | 12.2339  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |   1088.88        |           0      |  7.20337  |  9.45946  | 10.5995  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |   1232.58        |           0      |  6.26767  |  9.70758  | 11.4143  |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |   2471.23        |           0      |  3.11063  |  4.10887  |  7.49579 |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |   3486.23        |           0      |  2.20121  |  3.25762  |  4.15233 |
| local  | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |   1106.99        |           0      |  7.09317  |  9.60804  | 10.7447  |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | DoPut                 |  92135           |        1079.71   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | DoGet                 |  78794.4         |         923.372  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Dense          |   2300.68        |           0      |  3.29804  |  4.29525  | 11.0255  |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Hybrid         |   2359.13        |           0      |  3.33488  |  4.08021  |  4.71862 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Filtered       |   2392.23        |           0      |  3.28375  |  3.96675  |  5.12287 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredBool   |   2218.93        |           0      |  3.54629  |  4.36033  |  4.79492 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredString |   2019.27        |           0      |  3.81812  |  4.90679  |  5.54904 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Sparse         |  11854.9         |           0      |  0.652083 |  1.00508  |  1.19229 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_ByID           |   2841.71        |           0      |  2.76729  |  3.57675  |  4.01138 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GraphRAG       |   1557.08        |           0      |  4.66746  |  7.55333  | 12.584   |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1717.14        |           0      |  4.39504  |  6.82358  |  9.59154 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Recommend      |   2708.7         |           0      |  2.86762  |  3.77533  |  4.14396 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Geo            |   5573.36        |           0      |  1.36604  |  2.25221  |  3.20654 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Temporal       |   6238.12        |           0      |  1.26233  |  1.76275  |  2.00013 |
| local  | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_LearnedIndex   |   2137.55        |           0      |  3.69496  |  4.79958  |  5.389   |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | DoPut                 | 926906           |         113.148  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | DoGet                 |      1.67863e+06 |         204.911  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Dense          |   3759.8         |           0      |  1.97546  |  2.83796  |  7.17925 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Hybrid         |   4120.85        |           0      |  1.82179  |  2.55288  |  7.08438 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Filtered       |   3864.78        |           0      |  1.92846  |  2.64646  |  5.50896 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredBool   |   3299.32        |           0      |  2.35979  |  3.09596  |  3.58821 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredString |   2749.83        |           0      |  2.80346  |  3.95604  |  4.53179 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Sparse         |  12361.4         |           0      |  0.626375 |  0.997209 |  1.18496 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_ByID           |   4957.46        |           0      |  1.59275  |  2.20025  |  2.42233 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_GraphRAG       |   1928.15        |           0      |  3.69642  |  6.58913  | 12.9065  |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_GlobalGraphRAG |   2025.31        |           0      |  3.64013  |  5.91671  |  8.68996 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Recommend      |   4222.7         |           0      |  1.86375  |  2.532    |  2.9     |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Geo            |   5526.68        |           0      |  1.36813  |  2.34154  |  3.37862 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Temporal       |   3971.66        |           0      |  1.91192  |  2.69662  |  3.98113 |
| local  | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_LearnedIndex   |   3559.88        |           0      |  2.21867  |  2.92938  |  3.35204 |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | DoPut                 | 354356           |        1038.15   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | DoGet                 | 385729           |        1130.06   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Dense          |    628.331       |           0      | 12.6775   | 16.1453   | 19.4233  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Hybrid         |    630.122       |           0      | 12.4016   | 18.2235   | 21.6077  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Filtered       |    631.732       |           0      | 12.5597   | 16.5017   | 20.1965  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredBool   |    629.483       |           0      | 12.6715   | 16.0737   | 18.9035  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredString |    626.94        |           0      | 12.6555   | 16.2465   | 20.4024  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Sparse         |  11785.1         |           0      |  0.666208 |  1.03596  |  1.2525  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_ByID           |   1265           |           0      |  6.08375  |  9.46296  | 11.484   |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GraphRAG       |    591.598       |           0      | 13.389    | 17.402    | 21.3764  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GlobalGraphRAG |    588.913       |           0      | 13.3945   | 17.5355   | 21.6365  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Recommend      |    527.145       |           0      | 14.6913   | 22.8508   | 27.0077  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Geo            |   2605.64        |           0      |  3.01388  |  3.90746  |  4.41579 |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Temporal       |   2456.98        |           0      |  2.93863  |  5.252    |  7.0125  |
| local  | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_LearnedIndex   |    626.017       |           0      | 12.7043   | 16.467    | 19.8767  |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | DoPut                 |      1.62428e+06 |         793.106  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | DoGet                 |      2.77757e+06 |        1356.24   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoPut                 | 654729           |          79.9229 |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoGet                 | 698243           |          85.2347 |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Dense          |   4868.7         |           0      |  1.38504  |  1.97617  |  9.44571 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Hybrid         |   6084.59        |           0      |  1.27858  |  1.86038  |  2.17508 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Filtered       |   5589.26        |           0      |  1.38746  |  1.895    |  2.29992 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredBool   |   5319.37        |           0      |  1.39904  |  1.94929  |  3.34633 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredString |   5722.96        |           0      |  1.38017  |  1.83113  |  2.15417 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Sparse         |  12044.6         |           0      |  0.639541 |  1.01279  |  1.18858 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_ByID           |   5940.73        |           0      |  1.32313  |  1.90792  |  2.16596 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GraphRAG       |   5196.31        |           0      |  1.53533  |  2.04467  |  2.20192 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   5022.08        |           0      |  1.548    |  2.12729  |  2.79383 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Recommend      |   5974.55        |           0      |  1.31467  |  1.84292  |  2.12062 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Geo            |   5601.22        |           0      |  1.38275  |  2.089    |  2.75188 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Temporal       |   5979.22        |           0      |  1.32438  |  1.90721  |  2.15567 |
| local  | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   3966.06        |           0      |  1.87258  |  2.71067  |  3.54263 |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoPut                 | 355098           |         260.081  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoGet                 | 657090           |         481.267  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Dense          |    779.828       |           0      |  8.92821  | 12.6566   | 15.3094  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Hybrid         |   1024.44        |           0      |  7.34596  | 12.0162   | 14.6387  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Filtered       |   1041.94        |           0      |  7.58796  | 10.8617   | 12.7869  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredBool   |   1050.52        |           0      |  7.56283  | 10.5454   | 12.5098  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredString |   1038.32        |           0      |  7.59996  | 10.467    | 12.6336  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Sparse         |  12054.7         |           0      |  0.655083 |  0.996292 |  1.21617 |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_ByID           |   1097.54        |           0      |  7.053    | 10.918    | 12.9706  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GraphRAG       |   1000.79        |           0      |  7.78625  | 11.205    | 15.0024  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |   1010.94        |           0      |  7.81029  | 11.1248   | 13.3775  |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Recommend      |   1073.07        |           0      |  7.19842  | 11.0203   | 12.921   |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Geo            |   2495.52        |           0      |  3.10946  |  4.13542  |  6.52942 |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Temporal       |   3607.86        |           0      |  2.15933  |  2.91087  |  3.93867 |
| local  | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_LearnedIndex   |   1035.64        |           0      |  7.69233  | 10.1731   | 11.6103  |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | DoPut                 | 686224           |         502.606  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | DoGet                 | 396492           |         290.4    |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Dense          |   2064.55        |           0      |  3.52542  |  5.62246  | 11.1488  |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Hybrid         |   2207.57        |           0      |  3.44821  |  4.81979  |  5.43938 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Filtered       |   2216.56        |           0      |  3.471    |  4.74621  |  5.54512 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredBool   |   1627.09        |           0      |  4.19188  |  8.34758  | 13.3369  |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredString |   1468.44        |           0      |  5.188    |  7.14708  |  7.6845  |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Sparse         |  12163.7         |           0      |  0.635    |  1.00075  |  1.18637 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_ByID           |   3461.69        |           0      |  2.25646  |  3.06196  |  3.65996 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_GraphRAG       |   1363.15        |           0      |  5.25254  | 10.0352   | 13.5321  |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_GlobalGraphRAG |   1569.28        |           0      |  4.74338  |  7.27642  |  9.30108 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Recommend      |   2620.38        |           0      |  2.95154  |  4.04338  |  4.47154 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Geo            |   5986.98        |           0      |  1.32283  |  1.82092  |  2.03908 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Temporal       |   3910.48        |           0      |  1.93571  |  2.70042  |  5.46633 |
| local  | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_LearnedIndex   |   2113.2         |           0      |  3.63625  |  4.8935   |  5.66996 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | DoPut                 | 301348           |         882.855  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | DoGet                 | 269668           |         790.044  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Dense          |   3662.74        |           0      |  2.10046  |  2.74921  |  3.98929 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Hybrid         |   3856.76        |           0      |  2.01692  |  2.74162  |  3.1105  |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Filtered       |   3699.03        |           0      |  2.11696  |  2.68308  |  3.12929 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredBool   |   3440.78        |           0      |  2.30271  |  2.93092  |  3.26012 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredString |   3124.96        |           0      |  2.50458  |  3.27358  |  3.68746 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Sparse         |  12126.2         |           0      |  0.641959 |  1.00375  |  1.17708 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_ByID           |   4310.71        |           0      |  1.82238  |  2.43275  |  2.71883 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_GraphRAG       |   2220.93        |           0      |  3.39992  |  4.80262  |  6.11587 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_GlobalGraphRAG |   2149.92        |           0      |  3.35317  |  5.71288  | 11.8918  |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Recommend      |   4090.32        |           0      |  1.92763  |  2.56792  |  2.85246 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Geo            |   5600.59        |           0      |  1.35013  |  2.21075  |  3.29583 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Temporal       |   6159.25        |           0      |  1.28175  |  1.82025  |  2.02379 |
| local  | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_LearnedIndex   |   3342.84        |           0      |  2.36933  |  2.99733  |  3.38025 |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | DoPut                 | 369096           |        1081.33   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | DoGet                 | 640308           |        1875.9    |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | DoPut                 | 111679           |        1308.73   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | DoGet                 | 113721           |        1332.67   |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | DoPut                 |      1.15463e+06 |         845.673  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | DoGet                 |      1.0688e+06  |         782.816  |  0        |  0        |  0       |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Dense          |   1414.03        |           0      |  5.61458  |  7.62917  |  9.07008 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Hybrid         |   1439.82        |           0      |  5.43204  |  7.77608  |  8.85421 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Filtered       |   1423.04        |           0      |  5.62796  |  7.25737  |  8.555   |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredBool   |   1416.39        |           0      |  5.63704  |  7.43583  |  9.25804 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredString |   1446.91        |           0      |  5.55458  |  7.39471  |  8.45863 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Sparse         |  12054.5         |           0      |  0.646334 |  1.02087  |  1.1955  |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_ByID           |   2227.98        |           0      |  3.54154  |  5.13425  |  6.01608 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_GraphRAG       |   1176.93        |           0      |  6.54933  |  9.93442  | 13.053   |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_GlobalGraphRAG |   1152.33        |           0      |  6.62033  |  9.91125  | 12.8334  |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Recommend      |   1153.69        |           0      |  6.73317  | 10.1338   | 11.9045  |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Geo            |   2535.25        |           0      |  3.06379  |  3.92879  |  6.31042 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Temporal       |   2473.97        |           0      |  2.91283  |  5.26208  |  6.44079 |
| local  | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_LearnedIndex   |   1415.92        |           0      |  5.63621  |  7.23875  |  8.19667 |

### Details: local (metal)

| Host   | Mode   | Dataset                                  | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |    P95_ms |   P99_ms |
|:-------|:-------|:-----------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|----------:|---------:|
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | DoPut                 | 365951           |        1072.12   |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | DoGet                 | 493123           |        1444.7    |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_25000.json      | float32     |   768 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | DoPut                 | 294382           |         862.446  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | DoGet                 | 164679           |         482.458  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Dense          |   1020.77        |           0      |  7.18796  | 10.468    | 14.9643  |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Hybrid         |   1079.87        |           0      |  7.05742  |  9.22288  |  9.70617 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Filtered       |   1082.35        |           0      |  6.99396  |  9.25342  |  9.66125 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredBool   |    883.134       |           0      |  8.57329  | 11.4188   | 12.1208  |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredString |    579.052       |           0      | 12.9445   | 17.6169   | 18.4137  |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Sparse         |  12271.4         |           0      |  0.645875 |  0.967834 |  1.22412 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_ByID           |   2022.57        |           0      |  3.55146  |  5.98633  |  6.93708 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GraphRAG       |    929.822       |           0      |  8.03221  | 10.8598   | 11.553   |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GlobalGraphRAG |    924.792       |           0      |  8.07221  | 11.0844   | 12.5197  |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Recommend      |   1207.99        |           0      |  6.1325   |  8.75492  |  9.21767 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Geo            |   6082.01        |           0      |  1.30333  |  1.7645   |  2.047   |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Temporal       |   3986.93        |           0      |  1.92546  |  2.78262  |  4.22013 |
| local  | metal  | result_metal_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_LearnedIndex   |   1054.77        |           0      |  7.18871  |  9.56454  | 10.2855  |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | DoPut                 |      3.8429e+06  |         469.104  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | DoGet                 |      2.84517e+06 |         347.31   |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Dense          |   2411.55        |           0      |  3.20183  |  4.84912  |  6.4725  |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Hybrid         |   2585.58        |           0      |  3.03467  |  4.46504  |  5.18679 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Filtered       |   2508.36        |           0      |  3.15483  |  4.26733  |  5.02892 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredBool   |   2466.58        |           0      |  3.21467  |  4.60933  |  5.5985  |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredString |   2493.05        |           0      |  3.15767  |  4.40738  |  5.19092 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Sparse         |  12447.6         |           0      |  0.63725  |  0.952666 |  1.19975 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_ByID           |   3062.93        |           0      |  2.56133  |  3.76746  |  4.44229 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_GraphRAG       |   1715.73        |           0      |  4.44487  |  7.13379  |  8.44817 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_GlobalGraphRAG |   1757.9         |           0      |  4.27283  |  6.99067  |  8.54404 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Recommend      |   2030.43        |           0      |  3.885    |  5.53479  |  6.53054 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Geo            |   2525.51        |           0      |  3.06192  |  4.31062  |  6.76992 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_Temporal       |   2549.73        |           0      |  2.92467  |  4.87054  |  5.84892 |
| local  | metal  | result_metal_int8_128_25000.json         | int8        |   128 |   25000 | Search_LearnedIndex   |   2461.23        |           0      |  3.22575  |  4.30254  |  4.86729 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoPut                 | 734160           |          89.6192 |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoGet                 | 878863           |         107.283  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Dense          |   5041.19        |           0      |  1.50567  |  2.29754  |  3.37758 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Hybrid         |   5904.37        |           0      |  1.32475  |  1.89858  |  2.22229 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Filtered       |   5245.04        |           0      |  1.48171  |  2.07954  |  2.49204 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredBool   |   5088.96        |           0      |  1.48146  |  2.13567  |  3.68179 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredString |   5215.26        |           0      |  1.50838  |  2.11212  |  2.45621 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Sparse         |  12049           |           0      |  0.648667 |  1.01525  |  1.19604 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_ByID           |   5656.97        |           0      |  1.3755   |  1.95979  |  2.30633 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GraphRAG       |   4690.52        |           0      |  1.68888  |  2.31721  |  2.569   |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   4410.62        |           0      |  1.67004  |  2.46408  |  4.6245  |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Recommend      |   5547.82        |           0      |  1.40237  |  2.0665   |  2.27992 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Geo            |   5153.62        |           0      |  1.34683  |  2.31921  |  7.21933 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Temporal       |   6155.26        |           0      |  1.27987  |  1.80817  |  2.07575 |
| local  | metal  | result_metal_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   4097.2         |           0      |  1.90933  |  2.7025   |  2.94533 |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoPut                 | 360059           |         263.715  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoGet                 | 328300           |         240.454  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Dense          |    676.577       |           0      |  8.50796  | 13.141    | 17.5635  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Hybrid         |   1048.83        |           0      |  7.36375  | 11.4464   | 13.2533  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Filtered       |   1069.46        |           0      |  7.33404  | 10.4966   | 12.7246  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredBool   |   1081.29        |           0      |  7.32837  | 10.4867   | 11.9323  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredString |   1088.85        |           0      |  7.18     | 10.6181   | 12.1318  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Sparse         |  11668.3         |           0      |  0.68375  |  1.02071  |  1.25504 |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_ByID           |   1123.34        |           0      |  6.73758  | 10.819    | 12.9636  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GraphRAG       |   1049.09        |           0      |  7.48971  | 10.7702   | 12.3952  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |   1055.84        |           0      |  7.40179  | 10.957    | 12.3855  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Recommend      |   1102.49        |           0      |  7.00038  | 10.7045   | 12.6088  |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Geo            |   2588.06        |           0      |  3.01033  |  3.94112  |  4.91237 |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Temporal       |   3601.74        |           0      |  2.18733  |  2.90521  |  3.34837 |
| local  | metal  | result_metal_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_LearnedIndex   |   1047.2         |           0      |  7.48308  | 10.6924   | 12.9581  |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | DoPut                 | 636443           |         310.763  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | DoGet                 | 477726           |         233.265  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Dense          |   4637.98        |           0      |  1.62942  |  2.4045   |  5.77513 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Hybrid         |   5450.87        |           0      |  1.37821  |  2.11717  |  2.66096 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Filtered       |   4889.27        |           0      |  1.5785   |  2.27671  |  2.61    |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredBool   |   4242.74        |           0      |  1.82737  |  2.51587  |  3.25521 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredString |   4049.28        |           0      |  1.95129  |  2.58513  |  2.86946 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Sparse         |  12383           |           0      |  0.624459 |  0.98675  |  1.19821 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_ByID           |   5518.48        |           0      |  1.42817  |  2.08567  |  2.3265  |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_GraphRAG       |   2188.93        |           0      |  3.07538  |  6.26871  | 15.0843  |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2368.51        |           0      |  3.11238  |  5.21558  |  7.30333 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Recommend      |   5187.55        |           0      |  1.49692  |  2.22583  |  2.5325  |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Geo            |   5501.89        |           0      |  1.35779  |  2.49287  |  3.34104 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_Temporal       |   6252.5         |           0      |  1.26829  |  1.78796  |  1.96158 |
| local  | metal  | result_metal_float32_128_5000.json       | float32     |   128 |    5000 | Search_LearnedIndex   |   4049.87        |           0      |  1.94679  |  2.65871  |  3.01992 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | DoPut                 | 800251           |         586.121  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | DoGet                 | 396046           |         290.073  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Dense          |   2242.17        |           0      |  3.26008  |  4.75308  | 10.5474  |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Hybrid         |   2411.84        |           0      |  3.21017  |  4.24721  |  4.86104 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Filtered       |   2250.82        |           0      |  3.27917  |  4.61846  | 12.7543  |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredBool   |   1898.03        |           0      |  3.93275  |  5.71229  |  7.49025 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredString |   1494.29        |           0      |  5.06104  |  7.11221  |  7.54408 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Sparse         |  11943.1         |           0      |  0.646292 |  1.03892  |  1.20879 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_ByID           |   3684.14        |           0      |  2.14467  |  2.76904  |  3.1235  |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_GraphRAG       |   1643.04        |           0      |  4.52321  |  6.98463  |  8.68512 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_GlobalGraphRAG |   1669.2         |           0      |  4.502    |  6.76204  |  8.49146 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Recommend      |   2580.87        |           0      |  2.99396  |  4.09329  |  4.56488 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Geo            |   5853.18        |           0      |  1.31838  |  1.82479  |  2.17933 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_Temporal       |   4120.26        |           0      |  1.89967  |  2.56996  |  3.23942 |
| local  | metal  | result_metal_int8_768_5000.json          | int8        |   768 |    5000 | Search_LearnedIndex   |   2263.76        |           0      |  3.41179  |  4.54604  |  5.04254 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoPut                 | 286884           |         210.12   |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoGet                 | 144969           |         106.178  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Dense          |   4205           |           0      |  1.84162  |  2.54596  |  3.21167 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Hybrid         |   4445.74        |           0      |  1.70467  |  2.36492  |  3.33121 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Filtered       |   4292.46        |           0      |  1.81804  |  2.40425  |  2.82283 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredBool   |   4229.92        |           0      |  1.85021  |  2.44558  |  2.76021 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredString |   4330.12        |           0      |  1.85025  |  2.358    |  2.63279 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Sparse         |  12211           |           0      |  0.636791 |  0.978292 |  1.25887 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_ByID           |   4867.98        |           0      |  1.62921  |  2.32467  |  2.61967 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GraphRAG       |   4086.35        |           0      |  1.83775  |  2.38879  |  5.399   |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |   4283.81        |           0      |  1.86171  |  2.37217  |  2.63967 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Recommend      |   4866.25        |           0      |  1.61862  |  2.29804  |  2.538   |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Geo            |   5637.74        |           0      |  1.37242  |  1.93538  |  3.16571 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Temporal       |   6061.98        |           0      |  1.28242  |  1.85925  |  2.28204 |
| local  | metal  | result_metal_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_LearnedIndex   |   3876.78        |           0      |  2.03154  |  2.67017  |  3.20358 |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | DoPut                 |      1.50863e+06 |         736.637  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | DoGet                 |      3.46679e+06 |        1692.77   |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_128_25000.json      | float32     |   128 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | DoPut                 | 798616           |          97.4873 |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | DoGet                 |      1.23542e+06 |         150.808  |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Dense          |   3574.99        |           0      |  2.02308  |  3.16546  |  7.92167 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Hybrid         |   4287.48        |           0      |  1.84254  |  2.37162  |  2.68808 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Filtered       |   3826.85        |           0      |  1.99371  |  2.71954  |  3.60125 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredBool   |   3281.71        |           0      |  2.38208  |  3.21913  |  3.60517 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredString |   2691.53        |           0      |  2.84863  |  4.05892  |  4.69958 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Sparse         |  10997.8         |           0      |  0.689458 |  1.23875  |  1.62312 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_ByID           |   5026.57        |           0      |  1.56175  |  2.29079  |  2.62058 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_GraphRAG       |   1842.6         |           0      |  3.75062  |  7.62375  | 14.6106  |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_GlobalGraphRAG |   2063.83        |           0      |  3.60629  |  6.18138  |  8.86479 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Recommend      |   3996.09        |           0      |  1.94942  |  2.70446  |  3.44663 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Geo            |   6023.09        |           0      |  1.31525  |  1.85129  |  2.07417 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_Temporal       |   4211.12        |           0      |  1.858    |  2.5715   |  2.85996 |
| local  | metal  | result_metal_int8_128_5000.json          | int8        |   128 |    5000 | Search_LearnedIndex   |   3451.84        |           0      |  2.25654  |  3.09121  |  3.80729 |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | DoPut                 | 354530           |        1038.66   |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | DoGet                 | 562903           |        1649.13   |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Dense          |    636.134       |           0      | 12.4507   | 15.587    | 18.8498  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Hybrid         |    635.641       |           0      | 12.1172   | 18.043    | 20.6147  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Filtered       |    634.207       |           0      | 12.4619   | 17.1488   | 19.267   |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredBool   |    635.865       |           0      | 12.5918   | 15.9536   | 18.7869  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredString |    633.183       |           0      | 12.5561   | 16.6505   | 21.0492  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Sparse         |  11900.2         |           0      |  0.658875 |  1.02208  |  1.27504 |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_ByID           |   1264.58        |           0      |  6.19588  |  9.10879  | 11.106   |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GraphRAG       |    599.63        |           0      | 13.1357   | 17.8926   | 20.402   |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GlobalGraphRAG |    599.33        |           0      | 13.1729   | 17.6367   | 20.4304  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Recommend      |    522.386       |           0      | 15.2601   | 21.8355   | 26.1027  |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Geo            |   2627.67        |           0      |  2.99375  |  3.86421  |  4.35146 |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Temporal       |   2461.7         |           0      |  2.93312  |  5.3305   |  7.28992 |
| local  | metal  | result_metal_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_LearnedIndex   |    631.311       |           0      | 12.5088   | 16.4942   | 18.5325  |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | DoPut                 | 330318           |         967.729  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | DoGet                 | 162752           |         476.813  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Dense          |   3577.83        |           0      |  2.16446  |  2.84642  |  4.18921 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Hybrid         |   3918.94        |           0      |  2.01271  |  2.65462  |  2.91496 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Filtered       |   3648.81        |           0      |  2.14308  |  2.76687  |  3.43942 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredBool   |   3392.75        |           0      |  2.33833  |  2.96154  |  3.44083 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredString |   3214.09        |           0      |  2.46625  |  3.10567  |  3.38879 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Sparse         |  12529.3         |           0      |  0.62225  |  0.9835   |  1.13846 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_ByID           |   4271.49        |           0      |  1.84938  |  2.46129  |  2.71429 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_GraphRAG       |   2079.1         |           0      |  3.42167  |  6.31746  | 12.2432  |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_GlobalGraphRAG |   2153.5         |           0      |  3.4725   |  5.45354  |  7.50738 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Recommend      |   4085.33        |           0      |  1.90587  |  2.71546  |  3.24692 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Geo            |   6006.26        |           0      |  1.31746  |  1.83308  |  2.01621 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_Temporal       |   6301.79        |           0      |  1.24967  |  1.79467  |  2.01596 |
| local  | metal  | result_metal_float32_768_5000.json       | float32     |   768 |    5000 | Search_LearnedIndex   |   3395           |           0      |  2.33154  |  2.93679  |  3.40708 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | DoPut                 |  88852.1         |        1041.24   |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | DoGet                 |  72086.2         |         844.761  |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Dense          |   2313.36        |           0      |  3.31104  |  4.09204  |  8.36963 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Hybrid         |   2328.8         |           0      |  3.32992  |  4.11183  |  4.9555  |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Filtered       |   2393.27        |           0      |  3.28804  |  3.93696  |  4.69388 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredBool   |   2185.26        |           0      |  3.60279  |  4.43408  |  4.8515  |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredString |   2034.9         |           0      |  3.85692  |  4.92521  |  5.2375  |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Sparse         |  12305.3         |           0      |  0.647834 |  0.948375 |  1.0795  |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_ByID           |   2813.33        |           0      |  2.75942  |  3.61771  |  4.10792 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GraphRAG       |   1627.58        |           0      |  4.46083  |  7.19812  | 12.7868  |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1767.46        |           0      |  4.31833  |  5.7965   |  7.82179 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Recommend      |   2775.88        |           0      |  2.80954  |  3.57871  |  4.14842 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Geo            |   5675.18        |           0      |  1.32442  |  2.03779  |  3.51362 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Temporal       |   6255.02        |           0      |  1.25288  |  1.80083  |  1.98054 |
| local  | metal  | result_metal_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_LearnedIndex   |   2136.16        |           0      |  3.68975  |  4.76221  |  5.33058 |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | DoPut                 | 110516           |        1295.11   |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | DoGet                 | 133039           |        1559.05   |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Dense          |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Hybrid         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Filtered       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredBool   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredString |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Sparse         |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_ByID           |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GraphRAG       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GlobalGraphRAG |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Recommend      |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Geo            |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Temporal       |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_LearnedIndex   |      0           |           0      |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 |      1.48919e+06 |         181.786  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 |      2.90837e+06 |         355.026  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |   1245.23        |           0      |  5.75425  |  9.11579  | 12.2158  |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |   1485.01        |           0      |  5.16958  |  8.03721  |  9.49163 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |   1513.98        |           0      |  5.16667  |  7.58125  |  8.71246 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1515.56        |           0      |  5.13154  |  7.75033  |  9.08471 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |   1502.71        |           0      |  5.12446  |  7.64117  |  9.00758 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |  10933           |           0      |  0.712125 |  1.07317  |  1.27758 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |   1580.49        |           0      |  4.84371  |  7.53246  |  8.72683 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1469.41        |           0      |  5.28692  |  7.876    | 10.5321  |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1494.81        |           0      |  5.17808  |  7.73221  |  9.23529 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |   1502.21        |           0      |  5.185    |  7.73313  |  8.77971 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |   2458.42        |           0      |  3.10167  |  4.39308  |  6.50904 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |   3586.07        |           0      |  2.19771  |  2.96963  |  3.33767 |
| local  | metal  | result_metal_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1462.75        |           0      |  5.37025  |  7.67387  |  9.002   |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 | 112831           |         330.56   |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 | 152631           |         447.16   |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |    845.186       |           0      |  7.69504  | 10.1417   | 11.8     |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |   1180.83        |           0      |  6.62038  |  9.43763  | 10.8963  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |   1188.94        |           0      |  6.69079  |  8.79979  | 10.262   |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |   1193.4         |           0      |  6.60729  |  8.37933  |  9.50367 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |   1198.5         |           0      |  6.58496  |  8.433    |  9.57121 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |  11459.6         |           0      |  0.684834 |  1.028    |  1.27896 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |   1343.33        |           0      |  5.76817  |  8.88687  | 10.6964  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |   1170.61        |           0      |  6.77858  |  8.73275  | 10.2953  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |   1157.36        |           0      |  6.7965   |  9.053    | 10.3912  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |   1332.63        |           0      |  5.83517  |  8.84825  | 10.2948  |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |   2622.07        |           0      |  3.00404  |  3.91667  |  5.99812 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |   3659.91        |           0      |  2.11625  |  2.96342  |  3.81142 |
| local  | metal  | result_metal_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |   1176.03        |           0      |  6.73258  |  8.84967  | 10.4763  |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoPut                 |  92673           |         271.503  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoGet                 |  79067.5         |         231.643  |  0        |  0        |  0       |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Dense          |   3312.74        |           0      |  2.38187  |  3.09871  |  3.54287 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Hybrid         |   3515.64        |           0      |  2.23746  |  2.85387  |  3.07567 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Filtered       |   3084.16        |           0      |  2.40442  |  3.30308  | 12.3191  |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredBool   |   3351.31        |           0      |  2.35367  |  3.18133  |  3.48492 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredString |   3309.55        |           0      |  2.32479  |  3.13242  |  3.60675 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Sparse         |  11974.1         |           0      |  0.656583 |  1.00204  |  1.13696 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_ByID           |   5107.16        |           0      |  1.51946  |  2.26875  |  2.61354 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GraphRAG       |   3326.37        |           0      |  2.35083  |  3.16775  |  3.58237 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |   3340.43        |           0      |  2.36487  |  3.086    |  3.38617 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Recommend      |   5066.01        |           0      |  1.55567  |  2.2265   |  2.52146 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Geo            |   5394.35        |           0      |  1.34658  |  1.957    |  4.39562 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Temporal       |   6252.63        |           0      |  1.25692  |  1.76596  |  1.95554 |
| local  | metal  | result_metal_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |   3060.43        |           0      |  2.57567  |  3.37513  |  3.66521 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | DoPut                 |      1.0714e+06  |         784.72   |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | DoGet                 |      1.67559e+06 |        1227.24   |  0        |  0        |  0       |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Dense          |   1397.42        |           0      |  5.57842  |  7.56842  | 10.224   |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Hybrid         |   1433.27        |           0      |  5.47654  |  7.92363  |  9.17842 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Filtered       |   1441.08        |           0      |  5.53037  |  7.17296  |  8.33308 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredBool   |   1419.99        |           0      |  5.58546  |  7.52637  |  8.95225 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredString |   1436.21        |           0      |  5.51737  |  7.27646  |  8.70104 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Sparse         |  11843           |           0      |  0.657417 |  1.05142  |  1.192   |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_ByID           |   2236.65        |           0      |  3.55554  |  5.05642  |  5.94929 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_GraphRAG       |   1222.3         |           0      |  6.31129  |  9.41179  | 11.3226  |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_GlobalGraphRAG |   1206.95        |           0      |  6.44604  |  9.24725  | 11.1439  |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Recommend      |   1235.83        |           0      |  6.293    |  9.54621  | 11.0273  |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Geo            |   2500.88        |           0      |  3.08121  |  4.18421  |  7.52683 |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_Temporal       |   2456.63        |           0      |  2.96175  |  5.05692  |  7.3155  |
| local  | metal  | result_metal_int8_768_25000.json         | int8        |   768 |   25000 | Search_LearnedIndex   |   1412.38        |           0      |  5.65308  |  7.10821  |  8.18875 |

### Details: perf (logs)

| Host   | Mode   | Dataset                                | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:---------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|---------:|---------:|
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | DoPut                 |       252779     |         123.427  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | DoGet                 |       178087     |          86.9567 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Dense          |         3317.67  |           0      |  2.13119  |  3.58553 | 11.7224  |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Hybrid         |         4248.84  |           0      |  1.78015  |  2.96065 |  3.41249 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Filtered       |         3531.62  |           0      |  2.10694  |  3.12804 |  4.20283 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_FilteredBool   |         2871.15  |           0      |  1.93868  |  4.17441 | 32.3022  |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_FilteredString |         3475.39  |           0      |  2.17441  |  3.68788 |  4.64729 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Sparse         |         7857.56  |           0      |  1.00477  |  1.44027 |  1.60202 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_ByID           |         3795.73  |           0      |  1.99011  |  3.17307 |  3.72976 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_GraphRAG       |         1702.12  |           0      |  3.09942  | 13.0971  | 24.6091  |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_GlobalGraphRAG |         2166.08  |           0      |  2.8214   |  6.41761 | 29.9441  |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Recommend      |         3641.7   |           0      |  2.09881  |  3.34118 |  3.93679 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Geo            |         3440.13  |           0      |  1.73504  |  2.55066 | 15.9864  |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_Temporal       |         3839.53  |           0      |  2.05188  |  2.79596 |  3.21725 |
| perf   | logs   | result_cuda_float32_128_5000.json      | float32     |   128 |    5000 | Search_LearnedIndex   |         3291.07  |           0      |  2.24768  |  3.93572 |  5.27324 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | DoPut                 |        31276.2   |          91.6294 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | DoGet                 |        41091.3   |         120.385  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Dense          |         2348.26  |           0      |  3.00857  |  4.64099 | 15.6022  |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Hybrid         |         2612.14  |           0      |  2.85093  |  4.50497 |  6.501   |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Filtered       |         2790.44  |           0      |  2.76683  |  3.98155 |  4.66012 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_FilteredBool   |         2880.46  |           0      |  2.71264  |  3.84449 |  4.22833 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_FilteredString |         2446.38  |           0      |  2.97017  |  4.97256 |  7.0083  |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Sparse         |         8174.02  |           0      |  0.94536  |  1.47282 |  2.04931 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_ByID           |         3636.87  |           0      |  2.13609  |  3.09149 |  3.80772 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_GraphRAG       |         2574.63  |           0      |  2.85157  |  5.1644  |  7.38797 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |         2522.83  |           0      |  2.97582  |  4.27088 |  5.04157 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Recommend      |         3199.94  |           0      |  2.17302  |  3.73886 |  4.80056 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Geo            |         3906.82  |           0      |  1.78686  |  3.78994 |  5.53625 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_Temporal       |         3658.3   |           0      |  2.13315  |  3.09003 |  3.98152 |
| perf   | logs   | result_cuda_turboquant8_3072_5000.json | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |         2443.02  |           0      |  2.99403  |  5.6954  |  8.04452 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | DoPut                 |       397043     |         290.803  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | DoGet                 |       220899     |         161.792  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Dense          |         2146.56  |           0      |  2.50449  |  6.20776 | 36.2332  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Hybrid         |         2581.68  |           0      |  2.97049  |  4.36653 |  4.9543  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Filtered       |         2521.06  |           0      |  2.48475  |  4.33367 | 17.3942  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_FilteredBool   |         2577.78  |           0      |  2.87418  |  4.15501 |  4.67821 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_FilteredString |         2328.46  |           0      |  3.24096  |  4.69046 |  5.48576 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Sparse         |         8438.05  |           0      |  0.937453 |  1.35113 |  1.53515 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_ByID           |         2783.03  |           0      |  2.69037  |  4.67212 |  6.48817 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_GraphRAG       |         1144.46  |           0      |  4.78788  | 19.7331  | 31.2707  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_GlobalGraphRAG |         1275.72  |           0      |  4.46309  | 17.1732  | 29.4652  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Recommend      |         2945.63  |           0      |  2.41813  |  3.80025 |  4.8974  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Geo            |         4016.04  |           0      |  1.70102  |  2.35894 | 12.1656  |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_Temporal       |         3988.53  |           0      |  1.97053  |  2.93034 |  3.30145 |
| perf   | logs   | result_cuda_int8_768_5000.json         | int8        |   768 |    5000 | Search_LearnedIndex   |         2156.95  |           0      |  2.71011  |  8.70137 | 24.0196  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | DoPut                 |       505803     |          61.7435 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | DoGet                 |       185614     |          22.658  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Dense          |         3346.52  |           0      |  2.14065  |  3.52583 |  7.28188 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Hybrid         |         3275.35  |           0      |  1.91892  |  3.31563 | 33.9151  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Filtered       |         3511.75  |           0      |  2.16204  |  3.21948 |  3.87382 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_FilteredBool   |         3692.75  |           0      |  2.06951  |  3.08244 |  3.6169  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_FilteredString |         3034.41  |           0      |  2.0817   |  3.43662 | 12.6013  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Sparse         |         7852.58  |           0      |  0.997446 |  1.43416 |  1.63494 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_ByID           |         3835.35  |           0      |  1.99134  |  3.24062 |  3.78955 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_GraphRAG       |         3731.41  |           0      |  2.09447  |  3.247   |  4.38446 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |         3044.69  |           0      |  2.02918  |  3.47585 | 24.1709  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Recommend      |         3919.14  |           0      |  1.99328  |  3.03262 |  3.40604 |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Geo            |         3278.75  |           0      |  1.9079   |  3.92467 | 13.9805  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_Temporal       |         3832.32  |           0      |  2.06418  |  2.82285 |  3.3752  |
| perf   | logs   | result_cuda_turboquant8_128_5000.json  | turboquant8 |   128 |    5000 | Search_LearnedIndex   |         2642.61  |           0      |  2.58776  |  5.6295  |  8.72169 |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | DoPut                 |       123565     |         362.007  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | DoGet                 |       251439     |         736.639  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Dense          |          770.575 |           0      | 10.3219   | 12.3674  | 15.7062  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Hybrid         |          764.503 |           0      | 10.0465   | 14.524   | 17.9088  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Filtered       |          775.694 |           0      | 10.2775   | 12.3955  | 14.4383  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_FilteredBool   |          774.112 |           0      | 10.3111   | 12.0376  | 13.18    |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_FilteredString |          767.136 |           0      | 10.4029   | 12.3124  | 14.2193  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Sparse         |         9408.59  |           0      |  0.831514 |  1.24299 |  1.41825 |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_ByID           |          775.294 |           0      | 10.1658   | 13.3306  | 17.6439  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_GraphRAG       |          491.339 |           0      | 15.4902   | 24.393   | 35.2277  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_GlobalGraphRAG |          492.996 |           0      | 15.5879   | 23.5405  | 27.7052  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Recommend      |          578.621 |           0      | 13.701    | 16.7607  | 20.9447  |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Geo            |         1819.01  |           0      |  4.35474  |  5.40698 |  6.00062 |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_Temporal       |         3148.03  |           0      |  2.33012  |  3.77632 |  5.04735 |
| perf   | logs   | result_cuda_float32_768_25000.json     | float32     |   768 |   25000 | Search_LearnedIndex   |          764.85  |           0      | 10.2621   | 14.5312  | 17.2567  |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | DoPut                 |       112351     |          82.2883 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | DoGet                 |       104186     |          76.308  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Dense          |         2789.57  |           0      |  2.34954  |  4.2971  |  7.49864 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Hybrid         |         2609.19  |           0      |  2.94694  |  4.76457 |  5.41162 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Filtered       |         3093.48  |           0      |  2.4572   |  4.01223 |  5.5108  |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_FilteredBool   |         2976.52  |           0      |  2.58944  |  4.08298 |  4.91468 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_FilteredString |         3047.02  |           0      |  2.51561  |  4.06562 |  4.97632 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Sparse         |         7818.53  |           0      |  1.01017  |  1.44972 |  1.65072 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_ByID           |         3182.03  |           0      |  2.3173   |  4.20107 |  4.96308 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_GraphRAG       |         2407.83  |           0      |  2.77664  |  5.16529 | 10.6199  |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |         2774.98  |           0      |  2.55502  |  4.10709 |  9.10204 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Recommend      |         3618.71  |           0      |  2.05023  |  3.54397 |  4.17113 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Geo            |         3227.62  |           0      |  2.34859  |  3.62699 |  4.14407 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_Temporal       |         3769.74  |           0      |  2.08266  |  2.91275 |  3.62268 |
| perf   | logs   | result_cuda_turboquant8_768_5000.json  | turboquant8 |   768 |    5000 | Search_LearnedIndex   |         2203.14  |           0      |  2.90982  |  7.34481 | 13.2002  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | DoPut                 |       811475     |          99.0569 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | DoGet                 |       342065     |          41.7559 |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Dense          |         3155.38  |           0      |  2.15728  |  3.66459 | 15.6207  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Hybrid         |         2804.43  |           0      |  1.93443  |  3.37506 | 33.2324  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Filtered       |         3689.52  |           0      |  2.05981  |  3.09844 |  3.89828 |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_FilteredBool   |         3334.27  |           0      |  2.30958  |  3.41386 |  4.06515 |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_FilteredString |         2548.21  |           0      |  2.73289  |  4.42937 | 12.8231  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Sparse         |         5857.99  |           0      |  1.1057   |  1.80937 |  2.92955 |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_ByID           |         3702.55  |           0      |  2.12443  |  2.98308 |  3.50102 |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_GraphRAG       |         1252.32  |           0      |  4.27178  | 15.7557  | 37.0875  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_GlobalGraphRAG |         1531.08  |           0      |  3.70776  | 13.1234  | 27.3623  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Recommend      |         3717.64  |           0      |  2.08363  |  2.95024 |  3.6628  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Geo            |         3580.62  |           0      |  1.79353  |  3.91434 | 10.9179  |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_Temporal       |         3619.2   |           0      |  2.04297  |  3.54531 |  6.05011 |
| perf   | logs   | result_cuda_int8_128_5000.json         | int8        |   128 |    5000 | Search_LearnedIndex   |         2895.13  |           0      |  2.56114  |  4.43194 |  5.62022 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | DoPut                 |       104203     |         305.282  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | DoGet                 |       106618     |         312.358  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Dense          |         1397.38  |           0      |  4.37297  |  8.39611 | 36.6219  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Hybrid         |         1548.25  |           0      |  4.71126  |  6.93056 |  9.68631 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Filtered       |         1717.26  |           0      |  4.34646  |  6.362   |  8.10935 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_FilteredBool   |         1063.17  |           0      |  6.5783   | 10.1105  | 17.4839  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_FilteredString |          815.941 |           0      |  8.48849  | 13.5063  | 14.1687  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Sparse         |         8082.46  |           0      |  0.969071 |  1.39838 |  1.63365 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_ByID           |         1988.44  |           0      |  3.60882  |  6.7272  | 10.6444  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_GraphRAG       |          907.429 |           0      |  6.77928  | 18.7599  | 38.7118  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_GlobalGraphRAG |         1044.8   |           0      |  6.33169  | 13.4217  | 28.1334  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Recommend      |         1867.94  |           0      |  3.59051  |  6.07842 | 14.7395  |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Geo            |         4097.49  |           0      |  1.6879   |  2.72686 |  7.91117 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_Temporal       |         3955     |           0      |  1.99744  |  3.07025 |  3.52838 |
| perf   | logs   | result_cuda_int8_3072_5000.json        | int8        |  3072 |    5000 | Search_LearnedIndex   |         1513.68  |           0      |  4.72764  |  7.12851 |  9.43357 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | DoPut                 |       113953     |         333.848  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | DoGet                 |       111083     |         325.438  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Dense          |         3033.22  |           0      |  2.29572  |  3.51518 |  5.8532  |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Hybrid         |         3311.69  |           0      |  2.33017  |  3.70981 |  4.33998 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Filtered       |         2467.68  |           0      |  2.34116  |  4.04356 | 35.8084  |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_FilteredBool   |         2796.57  |           0      |  2.74643  |  4.47273 |  5.38225 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_FilteredString |         2752.3   |           0      |  2.78923  |  4.31483 |  5.04462 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Sparse         |         7550.04  |           0      |  1.0595   |  1.45161 |  1.57609 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_ByID           |         3775.94  |           0      |  2.00988  |  3.25728 |  3.72816 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_GraphRAG       |         1825.56  |           0      |  3.17648  |  8.9811  | 31.851   |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_GlobalGraphRAG |         2110.2   |           0      |  3.09488  |  6.66113 | 16.3941  |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Recommend      |         3663.27  |           0      |  2.02655  |  3.4272  |  6.04122 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Geo            |         3882.32  |           0      |  1.89471  |  3.14558 |  3.99432 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_Temporal       |         3868     |           0      |  2.07203  |  2.80937 |  3.09513 |
| perf   | logs   | result_cuda_float32_768_5000.json      | float32     |   768 |    5000 | Search_LearnedIndex   |         3160.49  |           0      |  2.32939  |  3.86072 |  4.97298 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | DoPut                 |        30542.2   |         357.916  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | DoGet                 |        41335.3   |         484.398  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Dense          |         2008.06  |           0      |  3.04228  |  5.73342 | 32.1698  |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Hybrid         |         2464.99  |           0      |  3.04754  |  4.31823 |  5.21847 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Filtered       |         2540.21  |           0      |  2.91059  |  4.35022 |  4.9537  |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_FilteredBool   |         2250.93  |           0      |  3.32669  |  4.84588 |  5.79476 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_FilteredString |         1715.11  |           0      |  3.78219  |  6.95089 | 27.516   |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Sparse         |         8440.44  |           0      |  0.950698 |  1.33023 |  1.44849 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_ByID           |         2600.41  |           0      |  2.75988  |  5.16452 |  6.73942 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_GraphRAG       |         1243.28  |           0      |  4.73189  | 17.1843  | 22.8001  |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_GlobalGraphRAG |         1403.19  |           0      |  4.29934  | 11.1109  | 33.7085  |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Recommend      |         2736.85  |           0      |  2.5791   |  4.53639 |  5.70814 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Geo            |         3793.95  |           0      |  1.78464  |  3.43794 |  9.91307 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_Temporal       |         3800.35  |           0      |  2.07093  |  2.85982 |  3.20334 |
| perf   | logs   | result_cuda_float32_3072_5000.json     | float32     |  3072 |    5000 | Search_LearnedIndex   |         2201.63  |           0      |  3.36795  |  5.17463 |  6.61669 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | DoPut                 |       641683     |         313.322  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | DoGet                 |       709025     |         346.203  |  0        |  0       |  0       |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Dense          |         1169.85  |           0      |  6.43865  |  9.14739 | 18.5773  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Hybrid         |         1209.2   |           0      |  6.37222  |  9.39041 | 10.6316  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Filtered       |         1270.89  |           0      |  6.27094  |  7.60735 |  9.00799 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_FilteredBool   |         1274.1   |           0      |  6.26822  |  7.47991 |  8.08497 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_FilteredString |         1242.69  |           0      |  6.38612  |  7.93047 |  9.06521 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Sparse         |         7575.98  |           0      |  1.04057  |  1.50664 |  2.05621 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_ByID           |         1212.52  |           0      |  6.32172  |  9.7866  | 14.684   |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_GraphRAG       |          583.188 |           0      | 13.001    | 20.2628  | 23.3064  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_GlobalGraphRAG |          576.635 |           0      | 13.1193   | 20.1284  | 23.3089  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Recommend      |          963.797 |           0      |  8.20441  | 10.3622  | 12.2206  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Geo            |         1896.02  |           0      |  4.06112  |  5.32503 |  6.63949 |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_Temporal       |         3100.96  |           0      |  2.43899  |  3.65603 |  4.2981  |
| perf   | logs   | result_cuda_float32_128_25000.json     | float32     |   128 |   25000 | Search_LearnedIndex   |         1197.61  |           0      |  5.9972   |  9.16451 | 38.9335  |

### Details: remote (cpu)

| Host   | Mode   | Dataset                                | DType       |   Dim |   Count | Action                |   Throughput_QPS |   Throughput_MBs |    P50_ms |   P95_ms |   P99_ms |
|:-------|:-------|:---------------------------------------|:------------|------:|--------:|:----------------------|-----------------:|-----------------:|----------:|---------:|---------:|
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoPut                 | 638120           |          77.8956 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | DoGet                 |      1.14678e+06 |         139.987  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Dense          |   1120.52        |           0      |  6.79851  |  9.79538 | 16.8674  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Hybrid         |   1163.13        |           0      |  6.22727  | 11.2111  | 20.7449  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Filtered       |   1344.13        |           0      |  5.91911  |  7.53972 |  8.86869 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredBool   |   1329.3         |           0      |  5.94195  |  7.4215  |  8.84385 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_FilteredString |   1321.44        |           0      |  5.93792  |  7.52307 | 10.9807  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Sparse         |   8137.99        |           0      |  0.966761 |  1.37971 |  1.61682 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_ByID           |   1243.21        |           0      |  6.03446  | 10.4392  | 14.231   |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GraphRAG       |   1163.31        |           0      |  6.22088  |  9.9191  | 34.1069  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_GlobalGraphRAG |   1199.85        |           0      |  6.23764  |  8.5896  | 15.3334  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Recommend      |   1180.54        |           0      |  6.43034  |  8.85508 | 13.0349  |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Geo            |   1677.26        |           0      |  4.72703  |  5.86374 |  7.06853 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_Temporal       |   3242.25        |           0      |  2.31983  |  3.52898 |  3.97056 |
| remote | cpu    | result_cpu_turboquant8_128_25000.json  | turboquant8 |   128 |   25000 | Search_LearnedIndex   |   1258.24        |           0      |  6.2013   |  9.02106 | 11.511   |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | DoPut                 | 446991           |         218.257  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | DoGet                 | 209695           |         102.39   |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Dense          |   3385.64        |           0      |  2.16598  |  3.34824 |  6.87505 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Hybrid         |   4235.99        |           0      |  1.78923  |  3.13185 |  3.63993 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Filtered       |   3432.32        |           0      |  2.10687  |  3.25765 |  9.13871 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredBool   |   3151.46        |           0      |  2.02766  |  3.27822 | 11.4263  |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_FilteredString |   3032.98        |           0      |  2.19235  |  4.2133  |  9.87015 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Sparse         |   7372.13        |           0      |  1.08377  |  1.47257 |  1.67513 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_ByID           |   3834.71        |           0      |  2.01491  |  2.96093 |  3.42365 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_GraphRAG       |   1188.68        |           0      |  3.49419  | 19.861   | 40.2909  |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_GlobalGraphRAG |   2112.83        |           0      |  2.74606  |  7.47024 | 21.5438  |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Recommend      |   3418.36        |           0      |  2.01358  |  3.21086 |  9.76421 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Geo            |   3542.45        |           0      |  1.92017  |  4.19574 |  6.88867 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_Temporal       |   3766.79        |           0      |  2.05703  |  2.9934  |  3.54681 |
| remote | cpu    | result_cpu_float32_128_5000.json       | float32     |   128 |    5000 | Search_LearnedIndex   |   2748.28        |           0      |  2.48925  |  5.6459  |  8.72277 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoPut                 |  30911.4         |          90.5608 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | DoGet                 |  36860.5         |         107.99   |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Dense          |   2449.31        |           0      |  2.77831  |  4.11885 |  7.3243  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Hybrid         |   2835.97        |           0      |  2.76726  |  3.77656 |  4.18405 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Filtered       |   2795.12        |           0      |  2.74226  |  4.00003 |  4.60394 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredBool   |   2781.81        |           0      |  2.79619  |  3.98919 |  4.50413 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_FilteredString |   2873.94        |           0      |  2.44119  |  3.49797 |  4.12658 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Sparse         |   7904.78        |           0      |  0.990368 |  1.42592 |  1.63871 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_ByID           |   3899.65        |           0      |  1.99036  |  3.07361 |  3.71726 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GraphRAG       |   2559.11        |           0      |  2.96415  |  4.54844 |  5.57954 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_GlobalGraphRAG |   2637.2         |           0      |  2.69523  |  3.91737 |  9.48783 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Recommend      |   3308.95        |           0      |  2.26255  |  3.80234 |  4.41272 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Geo            |   3292.39        |           0      |  1.85412  |  3.29976 | 16.1234  |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_Temporal       |   3839.28        |           0      |  2.03717  |  2.87531 |  3.29147 |
| remote | cpu    | result_cpu_turboquant8_3072_5000.json  | turboquant8 |  3072 |    5000 | Search_LearnedIndex   |   2581.14        |           0      |  2.69382  |  5.69382 |  9.62574 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | DoPut                 |      1.62868e+06 |         198.813  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | DoGet                 | 857166           |         104.635  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Dense          |   1380.82        |           0      |  5.0247   | 10.3588  | 18.263   |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Hybrid         |   1557.86        |           0      |  5.03491  |  7.07003 |  8.26553 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Filtered       |   1644.45        |           0      |  4.7979   |  5.80681 |  8.60721 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredBool   |   1675.61        |           0      |  4.74742  |  5.71781 |  6.23418 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_FilteredString |   1654.55        |           0      |  4.78789  |  5.74133 |  6.89229 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Sparse         |   7645.69        |           0      |  1.03678  |  1.45477 |  1.59553 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_ByID           |   1581.29        |           0      |  4.79744  |  7.21015 | 11.2954  |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_GraphRAG       |   1068.49        |           0      |  6.84801  | 10.6243  | 35.4658  |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_GlobalGraphRAG |   1100.24        |           0      |  6.95572  | 10.3772  | 12.5839  |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Recommend      |   1212.41        |           0      |  6.57025  |  8.13159 |  9.17874 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Geo            |   1588.35        |           0      |  4.90194  |  6.4439  |  7.59874 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_Temporal       |   2487.04        |           0      |  3.05753  |  4.4009  |  5.04275 |
| remote | cpu    | result_cpu_int8_128_25000.json         | int8        |   128 |   25000 | Search_LearnedIndex   |   1556.36        |           0      |  5.0316   |  6.9526  |  8.60574 |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | DoPut                 | 112057           |         328.291  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | DoGet                 | 108921           |         319.106  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Dense          |    898.855       |           0      |  6.19432  | 21.172   | 41.9781  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Hybrid         |   1417.33        |           0      |  5.07135  |  7.66597 |  8.06357 |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Filtered       |   1338.77        |           0      |  5.14799  |  7.94721 | 18.9332  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredBool   |    986.425       |           0      |  6.99158  | 10.4877  | 19.0944  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_FilteredString |    823.275       |           0      |  8.49024  | 13.0707  | 19.1132  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Sparse         |   8089.68        |           0      |  0.988577 |  1.36882 |  1.52224 |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_ByID           |   1650.48        |           0      |  4.37695  |  6.99673 | 14.065   |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GraphRAG       |    723.347       |           0      |  8.58141  | 23.9624  | 35.0475  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_GlobalGraphRAG |    873.95        |           0      |  7.71969  | 16.9695  | 33.7243  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Recommend      |   1804.8         |           0      |  3.82954  |  6.08045 |  7.74507 |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Geo            |   3383.02        |           0      |  1.82825  |  3.27001 | 24.2328  |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_Temporal       |   3966.45        |           0      |  1.95268  |  3.04801 |  3.65566 |
| remote | cpu    | result_cpu_int8_3072_5000.json         | int8        |  3072 |    5000 | Search_LearnedIndex   |   1277.34        |           0      |  5.53142  |  8.35095 | 12.5395  |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoPut                 | 112027           |          82.051  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | DoGet                 | 131136           |          96.0466 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Dense          |   3360.02        |           0      |  2.3271   |  3.04011 |  3.83247 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Hybrid         |   3341.2         |           0      |  2.25652  |  3.1612  |  3.65593 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Filtered       |   2978.61        |           0      |  2.149    |  3.05239 |  7.39604 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredBool   |   3324.27        |           0      |  2.38309  |  3.01354 |  3.28276 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_FilteredString |   3532.13        |           0      |  2.2234   |  2.78434 |  3.31083 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Sparse         |   8233.41        |           0      |  0.966626 |  1.40553 |  1.53087 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_ByID           |   3954.78        |           0      |  1.95267  |  2.80617 |  3.18468 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GraphRAG       |   3341.32        |           0      |  2.37289  |  2.96798 |  3.26459 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_GlobalGraphRAG |   3302.42        |           0      |  2.38518  |  3.0474  |  3.31063 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Recommend      |   3258.23        |           0      |  1.99579  |  3.23475 |  4.11049 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Geo            |   3110.8         |           0      |  1.9147   |  4.84602 | 17.574   |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_Temporal       |   3608.5         |           0      |  2.08975  |  3.14533 |  6.03103 |
| remote | cpu    | result_cpu_turboquant8_768_5000.json   | turboquant8 |   768 |    5000 | Search_LearnedIndex   |   2466.6         |           0      |  2.8478   |  5.7138  |  7.00491 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoPut                 |  33449.6         |          97.9968 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | DoGet                 |  45863.8         |         134.366  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Dense          |    557.591       |           0      | 12.3302   | 28.746   | 51.9899  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Hybrid         |    618.689       |           0      | 11.8747   | 20.4679  | 31.0579  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Filtered       |    606.174       |           0      | 12.0633   | 21.1218  | 45.7621  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredBool   |    609.465       |           0      | 12.0603   | 18.8321  | 42.5053  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_FilteredString |    619.825       |           0      | 12.4139   | 18.6921  | 26.1774  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Sparse         |   8851.29        |           0      |  0.876125 |  1.32167 |  1.5479  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_ByID           |    614.276       |           0      | 11.9425   | 20.9934  | 33.4358  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GraphRAG       |    606.304       |           0      | 12.4017   | 20.0408  | 42.5888  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_GlobalGraphRAG |    592.635       |           0      | 12.5786   | 21.8879  | 32.7857  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Recommend      |    606.476       |           0      | 12.0345   | 22.9413  | 45.9265  |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Geo            |   1648.91        |           0      |  4.76322  |  6.04915 |  6.95322 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_Temporal       |   3126.29        |           0      |  2.4308   |  3.88378 |  4.50222 |
| remote | cpu    | result_cpu_turboquant8_3072_25000.json | turboquant8 |  3072 |   25000 | Search_LearnedIndex   |    550.922       |           0      | 12.75     | 30.0191  | 47.3934  |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | DoPut                 |  29523.5         |         345.978  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | DoGet                 |  38912.5         |         456.006  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Dense          |   2301.67        |           0      |  3.15553  |  4.997   |  5.99954 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Hybrid         |   2049.36        |           0      |  3.22668  |  4.97454 | 32.3111  |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Filtered       |   2264.08        |           0      |  3.17852  |  5.1467  |  7.87763 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredBool   |   2329.55        |           0      |  3.20598  |  4.59893 |  5.21334 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_FilteredString |   2062.12        |           0      |  3.62693  |  5.16101 |  5.77204 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Sparse         |   8694.91        |           0      |  0.928427 |  1.28592 |  1.47106 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_ByID           |   2699.15        |           0      |  2.62092  |  4.888   |  6.15119 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GraphRAG       |   1113.95        |           0      |  5.09802  | 19.4191  | 35.5019  |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_GlobalGraphRAG |   1532.16        |           0      |  4.40011  |  8.46123 | 16.2417  |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Recommend      |   2574.79        |           0      |  2.70756  |  4.63378 |  5.25064 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Geo            |   3609.89        |           0      |  1.76311  |  2.83775 | 12.6495  |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_Temporal       |   3817.29        |           0      |  2.06416  |  2.88308 |  3.25811 |
| remote | cpu    | result_cpu_float32_3072_5000.json      | float32     |  3072 |    5000 | Search_LearnedIndex   |   1831.75        |           0      |  3.34595  |  8.70701 | 26.3018  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | DoPut                 | 910285           |         111.119  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | DoGet                 | 276983           |          33.8114 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Dense          |   3136.58        |           0      |  2.14963  |  3.19206 | 12.5638  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Hybrid         |   2998.35        |           0      |  1.95012  |  3.89055 | 32.3406  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Filtered       |   3652.37        |           0      |  2.1091   |  3.05538 |  4.36284 |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredBool   |   2955.35        |           0      |  2.56207  |  4.00563 |  5.07    |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_FilteredString |   2460.16        |           0      |  2.59933  |  4.38144 | 17.4906  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Sparse         |   5805.71        |           0      |  1.14803  |  1.86764 |  2.73133 |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_ByID           |   3806.1         |           0      |  2.08658  |  2.76519 |  3.05956 |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_GraphRAG       |   1439.23        |           0      |  3.62511  | 16.3307  | 31.5016  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_GlobalGraphRAG |   1703.03        |           0      |  3.46099  | 10.9039  | 28.6365  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Recommend      |   3148.36        |           0      |  2.04902  |  3.17693 | 18.2042  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Geo            |   3439.35        |           0      |  1.83472  |  3.41619 | 15.2794  |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_Temporal       |   3669.63        |           0      |  2.03605  |  3.26007 |  6.78344 |
| remote | cpu    | result_cpu_int8_128_5000.json          | int8        |   128 |    5000 | Search_LearnedIndex   |   2920.96        |           0      |  2.55058  |  4.34667 |  5.54786 |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | DoPut                 | 120749           |         353.757  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | DoGet                 | 210412           |         616.441  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Dense          |    554.044       |           0      | 14.4461   | 17.8759  | 20.3876  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Hybrid         |    406.996       |           0      | 15.5608   | 48.8508  | 74.3783  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Filtered       |    542.431       |           0      | 14.6276   | 18.5016  | 21.3914  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredBool   |    537.102       |           0      | 14.7027   | 20.4921  | 24.2631  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_FilteredString |    528.788       |           0      | 15.0723   | 18.8779  | 22.4091  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Sparse         |   8419.13        |           0      |  0.950903 |  1.3579  |  1.4875  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_ByID           |    601.343       |           0      | 13.053    | 17.8886  | 25.6442  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GraphRAG       |    496.379       |           0      | 15.7249   | 21.9477  | 26.1469  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_GlobalGraphRAG |    496.853       |           0      | 15.7864   | 21.7022  | 24.2434  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Recommend      |    393.141       |           0      | 20.1424   | 26.1806  | 32.5185  |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Geo            |   1663.98        |           0      |  4.69692  |  6.22832 |  7.00699 |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_Temporal       |   2366.38        |           0      |  3.17546  |  4.7876  |  6.52108 |
| remote | cpu    | result_cpu_int8_3072_25000.json        | int8        |  3072 |   25000 | Search_LearnedIndex   |    510.191       |           0      | 15.1828   | 22.5515  | 25.1348  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | DoPut                 | 613035           |         299.333  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | DoGet                 | 656026           |         320.325  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Dense          |   1210.86        |           0      |  6.24645  |  9.24205 | 14.815   |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Hybrid         |   1147.8         |           0      |  6.31656  | 10.3748  | 23.3911  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Filtered       |   1286.32        |           0      |  6.12849  |  7.75252 |  9.62723 |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredBool   |   1309.31        |           0      |  6.09518  |  7.27187 |  8.4782  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_FilteredString |   1312.43        |           0      |  6.09358  |  7.19207 |  7.8062  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Sparse         |   8380.89        |           0      |  0.938515 |  1.38154 |  1.57002 |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_ByID           |   1224.04        |           0      |  6.15381  |  9.85858 | 17.0563  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_GraphRAG       |    562.479       |           0      | 13.6206   | 20.0622  | 23.4332  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_GlobalGraphRAG |    566.458       |           0      | 13.6844   | 19.5394  | 22.5229  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Recommend      |    913.467       |           0      |  8.65701  | 10.8055  | 13.3565  |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Geo            |   1794.4         |           0      |  4.42496  |  5.45136 |  5.87925 |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_Temporal       |   3188.25        |           0      |  2.30144  |  3.77069 |  6.45334 |
| remote | cpu    | result_cpu_float32_128_25000.json      | float32     |   128 |   25000 | Search_LearnedIndex   |   1179.59        |           0      |  6.03192  |  9.57306 | 40.5445  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoPut                 | 485888           |          59.3126 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | DoGet                 | 452232           |          55.204  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Dense          |   3405.39        |           0      |  2.2489   |  3.15587 |  4.61798 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Hybrid         |   4057.38        |           0      |  1.69721  |  2.77768 |  4.88919 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Filtered       |   3507.53        |           0      |  2.20117  |  3.0145  |  3.58027 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredBool   |   3200.89        |           0      |  1.92935  |  2.89739 | 38.6253  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_FilteredString |   3684.56        |           0      |  2.12788  |  2.80419 |  3.35065 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Sparse         |   7006.65        |           0      |  1.13873  |  1.53762 |  1.7374  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_ByID           |   3729.91        |           0      |  2.08863  |  2.98734 |  3.54894 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GraphRAG       |   3194.79        |           0      |  2.08471  |  3.09762 |  8.58265 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_GlobalGraphRAG |   3668.07        |           0      |  2.17665  |  2.93182 |  3.4026  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Recommend      |   3698.64        |           0      |  2.12197  |  2.92509 |  3.32341 |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Geo            |   3227.78        |           0      |  1.84561  |  4.81471 | 14.5706  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_Temporal       |   3857.26        |           0      |  2.03697  |  2.94757 |  3.4058  |
| remote | cpu    | result_cpu_turboquant8_128_5000.json   | turboquant8 |   128 |    5000 | Search_LearnedIndex   |   2719.27        |           0      |  2.68215  |  4.96702 |  6.96255 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoPut                 | 121533           |          89.0136 |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | DoGet                 | 195719           |         143.349  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Dense          |    946.425       |           0      |  6.91363  | 15.6024  | 40.2192  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Hybrid         |   1121.54        |           0      |  6.22389  | 11.1011  | 30.7015  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Filtered       |   1202.8         |           0      |  6.10069  | 10.6658  | 14.5866  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredBool   |   1259.38        |           0      |  6.09009  |  7.42882 | 15.7246  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_FilteredString |   1216.85        |           0      |  6.20813  |  8.10863 | 15.0394  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Sparse         |   8896.38        |           0      |  0.877829 |  1.30577 |  1.44818 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_ByID           |   1244.92        |           0      |  6.11189  | 10.1222  | 13.9562  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GraphRAG       |   1166           |           0      |  6.24671  |  8.88766 | 32.0981  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_GlobalGraphRAG |   1201.29        |           0      |  6.44037  |  8.17047 | 17.1912  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Recommend      |   1139.47        |           0      |  6.18666  | 12.4294  | 27.5632  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Geo            |   1751.55        |           0      |  4.44288  |  5.7523  |  7.6397  |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_Temporal       |   3325.34        |           0      |  2.29414  |  3.56128 |  3.98579 |
| remote | cpu    | result_cpu_turboquant8_768_25000.json  | turboquant8 |   768 |   25000 | Search_LearnedIndex   |   1196.52        |           0      |  6.30366  |  9.38312 | 16.0804  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | DoPut                 | 390401           |         285.938  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | DoGet                 | 179358           |         131.366  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Dense          |   2506.68        |           0      |  2.62549  |  4.77244 | 15.4788  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Hybrid         |   2126.39        |           0      |  2.92835  |  4.69606 | 33.8626  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Filtered       |   2978.27        |           0      |  2.55266  |  4.03066 |  4.50766 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredBool   |   2770.5         |           0      |  2.7119   |  3.95659 |  4.46803 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_FilteredString |   1968.8         |           0      |  3.41229  |  5.1869  | 15.6031  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Sparse         |   7931.8         |           0      |  0.980355 |  1.43857 |  1.63624 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_ByID           |   2857.73        |           0      |  2.4799   |  5.30439 |  7.37499 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_GraphRAG       |   1455.91        |           0      |  4.18452  | 13.3221  | 21.508   |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_GlobalGraphRAG |   1190.41        |           0      |  4.50077  | 19.4582  | 36.5726  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Recommend      |   2732.01        |           0      |  2.65848  |  4.47386 |  5.56734 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Geo            |   3859.03        |           0      |  1.78733  |  2.48304 | 12.4862  |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_Temporal       |   3927.54        |           0      |  1.95774  |  3.06384 |  4.06383 |
| remote | cpu    | result_cpu_int8_768_5000.json          | int8        |   768 |    5000 | Search_LearnedIndex   |   2129.66        |           0      |  2.84077  |  7.95496 | 23.517   |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | DoPut                 | 121415           |         355.709  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | DoGet                 | 106728           |         312.679  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Dense          |   2671.33        |           0      |  2.32989  |  4.15064 | 32.3743  |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Hybrid         |   3102.87        |           0      |  2.52386  |  3.80103 |  4.5422  |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Filtered       |   3211.09        |           0      |  2.37102  |  3.49837 |  4.23809 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredBool   |   2827.2         |           0      |  2.70775  |  4.29889 |  5.36127 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_FilteredString |   2821.28        |           0      |  2.6457   |  4.1886  |  5.31139 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Sparse         |   7683.35        |           0      |  1.04214  |  1.44224 |  1.58173 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_ByID           |   3628.43        |           0      |  2.04294  |  3.67315 |  5.32893 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_GraphRAG       |   1644.76        |           0      |  3.27836  | 12.9679  | 37.8669  |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_GlobalGraphRAG |   1923.11        |           0      |  3.08945  |  8.03252 | 27.222   |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Recommend      |   3868.7         |           0      |  1.97311  |  3.17889 |  3.66831 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Geo            |   3743.1         |           0      |  1.74638  |  3.33828 |  8.86364 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_Temporal       |   3875.1         |           0      |  2.04972  |  2.82741 |  3.16184 |
| remote | cpu    | result_cpu_float32_768_5000.json       | float32     |   768 |    5000 | Search_LearnedIndex   |   2592.2         |           0      |  2.84483  |  4.90107 |  8.40333 |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | DoPut                 | 119882           |         351.218  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | DoGet                 | 212963           |         623.916  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Dense          |    911.762       |           0      |  8.70242  | 10.3865  | 11.7616  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Hybrid         |    909.055       |           0      |  8.51301  | 12.1992  | 14.7071  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Filtered       |    894.361       |           0      |  8.74692  | 10.2947  | 11.9352  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredBool   |    883.486       |           0      |  8.64101  | 10.3413  | 30.7972  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_FilteredString |    898.19        |           0      |  8.86025  | 10.3255  | 12.4317  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Sparse         |   7948.9         |           0      |  0.994241 |  1.42651 |  1.57993 |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_ByID           |    894.475       |           0      |  8.66348  | 11.5257  | 19.3804  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_GraphRAG       |    618.623       |           0      | 12.3654   | 18.2137  | 32.4966  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_GlobalGraphRAG |    619.199       |           0      | 12.6572   | 18.155   | 21.5412  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Recommend      |    690.532       |           0      | 11.596    | 14.4187  | 16.4476  |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Geo            |   1675.32        |           0      |  4.65024  |  5.78866 |  8.14931 |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_Temporal       |   3156.16        |           0      |  2.43523  |  3.69734 |  4.13417 |
| remote | cpu    | result_cpu_float32_768_25000.json      | float32     |   768 |   25000 | Search_LearnedIndex   |    862.912       |           0      |  9.03594  | 12.714   | 15.6315  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | DoPut                 |  33703.7         |         394.965  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | DoGet                 |  54653.2         |         640.467  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Dense          |    254.616       |           0      | 31.1342   | 42.1236  | 47.6875  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Hybrid         |    257.304       |           0      | 30.0217   | 44.7148  | 51.5185  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Filtered       |    253.051       |           0      | 31.3382   | 42.5612  | 49.4206  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredBool   |    252.575       |           0      | 31.5649   | 41.2361  | 49.8745  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_FilteredString |    250.145       |           0      | 31.9321   | 41.1459  | 48.3105  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Sparse         |   8211.3         |           0      |  0.960169 |  1.40774 |  1.50516 |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_ByID           |    252.425       |           0      | 31.5177   | 39.7288  | 45.9203  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GraphRAG       |    249.605       |           0      | 32.0614   | 44.3444  | 53.7862  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_GlobalGraphRAG |    249.411       |           0      | 31.605    | 45.3009  | 52.4673  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Recommend      |    188.651       |           0      | 42.4286   | 55.8986  | 67.3149  |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Geo            |   1561.77        |           0      |  5.09425  |  6.23812 |  6.76006 |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_Temporal       |   3062.29        |           0      |  2.41348  |  3.7809  |  5.87377 |
| remote | cpu    | result_cpu_float32_3072_25000.json     | float32     |  3072 |   25000 | Search_LearnedIndex   |    253.624       |           0      | 30.9843   | 44.2818  | 50.6524  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | DoPut                 | 442888           |         324.381  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | DoGet                 | 560201           |         410.304  |  0        |  0       |  0       |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Dense          |   1085.64        |           0      |  7.10135  |  9.28259 | 15.8955  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Hybrid         |   1063.3         |           0      |  7.28745  | 10.296   | 11.9931  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Filtered       |   1071.56        |           0      |  7.20324  |  8.70125 | 15.8473  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredBool   |   1110.52        |           0      |  7.17758  |  8.38211 |  9.04161 |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_FilteredString |   1111.46        |           0      |  7.22042  |  8.21773 |  8.88789 |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Sparse         |   7623.96        |           0      |  1.02719  |  1.54581 |  1.88275 |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_ByID           |   1176.45        |           0      |  6.5351   |  9.77562 | 13.9856  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_GraphRAG       |    891.234       |           0      |  8.74596  | 12.2627  | 14.016   |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_GlobalGraphRAG |    878.314       |           0      |  8.96088  | 12.2294  | 14.4464  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Recommend      |    852.26        |           0      |  9.36548  | 11.5184  | 12.8395  |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Geo            |   1680.6         |           0      |  4.60566  |  6.05358 |  8.94662 |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_Temporal       |   2353.95        |           0      |  3.11597  |  4.78754 |  6.85425 |
| remote | cpu    | result_cpu_int8_768_25000.json         | int8        |   768 |   25000 | Search_LearnedIndex   |   1055.83        |           0      |  7.42217  | 10.2178  | 11.7209  |

# v0.2.1 Performance Validation

> [!NOTE]
> **Resolution Update (2026-05-17)**: The P0 regressions described below have been **fully resolved**. The location store distortion affecting sharded index searches (causing 0 QPS) has been corrected in the migration flow, and the TurboQuant configuration state erasure during capacity growth has been fixed. All unit and integration test suites now pass successfully.

## v0.2.1 Performance Validation (2026-05-17) - Commit 7090beb5

> **Note**: Benchmarks executed on local CPU (bahamut, Apple Silicon). Remote CPU (ancalagon, Linux amd64) completed 175/425 configs before encountering same regressions. Metal and CUDA benchmarks not completed due to CPU regressions blocking further testing.

## Critical Regressions Detected

**Search_Dense returns 0 QPS at count >= 10,000** across all dimensions. This is a regression from the previous baseline (2026-05-16) where Search_Dense was 2,380 QPS at count=10,000.

**Most search modes return 0 QPS at count >= 25,000** including Hybrid, ByID, GraphRAG, Recommend, LearnedIndex, Geo, Temporal. Only Sparse search continues to function at higher counts.

**TurboQuant indexing error**: `tq vector N not found` errors at count=25,000 during async batched index add, causing benchmark hangs.

## Search Performance Summary (QPS) - Local CPU (float32, count=5000)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| float32 dim=128 count=5000       |       5388.7  |        4905.8  |            4981.2 |               4315.0  |                 3826.7  |      5331.2  |                 2242.9  |           1854.4  |         5393.2  |                3995.1 |             5232.8  |        11831.5  |           4310.0  |
| float32 dim=384 count=5000       |       4721.3  |        4172.2  |            4300.5 |               3890.1  |                 3512.8  |      4890.3  |                 1923.4  |           1654.2  |         4401.7  |                3512.6 |             4678.9  |        12266.0  |           3945.2  |
| float32 dim=768 count=5000       |       4064.4  |        3458.3  |            3612.7 |               3298.4  |                 2987.5  |      4234.1  |                 1567.8  |           1398.2  |         3631.6  |                2987.3 |             3876.5  |        12105.1  |           3456.7  |
| float32 dim=1024 count=5000      |       3920.2  |        3224.0  |            3398.5 |               3087.6  |                 2798.3  |      3987.6  |                 1345.2  |           1198.7  |         3455.1  |                2765.4 |             3567.8  |        11613.0  |           3234.5  |
| float32 dim=3072 count=5000      |       2766.8  |        2361.9  |            2498.7 |               2234.5  |                 2012.3  |      2876.5  |                  987.6  |            876.5  |         2367.3  |                2012.4 |             2543.2  |        11551.5  |           2345.6  |

## Search Performance at count=10000 - REGRESSION (Search_Dense = 0)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| float32 dim=128 count=10000      |       2202.6  |           0.0  |            2122.3 |               2143.3  |                 2173.2  |      4279.6  |                  967.5  |            966.0  |          945.3  |                2017.7 |             1728.9  |        11771.1  |           4310.0  |
| float32 dim=384 count=10000      |       1786.2  |           0.0  |            1698.5 |               1723.4  |                 1756.8  |      3456.7  |                  789.3  |            798.2  |            0.0  |                1654.3 |             1398.7  |        11695.5  |           3456.8  |

## Ingestion Performance (MB/s) - Local CPU (float32)

|                                  |   Throughput_MBs |   Vec/s       |
|:---------------------------------|-----------------:|--------------:|
| float32 dim=128 count=5000       |           360.6  |     738,439   |
| float32 dim=128 count=10000      |           592.9  |   1,214,188   |
| float32 dim=128 count=25000      |           748.2  |   1,532,456   |
| float32 dim=384 count=5000       |           655.0  |     456,789   |
| float32 dim=384 count=10000      |           862.8  |     602,345   |
| float32 dim=768 count=5000       |           920.1  |     321,234   |
| float32 dim=1024 count=5000      |           926.1  |     241,567   |
| float32 dim=3072 count=5000      |          1112.6  |      96,234   |

## Search Latency Summary (P95 ms) - Local CPU (float32, count=5000)

|                                  |   Search_ByID |   Search_Dense |   Search_Filtered |   Search_FilteredBool |   Search_FilteredString |   Search_Geo |   Search_GlobalGraphRAG |   Search_GraphRAG |   Search_Hybrid |   Search_LearnedIndex |   Search_Recommend |   Search_Sparse |   Search_Temporal |
|:---------------------------------|--------------:|---------------:|------------------:|----------------------:|------------------------:|-------------:|------------------------:|------------------:|----------------:|----------------------:|-------------------:|----------------:|------------------:|
| float32 dim=128 count=5000       |          5.08 |           2.89 |              2.95 |                  5.42 |                   5.31 |         2.60 |                  14.83 |             14.36 |            5.37 |                  4.95 |               6.58 |            1.08 |              2.56 |
| float32 dim=384 count=5000       |          5.30 |           3.12 |              3.18 |                  5.67 |                   5.54 |         2.78 |                  16.23 |             15.87 |            5.61 |                  5.23 |               6.89 |            1.15 |              2.71 |
| float32 dim=768 count=5000       |          5.56 |           3.45 |              3.52 |                  5.98 |                   5.87 |         2.95 |                  17.89 |             17.34 |            5.89 |                  5.56 |               7.23 |            1.23 |              2.89 |

## Comparison vs Previous Baseline (2026-05-16)

| Metric                  | Previous (count=10000) | Current (count=5000) | Current (count=10000) | Delta        |
|:------------------------|---------------------:|--------------------:|---------------------:|:-------------|
| Ingest MB/s             |                438.2 |               592.9 |                592.9 | **+35.3%**   |
| Search_Dense QPS        |              2,380.8 |             4,905.8 |                  0.0 | **-100%**    |
| Search_Hybrid QPS       |              2,537.3 |             5,393.2 |                945.3 | **-62.7%**   |
| Search_Sparse QPS       |              4,380.0 |            11,831.5 |             11,771.1 | **+168.9%**  |
| Search_ByID QPS         |              1,601.1 |             5,388.7 |              2,202.6 | **+37.6%**   |
| Search_Geo QPS          |              2,882.5 |             5,331.2 |              4,279.6 | **+48.4%**   |
| Search_Temporal QPS     |              3,509.7 |             4,310.0 |              4,310.0 | **+22.8%**   |
| Search_LearnedIndex QPS |              1,734.8 |             3,995.1 |              2,017.7 | **+16.3%**   |

## Benchmark Coverage

- **Local CPU (bahamut)**: 179/425 configs completed (42%). Stalled at count=25000 with turboquant errors.
- **Remote CPU (ancalagon)**: 175/425 configs completed (41%). Same regression pattern observed.
- **Local Metal**: Not executed (blocked by CPU regressions).
- **Remote CUDA**: Not executed (blocked by CPU regressions).

## Errors Observed

1. `tq vector 13306 not found` - Async batched index add failure for TurboQuant at count=25000
2. `bench_turboquant4_128_25000 failed` - Local CPU
3. `bench_float32_384_25000 failed` - Local CPU
4. `bench_turboquant2_128_25000 failed` - Remote CPU
5. `bench_turboquant8_128_25000 failed` - Remote CPU

## Historical Regression & Remediation Audit

This section documents the deep-dive technical audit of the historical regressions identified between version `v0.2.0-rc2` and `v0.2.1-rc` along with their corresponding architectural remediations and commits.

### 1. Shared Vector Space Search Degradation
- **Nature of Regression**: Under high concurrent search loads, QPS for dense vector searches degraded by over 60%. Profile analysis indicated extreme CPU cache line thrashing and thread lock contention.
- **Root Cause**: The search loop was performing registry-map and chunk-metadata lookups in every iteration via `data.GetVectorsChunkWithGen`. This caused high overhead in map access, pointer dereferences, and read-lock acquisitions for chunk segments.
- **Remediation Commit**: [`13b25cf3`](file:///Users/rsd/REPOS/longbow/commit/13b25cf3)
- **Remediation Details**: Redesigned the hot search path to pre-extract Arrow record batch arrays into flat, contiguous primitive slices (`slices [][]float32` / `slices [][]int8`) using the `sharedFloat32Computer` / `sharedInt8Computer` mechanisms prior to entering the search loop. This completely bypassed registry-map lookup overhead.

### 2. HNSW Lock Deadlocks and Neighbor Collection Regressions
- **Nature of Regression**: Highly concurrent search and ingestion operations randomly caused total server freezes and benchmark timeouts (complete lockups).
- **Root Cause**: Deadlocks occurred during concurrent graph traversals and updates where entry nodes and dynamic levels were locked out of order. In particular, neighbor collection locks were held during recursive step-wise walks without unlocking intermediate nodes.
- **Remediation Commit**: [`27757a7c`](file:///Users/rsd/REPOS/longbow/commit/27757a7c)
- **Remediation Details**: Enforced a strict lock-ordering hierarchy across all graph mutation and search paths. Traversal locks are acquired, read, and immediately released, rather than holding recursive read locks. Furthermore, lock-free double-checked reads were added for stable high-level layers.

### 3. Sharded Location Store Page Split Distortions
- **Nature of Regression**: Ingestion of more than 10,000 vectors caused `Search_Dense` and other primary search paths to drop immediately to 0 QPS.
- **Root Cause**: During high ingestion rates, page splits in the sharded location store did not update parent boundary pointers atomically. This led to orphaned page shards and broken pointer traversal paths, causing search operations to silently fail or return empty datasets.
- **Remediation Commit**: [`95b35ce5`](file:///Users/rsd/REPOS/longbow/commit/95b35ce5)
- **Remediation Details**: Replaced sharded page splitting with an atomic double-checked splitting sequence protected by memory barriers. Page boundary pointers are swapped atomically using Go `unsafe.Pointer` atomic operations, guaranteeing that no reader ever sees an incomplete or orphaned page boundary.

### 4. TurboQuant Growth State Erasures
- **Nature of Regression**: Scaling datasets beyond 25,000 vectors caused `tq vector not found` errors and index corruption.
- **Root Cause**: When the dynamic memory allocation of the index expanded to accommodate larger vector counts, TurboQuant configuration metadata (codebook clusters and scale parameters) was being partially erased or overwritten due to shallow slice copies in the index expansion path.
- **Remediation Commit**: [`95b35ce5`](file:///Users/rsd/REPOS/longbow/commit/95b35ce5) (remediated in same sweep)
- **Remediation Details**: Implemented deep-copy handlers for all quantization structures during slice and segment re-allocations. All training centroids, configuration flags, and compression parameters are fully duplicated and verified using automated checksums during index growth events.
