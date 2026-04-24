# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-24 03:43:05

## Executive Summary

Benchmarks are still in progress. The following data represents partial results collected so far.

## 1. Ingest Performance (vec/s)

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |       229749 |     108988   |        348461 |        134431 |
| ('Darwin arm64', 'cpu', 'complex64')    |       358370 |        nan   |        375945 |        273295 |
| ('Darwin arm64', 'cpu', 'float16')      |       565504 |        nan   |        865827 |        417694 |
| ('Darwin arm64', 'cpu', 'float32')      |       354244 |      89353.5 |        592782 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |     227544   |        339605 |        248298 |
| ('Darwin arm64', 'cpu', 'int16')        |          nan |        nan   |        782043 |           nan |
| ('Darwin arm64', 'cpu', 'int32')        |          nan |        nan   |        646099 |        390549 |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |     150350   |        468832 |        205529 |
| ('Darwin arm64', 'cpu', 'int8')         |       453498 |        nan   |        881213 |        616796 |
| ('Darwin arm64', 'cpu', 'turboquant')   |       475624 |     209713   |        613694 |        367542 |
| ('Darwin arm64', 'cpu', 'uint16')       |          nan |        nan   |           nan |        479428 |
| ('Darwin arm64', 'cpu', 'uint32')       |       424929 |        nan   |        513038 |        417219 |
| ('Darwin arm64', 'cpu', 'uint64')       |       224207 |        nan   |        344039 |        236219 |
| ('Darwin arm64', 'cpu', 'uint8')        |       531326 |     185237   |        875001 |        621825 |
| ('Darwin arm64', 'metal', 'complex128') |       157623 |     124163   |        345326 |        123206 |
| ('Darwin arm64', 'metal', 'complex64')  |       277733 |        nan   |        340670 |        277312 |
| ('Darwin arm64', 'metal', 'float16')    |       496278 |     318454   |        702942 |        471766 |
| ('Darwin arm64', 'metal', 'float32')    |       364764 |     239244   |        598177 |           nan |
| ('Darwin arm64', 'metal', 'float64')    |       338772 |        nan   |        466096 |        175027 |
| ('Darwin arm64', 'metal', 'int16')      |          nan |     397009   |        841221 |           nan |
| ('Darwin arm64', 'metal', 'int32')      |          nan |        nan   |        566276 |        365614 |
| ('Darwin arm64', 'metal', 'int64')      |          nan |     169530   |        469503 |        249546 |
| ('Darwin arm64', 'metal', 'int8')       |       456100 |        nan   |        848897 |        572333 |
| ('Darwin arm64', 'metal', 'turboquant') |       251752 |     190682   |        562958 |        324942 |
| ('Darwin arm64', 'metal', 'uint16')     |          nan |        nan   |           nan |        475262 |
| ('Darwin arm64', 'metal', 'uint32')     |          nan |        nan   |        469526 |        412133 |
| ('Darwin arm64', 'metal', 'uint64')     |       275065 |        nan   |        377097 |        245579 |
| ('Darwin arm64', 'metal', 'uint8')      |       401043 |     274650   |        832775 |        661243 |

## 2. Standard Search Performance (QPS)

### BYID QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      5156.57 |      3818.14 |      5469.64  |       3786.88 |
| ('Darwin arm64', 'cpu', 'complex64')    |      5116.18 |       nan    |      5728.31  |       4625.52 |
| ('Darwin arm64', 'cpu', 'float16')      |      4381.34 |       nan    |      3755.88  |       2651.78 |
| ('Darwin arm64', 'cpu', 'float32')      |      4387.91 |      4807.62 |      2211.94  |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      5357.67 |      5641.54  |       3350.48 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       757.971 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      7558.07  |       6534.42 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3790.74 |      4532.8   |       4230.3  |
| ('Darwin arm64', 'cpu', 'int8')         |      4825.9  |       nan    |      5088.98  |       3415.36 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      6013.54 |      5118.82 |      6877.43  |       5925.15 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |       nan     |       1316.98 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4679.94 |       nan    |      3775.57  |       2951.72 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3676.37 |       nan    |      4312.75  |       4129.34 |
| ('Darwin arm64', 'cpu', 'uint8')        |      8314.19 |      6775.03 |      9688.84  |       7837    |
| ('Darwin arm64', 'metal', 'complex128') |      5117.06 |      4207.37 |      5435.27  |       3924.93 |
| ('Darwin arm64', 'metal', 'complex64')  |      5485.48 |       nan    |      5960.92  |       5093.55 |
| ('Darwin arm64', 'metal', 'float16')    |      4979.35 |      4410.84 |      3468.26  |       2769.53 |
| ('Darwin arm64', 'metal', 'float32')    |      4834.45 |      4104.28 |      4749.19  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      6580.56 |       nan    |      6312.68  |       4908.99 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      2074.35 |      1249.2   |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      7523.4   |       6407.15 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3463    |      4191.11  |       4190.66 |
| ('Darwin arm64', 'metal', 'int8')       |      5134.18 |       nan    |      4534.97  |       3566.66 |
| ('Darwin arm64', 'metal', 'turboquant') |      5844.51 |      4919.72 |      6902.19  |       5810    |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |       nan     |       1286.78 |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |      2168.09  |       3230.51 |
| ('Darwin arm64', 'metal', 'uint64')     |      3875.47 |       nan    |      4279.25  |       4241.38 |
| ('Darwin arm64', 'metal', 'uint8')      |      6477.76 |      7412.85 |      9326.45  |       8223.22 |

### DENSE QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4387.43 |      2779.1  |       4137.17 |       2553.38 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4580.86 |       nan    |       4602.93 |       3004.52 |
| ('Darwin arm64', 'cpu', 'float16')      |      3528.59 |       nan    |       3249.2  |       2558.83 |
| ('Darwin arm64', 'cpu', 'float32')      |      4799.48 |      3342.1  |       3539.44 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3602.13 |       4138.66 |       2427    |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       1062.03 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |       6373.01 |       4758.97 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3216.2  |       3965.02 |       3187.76 |
| ('Darwin arm64', 'cpu', 'int8')         |      4425.85 |       nan    |       4334.95 |       2721.08 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5301.49 |      4025.61 |       5515.74 |       4346.92 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |        nan    |       1197.58 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4859.85 |       nan    |       3404.25 |       2372.13 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3134.16 |       nan    |       3764.06 |       3275.27 |
| ('Darwin arm64', 'cpu', 'uint8')        |      9098.1  |      5064    |       7213.57 |       4971.81 |
| ('Darwin arm64', 'metal', 'complex128') |      4111.37 |      2821.21 |       3971.48 |       2627.45 |
| ('Darwin arm64', 'metal', 'complex64')  |      4599.61 |       nan    |       4899.27 |       3100    |
| ('Darwin arm64', 'metal', 'float16')    |      4529.57 |      3716.78 |       3003.26 |       2328.88 |
| ('Darwin arm64', 'metal', 'float32')    |      4332.09 |      3349.86 |       3785.26 |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5521.28 |       nan    |       5339.22 |       3678.25 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1808.91 |       1199.32 |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |       6254.84 |       4766.03 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3259.52 |       3778.59 |       3337.46 |
| ('Darwin arm64', 'metal', 'int8')       |      4600.37 |       nan    |       4377.42 |       2977.32 |
| ('Darwin arm64', 'metal', 'turboquant') |      5301.95 |      3837.15 |       5593.61 |       4231.2  |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |        nan    |       1181.59 |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |       2921.96 |       2656.36 |
| ('Darwin arm64', 'metal', 'uint64')     |      3806.08 |       nan    |       3826.73 |       3296.38 |
| ('Darwin arm64', 'metal', 'uint8')      |      6082.59 |      5591.42 |       7047.68 |       5472.06 |

### FILTERED QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4176.02 |      2787.66 |      4453.8   |       2571.85 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4358.07 |       nan    |      4711.65  |       3186.88 |
| ('Darwin arm64', 'cpu', 'float16')      |      3479.09 |       nan    |      3262.4   |       2566.21 |
| ('Darwin arm64', 'cpu', 'float32')      |      4410.01 |      3335.62 |      3117.36  |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3544.39 |      4085.66  |       2500.46 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       735.241 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      6658.38  |       4868.25 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3251.44 |      3875.38  |       3335.5  |
| ('Darwin arm64', 'cpu', 'int8')         |      4380.77 |       nan    |      4472.61  |       2734.43 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5412.8  |      4066.49 |      5934.78  |       4555.56 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |       nan     |       1223.13 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4306.3  |       nan    |      3341.27  |       2417.16 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3752.55 |       nan    |      3858.74  |       3245.69 |
| ('Darwin arm64', 'cpu', 'uint8')        |      7696.72 |      5172.01 |      8243.82  |       5619.2  |
| ('Darwin arm64', 'metal', 'complex128') |      4204.92 |      2862.45 |      4480.02  |       2621.89 |
| ('Darwin arm64', 'metal', 'complex64')  |      3699.22 |       nan    |      4886.33  |       3244.7  |
| ('Darwin arm64', 'metal', 'float16')    |      4655.86 |      3636.39 |      3004.91  |       2325.34 |
| ('Darwin arm64', 'metal', 'float32')    |      4201.57 |      3345    |      3851.31  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5653.34 |       nan    |      5003.49  |       3767.32 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1810.02 |      1215.28  |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      6608.58  |       4941.72 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3237.69 |      3743.09  |       3321.17 |
| ('Darwin arm64', 'metal', 'int8')       |      4465.4  |       nan    |      4248.07  |       2999.26 |
| ('Darwin arm64', 'metal', 'turboquant') |      5382.55 |      3819.02 |      5723.49  |       4536.08 |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |       nan     |       1185.1  |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |      2894.23  |       2709.54 |
| ('Darwin arm64', 'metal', 'uint64')     |      3819.96 |       nan    |      3904.14  |       3292.5  |
| ('Darwin arm64', 'metal', 'uint8')      |      6214.37 |      5545.78 |      7938.35  |       5863.37 |

### FILTEREDBOOL QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4315.83 |      2785    |      4473.41  |       2610.13 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4388.65 |       nan    |      4619.21  |       3145.65 |
| ('Darwin arm64', 'cpu', 'float16')      |      3500.02 |       nan    |      3268.18  |       2562.63 |
| ('Darwin arm64', 'cpu', 'float32')      |      4161.74 |      3339.55 |      2910.31  |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3470.07 |      4174.26  |       2587.83 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       740.453 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      6668.86  |       4886.51 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3175.71 |      3947.92  |       3326.59 |
| ('Darwin arm64', 'cpu', 'int8')         |      4191.42 |       nan    |      4434.66  |       2731.67 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5331    |      4017.24 |      5957.43  |       4513.95 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |       nan     |       1206.74 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4321.64 |       nan    |      3402.91  |       2415.28 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3613.31 |       nan    |      3869.16  |       3283.98 |
| ('Darwin arm64', 'cpu', 'uint8')        |      7575.39 |      5154.53 |      8174.94  |       5645.51 |
| ('Darwin arm64', 'metal', 'complex128') |      4188.21 |      2785.54 |      4369.86  |       2665.33 |
| ('Darwin arm64', 'metal', 'complex64')  |      3726.39 |       nan    |      4549.67  |       3288.66 |
| ('Darwin arm64', 'metal', 'float16')    |      4539.56 |      3433.52 |      3021.82  |       2314.4  |
| ('Darwin arm64', 'metal', 'float32')    |      4316.53 |      3364.99 |      3943.48  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5755.64 |       nan    |      5209.78  |       3764.59 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1799.73 |      1216.36  |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      6594.8   |       4834.71 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3248.12 |      3755.8   |       3153.72 |
| ('Darwin arm64', 'metal', 'int8')       |      4498.04 |       nan    |      4420.95  |       2991.62 |
| ('Darwin arm64', 'metal', 'turboquant') |      5346.22 |      3509.35 |      5863.36  |       4491.88 |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |       nan     |       1188.18 |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |      2861.74  |       2715.74 |
| ('Darwin arm64', 'metal', 'uint64')     |      3784.71 |       nan    |      3915.71  |       3298.99 |
| ('Darwin arm64', 'metal', 'uint8')      |      5351.27 |      5687.13 |      7891.99  |       5907.88 |

### FILTEREDSTRING QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4289.99 |      2787.98 |      4459.92  |       2513.65 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4328.48 |       nan    |      4639.68  |       3198.37 |
| ('Darwin arm64', 'cpu', 'float16')      |      3958.35 |       nan    |      3228.18  |       2526.01 |
| ('Darwin arm64', 'cpu', 'float32')      |      4302.37 |      3327.11 |      1837.74  |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3622.83 |      4588.2   |       2519.9  |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       741.086 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      6607.47  |       4858.57 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3224.46 |      3929.55  |       3384.98 |
| ('Darwin arm64', 'cpu', 'int8')         |      4169.53 |       nan    |      4514.16  |       2733.87 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5474.69 |      4063.42 |      5950.64  |       4565.11 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |       nan     |       1203.73 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4291.99 |       nan    |      3416.75  |       2451.82 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3645.99 |       nan    |      3744.12  |       3319.97 |
| ('Darwin arm64', 'cpu', 'uint8')        |      6901.86 |      4721.8  |      8184.66  |       5580.92 |
| ('Darwin arm64', 'metal', 'complex128') |      3735.38 |      2925.33 |      4380.97  |       2653.47 |
| ('Darwin arm64', 'metal', 'complex64')  |      4246.84 |       nan    |      4807.84  |       3295.42 |
| ('Darwin arm64', 'metal', 'float16')    |      4377.07 |      3584.93 |      3040.26  |       2322.08 |
| ('Darwin arm64', 'metal', 'float32')    |      3489.54 |      3384.21 |      4006.62  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5700.6  |       nan    |      5465.02  |       3663.5  |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1857.53 |      1217.28  |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      6630.8   |       4831.07 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3209.74 |      3777.91  |       3329.04 |
| ('Darwin arm64', 'metal', 'int8')       |      4307.3  |       nan    |      4358.45  |       3010.84 |
| ('Darwin arm64', 'metal', 'turboquant') |      4956.39 |      3845.83 |      5952.61  |       4460.96 |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |       nan     |       1189.38 |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |      1972.86  |       2723.02 |
| ('Darwin arm64', 'metal', 'uint64')     |      3796.8  |       nan    |      3919.94  |       3302.26 |
| ('Darwin arm64', 'metal', 'uint8')      |      5734.48 |      5542.82 |      7967.93  |       5779.32 |

### GEO QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int16')        |          nan |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'int32')        |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint16')       |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int32')      |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint16')     |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'metal', 'uint32')     |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |             0 |

### GRAPHRAG QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4355.57 |      2775.42 |      4394.28  |       2687.17 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4360.63 |       nan    |      4718.77  |       3200.91 |
| ('Darwin arm64', 'cpu', 'float16')      |      4008.85 |       nan    |      3508.29  |       1790.52 |
| ('Darwin arm64', 'cpu', 'float32')      |      4106.96 |      3235.71 |      2064.85  |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3966.23 |      4849.17  |       3523.41 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       743.139 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      6571.98  |       4928.61 |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3270.22 |      4273.34  |       3616.2  |
| ('Darwin arm64', 'cpu', 'int8')         |      4358.47 |       nan    |      4560.86  |       2837.1  |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5482.05 |      3946.56 |      5993.47  |       4563.42 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |       nan     |       1211.5  |
| ('Darwin arm64', 'cpu', 'uint32')       |      4390.89 |       nan    |      3463.75  |       2428.86 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3110.57 |       nan    |      4063.39  |       3489.63 |
| ('Darwin arm64', 'cpu', 'uint8')        |      7267.82 |      4861.37 |      8102.6   |       5490.95 |
| ('Darwin arm64', 'metal', 'complex128') |      4207.46 |      2728.35 |      4536.18  |       2777.24 |
| ('Darwin arm64', 'metal', 'complex64')  |      3670.48 |       nan    |      4877.06  |       3333.63 |
| ('Darwin arm64', 'metal', 'float16')    |      4555.59 |      3650.74 |      3232.38  |       1921.2  |
| ('Darwin arm64', 'metal', 'float32')    |      4195.57 |      3377.09 |      4073.96  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5690.52 |       nan    |      5466.1   |       3772.14 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1865.73 |      1215.3   |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      6634.7   |       4819.15 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3059.04 |      4087.63  |       3576.92 |
| ('Darwin arm64', 'metal', 'int8')       |      5245.39 |       nan    |      3550.92  |       3114.6  |
| ('Darwin arm64', 'metal', 'turboquant') |      5445.4  |      3846.6  |      6071.7   |       4530.63 |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |       nan     |       1196.8  |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |      2050.95  |       2778.66 |
| ('Darwin arm64', 'metal', 'uint64')     |      4090.43 |       nan    |      4043.92  |       3578.36 |
| ('Darwin arm64', 'metal', 'uint8')      |      5666.78 |      5411.06 |      7857.46  |       5872.32 |

### HYBRID QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4004.74 |      2648.78 |       4033.88 |       2385.22 |
| ('Darwin arm64', 'cpu', 'complex64')    |      4011.34 |       nan    |       4205.05 |       2874.49 |
| ('Darwin arm64', 'cpu', 'float16')      |      3232.11 |       nan    |       3028.38 |       2322.07 |
| ('Darwin arm64', 'cpu', 'float32')      |      4062.04 |      3053.87 |       3000.16 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3204.84 |       3783.02 |       2504.06 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       1021.67 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |       5822.28 |       4306.3  |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3052.29 |       3713.73 |       3124.43 |
| ('Darwin arm64', 'cpu', 'int8')         |      4006.33 |       nan    |       4042.12 |       2591.31 |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5010.17 |      3720.48 |       5273.29 |       4079.43 |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |        nan    |       1174.57 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4189.6  |       nan    |       3149.84 |       2286.83 |
| ('Darwin arm64', 'cpu', 'uint64')       |      3525.19 |       nan    |       3541.76 |       3076.71 |
| ('Darwin arm64', 'cpu', 'uint8')        |      6423.2  |      4584.32 |       6877.9  |       5006.08 |
| ('Darwin arm64', 'metal', 'complex128') |      3892.96 |      2646.91 |       3876.97 |       2449.4  |
| ('Darwin arm64', 'metal', 'complex64')  |      2957.71 |       nan    |       4462.04 |       2947.34 |
| ('Darwin arm64', 'metal', 'float16')    |      4267.35 |      3456.46 |       2812.98 |       2197.76 |
| ('Darwin arm64', 'metal', 'float32')    |      3997.34 |      3147.81 |       3534.47 |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5016.27 |       nan    |       4705.27 |       3411.31 |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1751.6  |       1167.14 |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |       5637.07 |       4353.89 |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3088.69 |       3538.91 |       3104.07 |
| ('Darwin arm64', 'metal', 'int8')       |      4109.68 |       nan    |       4008.06 |       2731.11 |
| ('Darwin arm64', 'metal', 'turboquant') |      4981.76 |      3568.51 |       5023.24 |       4033.23 |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |        nan    |       1151.78 |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |       2750.86 |       2508.67 |
| ('Darwin arm64', 'metal', 'uint64')     |      3472.47 |       nan    |       3659.84 |       3094.98 |
| ('Darwin arm64', 'metal', 'uint8')      |      5607.96 |      5033.14 |       6438.45 |       5334.59 |

### RECOMMEND QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int16')        |          nan |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'int32')        |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint16')       |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int32')      |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint16')     |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'metal', 'uint32')     |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |             0 |

### SPARSE QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      9043.17 |      8718.37 |      11690.5  |      11763.4  |
| ('Darwin arm64', 'cpu', 'complex64')    |      8194.95 |       nan    |      10998.1  |      11596    |
| ('Darwin arm64', 'cpu', 'float16')      |     12535.4  |       nan    |      11906.6  |      10318    |
| ('Darwin arm64', 'cpu', 'float32')      |      9425.07 |     10131.9  |       6113.36 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |     11606.9  |      11261.7  |       6686.67 |
| ('Darwin arm64', 'cpu', 'int16')        |       nan    |       nan    |       8625.73 |        nan    |
| ('Darwin arm64', 'cpu', 'int32')        |       nan    |       nan    |      14315.5  |      13609.7  |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      7947.79 |      12236.6  |      12353.9  |
| ('Darwin arm64', 'cpu', 'int8')         |      8677.42 |       nan    |      10770.8  |      10594.9  |
| ('Darwin arm64', 'cpu', 'turboquant')   |      9181.9  |      9119.94 |      11586.7  |      11191.5  |
| ('Darwin arm64', 'cpu', 'uint16')       |       nan    |       nan    |        nan    |      12187.3  |
| ('Darwin arm64', 'cpu', 'uint32')       |      9064.08 |       nan    |      11101.5  |      12356.3  |
| ('Darwin arm64', 'cpu', 'uint64')       |      8658.62 |       nan    |      12280.9  |      11601.4  |
| ('Darwin arm64', 'cpu', 'uint8')        |      6851.34 |      8096.72 |      10335.5  |       9677.11 |
| ('Darwin arm64', 'metal', 'complex128') |      7731.61 |      7767.64 |      12148.9  |      11997.5  |
| ('Darwin arm64', 'metal', 'complex64')  |      7982.11 |       nan    |      11150    |      11593.3  |
| ('Darwin arm64', 'metal', 'float16')    |      8980.43 |      8554.62 |      13213.7  |      10917.5  |
| ('Darwin arm64', 'metal', 'float32')    |      8887.67 |      8862.91 |      14250.5  |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |     13292.6  |       nan    |      12837    |      10222.4  |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |     12062.3  |      12951.3  |        nan    |
| ('Darwin arm64', 'metal', 'int32')      |       nan    |       nan    |      13628.2  |      13665.3  |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      8364.73 |      12505.7  |      12068.9  |
| ('Darwin arm64', 'metal', 'int8')       |      8484.92 |       nan    |      10269.2  |      11978.1  |
| ('Darwin arm64', 'metal', 'turboquant') |      5271.15 |      8855.3  |      11491.3  |      10906.7  |
| ('Darwin arm64', 'metal', 'uint16')     |       nan    |       nan    |        nan    |      12250.7  |
| ('Darwin arm64', 'metal', 'uint32')     |       nan    |       nan    |       7635.59 |      11055.5  |
| ('Darwin arm64', 'metal', 'uint64')     |      8576.57 |       nan    |      11312.1  |      11997.9  |
| ('Darwin arm64', 'metal', 'uint8')      |      7954.99 |      8166.62 |      11676.4  |      10602.6  |

### TEMPORAL QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int16')        |          nan |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'int32')        |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint16')       |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int32')      |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint16')     |          nan |          nan |           nan |             0 |
| ('Darwin arm64', 'metal', 'uint32')     |          nan |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |             0 |

## 3. Specialized Search Performance

### GEO Results

### TEMPORAL Results

### GRAPHRAG Results

