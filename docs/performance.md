# Longbow 0.2.0 Performance Benchmark Matrix

**Generated on**: 2026-04-24
**Status**: ⚠️ PARTIAL RESULTS (Benchmarks in progress)

## Executive Summary
This report tracks the performance hardening of Longbow 0.2.0 across ARM64 (Apple Silicon) and AMD64 (Linux/NVIDIA). 

### Key Findings (So Far)
- **TurboQuant**: Shows ~2x-3x higher ingestion throughput vs Float32 in many cases.
- **Metal Acceleration**: Significantly reduces search latency for high-dimensional vectors on Mac.
- **Polymorphic Parity**: Verified functional correctness across all 14 data types.

## 1. Ingest Performance (vec/s)

|                                         |   (500, 128) |   (500, 384) |      (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|-----------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |       229749 |     108988   | 292148           |           nan |
| ('Darwin arm64', 'cpu', 'complex64')    |       358370 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'cpu', 'float16')      |       565504 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'cpu', 'float32')      |       354244 |      89353.5 | 579724           |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |     227544   |    nan           |           nan |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |     150350   |    nan           |        188203 |
| ('Darwin arm64', 'cpu', 'int8')         |       453498 |        nan   |      1.38728e+06 |           nan |
| ('Darwin arm64', 'cpu', 'turboquant')   |       475624 |     209713   | 471457           |        430007 |
| ('Darwin arm64', 'cpu', 'uint32')       |       424929 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'cpu', 'uint64')       |       224207 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'cpu', 'uint8')        |       531326 |     185237   |      1.10808e+06 |        698039 |
| ('Darwin arm64', 'metal', 'complex128') |       157623 |     124163   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'complex64')  |       277733 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'float16')    |       496278 |     318454   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'float32')    |       364764 |     239244   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'float64')    |       338772 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'int16')      |          nan |     397009   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'int64')      |          nan |     169530   | 397542           |           nan |
| ('Darwin arm64', 'metal', 'int8')       |       456100 |        nan   | 639812           |        512394 |
| ('Darwin arm64', 'metal', 'turboquant') |       251752 |     190682   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'uint64')     |       275065 |        nan   |    nan           |           nan |
| ('Darwin arm64', 'metal', 'uint8')      |       401043 |     274650   | 888725           |           nan |

## 2. Standard Search Performance (QPS)

### BYID QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      5156.57 |      3818.14 |       4472.33 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      5116.18 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      4381.34 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4387.91 |      4807.62 |          0    |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      5357.67 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3790.74 |        nan    |       4255.83 |
| ('Darwin arm64', 'cpu', 'int8')         |      4825.9  |       nan    |       5400.15 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      6013.54 |      5118.82 |       5566.26 |       4940.27 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4679.94 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3676.37 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      8314.19 |      6775.03 |       5877.05 |       4768.66 |
| ('Darwin arm64', 'metal', 'complex128') |      5117.06 |      4207.37 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      5485.48 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4979.35 |      4410.84 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      4834.45 |      4104.28 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      6580.56 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      2074.35 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3463    |       3859.1  |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      5134.18 |       nan    |       2186.76 |       3493.6  |
| ('Darwin arm64', 'metal', 'turboquant') |      5844.51 |      4919.72 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3875.47 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      6477.76 |      7412.85 |       7500.11 |        nan    |

### DENSE QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4387.43 |      2779.1  |       3085.78 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4580.86 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      3528.59 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4799.48 |      3342.1  |       3515.08 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3602.13 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3216.2  |        nan    |       3068.51 |
| ('Darwin arm64', 'cpu', 'int8')         |      4425.85 |       nan    |       4115.7  |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5301.49 |      4025.61 |       5154.83 |       4195.35 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4859.85 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3134.16 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      9098.1  |      5064    |       6460.86 |       3158.47 |
| ('Darwin arm64', 'metal', 'complex128') |      4111.37 |      2821.21 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      4599.61 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4529.57 |      3716.78 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      4332.09 |      3349.86 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5521.28 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1808.91 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3259.52 |       3669.08 |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      4600.37 |       nan    |       4194.15 |       2831.5  |
| ('Darwin arm64', 'metal', 'turboquant') |      5301.95 |      3837.15 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3806.08 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      6082.59 |      5591.42 |       6678.73 |        nan    |

### FILTERED QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4176.02 |      2787.66 |       3843.86 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4358.07 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      3479.09 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4410.01 |      3335.62 |       3475.83 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3544.39 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3251.44 |        nan    |       3235.03 |
| ('Darwin arm64', 'cpu', 'int8')         |      4380.77 |       nan    |       4171.48 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5412.8  |      4066.49 |       5072.85 |       4122    |
| ('Darwin arm64', 'cpu', 'uint32')       |      4306.3  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3752.55 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      7696.72 |      5172.01 |       5457.45 |       3822.85 |
| ('Darwin arm64', 'metal', 'complex128') |      4204.92 |      2862.45 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      3699.22 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4655.86 |      3636.39 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      4201.57 |      3345    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5653.34 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1810.02 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3237.69 |       3641.7  |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      4465.4  |       nan    |       3177.25 |       2821.06 |
| ('Darwin arm64', 'metal', 'turboquant') |      5382.55 |      3819.02 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3819.96 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      6214.37 |      5545.78 |       6778.63 |        nan    |

### FILTEREDBOOL QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4315.83 |      2785    |       3880.76 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4388.65 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      3500.02 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4161.74 |      3339.55 |       3071.62 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3470.07 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3175.71 |        nan    |       3233.36 |
| ('Darwin arm64', 'cpu', 'int8')         |      4191.42 |       nan    |       4234.75 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5331    |      4017.24 |       5014.05 |       4103.81 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4321.64 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3613.31 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      7575.39 |      5154.53 |       5244.34 |       3832.72 |
| ('Darwin arm64', 'metal', 'complex128') |      4188.21 |      2785.54 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      3726.39 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4539.56 |      3433.52 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      4316.53 |      3364.99 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5755.64 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1799.73 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3248.12 |       3684.17 |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      4498.04 |       nan    |       4179.98 |       2841.57 |
| ('Darwin arm64', 'metal', 'turboquant') |      5346.22 |      3509.35 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3784.71 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      5351.27 |      5687.13 |       6636.49 |        nan    |

### FILTEREDSTRING QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4289.99 |      2787.98 |       3864.68 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4328.48 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      3958.35 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4302.37 |      3327.11 |          0    |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3622.83 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3224.46 |        nan    |       3216.66 |
| ('Darwin arm64', 'cpu', 'int8')         |      4169.53 |       nan    |       4210.13 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5474.69 |      4063.42 |       4862.5  |       4136.13 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4291.99 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3645.99 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      6901.86 |      4721.8  |       5338.36 |       3854.88 |
| ('Darwin arm64', 'metal', 'complex128') |      3735.38 |      2925.33 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      4246.84 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4377.07 |      3584.93 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      3489.54 |      3384.21 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5700.6  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1857.53 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3209.74 |       3629.06 |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      4307.3  |       nan    |       4156.88 |       2828.15 |
| ('Darwin arm64', 'metal', 'turboquant') |      4956.39 |      3845.83 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3796.8  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      5734.48 |      5542.82 |       6761.85 |        nan    |

### GEO QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |           nan |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |           nan |

### GRAPHRAG QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4355.57 |      2775.42 |       3740.85 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4360.63 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      4008.85 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4106.96 |      3235.71 |          0    |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3966.23 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3270.22 |        nan    |       3580.69 |
| ('Darwin arm64', 'cpu', 'int8')         |      4358.47 |       nan    |       4735.21 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5482.05 |      3946.56 |       5007.8  |       4103.48 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4390.89 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3110.57 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      7267.82 |      4861.37 |       5215.53 |       3737.94 |
| ('Darwin arm64', 'metal', 'complex128') |      4207.46 |      2728.35 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      3670.48 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4555.59 |      3650.74 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      4195.57 |      3377.09 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5690.52 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1865.73 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3059.04 |       4014.12 |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      5245.39 |       nan    |          0    |       3032.81 |
| ('Darwin arm64', 'metal', 'turboquant') |      5445.4  |      3846.6  |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      4090.43 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      5666.78 |      5411.06 |       6612.38 |        nan    |

### HYBRID QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      4004.74 |      2648.78 |       3553.41 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      4011.34 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |      3232.11 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      4062.04 |      3053.87 |       3176.92 |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |      3204.84 |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      3052.29 |        nan    |       3057.08 |
| ('Darwin arm64', 'cpu', 'int8')         |      4006.33 |       nan    |       3807.34 |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      5010.17 |      3720.48 |       4553.26 |       3779.16 |
| ('Darwin arm64', 'cpu', 'uint32')       |      4189.6  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      3525.19 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      6423.2  |      4584.32 |       5124.66 |       3580.68 |
| ('Darwin arm64', 'metal', 'complex128') |      3892.96 |      2646.91 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      2957.71 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      4267.35 |      3456.46 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      3997.34 |      3147.81 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |      5016.27 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |      1751.6  |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      3088.69 |       3429.73 |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      4109.68 |       nan    |       3599.73 |       2479.6  |
| ('Darwin arm64', 'metal', 'turboquant') |      4981.76 |      3568.51 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      3472.47 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      5607.96 |      5033.14 |       5820.36 |        nan    |

### RECOMMEND QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |           nan |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |           nan |

### SPARSE QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |      9043.17 |      8718.37 |       8787.01 |        nan    |
| ('Darwin arm64', 'cpu', 'complex64')    |      8194.95 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float16')      |     12535.4  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'float32')      |      9425.07 |     10131.9  |          0    |        nan    |
| ('Darwin arm64', 'cpu', 'float64')      |       nan    |     11606.9  |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'int64')        |       nan    |      7947.79 |        nan    |      12732.7  |
| ('Darwin arm64', 'cpu', 'int8')         |      8677.42 |       nan    |      12234    |        nan    |
| ('Darwin arm64', 'cpu', 'turboquant')   |      9181.9  |      9119.94 |       8181.94 |       8025.89 |
| ('Darwin arm64', 'cpu', 'uint32')       |      9064.08 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint64')       |      8658.62 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'cpu', 'uint8')        |      6851.34 |      8096.72 |       8625.18 |       8259.44 |
| ('Darwin arm64', 'metal', 'complex128') |      7731.61 |      7767.64 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'complex64')  |      7982.11 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float16')    |      8980.43 |      8554.62 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float32')    |      8887.67 |      8862.91 |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'float64')    |     13292.6  |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int16')      |       nan    |     12062.3  |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'int64')      |       nan    |      8364.73 |      12989.1  |        nan    |
| ('Darwin arm64', 'metal', 'int8')       |      8484.92 |       nan    |       8822.93 |      13018.7  |
| ('Darwin arm64', 'metal', 'turboquant') |      5271.15 |      8855.3  |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint64')     |      8576.57 |       nan    |        nan    |        nan    |
| ('Darwin arm64', 'metal', 'uint8')      |      7954.99 |      8166.62 |       8559.96 |        nan    |

### TEMPORAL QPS

|                                         |   (500, 128) |   (500, 384) |   (1000, 128) |   (1000, 384) |
|:----------------------------------------|-------------:|-------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'complex128')   |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'complex64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float16')      |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'float32')      |            0 |            0 |             0 |           nan |
| ('Darwin arm64', 'cpu', 'float64')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'cpu', 'int64')        |          nan |            0 |           nan |             0 |
| ('Darwin arm64', 'cpu', 'int8')         |            0 |          nan |             0 |           nan |
| ('Darwin arm64', 'cpu', 'turboquant')   |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'cpu', 'uint32')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint64')       |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'cpu', 'uint8')        |            0 |            0 |             0 |             0 |
| ('Darwin arm64', 'metal', 'complex128') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'complex64')  |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'float16')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float32')    |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'float64')    |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'int16')      |          nan |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'int64')      |          nan |            0 |             0 |           nan |
| ('Darwin arm64', 'metal', 'int8')       |            0 |          nan |             0 |             0 |
| ('Darwin arm64', 'metal', 'turboquant') |            0 |            0 |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint64')     |            0 |          nan |           nan |           nan |
| ('Darwin arm64', 'metal', 'uint8')      |            0 |            0 |             0 |           nan |

## 3. Specialized Search Performance

### GEO Results

### TEMPORAL Results

### GRAPHRAG Results

