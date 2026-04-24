# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-24 12:51:35

## Executive Summary

Benchmarks are still in progress. The following data represents partial results collected so far.

## 1. Ingest Performance (vec/s)

|                                         |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:----------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')      |        420800 |        409099 |        280095 |
| ('Darwin arm64', 'cpu', 'turboquant')   |        550513 |        413194 |        272386 |
| ('Darwin arm64', 'metal', 'float32')    |        606848 |        422177 |        252361 |
| ('Darwin arm64', 'metal', 'turboquant') |        598743 |        425973 |        269315 |

## 2. Standard Search Performance (QPS)

### BYID QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       4604.43 |       3639.69 |       2406.83 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       7723.8  |       6305.17 |       5832.69 |
| ('Darwin arm64', 'geo', 'float32')         |       4603.02 |       3674.78 |       2437.48 |
| ('Darwin arm64', 'geo', 'turboquant')      |       7787.15 |       6379.1  |       5966.64 |
| ('Darwin arm64', 'graphrag', 'float32')    |       4298.29 |       3441.7  |       2418.23 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       7524.93 |       6238    |       6198.36 |
| ('Darwin arm64', 'metal', 'float32')       |       4640.49 |       3681.12 |       2404.99 |
| ('Darwin arm64', 'metal', 'turboquant')    |       7957.8  |       6343.96 |       6056.34 |
| ('Darwin arm64', 'temporal', 'float32')    |       4555.02 |       3597.45 |       2417.95 |
| ('Darwin arm64', 'temporal', 'turboquant') |       7255.6  |       6412.04 |       6064.94 |

### DENSE QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       3448.42 |       2570.91 |       1739.83 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6299.37 |       4490.95 |       3235.26 |
| ('Darwin arm64', 'geo', 'float32')         |       3493.72 |       2580.61 |       1766.66 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6445.51 |       4566    |       3287.14 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3434.69 |       2485.54 |       1760.98 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6308    |       4534.64 |       3451.43 |
| ('Darwin arm64', 'metal', 'float32')       |       3267.92 |       2532.85 |       1756.58 |
| ('Darwin arm64', 'metal', 'turboquant')    |       6562.05 |       4531.29 |       3416.95 |
| ('Darwin arm64', 'temporal', 'float32')    |       3468.93 |       2636.13 |       1773.27 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6184.56 |       4656.14 |       3387.34 |

### FILTERED QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       3483.48 |       2586.98 |       1743.92 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6617.87 |       4699.89 |       3287.51 |
| ('Darwin arm64', 'geo', 'float32')         |       3507.81 |       2632.82 |       1769.52 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6668.28 |       4759.17 |       3307.71 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3421.51 |       2584.47 |       1761.6  |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6436.46 |       4683.51 |       3284.27 |
| ('Darwin arm64', 'metal', 'float32')       |       3482.85 |       2594.17 |       1763.1  |
| ('Darwin arm64', 'metal', 'turboquant')    |       6847.84 |       4892.29 |       3454.93 |
| ('Darwin arm64', 'temporal', 'float32')    |       3500.78 |       2633.23 |       1766.78 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6518.21 |       4747.42 |       3393.35 |

### FILTEREDBOOL QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       3497.97 |       2584.34 |       1744.07 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6620.63 |       4729.89 |       3337.72 |
| ('Darwin arm64', 'geo', 'float32')         |       3512.39 |       2629.09 |       1771.02 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6713.24 |       4752.96 |       3309.79 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3345.95 |       2576.49 |       1755.59 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6616.17 |       4688.05 |       3571.8  |
| ('Darwin arm64', 'metal', 'float32')       |       3498.65 |       2597.69 |       1732.44 |
| ('Darwin arm64', 'metal', 'turboquant')    |       6432.92 |       4896.38 |       3511.64 |
| ('Darwin arm64', 'temporal', 'float32')    |       3494.6  |       2632.07 |       1766.51 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6509.65 |       4751.83 |       3513.87 |

### FILTEREDSTRING QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       3501.91 |       2584.27 |       1721.39 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6652.71 |       4689.11 |       3348.9  |
| ('Darwin arm64', 'geo', 'float32')         |       3519.25 |       2613.02 |       1770.72 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6725.65 |       4755.84 |       3317.56 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3400.52 |       2513.76 |       1757.53 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6569.6  |       4622.55 |       3619    |
| ('Darwin arm64', 'metal', 'float32')       |       3519.89 |       2586.77 |       1771.94 |
| ('Darwin arm64', 'metal', 'turboquant')    |       6828.42 |       4796.98 |       3543.21 |
| ('Darwin arm64', 'temporal', 'float32')    |       3479.97 |       2645.04 |       1764.61 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6382.48 |       4651.03 |       3609.59 |

### GRAPHRAG QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       4000.25 |       2715.64 |       1735.27 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6552.71 |       4751.68 |       3704.33 |
| ('Darwin arm64', 'geo', 'float32')         |       4033.23 |       2852.44 |       1768.53 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6731.63 |       4780.06 |       3763.81 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3657.63 |       2742.8  |       1747.22 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6397.56 |       4677.96 |       3811.86 |
| ('Darwin arm64', 'metal', 'float32')       |       4015.92 |       2764.98 |       1762.07 |
| ('Darwin arm64', 'metal', 'turboquant')    |       6828.5  |       4813.82 |       3866.1  |
| ('Darwin arm64', 'temporal', 'float32')    |       3753.09 |       2834.77 |       1766.67 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6204.61 |       4809.72 |       3745.99 |

### HYBRID QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       3217.23 |       2390.79 |       1656.48 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       5743.37 |       4229.8  |       3001.13 |
| ('Darwin arm64', 'geo', 'float32')         |       3241.51 |       2459.44 |       1689.21 |
| ('Darwin arm64', 'geo', 'turboquant')      |       5858.61 |       4250.22 |       3045.03 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3174.14 |       2417.94 |       1682.06 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       5619.93 |       4190.06 |       3137.16 |
| ('Darwin arm64', 'metal', 'float32')       |       3067.7  |       2424.97 |       1671.84 |
| ('Darwin arm64', 'metal', 'turboquant')    |       5870.16 |       4361.81 |       3142.38 |
| ('Darwin arm64', 'temporal', 'float32')    |       3217.12 |       2441.82 |       1663.7  |
| ('Darwin arm64', 'temporal', 'turboquant') |       5665.95 |       4275.19 |       3104.45 |

### RECOMMEND QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       4361.51 |       3058.82 |       2122.66 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       4843.2  |       3729.23 |       3440.9  |
| ('Darwin arm64', 'geo', 'float32')         |       4377.9  |       2967.47 |       2087.5  |
| ('Darwin arm64', 'geo', 'turboquant')      |       4932.03 |       3805.19 |       3371.87 |
| ('Darwin arm64', 'graphrag', 'float32')    |       4380.47 |       2986.97 |       2172.18 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       4635.22 |       3711.7  |       3547.61 |
| ('Darwin arm64', 'metal', 'float32')       |       4403.41 |       3021.17 |       2106.02 |
| ('Darwin arm64', 'metal', 'turboquant')    |       4996.32 |       3831.69 |       3519.43 |
| ('Darwin arm64', 'temporal', 'float32')    |       4386.1  |       3066.27 |       2118.36 |
| ('Darwin arm64', 'temporal', 'turboquant') |       4796.01 |       3735.42 |       3563.26 |

### SPARSE QPS

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       13088.3 |       12079.7 |       11535.3 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       14021.9 |       13493.5 |       11546.2 |
| ('Darwin arm64', 'geo', 'float32')         |       13349.8 |       11855   |       11898.8 |
| ('Darwin arm64', 'geo', 'turboquant')      |       14096.9 |       13692.9 |       11960.5 |
| ('Darwin arm64', 'graphrag', 'float32')    |       11475.5 |       12358.1 |       11838.6 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       13621.5 |       13571.4 |       12483   |
| ('Darwin arm64', 'metal', 'float32')       |       13543.7 |       12307.3 |       11871.1 |
| ('Darwin arm64', 'metal', 'turboquant')    |       13977.1 |       13136.1 |       12631.4 |
| ('Darwin arm64', 'temporal', 'float32')    |       12862.2 |       12310.6 |       11711.1 |
| ('Darwin arm64', 'temporal', 'turboquant') |       13511.6 |       13301.8 |       12334.8 |

## 3. Specialized Search Performance

### GRAPHRAG Results

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       4000.25 |       2715.64 |       1735.27 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       6552.71 |       4751.68 |       3704.33 |
| ('Darwin arm64', 'geo', 'float32')         |       4033.23 |       2852.44 |       1768.53 |
| ('Darwin arm64', 'geo', 'turboquant')      |       6731.63 |       4780.06 |       3763.81 |
| ('Darwin arm64', 'graphrag', 'float32')    |       3657.63 |       2742.8  |       1747.22 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       6397.56 |       4677.96 |       3811.86 |
| ('Darwin arm64', 'metal', 'float32')       |       4015.92 |       2764.98 |       1762.07 |
| ('Darwin arm64', 'metal', 'turboquant')    |       6828.5  |       4813.82 |       3866.1  |
| ('Darwin arm64', 'temporal', 'float32')    |       3753.09 |       2834.77 |       1766.67 |
| ('Darwin arm64', 'temporal', 'turboquant') |       6204.61 |       4809.72 |       3745.99 |

### RECOMMEND Results

|                                            |   (1000, 128) |   (1000, 384) |   (1000, 768) |
|:-------------------------------------------|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float32')         |       4361.51 |       3058.82 |       2122.66 |
| ('Darwin arm64', 'cpu', 'turboquant')      |       4843.2  |       3729.23 |       3440.9  |
| ('Darwin arm64', 'geo', 'float32')         |       4377.9  |       2967.47 |       2087.5  |
| ('Darwin arm64', 'geo', 'turboquant')      |       4932.03 |       3805.19 |       3371.87 |
| ('Darwin arm64', 'graphrag', 'float32')    |       4380.47 |       2986.97 |       2172.18 |
| ('Darwin arm64', 'graphrag', 'turboquant') |       4635.22 |       3711.7  |       3547.61 |
| ('Darwin arm64', 'metal', 'float32')       |       4403.41 |       3021.17 |       2106.02 |
| ('Darwin arm64', 'metal', 'turboquant')    |       4996.32 |       3831.69 |       3519.43 |
| ('Darwin arm64', 'temporal', 'float32')    |       4386.1  |       3066.27 |       2118.36 |
| ('Darwin arm64', 'temporal', 'turboquant') |       4796.01 |       3735.42 |       3563.26 |

