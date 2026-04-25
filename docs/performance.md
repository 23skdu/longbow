# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-25 08:44:54

## Executive Summary

Benchmarks are still in progress. The following data represents partial results collected so far.

## 1. Ingest Performance (vec/s)

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |     314994   |
| ('Darwin arm64', 'metal', 'float64') |     247377   |
| ('Linux x86_64', 'cpu', 'float32')   |      54073.5 |
| ('Linux x86_64', 'cpu', 'float64')   |     199130   |

## 2. Standard Search Performance (QPS)

### BYID QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5644.67 |
| ('Darwin arm64', 'metal', 'float64') |      7138.94 |
| ('Linux x86_64', 'cpu', 'float32')   |      3434.2  |
| ('Linux x86_64', 'cpu', 'float64')   |      3287.83 |

### DENSE QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      4792.24 |
| ('Darwin arm64', 'metal', 'float64') |      5835.64 |
| ('Linux x86_64', 'cpu', 'float32')   |      2480.74 |
| ('Linux x86_64', 'cpu', 'float64')   |      2798.79 |

### FILTERED QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5024.23 |
| ('Darwin arm64', 'metal', 'float64') |      6060.92 |
| ('Linux x86_64', 'cpu', 'float32')   |      2713.87 |
| ('Linux x86_64', 'cpu', 'float64')   |      3129.04 |

### FILTEREDBOOL QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5055.23 |
| ('Darwin arm64', 'metal', 'float64') |      6080.78 |
| ('Linux x86_64', 'cpu', 'float32')   |      2664.68 |
| ('Linux x86_64', 'cpu', 'float64')   |      2922.81 |

### FILTEREDSTRING QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5048.46 |
| ('Darwin arm64', 'metal', 'float64') |      6118.2  |
| ('Linux x86_64', 'cpu', 'float32')   |      2658.61 |
| ('Linux x86_64', 'cpu', 'float64')   |      3181.14 |

### GEO QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      6109.29 |
| ('Darwin arm64', 'metal', 'float64') |      6081.84 |
| ('Linux x86_64', 'cpu', 'float32')   |      2931.67 |
| ('Linux x86_64', 'cpu', 'float64')   |      2936.39 |

### GRAPHRAG QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      4882.18 |
| ('Darwin arm64', 'metal', 'float64') |      6109.44 |
| ('Linux x86_64', 'cpu', 'float32')   |      2911.13 |
| ('Linux x86_64', 'cpu', 'float64')   |      2989.9  |

### HYBRID QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      4541.15 |
| ('Darwin arm64', 'metal', 'float64') |      5342.59 |
| ('Linux x86_64', 'cpu', 'float32')   |      2468.58 |
| ('Linux x86_64', 'cpu', 'float64')   |      2751.66 |

### RECOMMEND QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5472.01 |
| ('Darwin arm64', 'metal', 'float64') |      7199.25 |
| ('Linux x86_64', 'cpu', 'float32')   |      2788.22 |
| ('Linux x86_64', 'cpu', 'float64')   |      3366.53 |

### SPARSE QPS

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |     14033.1  |
| ('Darwin arm64', 'metal', 'float64') |     13925.7  |
| ('Linux x86_64', 'cpu', 'float32')   |      6742.58 |
| ('Linux x86_64', 'cpu', 'float64')   |      6701.87 |

## 3. Specialized Search Performance

### GEO Results

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      6109.29 |
| ('Darwin arm64', 'metal', 'float64') |      6081.84 |
| ('Linux x86_64', 'cpu', 'float32')   |      2931.67 |
| ('Linux x86_64', 'cpu', 'float64')   |      2936.39 |

### GRAPHRAG Results

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      4882.18 |
| ('Darwin arm64', 'metal', 'float64') |      6109.44 |
| ('Linux x86_64', 'cpu', 'float32')   |      2911.13 |
| ('Linux x86_64', 'cpu', 'float64')   |      2989.9  |

### RECOMMEND Results

|                                      |   (500, 128) |
|:-------------------------------------|-------------:|
| ('Darwin arm64', 'metal', 'float32') |      5472.01 |
| ('Darwin arm64', 'metal', 'float64') |      7199.25 |
| ('Linux x86_64', 'cpu', 'float32')   |      2788.22 |
| ('Linux x86_64', 'cpu', 'float64')   |      3366.53 |

