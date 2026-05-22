# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-25 09:50:20

## Executive Summary

Benchmarks are still in progress. The following data represents partial results collected so far.

## 1. Ingest Performance (vec/s)

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |       442903 |          nan |
| ('Darwin arm64', 'cpu', 'float32') |       382881 |       240404 |
| ('Darwin arm64', 'cpu', 'float64') |       299631 |       229529 |

## 2. Standard Search Performance (QPS)

### BYID QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      9052.98 |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      5823.75 |      5039.09 |
| ('Darwin arm64', 'cpu', 'float64') |      8367.93 |      7298.86 |

### DENSE QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6125.6  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4661.44 |      2942.51 |
| ('Darwin arm64', 'cpu', 'float64') |      5878.78 |      4290.34 |

### FILTERED QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6466.5  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4748.77 |      3072.03 |
| ('Darwin arm64', 'cpu', 'float64') |      6116.07 |      4067.33 |

### FILTEREDBOOL QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6432.92 |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4762.17 |      3026.41 |
| ('Darwin arm64', 'cpu', 'float64') |      6036.36 |      3449.14 |

### FILTEREDSTRING QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6414.99 |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4783.21 |      2976.11 |
| ('Darwin arm64', 'cpu', 'float64') |      5994.7  |      4291.47 |

### GEO QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      5977.8  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      6001.67 |      5937.13 |
| ('Darwin arm64', 'cpu', 'float64') |      5682.31 |      5709.89 |

### GRAPHRAG QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6423.1  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4891.59 |      3574.08 |
| ('Darwin arm64', 'cpu', 'float64') |      5954.45 |      4266.64 |

### HYBRID QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      5379.25 |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4271.99 |      2873.17 |
| ('Darwin arm64', 'cpu', 'float64') |      5283.29 |      3879.46 |

### RECOMMEND QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float32') |      5690.37 |      4852.13 |
| ('Darwin arm64', 'cpu', 'float64') |      6972.16 |      5895.02 |

### SPARSE QPS

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      13615.3 |        nan   |
| ('Darwin arm64', 'cpu', 'float32') |      14006.4 |      12277.3 |
| ('Darwin arm64', 'cpu', 'float64') |      12934.2 |      12967.7 |

## 3. Specialized Search Performance

### GEO Results

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      5977.8  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      6001.67 |      5937.13 |
| ('Darwin arm64', 'cpu', 'float64') |      5682.31 |      5709.89 |

### GRAPHRAG Results

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float16') |      6423.1  |       nan    |
| ('Darwin arm64', 'cpu', 'float32') |      4891.59 |      3574.08 |
| ('Darwin arm64', 'cpu', 'float64') |      5954.45 |      4266.64 |

### RECOMMEND Results

|                                    |   (500, 128) |   (500, 384) |
|:-----------------------------------|-------------:|-------------:|
| ('Darwin arm64', 'cpu', 'float32') |      5690.37 |      4852.13 |
| ('Darwin arm64', 'cpu', 'float64') |      6972.16 |      5895.02 |

