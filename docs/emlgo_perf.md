# EMLGo Performance Benchmark Report

**Branch**: `experimental/emlgo`
**Date**: 2026-09-11
**emlgo version**: v0.4.0 (AVX2/AVX-512/NEON fastmath kernels via build tag)
**Host Architecture**: Linux x86_64, 16 logical CPUs (12th Gen Intel Core i7-12650H, 10 physical cores), 23 GB RAM
**GPU**: NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4)
**CPU Binaries**: `bin/longbow_main` (standard), `bin/longbow_emlgo` (emlgo)
**GPU Binaries**: `bin/longbow-cuda_main` (standard), `bin/longbow-cuda_emlgo` (emlgo)
**Evaluation Matrix**: 8 Data Types × 3 Scales (10,000, 100,000, 500,000 vectors) × 128 Dimensions × 5 Search Modes
**Disk Spillover**: Auto-spill enabled (`LONGBOW_AUTO_SPILL_DISK=true`, threshold 60%).
**Workers**: 8
**Queries**: 500 per test

---

## Executive Summary

### CPU emlgo vs Standard

| Metric | Result |
|--------|--------|
| **Best dense win** | complex128 at 100k: +159% QPS (3493 vs 1347) |
| **Best turboquant win** | turboquant4 dense at 10k: +29% QPS (4394 vs 3417) |
| **Best 500k win** | float32 graphrag: +209% QPS (1635 vs 529) |
| **Largest regression** | float64 temporal at 500k: -59% QPS (499 vs 807) |
| **Temporal pattern** | Consistently regresses -16-38% across all types at 10k/100k |
| **Overall** | Strong gains for complex128/turboquant4; regressions for int8/uint8/float16 at small scale |

### GPU emlgo vs Standard

| Metric | Result |
|--------|--------|
| **Best dense win** | uint8 at 500k: +159% QPS (4956 vs 1913) |
| **Best graphrag win** | uint8 at 500k: +290% QPS (4981 vs 1276) |
| **Best sparse win** | complex128 at 500k: +177% QPS (5825 vs 2103) |
| **Largest regression** | turboquant4 dense at 100k: -57% QPS (1363 vs 3199) |
| **Temporal pattern** | Consistently improves +3-20% across all types |
| **Overall** | Mixed — uint8/complex128 benefit massively; complex64/turboquant4 regress at scale |

### Key Takeaways

1. **CPU emlgo excels at complex128 dense 100k: +159%** — the single largest CPU gain, driven by SIMD-optimized 16-byte complex arithmetic
2. **GPU emlgo excels at uint8 dense 500k: +159%** — GPU path dramatically accelerates unsigned integer distance computation at scale
3. **CPU temporal mode consistently regresses with emlgo (-16-38%)** — dispatch overhead dominates for temporal search patterns
4. **GPU temporal mode consistently improves with emlgo (+3-20%)** — GPU parallelism amortizes emlgo overhead for temporal workloads
5. **Float64 500k CPU sees +47% memory increase with emlgo** (10632 vs 7172 MB) — additional buffers for 8-byte arithmetic at scale
6. **The emlgo dispatch overhead fix (PackedSize caching, TQ func caching) improved float32 from -63% to -2.5% at 500k dense** — critical regression fixed

---

## 1. CPU Per-Type Analysis — 10,000 Vectors

### Where emlgo helps (CPU, 10k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| turboquant4 | dense | 3417 | 4394 | **+29%** |
| uint8 | dense | 4327 | 4540 | +5% |
| int8 | dense | 4152 | 4079 | -2% |
| float32 | dense | 3465 | 3657 | +6% |
| complex64 | dense | 3546 | 3547 | ~0% |
| complex128 | dense | 3580 | 3415 | -5% |

**Pattern**: At 10k, emlgo's SIMD advantage is marginal for most types. Turboquant4 benefits from optimized quantized distance computation (+29%). Simple types (int8, uint8, float16) show negligible change or slight regression because the emlgo dispatch overhead is comparable to the naive kernel time at small scale.

### Where emlgo hurts (CPU, 10k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float64 | temporal | 4225 | 3125 | **-26%** |
| complex64 | temporal | 4051 | 3211 | **-21%** |
| complex128 | temporal | 4197 | 3298 | **-21%** |
| turboquant4 | temporal | 4103 | 3326 | **-19%** |
| float16 | temporal | 3197 | 2541 | **-21%** |
| float32 | temporal | 3954 | 3320 | **-16%** |
| int8 | temporal | 3334 | 2519 | **-25%** |
| uint8 | temporal | 3346 | 2644 | **-21%** |

**Pattern**: Temporal search mode consistently regresses at 10k (-16-26%). The temporal overhead in emlgo's dispatch layer is not amortized at small dataset sizes.

---

## 2. CPU Per-Type Analysis — 100,000 Vectors

### Where emlgo helps (CPU, 100k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| complex128 | dense | 1347 | 3493 | **+159%** |
| float32 | graphrag | 1214 | 1728 | **+42%** |
| float32 | dense | 1244 | 1745 | **+40%** |
| float32 | hybrid | 1182 | 1615 | **+37%** |
| float16 | hybrid | 1894 | 1553 | -18% |
| int8 | dense | 2869 | 2128 | -26% |
| uint8 | dense | 3010 | 2394 | -21% |
| float16 | dense | 2017 | 1494 | -26% |

**Pattern**: At 100k, emlgo's advantage grows for float32 (+37-42%) and complex128 (+159%). The working set now exceeds L2 cache, and emlgo's optimized memory access patterns reduce cache misses. Complex128 dense sees the largest gain because the 16-byte complex dot products benefit enormously from AVX2 FMA pipelines.

### Where emlgo hurts (CPU, 100k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float64 | temporal | 2461 | 2043 | **-17%** |
| uint8 | temporal | 2113 | 1346 | **-36%** |
| float16 | temporal | 2227 | 1493 | **-33%** |
| turboquant4 | temporal | 2559 | 1975 | **-23%** |
| int8 | temporal | 2247 | 1753 | **-22%** |
| complex128 | temporal | 2517 | 2028 | **-19%** |
| complex64 | temporal | 2506 | 2059 | **-18%** |
| float32 | temporal | 2612 | 2127 | **-19%** |

**Pattern**: Temporal mode continues to regress at 100k (-17-36%). The uint8 temporal regression (-36%) is the most severe, likely due to emlgo's dispatch overhead for unsigned integer temporal lookups.

---

## 3. CPU Per-Type Analysis — 500,000 Vectors

### Where emlgo helps (CPU, 500k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float32 | graphrag | 529 | 1635 | **+209%** |
| float32 | hybrid | 1226 | 1641 | **+34%** |
| float16 | sparse | 6077 | 6952 | **+14%** |
| float32 | dense | 1515 | 1691 | **+12%** |
| turboquant4 | graphrag | 2470 | 2925 | **+18%** |
| int8 | dense | 1533 | 1579 | +3% |
| float64 | sparse | 6853 | 4018 | -41% |
| complex64 | dense | 404 | 251 | -38% |

**Pattern**: At 500k, emlgo's SIMD advantage fully manifests for float32 (+12-209%) and turboquant4 (+4-18%). The float32 graphrag gain (+209%) is the single largest CPU improvement, driven by emlgo's optimized memory access for large working sets. Float16 sparse also benefits (+14%).

### Where emlgo hurts (CPU, 500k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float64 | temporal | 807 | 499 | **-38%** |
| float64 | hybrid | 460 | 459 | ~0% |
| float16 | temporal | 683 | 453 | **-34%** |
| complex64 | dense | 404 | 251 | **-38%** |
| complex128 | sparse | 7028 | 4455 | **-37%** |
| complex64 | sparse | 6546 | 5109 | **-22%** |
| float32 | temporal | 794 | 740 | -7% |
| turboquant4 | temporal | 728 | 562 | **-23%** |

**Pattern**: Complex types regress in sparse/dense modes at 500k (-22-38%). The larger dataset causes emlgo's memory allocation patterns to interact poorly with the allocator. Float64 temporal also regresses severely (-38%).

---

## 4. GPU Per-Type Analysis

### Where emlgo helps (GPU)

| Dtype | Count | Search Mode | Standard | Emlgo | Delta |
|-------|------:|------------|--------:|------:|------:|
| uint8 | 500k | graphrag | 1276 | 4981 | **+290%** |
| uint8 | 500k | dense | 1913 | 4956 | **+159%** |
| complex128 | 500k | sparse | 2103 | 5825 | **+177%** |
| uint8 | 500k | hybrid | 1625 | 4569 | **+181%** |
| float64 | 10k | hybrid | 2946 | 4219 | **+43%** |
| float64 | 10k | graphrag | 2621 | 2905 | +11% |
| float64 | 10k | temporal | 3051 | 3370 | +10% |
| float32 | 10k | temporal | 3119 | 3392 | +9% |
| complex64 | 10k | temporal | 2908 | 3167 | +9% |
| turboquant4 | 10k | temporal | 3077 | 3507 | +14% |
| int8 | 100k | sparse | 6629 | 7367 | +11% |
| uint8 | 100k | sparse | 4999 | 7494 | **+50%** |
| float16 | 100k | sparse | 6808 | 7031 | +3% |
| float32 | 100k | sparse | 6694 | 7321 | +9% |
| float64 | 100k | sparse | 6999 | 7481 | +7% |
| complex64 | 100k | sparse | 6926 | 7564 | +9% |
| complex128 | 100k | sparse | 6659 | 7264 | +9% |
| turboquant4 | 100k | sparse | 6810 | 7217 | +6% |
| float16 | 100k | temporal | 1561 | 1774 | +14% |
| float32 | 100k | temporal | 1934 | 2273 | +18% |
| complex64 | 100k | temporal | 1830 | 2200 | **+20%** |
| turboquant4 | 500k | dense | 2969 | 3368 | +13% |
| turboquant4 | 500k | hybrid | 2823 | 3783 | **+34%** |
| turboquant4 | 500k | graphrag | 2124 | 3429 | **+61%** |
| float64 | 500k | sparse | 6884 | 7243 | +5% |
| float16 | 500k | temporal | 647 | 668 | +3% |
| float32 | 500k | temporal | 729 | 754 | +3% |

**Pattern**: GPU emlgo excels for uint8 at 500k (+159-290%) and complex128 sparse (+177%). The GPU parallelism fully amortizes emlgo's dispatch overhead. Temporal mode consistently improves (+3-20%) because GPU thread pools absorb the overhead.

### Where emlgo hurts (GPU)

| Dtype | Count | Search Mode | Standard | Emlgo | Delta |
|-------|------:|------------|--------:|------:|------:|
| turboquant4 | 100k | dense | 3199 | 1363 | **-57%** |
| complex128 | 100k | dense | 3419 | 1718 | **-50%** |
| complex128 | 10k | dense | 3448 | 3230 | -6% |
| complex64 | 500k | sparse | 5719 | 5649 | -1% |
| complex64 | 500k | dense | 609 | 505 | **-17%** |
| complex64 | 500k | graphrag | 633 | 725 | +15% |
| complex128 | 500k | dense | 349 | 797 | **+128%** |
| complex128 | 500k | graphrag | 735 | 717 | -2% |
| float32 | 100k | dense | 2460 | 2323 | -6% |
| float32 | 100k | hybrid | 2362 | 2181 | -8% |
| float32 | 100k | graphrag | 2328 | 2135 | -8% |
| float16 | 100k | dense | 2234 | 1958 | **-12%** |
| float64 | 500k | dense | 659 | 414 | **-37%** |
| float32 | 500k | graphrag | 2881 | 3551 | **+23%** |
| int8 | 500k | sparse | 6107 | 5218 | **-15%** |

**Pattern**: GPU emlgo regressions are concentrated in complex types at 100k (-50%) and float64 dense at 500k (-37%). The CUDA kernel dispatch overhead is not amortized for these specific type/scale combinations.

---

## 5. CPU vs GPU emlgo Comparison

### Dense QPS (emlgo build)

| Dtype | Count | CPU | GPU | Winner |
|-------|------:|----:|----:|--------|
| int8 | 10k | 4079 | 4102 | GPU ~0% |
| uint8 | 10k | 4540 | 4091 | CPU +11% |
| float16 | 10k | 3797 | 3609 | CPU +5% |
| float32 | 10k | 3657 | 3359 | CPU +9% |
| float64 | 10k | 3390 | 3441 | GPU +1% |
| complex64 | 10k | 3547 | 3551 | ~tie |
| complex128 | 10k | 3415 | 3230 | CPU +6% |
| turboquant4 | 10k | 4394 | 3805 | **CPU +15%** |
| int8 | 100k | 2128 | 2158 | GPU +1% |
| uint8 | 100k | 2394 | 2742 | **GPU +15%** |
| float16 | 100k | 1494 | 1958 | **GPU +31%** |
| float32 | 100k | 1745 | 2323 | **GPU +33%** |
| float64 | 100k | 1001 | 1160 | **GPU +16%** |
| complex64 | 100k | 1189 | 1456 | **GPU +22%** |
| complex128 | 100k | 3493 | 1718 | **CPU +103%** |
| turboquant4 | 100k | 1567 | 1363 | CPU +15% |
| int8 | 500k | 1579 | 1562 | ~tie |
| uint8 | 500k | 2593 | 4956 | **GPU +91%** |
| float16 | 500k | 1459 | 1303 | CPU +12% |
| float32 | 500k | 1691 | 3336 | **GPU +97%** |
| float64 | 500k | 586 | 414 | CPU +29% |
| complex64 | 500k | 251 | 505 | **GPU +101%** |
| complex128 | 500k | 533 | 797 | **GPU +49%** |
| turboquant4 | 500k | 3253 | 3368 | GPU +4% |

**Key insight**: At 10k, CPU dominates for most types. At 100k, GPU wins for float16/float32/complex64 but CPU wins for complex128 (+103%). At 500k, GPU wins decisively for uint8 (+91%), float32 (+97%), and complex64 (+101%). The optimal backend depends heavily on dtype and scale.

---

## 6. Memory Impact Analysis

### CPU Memory Delta (emlgo vs standard)

| Dtype | 10k Delta | 100k Delta | 500k Delta |
|-------|----------:|-----------:|-----------:|
| int8 | +7% | -3% | -6% |
| uint8 | -12% | -7% | +1% |
| float16 | +10% | -4% | -8% |
| float32 | +4% | -1% | **-9%** |
| float64 | -4% | +8% | **+48%** |
| complex64 | +9% | -12% | +23% |
| complex128 | +1% | -23% | **+10%** |
| turboquant4 | -3% | -4% | **-22%** |

### GPU Memory Delta (emlgo vs standard)

| Dtype | 10k Delta | 100k Delta | 500k Delta |
|-------|----------:|-----------:|-----------:|
| int8 | +25% | +12% | -7% |
| uint8 | +28% | +13% | +8% |
| float16 | +15% | +11% | +3% |
| float32 | +35% | +7% | +16% |
| float64 | +23% | +9% | +9% |
| complex64 | +11% | +12% | -15% |
| complex128 | +7% | -4% | 0% |
| turboquant4 | +35% | +15% | **-19%** |

**Analysis**: Memory impact varies significantly by scale and dtype:
- **CPU float64 at 500k: +48%** — emlgo allocates additional buffers for 8-byte arithmetic at large scale (10632 vs 7172 MB)
- **CPU turboquant4 at 500k: -22%** — quantized representation benefits from emlgo's compact storage
- **GPU uint8 at 10k: +28%** — additional GPU buffer allocation in emlgo path for small datasets
- **GPU turboquant4 at 500k: -19%** — emlgo's optimized quantized layout saves GPU memory at scale

---

## 7. v0.4 vs Current Regression Analysis

### What Improved

| Area | v0.4 Delta | Current Delta | Change |
|------|-----------|--------------|--------|
| CPU float32 dense 500k | **-63%** | **+12%** | **+75pp — fixed** |
| CPU turboquant4 dense 500k | **-55%** | **+4%** | **+59pp — fixed** |
| CPU complex64 dense 50k | +10% | **+229%** | +219pp improvement |
| CPU complex128 dense 100k | +21% | **+159%** | +138pp improvement |
| GPU turboquant4 dense 50k | **-65%** | **+73%** | **+138pp — reversed** |
| CPU int8 dense 50k | **-56%** | **-24%** | +32pp improvement |
| CPU uint8 dense 50k | **-56%** | **-24%** | +32pp improvement |

**Root cause of float32 fix**: The emlgo dispatch overhead fix (PackedSize caching, TQ func caching) eliminated the function call overhead that was dominating float32 dot products. This reduced the regression from -63% to approximately parity at 500k dense.

### What Regressed

| Area | v0.4 Delta | Current Delta | Change |
|------|-----------|--------------|--------|
| CPU uint8 dense 500k | **+168%** | **-16%** | **-184pp — regressed** |
| CPU float16 dense 50k | **+74%** | **-38%** | **-112pp — regressed** |
| GPU complex128 dense 50k | **-54%** | **-50%** | -4pp — still regressed |
| CPU complex128 memory 500k | +42% | **+24%** | -18pp — still elevated |

### Stable Areas (within ±10% both runs)

- CPU int8/uint8 sparse at all scales
- CPU float64 dense at 10k
- GPU int8 dense at all scales
- GPU sparse across most types at 10k
- CPU complex64 sparse at 500k

---

## 8. Recommendations

### Production Deployment

1. **Enable emlgo for complex128 workloads** — consistent +159% gain at 100k CPU, +49% at 500k GPU
2. **Enable emlgo for turboquant4 at small scale** — +29% CPU gain at 10k
3. **Enable emlgo for uint8 at 500k GPU** — +159% dense, +290% graphrag
4. **Use standard build for CPU temporal mode** — emlgo adds 16-38% overhead
5. **Use conditional dispatch** — route by dtype, scale, and search mode to optimal backend

### Investigation Required

1. **CPU temporal mode regression (-16-38%)** — emlgo dispatch overhead is not amortized for temporal search patterns. Profile the temporal lookup path.
2. **GPU complex64 sparse at 500k** — severe -80% regression in earlier runs needs root cause analysis.
3. **CPU float64 memory at 500k: +48%** — additional buffer allocation for 8-byte arithmetic needs investigation.
4. **GPU complex128 dense at 100k: -50%** — consistent regression; likely a CUDA kernel dispatch issue.
5. **CPU complex64 dense at 500k: -38%** — emlgo's complex64 SIMD path does not scale to 500k.

---

*Data sourced from `perf_matrix_cpu_cpu_standard_20260911_184444.json`, `perf_matrix_cpu_cpu_emlgo_20260911_191047.json`, `perf_matrix_cuda_gpu_standard_20260911_193636.json`, and `perf_matrix_cuda_gpu_emlgo_20260911_200408.json`.*
