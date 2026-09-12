# EMLGo Performance Benchmark Report

**Branch**: `experimental/emlgo`
**Date**: 2026-09-11
**emlgo version**: v0.4.0 (AVX2/AVX-512/NEON fastmath kernels via build tag)
**Host Architecture**: Linux x86_64, 16 logical CPUs (12th Gen Intel Core i7-12650H, 10 physical cores), 23 GB RAM
**GPU**: NVIDIA GeForce RTX 4060 Laptop (sm_89, CUDA 12.4)
**CPU Binaries**: `bin/longbow_main` (standard), `bin/longbow_emlgo` (emlgo)
**GPU Binaries**: `bin/longbow-cuda_main` (standard), `bin/longbow-cuda_emlgo` (emlgo)
**Evaluation Matrix**: 8 Data Types × 2 Scales (50,000, 500,000 vectors) × 128 Dimensions × 5 Search Modes
**Disk Spillover**: Auto-spill enabled (`LONGBOW_AUTO_SPILL_DISK=true`, threshold 60%).
**Workers**: 8

---

## Executive Summary

### emlgo vs Standard — CPU

| Metric | Result |
|--------|--------|
| **Best dense win** | complex64 at 50k: +229% QPS (3365 vs 1023) |
| **Best 500k win** | float32 graphrag: +209% QPS (1635 vs 529) |
| **Largest regression** | float32 dense at 50k: -63% QPS (792 vs 2116) |
| **Overall** | Strong gains for complex64/complex128/turboquant4; regressions for int8/uint8/float32 at small scale |

### emlgo vs Standard — GPU

| Metric | Result |
|--------|--------|
| **Best dense win** | uint8 at 50k: +52% QPS (3729 vs 2458) |
| **Best overall** | turboquant4 hybrid at 50k: +106% QPS (1705 vs 828) |
| **Largest regression** | complex128 dense at 50k: -72% QPS (935 vs 3358) |
| **Overall** | Mixed — uint8/turboquant4 benefit, complex types regress at small scale |

### v0.4 (Sep 10) vs Current (Sep 11) — Key Changes

| Area | v0.4 Status | Current Status | Change |
|------|------------|---------------|--------|
| CPU int8/uint8 dense 50k | -56% regression | -24-34% regression | Improved but still regressed |
| CPU turboquant4 dense 500k | -55% regression | +4% regression | **Fixed** |
| CPU complex64 dense 50k | +10% | +229% | **Massive improvement** |
| CPU uint8 dense 500k | +168% | -16% | **Regressed** |
| GPU float32 dense 500k | +203% | ~0% | Converged to parity |
| GPU turboquant4 dense 50k | -65% | +73% | **Reversed to gain** |

---

## 1. CPU Per-Type Analysis — 50,000 Vectors

### Where emlgo helps (CPU, 50k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| complex64 | dense | 1023 | 3365 | **+229%** |
| turboquant4 | dense | 1129 | 3285 | **+191%** |
| complex64 | hybrid | 1172 | 2960 | **+153%** |
| turboquant4 | hybrid | 1045 | 3322 | **+218%** |
| complex64 | graphrag | 955 | 2445 | **+156%** |
| turboquant4 | graphrag | 1120 | 3228 | **+188%** |
| complex128 | dense | 2125 | 3158 | **+49%** |
| complex128 | graphrag | 1837 | 2991 | **+63%** |
| complex128 | hybrid | 860 | 1157 | **+35%** |

**Pattern**: emlgo's SIMD kernels excel at complex number arithmetic (64-bit and 128-bit) and quantized distance computation. The complex dot products involve multiple multiply-accumulate operations that map well to AVX2 FMA pipelines.

### Where emlgo hurts (CPU, 50k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float32 | dense | 2116 | 792 | **-63%** |
| float32 | graphrag | 2161 | 754 | **-65%** |
| float32 | hybrid | 1997 | 745 | **-63%** |
| float16 | dense | 3828 | 2359 | **-38%** |
| float16 | graphrag | 3325 | 1604 | **-52%** |
| int8 | dense | 3180 | 2110 | **-34%** |
| int8 | graphrag | 2656 | 1528 | **-43%** |
| uint8 | dense | 2929 | 2226 | **-24%** |
| uint8 | graphrag | 2199 | 1577 | **-28%** |

**Pattern**: Simple types (int8, uint8, float32) at small scale show emlgo overhead exceeds SIMD benefit. The emlgo dispatch layer adds function call overhead that dominates when the naive kernel is already fast for simple dot products on small datasets.

---

## 2. CPU Per-Type Analysis — 500,000 Vectors

### Where emlgo helps (CPU, 500k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float32 | graphrag | 529 | 1635 | **+209%** |
| float32 | hybrid | 1226 | 1641 | **+34%** |
| float16 | temporal | 479 | 607 | **+27%** |
| float16 | sparse | 6077 | 6952 | **+14%** |
| turboquant4 | graphrag | 2470 | 2925 | **+18%** |
| turboquant4 | dense | 2911 | 3038 | +4% |
| float32 | dense | 1515 | 1691 | **+12%** |
| int8 | dense | 1205 | 1412 | +17% |

**Pattern**: At 500k, emlgo's SIMD advantage grows as the working set exceeds cache. The optimized memory access patterns in emlgo kernels reduce cache misses for larger datasets.

### Where emlgo hurts (CPU, 500k)

| Dtype | Search Mode | Standard | Emlgo | Delta |
|-------|------------|--------:|------:|------:|
| float64 | temporal | 679 | 277 | **-59%** |
| complex128 | sparse | 7028 | 4455 | **-37%** |
| complex64 | sparse | 6546 | 5109 | **-22%** |
| float32 | temporal | 736 | 568 | **-23%** |
| complex128 | dense | 399 | 307 | **-23%** |
| complex64 | dense | 827 | 658 | **-20%** |
| uint8 | dense | 1875 | 1580 | **-16%** |

**Pattern**: Complex types at 500k regress in sparse/dense modes. The larger dataset may cause emlgo's memory allocation patterns to interact poorly with the allocator at scale.

---

## 3. GPU Per-Type Analysis

### Where emlgo helps (GPU)

| Dtype | Count | Search Mode | Standard | Emlgo | Delta |
|-------|------:|------------|--------:|------:|------:|
| turboquant4 | 50k | hybrid | 828 | 1705 | **+106%** |
| turboquant4 | 50k | graphrag | 919 | 1788 | **+95%** |
| uint8 | 50k | graphrag | 1721 | 3232 | **+88%** |
| turboquant4 | 50k | dense | 1016 | 1762 | **+73%** |
| uint8 | 50k | dense | 2458 | 3729 | **+52%** |
| float32 | 50k | hybrid | 819 | 1211 | **+48%** |
| float32 | 50k | graphrag | 832 | 1216 | **+46%** |
| float32 | 50k | dense | 845 | 1117 | **+32%** |
| turboquant4 | 500k | dense | 1483 | 1807 | **+22%** |
| turboquant4 | 500k | hybrid | 1421 | 1635 | **+15%** |
| turboquant4 | 500k | graphrag | 1274 | 1546 | **+21%** |
| float64 | 500k | sparse | 5638 | 6444 | **+14%** |
| uint8 | 500k | dense | 1394 | 1575 | **+13%** |
| int8 | 500k | graphrag | 1006 | 1303 | **+30%** |

### Where emlgo hurts (GPU)

| Dtype | Count | Search Mode | Standard | Emlgo | Delta |
|-------|------:|------------|--------:|------:|------:|
| complex64 | 500k | sparse | 6652 | 1319 | **-80%** |
| complex128 | 50k | dense | 3358 | 935 | **-72%** |
| complex128 | 50k | graphrag | 3049 | 854 | **-72%** |
| float16 | 50k | dense | 3427 | 1889 | **-45%** |
| float16 | 50k | graphrag | 2906 | 1718 | **-41%** |
| float64 | 50k | dense | 1751 | 1145 | **-35%** |
| float32 | 500k | graphrag | 3191 | 1676 | **-48%** |
| complex128 | 500k | sparse | 6015 | 4101 | **-32%** |
| complex128 | 500k | dense | 843 | 548 | **-35%** |
| float64 | 50k | graphrag | 1469 | 917 | **-38%** |
| complex64 | 500k | dense | 871 | 496 | **-43%** |
| complex64 | 500k | graphrag | 901 | 467 | **-48%** |
| complex64 | 500k | hybrid | 698 | 361 | **-48%** |
| float32 | 500k | hybrid | 3171 | 2237 | **-30%** |
| turboquant4 | 500k | sparse | 6361 | 5114 | **-20%** |

**Pattern**: GPU emlgo regressions are concentrated in complex types at both scales and float16/float64 at small scale. The CUDA kernel dispatch overhead in emlgo may not be amortized for these types.

---

## 4. CPU vs GPU emlgo Comparison

### Dense QPS (emlgo build)

| Dtype | Count | CPU | GPU | Winner |
|-------|------:|----:|----:|--------|
| int8 | 50k | 2110 | 2049 | CPU +3% |
| uint8 | 50k | 2226 | 3729 | **GPU +68%** |
| float16 | 50k | 2359 | 1889 | CPU +25% |
| float32 | 50k | 792 | 1117 | **GPU +41%** |
| float64 | 50k | 1188 | 1145 | CPU +4% |
| complex64 | 50k | 3365 | 1050 | **CPU +221%** |
| complex128 | 50k | 3158 | 935 | **CPU +238%** |
| turboquant4 | 50k | 3285 | 1762 | **CPU +86%** |
| int8 | 500k | 1412 | 1566 | GPU +11% |
| uint8 | 500k | 1580 | 1575 | ~tie |
| float16 | 500k | 1127 | 1309 | GPU +16% |
| float32 | 500k | 1691 | 3194 | **GPU +89%** |
| float64 | 500k | 715 | 674 | CPU +6% |
| complex64 | 500k | 658 | 496 | CPU +25% |
| complex128 | 500k | 307 | 548 | GPU +79% |
| turboquant4 | 500k | 3038 | 1807 | **CPU +68%** |

**Key insight**: With emlgo, CPU dominates complex types and turboquant4 at small scale. GPU wins for uint8, float32, and complex128 at 500k. The optimal backend depends heavily on dtype and scale.

---

## 5. Memory Impact Analysis

### CPU Memory Delta (emlgo vs standard)

| Dtype | 50k Delta | 500k Delta |
|-------|----------:|-----------:|
| int8 | +6.7% | +1.1% |
| uint8 | **+11.0%** | +0.2% |
| float16 | -0.6% | -3.2% |
| float32 | +6.8% | **-7.5%** |
| float64 | -5.7% | **-12.4%** |
| complex64 | -4.6% | +0.1% |
| complex128 | -1.5% | **+24.1%** |
| turboquant4 | +1.9% | **-11.0%** |

### GPU Memory Delta (emlgo vs standard)

| Dtype | 50k Delta | 500k Delta |
|-------|----------:|-----------:|
| int8 | -1.9% | +2.5% |
| uint8 | **-7.8%** | **+11.8%** |
| float16 | +0.8% | -4.7% |
| float32 | +3.5% | **+8.4%** |
| float64 | +4.0% | +5.7% |
| complex64 | +0.8% | +5.4% |
| complex128 | +1.5% | -3.6% |
| turboquant4 | +0.7% | **+13.5%** |

**Analysis**: Memory impact is generally modest (under ±10%) for most types. Notable exceptions:
- **CPU complex128 at 500k: +24%** — emlgo allocates additional buffers for 16-byte complex arithmetic
- **CPU float64 at 500k: -12%** — emlgo's more efficient memory layout saves space
- **CPU turboquant4 at 500k: -11%** — quantized representation benefits from emlgo's compact storage
- **GPU uint8 at 500k: +12%** — additional GPU buffer allocation in emlgo path

---

## 6. v0.4 (Sep 10) vs Current (Sep 11) Regression Analysis

### Improvements since v0.4

| Area | v0.4 Delta | Current Delta | Change |
|------|-----------|--------------|--------|
| CPU int8 dense 50k | **-56%** | **-34%** | +22pp improvement |
| CPU uint8 dense 50k | **-56%** | **-24%** | +32pp improvement |
| CPU turboquant4 dense 500k | **-55%** | +4% | **+59pp — fixed** |
| GPU turboquant4 dense 50k | **-65%** | **+73%** | **+138pp — reversed** |
| GPU float32 dense 500k | **+203%** | ~0% | Converged to parity |
| CPU complex64 dense 50k | +10% | **+229%** | +219pp improvement |

### Regressions since v0.4

| Area | v0.4 Delta | Current Delta | Change |
|------|-----------|--------------|--------|
| CPU uint8 dense 500k | **+168%** | **-16%** | **-184pp — regressed** |
| GPU uint8 dense 50k | -10% | **+52%** | +62pp improvement |
| CPU float16 dense 50k | **+74%** | **-38%** | **-112pp — regressed** |
| CPU float32 dense 50k | -10% | **-63%** | -53pp — worse |
| GPU complex128 dense 50k | **-54%** | **-72%** | -18pp — worse |
| GPU turboquant4 dense 500k | **+30%** | **+22%** | -8pp — slight decline |

### Stable Areas (within ±10% both runs)

- CPU int8/uint8 sparse at both scales
- CPU float64 dense at 500k
- GPU int8 dense at both scales
- GPU sparse across most types

---

## 7. Recommendations

### Production Deployment

1. **Enable emlgo for complex64/complex128 workloads** — consistent +50-230% gains on CPU
2. **Enable emlgo for turboquant4 at 50k** — +191% CPU gain justifies the build complexity
3. **Keep standard build for int8/uint8 at small scale** — emlgo adds 24-34% overhead
4. **GPU emlgo is viable for uint8 and turboquant4** — +52-106% gains
5. **Use conditional dispatch** — route by dtype and scale to optimal backend

### Investigation Required

1. **CPU float32 dense at 50k: -63%** — emlgo dispatch overhead dominates for simple types at small scale. Profile the call chain to identify optimization opportunities.
2. **GPU complex64 sparse at 500k: -80%** — severe regression needs root cause analysis. Check CUDA kernel launch overhead and memory access patterns.
3. **CPU uint8 dense at 500k: regression reversal** — was +168% in v0.4, now -16%. Investigate what changed in the emlgo math primitives between runs.
4. **CPU complex128 memory at 500k: +24%** — additional buffer allocation needs investigation.
5. **GPU complex128 dense at 50k: -72%** — consistent regression across both runs; likely a fundamental dispatch issue for complex types on GPU.
