# Longbow Multi-Dimension Optimization Plan

**Date**: 2026-03-27 (Updated)
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)
**Scope**: Dimensions 128, 256, 384, 768, 1024, 1536, 2048, 3072
**Data Types**: All supported (float32, int8, complex, turboquant, etc.)

---

## Optimization Status (2026-03-27 Update)

### ✅ COMPLETED OPTIMIZATIONS

| Optimization | Status | Impact |
|-------------|--------|--------|
| Blocked SIMD for float/int/uint (768+) | ✅ Complete | +30-50% QPS |
| Complex64/128 blocked via cast | ✅ Complete | +20-30% QPS |
| TurboQuant NEON Kernels (FWHT) | ✅ Complete | +3.7x Core / +40% QPS |
| HNSW M=32 for 768+ dims | ✅ Complete | +15-20% QPS |
| Prefetch for 1536+ dims | ✅ Complete | +10-15% QPS |
| Full Audit (18GB, 1k-15k count) | ✅ Complete | Baseline Estab |

### 📋 REMAINING WORK

| Task | Priority | Est. Effort |
|------|----------|-------------|
| Add search-layer metric sampling | Low | 2 hours |
| Final regression test for full matrix | Med | 8 hours |
| Local buffer pool for high-dim vectors | Med | 4 hours |

---

## Executive Summary

Enable optimized SIMD kernels for all supported dimensions (128-3072) across all data types. This plan addresses performance degradation at high dimensions (≥768) and ensures consistent QPS across the entire supported dimension range.

## Current State Analysis (Updated 2026-03-27)

| Dimension | float32 | float64 | int32 | int16 | int8 | complex64 | turboquant |
|-----------|---------|---------|-------|-------|------|-----------|------------|
| 128 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 256 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 384 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 768 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1024 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1536 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 2048 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 3072 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |

**Legend:**

- ✅ Blocked = Blocked SIMD implementation (256/512 byte blocks)
- ✅ Blocked+Prefetch = Blocked SIMD with prefetch hints (1536+ only)
- ✅ Optimized = Direct SIMD kernels (128-384)

---

## 10-Step Implementation Roadmap (Progress)

### Step 1: Analyze Current SIMD Kernel Implementations

**Status**: ✅ COMPLETE

### Step 2: Benchmark Baseline for All Dimensions/Types

**Status**: ✅ COMPLETE

### Step 3: Implement Blocked SIMD for Missing Dimensions

**Status**: ✅ COMPLETE

### Step 4: Add Type-Specific Optimizations (TurboQuant/Complex)

**Status**: ✅ COMPLETE

### Step 5: Add Unit Tests for All Kernel Variants

**Status**: ✅ COMPLETE

### Step 6: Add Prometheus Metrics for Performance Stability

**Status**: ✅ COMPLETE

### Step 7: Add Memory Pressure Metrics

**Status**: ✅ COMPLETE (Allocation tracking and zero-copy monitoring enabled)

### Step 8: Integrate Metrics into Search Hot Paths (with Sampling)

**Status**: 📋 IN PROGRESS

- Sampling logic for search layer to avoid overhead.

### Step 9: Run Final Performance Matrix

**Status**: 📋 PENDING

### Step 10: Document Results and Final Update

**Status**: 📋 PENDING

---

## 2026-03-27 Optimization Achievements

- **float32/64**: Fully optimized for all dimensions 128-3072 using blocked SIMD + prefetch.
- **int8/16/32/64**: Fully optimized for all dimensions using blocked SIMD.
- **complex64/128**: Fully optimized via zero-copy casting to float paths.
- **turboquant**: Fully optimized with NEON-vectorized rotation and Hadamard kernels.

---

**Last Updated**: 2026-03-27
