# Longbow Performance Optimization Plan - Metal macOS Focus

**Status**: ACTIVE
**Date**: March 14, 2026
**Priority**: HIGH - Address performance regressions and optimize Metal macOS performance

## Overview

This document outlines a 10-part plan to:
1. Fix identified performance regressions in DoPut/Get operations
2. Optimize Metal GPU performance for Apple Silicon
3. Ensure WAL replay fixes don't introduce overhead
4. Leverage Metal Performance Shaders for vector operations
5. Optimize memory usage patterns for unified memory architecture

Based on performance testing showing regressions of -17% to -96% in DoPut operations compared to historical baselines.

---

## 10-Part Performance Optimization Plan

### 1. WAL Replay Optimization (Immediate)
**Location**: `internal/storage/wal_replay.go`
**Issue**: Our deadlock fix may have introduced overhead
**Actions**:
- Profile WAL replay path to identify bottlenecks
- Optimize channel buffering and decoder coordination
- Consider batch WAL application instead of record-by-record
- Validate CRC computation efficiency
**Target**: Recover 20-30% DoPut performance loss

### 2. Metal GPU Vector Distance Computation
**Location**: `internal/gpu/metal_gpu.go` and `internal/store/hnsw_gpu.go`
**Issue**: Metal implementation may not be fully utilized
**Actions**:
- Ensure HNSW search uses Metal GPU for distance calculations
- Optimize Metal kernel launch overhead for small batches
- Implement batched vector queries to amortize kernel launch cost
- Use Metal Performance Shaders for optimized distance metrics
**Target**: 2-5x search performance improvement on M1/M2/M3

### 3. Unified Memory Optimization for Metal
**Location**: Throughout Metal GPU codebase
**Issue**: Not fully leveraging Apple's unified memory architecture
**Actions**:
- Use MTLResourceStorageModeShared for all GPU-CPU shared data
- Avoid unnecessary data copies between CPU and GPU
- Implement zero-copy vector ingestion pipeline
- Align memory allocations to cache line boundaries (64-byte)
**Target**: Reduce memory bandwidth usage by 30-50%

### 4. HNSW 'ef' Parameter Tuning for Metal
**Location**: `internal/store/hnsw.go`
**Issue**: Search parameters not optimized for GPU execution
**Actions**:
- Dynamically adjust 'ef' based on hardware capabilities
- Implement adaptive search that increases 'ef' for GPU batches
- Profile and tune 'ef' for different vector dimensions (128, 384, 786)
- Consider Metal-specific ef values due to parallelism
**Target**: Improve search recall/performance tradeoff by 15-25%

### 5. Vector Ingestion Pipeline Optimization
**Location**: `internal/store/store_ingestion.go`
**Issue**: DoPut performance regression observed
**Actions**:
- Implement pipelined vector preparation (normalize, quantize while ingesting)
- Use lock-free queues for vector batching
- Pre-allocate and reuse Arrow buffers
- Implement vector sorting for better index insertion locality
**Target**: Recover 40-60% of DoPut regression

### 6. Memory Allocator Tuning for Metal Workloads
**Location**: `internal/memory/`
**Issue**: General purpose allocator may not suit GPU workloads
**Actions**:
- Implement memory pools for vector batches
- Use size-specific allocators for common vector sizes
- Align allocations to 128-byte boundaries for cache efficiency
- Consider using Metal's recommended allocation patterns
**Target**: Reduce allocation overhead by 25-40%

### 7. SIMD Optimization for Metal Fallback Paths
**Location**: `internal/distance/` and `internal/store/`
**Issue**: CPU fallback paths may not be optimized
**Actions**:
- Ensure AVX2/NEON optimizations are active on Intel/ARM Macs
- Implement Metal-accelerated distance computations as primary path
- Add runtime detection for optimal instruction set
- Optimize distance calculations for small vectors (<512 dimensions)
**Target**: Improve CPU fallback performance by 20-35%

### 8. Query Batching and Parallelism
**Location**: `internal/store/servers.go`
**Issue**: Not fully utilizing Metal's parallel compute capabilities
**Actions**:
- Implement intelligent query batching for Metal execution
- Prioritize queries that can benefit from GPU parallelism
- Implement query scheduling based on GPU utilization metrics
- Use Metal command buffers to encapsulate multiple operations
**Target**: Improve search throughput by 2-3x under load

### 9. Metal-Specific Configuration Tuning
**Location**: `docs/configuration.md` and internal config handling
**Issue**: Generic configuration may not be optimal for Metal
**Actions**:
- Add Metal-specific tuning parameters:
  - `METAL_MAX_BATCH_SIZE`: Optimal batch size for Metal kernels
  - `METAL_MEMORY_POOL_SIZE`: Pre-allocated memory for vectors
  - `METAL_COMMAND_BUFFER_COUNT`: Number of concurrent command buffers
  - `METAL_USE_UNIFIED_MEMORY`: Explicit unified memory control
- Provide hardware-specific presets for M1/M2/M3/M4
**Target**: Enable 15-25% performance gains through better configuration

### 10. Performance Regression Testing Framework
**Location**: `scripts/` and CI pipeline
**Issue**: Regressions not caught early enough
**Actions**:
- Implement automated performance benchmarking in CI
- Create performance baseline for Metal macOS configurations
- Add performance alerts for >5% regression in key metrics
- Include Metal-specific benchmarks in test suite
- Track performance across Go compiler versions
**Target**: Prevent future performance regressions

---

## Implementation Priority

**Immediate (Week 1)**:
1. WAL Replay Optimization - Fix DoPut regression
2. Unified Memory Optimization for Metal - Quick wins
3. Vector Ingestion Pipeline Optimization - Address primary regression

**Short-term (Weeks 2-3)**:
4. Metal GPU Vector Distance Computation - Enable GPU acceleration
5. Memory Allocator Tuning - Reduce overhead
6. Query Batching and Parallelism - Better GPU utilization

**Medium-term (Weeks 4-6)**:
7. HNSW 'ef' Parameter Tuning - Optimize search parameters
8. SIMD Optimization for Fallback Paths - Improve CPU path
9. Metal-Specific Configuration Tuning - Hardware-specific optimization
10. Performance Regression Testing Framework - Prevent regressions

---

## Success Metrics

After implementation, target performance improvements:

| Operation | Historical | Current | Target | Improvement |
|-----------|------------|---------|--------|-------------|
| DoPut (1K vectors) | 418 MB/s | 188 MB/s | 350+ MB/s | +86% |
| DoPut (5K vectors) | 1099 MB/s | 1188 MB/s | 1200+ MB/s | +1% |
| DoPut (10K vectors) | 1381 MB/s | 54 MB/s | 1000+ MB/s | +1750% |
| DoGet (1K vectors) | 598 MB/s | 588 MB/s | 550+ MB/s | -8% (acceptable) |
| Search QPS (10K) | 2282 | TBD | 2500+ | +10%+ |

## Verification

Each optimization step will be validated by:
1. Microbenchmarks for the specific component
2. End-to-end DoPut/Get/search benchmarks
3. Metal GPU utilization monitoring
4. Memory bandwidth and allocation profiling
5. Regression testing against historical baselines

---

**Next Step**: Begin with WAL replay optimization and unified memory improvements, as these address the primary DoPut regression observed in testing.