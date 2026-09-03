# Longbow Next Steps & Active Roadmap

This document outlines the remaining active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases (**v0.3.0+**).

Completed release tasks (including the 0.2.3-rc1 P0 blockers, comprehensive 500k CPU/GPU benchmark suite, `GCTuner` emergency rate limiter, fast-fail readiness check, and asynchronous pinned CUDA transfers) have been fully verified, tested with unit/fuzz tests, and merged.

---

## 1. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Automatic Spill-to-Disk Paging at High Scale** | v0.3.0 | Storage / Memory | Implement automatic fallback to disk backing (`LONGBOW_USE_DISK=1` / memory-mapped vector partitions) when vector count and index dimensions exceed 70% of available physical RAM, preventing OS swap thrashing on uncompressed 64/128-bit types. |
| **3** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |
| **4** | **TurboQuant Default Storage Engine for Constrained Deployments** | v0.3.0 | Engine / Config | Standardize TurboQuant as the default vector storage mode for 500k+ vector configurations on memory-constrained infrastructure, exposing automatic quantization thresholds in configuration. |

---

## 2. Architectural & Engine Enhancements

### A. Automatic Spill-to-Disk Paging for High-Dimension Vectors
- **Problem**: At 500,000 vectors with 384 dimensions, uncompressed 64-bit and 128-bit types (`float64`, `complex64`, `complex128`, `int64`) consume 1.54 GB – 3.07 GB in raw vectors, expanding into ~20–24 GB during multi-layer HNSW graph construction. On hosts with ≤24 GB RAM, this triggers kernel swap paging.
- **Action Item**:
  - Implement heuristic in admission controller / store initialization that checks available system RAM against estimated graph memory.
  - Automatically engage disk-backed mmap partition backing when estimated memory exceeds 70% of host RAM.

### B. TurboQuant Default Configuration & Thresholds
- **Problem**: Large unquantized vector spaces exhaust host memory without significant search accuracy benefits over polar-quantized representations.
- **Action Item**:
  - Expose `LONGBOW_AUTO_QUANTIZE=1` with default quantization to TurboQuant (4-bit / 8-bit) when row counts exceed configured thresholds (e.g. >100,000 vectors).
  - Add benchmark-verified configuration presets in server startup flags.

---

## 3. Extended Roadmap Initiatives (from `docs/roadmap.md`)

### A. Native Tensor Calculus Engine
- **Phase 1: Tensor IR & Einstein Notation Parser**: Implement parser for Einstein summation notation (e.g. `"ij,jk->ik"`) and construct a DAG IR (`Contract`, `Transpose`, `Reshape`, `Elementwise`, `Reduce`).
- **Phase 2: Index Rewriting Optimizer**: Dynamic programming contraction order optimization, common sub-expression elimination, and algebraic simplification.
- **Phase 3: JIT-Compiled Kernels**: AVX2 and CUDA tensor kernels (cuBLAS GEMM, elementwise intrinsics, trig/tensor calculus intrinsics).
- **Phase 4: Hybrid Auto-Scheduling & Zero-Copy Views**: Cost-model scheduler between CPU/GPU and zero-copy tensor slicing over Arrow buffers.

### B. Experimental Fast Math Integration (`feature/emlgo-math`)
- **Objective**: Integrate the `emlgo` math library (https://github.com/23skdu/emlgo) to replace standard math library calls in distance calculation hotspots.
- **Action Item**:
  - Set up feature branch and wrapper interface in `internal/store/index`.
  - A/B benchmark HNSW graph build times, peak memory, and Recall@K accuracy against Go `math` standard library.
