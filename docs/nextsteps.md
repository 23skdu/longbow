# Longbow Next Steps & Active Roadmap

This document outlines the remaining active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases (**v0.3.0+**).

Completed release tasks (including the 0.2.3-rc1 P0 blockers, comprehensive 500k CPU/GPU benchmark suite, `GCTuner` emergency rate limiter, fast-fail readiness check, asynchronous pinned CUDA transfers, automatic spill-to-disk paging at 70% RAM threshold, and TurboQuant standardization via `LONGBOW_AUTO_QUANTIZE=1`) have been fully verified, tested with unit/fuzz tests, and merged.

---

## 1. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |

---

## 2. Completed in v0.3.0 Engine Hardening

### A. Automatic Spill-to-Disk Paging at High Scale (`LONGBOW_AUTO_SPILL_DISK=1`)
- Automatic fallback to disk backing (`DiskVectorStore` / memory-mapped vector partitions) when vector count and index dimensions exceed 70% of physical RAM (`LONGBOW_SPILL_THRESHOLD_RATIO=0.70`).
- Linux cgroup v1/v2 limit and `/proc/meminfo` total physical memory detection with `LONGBOW_PHYSICAL_RAM` override.
- Zero-copy block-compressed batch appends and transparent HNSW disk extraction.
- Prometheus counter `longbow_auto_spill_to_disk_engaged_total`.

### B. TurboQuant Default Storage Engine Configuration (`LONGBOW_AUTO_QUANTIZE=1`)
- Standardized TurboQuant (default 4-bit, configurable to 2 or 8) as default storage mode for 500k+ vector configurations on memory-constrained infrastructure (≤64 GB RAM) or when uncompressed memory exceeds 30% of physical RAM.
- Integrated with `ArrowHNSWConfig`, CLI flags (`--auto-quantize`, `--auto-quantize-threshold`, `--auto-quantize-bits`), and runtime `QuantizationTuner`.
- Prometheus counter `longbow_auto_quantize_engaged_total`.

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
