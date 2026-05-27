# Next Steps & Production Validation

This document outlines the remaining post-optimization validation steps and future production roadmap for the Longbow vector search engine, following the successful implementation of high-performance GPU acceleration, double-buffered pipelines, and decoupled batched distance offloading.

---

## ✅ Completed Milestones

### 1. Asynchronous GPU Transfers & Pipelined Execution (P1)

- **Asynchronous Command Streams**: Migrated the Metal backend to utilize async enqueuing with command completion handlers and thread-safe Go `runtime/cgo.Handle` notifications instead of synchronous blocking.
- **Double-Buffered Transfer Pipeline**: Implemented a thread-safe `DoubleBufferedQueue` in `internal/gpu/memory/double_buffer.go` featuring pre-allocated host buffers (`BufferA`, `BufferB`) to overlap data copying with active GPU compute kernels.

### 2. Decoupled HNSW Distance Offloading & Fallback Gating (P1)

- **Decoupled Metric Computations**: Decoupled intensive distance computation from the sequential CPU HNSW graph traversal loop (`ComputeDistancesBatch`), keeping structural updates CPU-bound.
- **Batched Distance Kernels**: Implemented customized batch evaluation kernels for exact L2 distance computations.
- **CPU Fallback Heuristics**: Added dynamic fallback gating so that neighbor candidates under a threshold (e.g. `256` elements) execute instantly via SIMD CPU instructions to bypass CGO/driver dispatch overheads.

### 3. Dynamic High-Scale Index Abstractions (P3)

- **Unified Routing Layer**: Created `GPUIndexWrapper` to act as a seamless CPU-GPU dynamic router.
- **Off-heap Hybrid Ingestion**: Implemented a policy to route high-frequency ingestion to memory-mapped HNSW, while flushing massive query loads to high-throughput cuVS or Faiss GPU backends in optimized chunks.

---

## 🎯 Active Priorities & Future Roadmap

### P1 — Hardware-Specific Remote Validation

#### 1. Remote `ancalagon` Benchmark Execution

- [ ] **Deploy Unified Benchmark Suite**: Move from local Apple Silicon Metal ARM64 validation to the remote x86_64/CUDA hardware platform `ancalagon`.
- [ ] **High-Scale Matrix Evaluation**: Execute performance profiling across the full matrix of:
  - Vector dimensions (e.g., 64, 128, 256, 384, 768, 1536, 3072)
  - Quantization types (e.g., SQ8, PQ, TQ, F16)
  - Scale constraints (from 10K up to 10M+ vectors)

#### 2. Profiling & Tail Latency Tuning

- [ ] **Analyze Kernel Overhead**: Profile thread-group sizes and launch latency under maximum QPS on the NVIDIA target architecture.
- [ ] **Buffer Eviction Optimization**: Optimize dynamic buffer eviction policies for continuous stream workloads where index scale exceeds physical GPU VRAM.
