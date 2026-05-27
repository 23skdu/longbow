# Next Steps & Production Validation

This document outlines the critical active optimizations, validation roadmap, and priority tasks for the Longbow vector search engine, following the successful resolution of all initial production blockers.

---

## 🚨 P0 Blockers: High-Scale Ingestion & Graph Latency Optimizations

These represent critical engineering improvements to completely eliminate graph traversal latencies, reduce CPU memory bus thrashing under high-concurrency ingestion, and maximize SIMD hardware compute density.

### 1. Highly-Optimized One-to-Many Batch Distance Kernels [CRITICAL]
* **Status**: Not Started `[ ]`
* **Goal**: Maximize SIMD lane utility and memory throughput by comparing a single query vector against $K$ candidate vectors concurrently in CPU registers.
* **Subtasks**:
  * [ ] Design AVX2 and AVX-512 assembly prototypes for batch-4 and batch-8 dot-product/L2 distance calculations (using `Avo` in `internal/simd/`).
  * [ ] Develop ARM NEON equivalents executing register-blocked float32 multiply-accumulate operations.
  * [ ] Refactor `selectNeighbors` inside HNSW to leverage these batch kernels during pairwise distance computation, replacing slow sequential one-to-one loops.

### 2. Active Software Prefetching
* **Status**: Not Started `[ ]`
* **Goal**: Preemptively load neighbor vector and connection data into CPU L1/L2 caches to mask memory access latency during graph traversal.
* **Subtasks**:
  * [ ] Add `simd.Prefetch` checkpoints inside the HNSW `searchLayerForInsert` loop.
  * [ ] Prefetch the vectors of all next-candidate neighbor IDs immediately upon extracting them from current node neighbor arrays.
  * [ ] Tune prefetch offsets (e.g. prefetching 1-2 steps ahead) to align perfectly with CPU pipeline instruction bubbles.

### 3. Quantized Navigation on Upper Layers
* **Status**: Not Started `[ ]`
* **Goal**: Reduce DRAM-to-L3 memory bus bandwidth usage by 4x to 8x during graph descent.
* **Subtasks**:
  * [ ] Adapt the upper layer graph traversal search (`searchLayerForInsert` for layers > 0) to use quantized representations (such as `SQ8` or `TQ`).
  * [ ] Ensure the HNSW search descent loads 1-byte quantized vectors on layers $> 0$.
  * [ ] Fallback dynamically to full-precision `Float32` calculations only upon entering `Layer 0` to preserve search recall.

### 4. Lock-Free & Allocation-Free Search Contexts
* **Status**: Not Started `[ ]`
* **Goal**: Completely eliminate garbage collection pressure during high-throughput searches.
* **Subtasks**:
  * [ ] Audit the `SearchContext` returned by `h.searchPool.Get()` to guarantee all tracking structures are pre-allocated.
  * [ ] Swap the visited-node map tracking with a recycled `roaring.Bitmap` or a pre-allocated flat byte array index.
  * [ ] Profile garbage collection allocation metrics under maximum QPS to verify zero-allocation search operations.

### 5. Flat & Packed Memory Chunks
* **Status**: Not Started `[ ]`
* **Goal**: Pack HNSW neighbor arrays contiguously in memory to enable sequential hardware prefetching.
* **Subtasks**:
  * [ ] Restructure neighbor lists in `GraphData` to be packed sequentially rather than being stored in disjoint slices.
  * [ ] Align packed list bounds to 64-byte boundaries (CPU cache line size) to optimize burst reads from memory.

---

## 🎯 Active Priorities & Future Roadmap

### P1 — Hardware-Specific Remote Validation & Tuning

#### 1. NVIDIA CUDA Baseline & Performance Sweeps
* **Status**: In Progress `[/]`
* **Task**: Execute the unified benchmark suite on the remote `ancalagon` system to collect complete CUDA GPU performance baselines and throughput curves across dimensions, quantization, and scale configurations.

#### 2. Kernel Overhead Analysis & Latency Profiling
* **Status**: Not Started `[ ]`
* **Task**: Profile CUDA GPU thread-group sizes and launch latency under maximum QPS on the target NVIDIA architecture to isolate performance gaps and optimize kernel execution efficiency.

---

### P2 — Production Scale Optimization

#### 1. Buffer Eviction & VRAM Management
* **Status**: Not Started `[ ]`
* **Task**: Optimize dynamic buffer eviction policies for continuous stream workloads where index scale exceeds physical GPU VRAM, ensuring seamless paging between main memory and GPU memory.
