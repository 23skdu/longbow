# Next Steps & Production Validation

This document outlines the critical active optimizations, validation roadmap, and priority tasks for the Longbow vector search engine, following the successful resolution of all initial production blockers.

---

## 🚨 P0 Blockers: High-Scale Ingestion & Graph Latency Optimizations

These represent critical engineering improvements to completely eliminate graph traversal latencies, reduce CPU memory bus thrashing under high-concurrency ingestion, and maximize SIMD hardware compute density.

### 1. Quantized Navigation on Upper Layers
* **Status**: Completed `[x]`
* **Goal**: Reduce DRAM-to-L3 memory bus bandwidth usage by 4x to 8x during graph descent.
* **Subtasks**:
  * [x] Adapt the upper layer graph traversal search (`searchLayerForInsert` for layers > 0) to use quantized representations (such as `SQ8` or `TQ`).
  * [x] Ensure the HNSW search descent loads 1-byte quantized vectors on layers $> 0$.
  * [x] Fallback dynamically to full-precision `Float32` calculations only upon entering `Layer 0` to preserve search recall.

### 2. Flat & Packed Memory Chunks
* **Status**: Completed `[x]`
* **Goal**: Pack HNSW neighbor arrays contiguously in memory to enable sequential hardware prefetching.
* **Subtasks**:
  * [x] Restructure neighbor lists in `GraphData` to be packed sequentially rather than being stored in disjoint slices (`FlatAdjacency`).
  * [x] Align packed list bounds to 64-byte boundaries (CPU cache line size) to optimize burst reads from memory.
  * [x] Fix adjacency chunk `EnsureCapacity` races and zero-offset bounds panics.

### 3. Lock-Free Search Contexts
* **Status**: Completed `[x]`
* **Goal**: Ensure that the `SearchContext` fetched from the worker pool is acquired and released lock-free, avoiding hardware contention on search workers.
* **Subtasks**:
  * [x] Remove cache-line bouncing `atomic.Int64` metrics from `ArrowSearchContextPool.Get()` and `Put()`.
  * [x] Ensure `sync.Pool` remains lock-free under extreme parallel insertion load (migrate to `LockFreeRingBuffer` generic implementation).

### 4. High-Concurrency Fuzzing & Race Fixes
* **Status**: Completed `[x]`
* **Goal**: Ensure absolute stability and zero data races during high-speed parallel vector insertions.
* **Subtasks**:
  * [x] Resolve `TurboQuantEncoder` workspace state data races by migrating from `sync.Pool` to `LockFreeRingBuffer`.
  * [x] Guarantee index ingestion and recall stability (`TestRecallConsistency` and `FuzzIngestionIntegrityConcurrent`) under 30-minute stress tests.

---

## 🎯 Active Priorities & Future Roadmap

### P1 — Hardware-Specific Remote Validation & Tuning

#### 1. NVIDIA CUDA Baseline & Performance Sweeps
* **Status**: In Progress `[/]`
* **Task**: Execute the unified benchmark suite on the remote `ancalagon` system to collect complete CUDA GPU performance baselines and throughput curves across dimensions, quantization, and scale configurations.
* **Findings** (2026-05-27, NVIDIA RTX 4060 Laptop GPU, 8GB VRAM):
  * **Comprehensive sweep in progress**: `--mode cuda --dims 128,384,768,1536,3072 --counts 1000,5000,15000,50000,100000 --dtypes float32,float16,int8,turboquant` (100 combinations, 43/100 complete)
  * **Dense search QPS at 1K scale**: 2,400-3,400 QPS across all dims (128-3072)
  * **Dense search QPS at 5K scale**: 1,800-5,300 QPS (float32 384d peaks at 5,311)
  * **Dense search QPS at 15K scale**: 700-1,200 QPS (2-4x drop as index exceeds L2/L3 cache)
  * **TurboQuant competitive with float32**: within 10-15% for most dims/scales
  * **int8 and float16 slower**: need data conversion overhead on GPU
  * **Ingest speed drops at 100K scale**: previous run showed float32 ingest at 396 vec/s for 384d/100K (likely VRAM pressure)
  * **Full matrix with P50/P95/P99 latency**: available in `data/perf_logs/perf_matrix_cuda_comprehensive_cuda_20260527_215529.json`

#### 2. Kernel Overhead Analysis & Latency Profiling
* **Status**: In Progress `[/]`
* **Task**: Profile CUDA GPU thread-group sizes and launch latency under maximum QPS on the target NVIDIA architecture to isolate performance gaps and optimize kernel execution efficiency.
* **Static Analysis** (kernel code in `internal/gpu/cuda/kernels.cu`):
  * **All distance kernels use 256 threads/block**: fixed one-size-fits-all config; optimal block size varies with dimension (128 vs 3072)
  * **Top-K selection is single-threaded**: `select_topk_kernel` uses thread 0 only with shared-memory max-heap — a bottleneck for large k
  * **FP16 kernels use shared-memory query caching**: good practice, reduces global memory reads
  * **TurboQuant does on-stack reconstruction**: `float recon[1024]` stack array limits occupancy; crashes if dim > 1024
  * **No kernel fusion**: distance computation and top-k selection are separate kernel launches, adding launch latency
  * **nsys profile file captured** (`.qdstrm`): needs server isolation to collect meaningful trace (comprehensive benchmark occupied GPU during this session)
* **Needed**: Re-run profiling with exclusive GPU access; collect `ncu` kernel occupancy metrics

---

### P2 — Production Scale Optimization

#### 1. Buffer Eviction & VRAM Management
* **Status**: In Progress `[/]`
* **Task**: Optimize dynamic buffer eviction policies for continuous stream workloads where index scale exceeds physical GPU VRAM (8GB on RTX 4060), ensuring seamless paging between main memory and GPU memory.
* **Progress**:
  * Added `GPUPager` (`internal/gpu/memory/pager.go`): generic GPU page table with LRU eviction, dirty-page writeback, and transparent restore from CPU pinned memory
  * Supports configurable VRAM budget (`maxVRAM`), page size, and capacity limits
  * Eviction policy: LRU list with `Access()` promotion, `Demote()` writeback, `Promote()` transparent restore
  * Test coverage: 11 test cases covering allocation, eviction, restore, dirty tracking, access ordering, and edge cases (double alloc, close, etc.)
* **Next Steps for Integration**:
  * Modify `CUDAIndex` (`internal/gpu/cuda/cuda_index.go`) to use chunked page-based allocation instead of monolithic `buffers[0..3]`
  - Replace raw `cudaMalloc` in C struct with `GPUPager.Promote()`/`Demote()` calls
  - Break monolithic per-type GPU buffers into fixed-size chunks (e.g., 1024 vectors/chunk)
  - Add page residency checks before kernel launches
  - Handle graph data chunking for eviction support
