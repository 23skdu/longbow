# Longbow v0.2.0+ Roadmap: Performance & SIMD Hardening

## P0: Blockers (Immediate Action)

### [FIX] `commitID` Scheduler Deadlock

- **Observation**: Busy-wait loop in `commitID` causes deadlocks when all `SharedWorkerPool` workers are spinning on out-of-order commits, preventing the required "in-order" task from being scheduled.
- **Action**: Replace `runtime.Gosched()` spin-loop with `sync.Cond` or a non-blocking commit queue.
- **Tests**: `TestArrowHNSW_AddBatch_Concurrent` must pass with high worker saturation.

### [FIX] Robust Record Resolution in `AddBatch`

- **Observation**: Passing single-record slices with global batch indices causes out-of-bounds panics.
- **Action**: Finalize the robust resolution logic (checked for slice length and nil entries).

---

## 1. Closing the GraphRAG Search Gap

**Goal**: Reduce the 50% performance delta between Dense and GraphRAG search.

### Subtasks

- [ ] **Visited Node Bitset**: Replace `map[uint32]struct{}` with a pooled bitset (e.g., `roaring.Bitmap` or a simple `[]uint64`) in `GraphData` expansion.
- [ ] **Candidate Set Caching**: Implement a small, thread-local LRU cache for expansion candidates to avoid redundant distance calculations for common hub nodes.
- [ ] **Expansion Loop Vectorization**: Manually unroll the neighbor traversal loop to improve instruction-level parallelism (ILP).
- [ ] **Prefetching**: Add software prefetch hints (`simd.Prefetch`) for neighbor vector data during expansion.

### Testing & Metrics

- **Metrics**:
  - `longbow_graphrag_expansion_duration_seconds`: Latency of the expansion step.
  - `longbow_graphrag_nodes_visited_total`: Efficiency of the search (visited vs total).
- **Fuzz Tests**: Random graph topologies with high connectivity to test expansion stability.

---

## 2. Vectorized Activation Kernels (NEON & AVX)

**Goal**: Replace generic fallbacks with architecture-specific assembly for non-linear activations.

### Subtasks

- [x] **AVX2/AVX512 (Avo)**:
  - Implement `Exp` and `Log` using rational approximations (e.g., Remez algorithm) or table-based methods.
  - Implement `Softmax` and `Sigmoid` using the new `Exp` kernel.
- [x] **NEON (ARM64)**:
  - Port AVX logic to NEON using `vexpq_f32` (if available via intrinsics) or manual polynomial approximation.
- [x] **Dispatch Integration**: Update `internal/simd/dispatch.go` to wire these into the `currentDispatch` table.

### Testing & Metrics

- **Metrics**: `longbow_simd_activation_duration_seconds`.
- **Accuracy Tests**: 1 ULP (Unit in the Last Place) accuracy check against `math.Exp` and `math.Log`.

---

## 3. Advanced Vectorized Math Ops

**Goal**: Complete the SIMD toolbox for high-performance tensor-like operations.

### Subtasks

- [ ] **Vectorized MatMul**:
  - Implement blocked matrix multiplication kernels for AVX-512 and NEON.
  - Focus on $M \times K \times N$ where $K$ is vector dimension.
- [ ] **Extended Dot Products**:
  - Implement Manhattan (L1), Chebyshev (LInf), and Bray-Curtis distances.
- [ ] **Reduction Kernels**:
  - `Sum`, `Max`, `Min` across large slices with SIMD horizontal reduction.
  - `ArgMax` / `ArgMin` for fast top-k candidate selection outside HNSW.

### Testing & Metrics

- **Metrics**: `longbow_simd_math_ops_total` (labeled by operation and architecture).
- **Parity Tests**: Automated comparison against `gonum` or standard Go loops for correctness.
