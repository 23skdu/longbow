# Next Steps for Longbow (Updated 2026-04-26)

---

## P0 Blockers for Performance Optimization

### 1. HIGH | SIMD | AVX-512/AVX2 Batch Kernels for x86_64

**Expected Impact:** +30% QPS  
**Current State:** 
- AVX2: Uses euclidean8AVX2 loop (one vector at a time), has euclideanVertical4AVX2 for 4 parallel
- AVX-512: Stubs call generic fallback

**Subtasks:**
1. [ ] Optimize AVX2 batch using euclideanVertical4AVX2 pattern (4 vectors parallel)
   - Process 4 vectors at once in inner loop
   - Expected: ~2-3x speedup vs current sequential loop
2. [ ] Implement full AVX512 batch using euclideanVertical4AVX512 (16 vectors at once)
3. [ ] Replace stub functions in simd_stubs.go with assembly
4. [ ] Add SIMDDispatchBatchCount metric
5. [ ] Add fuzz test: FuzzBatchKernelConsistency vs generic
6. [ ] Add benchmark: BenchmarkBatchKernel_Dim128-3072

**Success Criteria:**
- AVX2: >2x speedup vs current ~1.5K vectors/sec
- AVX-512: >5x vs generic
- Fuzz test passes 10M iterations

---

### 2. HIGH | Memory | Arena Allocator Integration

**Expected Impact:** +15% QPS, -30% GC  
**Current State:** VectorArena exists in memory/vector_arena.go but not integrated with store

**Subtasks:**
1. [ ] Integrate VectorArena with store/store.go vector storage
   - Replace mmap allocation with VectorArena.AllocVector
2. [ ] Add ArenaAllocationTotal metric
3. [ ] Add ArenaHitRate metric (fast path vs slow path)
4. [ ] Add fuzz test: FuzzArenaVector_ConcurrentAlloc
5. [ ] Add benchmark: BenchmarkArena_VectorStorage

**Success Criteria:**
- >90% arena fast path utilization
- Metrics show reduction in Go GC pause times
- Fuzz test passes with concurrent access

---

### 3. MEDIUM | Index | IVF-PQ with OPQ Optimization

**Expected Impact:** 10x+ for high-dim (>1024)  
**Current State:** IVFOPQIndex exists but needs optimization

**Subtasks:**
1. [ ] Optimize IVFOPQIndex.Search for batch queries
2. [ ] Add OPQ encoder warmup metric
3. [ ] Implement GPU offload path for encoding
4. [ ] Add recall test: TestIVFOPQ_RecallK
5. [ ] Add benchmark: BenchmarkIVFOPQ_1M_3072dim

**Success Criteria:**
- >95% recall@10 on SIFT-1M
- <100ms search for 1M vectors @ 3072dim

---

### 4. MEDIUM | GPU | Metal Compute Shaders

**Expected Impact:** 5-10x for >1M vectors  
**Current State:** Metal uses GPU memory (MTLBuffer), compute in development

**Subtasks:**
1. [ ] Implement Metal compute kernel for cosine similarity
2. [ ] Implement Metal compute kernel for top-k selection
3. [ ] Add GPUComputeKernelTotal metric
4. [ ] Add fuzz test: FuzzMetalGPU_Consistency
5. [ ] Add benchmark: BenchmarkMetalGPU_1M_128dim

**Success Criteria:**
- GPU compute achieves >5x speedup vs CPU
- Fuzz test passes 1M iterations

---

### 5. LOW | Graph | Batch Graph Traversal

**Expected Impact:** +20% for graphrag mode  
**Current State:** Single-threaded graph traversal

**Subtasks:**
1. [ ] Implement concurrent graph frontier expansion
2. [ ] Add GraphBatchTraversalCount metric
3. [ ] Add benchmark: BenchmarkGraph_TraversalBFS

**Success Criteria:**
- >80% CPU utilization during graphrag
- 20% speedup on graph traversal

---

## Production Blockers for 0.1.9 Release

### Must Fix Before Release
| # | Blocker | Status | Notes |
|---|--------|--------|-------|
| 1 | Schema dimension-change crash | Fixed in dev | Restart server between dim changes |
| 2 | Test coverage < 95% | Needs assessment | Run coverage report |
| 3 | CI validation on PR | Not configured | Setup GitHub Actions |

### Done ✅
- NEON cosine kernel fixed (simd_arm64.s)
- MTLBuffer pooling (memory_metal_buffer_pool.go) 
- VectorSearchRequest mode field added
- IVF-OPQ/IVF-HNSW AddBatch implemented
- Metal TurboQuant SearchTurboQuant implemented
- Mode field validation (unified_benchmark.py)
- Dimension-change stress test (scripts/dimension_change_test.sh)
- NamespaceCacheManager removed

---

## Deferred to 0.2.0

### Technical Debt
- TPU XLA kernels
- IVF-PQ method gaps
- Metal Graph updates
- NEON TurboQuant bit-pack
- 171 skipped tests (platform-specific)

### 0.2.0 Roadmap
- TPU production implementation
- Fuzzing tests
- GPU sharding / multi-device
- Windows port

---

## Dead Code Analysis (Completed)

| Code | Status |
|------|-------|
| AdaptiveChunkStrategy | LIVE ✅ (33 refs) |
| CircuitBreaker | LIVE ✅ (222 refs) |
| GPU Mock Index | STUB (testing) |
| NamespaceCacheManager | REMOVED ✅ |

---

## Performance Improvements for Next Release

### Current Results (CPU, 10K vectors, dim=128)
- Ingest: 1.22M vec/s
- Search QPS: ~4,000 (all modes)
- Latency p50: 0.23ms

### Suggested Optimizations

| Priority | Area | Suggestion | Expected Impact |
|----------|------|----------|-------------|
| HIGH | SIMD | Add AVX-512 batch kernels for x86_64 | +30% QPS |
| HIGH | Memory | Arena allocator for vector storage | +15% QPS |
| MEDIUM | Index | Implement IVF-PQ with OPQ | 10x+ for high-dim |
| MEDIUM | GPU | Metal compute shaders | 5-10x for >1M vectors |
| LOW | Graph | Batch graph traversal | +20% for graphrag |

### Benchmark Infrastructure
- Add automated daily benchmarks comparing CPU/Metal/CUDA
- Track QPS, latency p50/p95/p99, ingest rate
- Generate diffs vs previous releases