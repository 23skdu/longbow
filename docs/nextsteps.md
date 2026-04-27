# Next Steps for Longbow (Updated 2026-04-26)

---

## Production Blockers for 0.1.9 Release

### Must Fix Before Release
| # | Blocker | Status | Notes |
|---|--------|--------|-------|
| 1 | Schema dimension-change crash | Workaround only | Restart server between dim changes |
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