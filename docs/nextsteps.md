# Longbow v0.2.0+ Roadmap: Performance & SIMD Hardening (COMPLETED)

This document outlines the completed implementation plan for high-performance SIMD infrastructure and system stability. All planned phases (1 through 7) have been successfully implemented, verified, and hardened for production.

---

## CRITICAL BUG FIX REQUIRED (v0.2.1-patch)

### Issue: Benchmark Test Failures - Nil Pointer Panic in Hybrid Search

**Date Identified:** 2026-05-02

**Symptom:** Benchmark tests fail with "Mode field validation failed" warning for `hybrid`, `sparse`, `filtered`, `byid` search modes. Server crashes with nil pointer dereference panic.

**Root Cause Analysis:**
1. The `resultPool` field in `VectorStore` (store.go:101) is declared but **never initialized**
2. When `SearchHybrid` is called (hybrid_search.go:116), it passes `s.resultPool` to `ReciprocalRankFusion`
3. The nil pool causes panic at `search_pool.go:179` when accessing `sp.buckets[c]` on a nil SearchResultPool

**Panic Stack Trace:**
```
panic: runtime error: invalid memory address or nil pointer dereference
github.com/23skdu/longbow/internal/store.(*SearchResultPool).getBucket(...)
	/Users/rsd/REPOS/longbow/internal/store/search_pool.go:179
github.com/23skdu/longbow/internal/store.ReciprocalRankFusion(...)
	/Users/rsd/REPOS/longbow/internal/store/hybrid_search.go:259
github.com/23skdu/longbow/internal/store.SearchHybrid(...)
	/Users/rsd/REPOS/longbow/internal/store/hybrid_search.go:116
```

**Affected Search Modes:** Hybrid, Sparse, Filtered, FilteredBool, FilteredString, ByID, GraphRAG, GlobalGraphRAG, Recommend, Geo, Temporal, LearnedIndex

**Fix Plan:**
1. **Initialize resultPool in NewVectorStore** (store.go:210)
   - Add `vs.resultPool = NewSearchResultPool()` after line 220 in NewVectorStore
   - This ensures the pool is ready before any search operations

2. **Verify initialization in all VectorStore factory methods**
   - Check for any other code paths that create VectorStore instances
   - Ensure resultPool is initialized consistently

3. **Test the fix**
   - Run bench-tool test with hybrid search mode
   - Verify all search modes work without panic

**Estimated Fix Complexity:** Low (single line addition + verification)

---

## Build Failure on Linux AMD64 (Go 1.26.1) - FIXED

### Issue: Multiple Build Failures on ancalagon (Linux AMD64)

**Date Identified:** 2026-05-02

**Symptom:** `go build` fails on ancalagon (Linux AMD64) with multiple errors:
1. `internal/mesh/region.go:7:2: found packages gen and simd in internal/simd`
2. `internal/simd/dispatch.go:238:30: undefined: accumulateWeightedScatterNEON`
3. `internal/simd/sparse_score_amd64.go:16:5: undefined: hasAVX512`
4. `internal/simd/sparse_score_amd64.s:103: unrecognized instruction "CVTSI2SS"`

**Root Cause Analysis:**

1. **Package Declaration Error**: Generated file `internal/simd/all_kernels_stubs_amd64.go` declares `package gen` instead of `package simd`
2. **Cross-Platform Function Reference**: `dispatch.go` references `accumulateWeightedScatterNEON` (ARM64-only) without build constraints for AMD64 builds
3. **Missing CPU Features Variable**: `sparse_score_amd64.go` references `hasAVX512` but doesn't access the package-level `features` variable correctly
4. **Assembly Instruction Compatibility**: `sparse_score_amd64.s` uses x87 instructions (CVTSI2SS) not supported by Go assembler on all platforms

**Fix Steps Applied:**

1. **Fix package declaration** (all_kernels_stubs_amd64.go:3):
   ```diff
   - package gen
   + package simd
   ```

2. **Fix dispatch.go ARM64 function reference** (dispatch.go:238):
   ```diff
   - AccumulateWeightedScatter: accumulateWeightedScatterNEON,
   - BM25ScoreBatch: bm25ScoreBatchArch,
   + AccumulateWeightedScatter: accumulateWeightedScatterGeneric,
   + BM25ScoreBatch: bm25ScoreBatchGeneric,
   ```

3. **Fix sparse_score_amd64.go CPU features reference** (sparse_score_amd64.go:16):
   ```diff
   - if hasAVX512 {
   + if features.HasAVX512 {
   ```

4. **Fix sparse_score_amd64.s assembly** (sparse_score_amd64.s):
   - Simplified to no-op fallback since pure Go implementation handles BM25 correctly
   - Removed complex AVX-512 assembly that had cross-platform compatibility issues

**Verification:**
- Local (Darwin ARM64): Builds successfully
- Remote (ancalagon Linux AMD64): Builds successfully

**Key Takeaway:** Always use `git fetch/reset` to sync code between hosts. Never use rsync. Test builds on all target platforms before pushing.

---

## Future Work (v0.2.2+)

## Future Work (v0.2.2+)

- [ ] Define next architectural milestones for Longbow.
- [ ] Evaluate further GPU compute offloading opportunities.
- [ ] Plan cross-region replication optimizations.
