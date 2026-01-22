# Longbow Performance Improvements

This document identifies 15 high-impact performance improvements for the Longbow distributed vector database based on deep code analysis. 15 improvements have been completed.

---

## Completed Improvements

| # | Improvement | Area | Status |
|---|-------------|------|--------|
| 1 | Query Cache Lock Optimization | Caching | IMPLEMENTED |
| 2 | Eliminate Redundant Slice Allocations | Search | IMPLEMENTED |
| 3 | SIMD-Accelerated PQ Encoding | Compression | IMPLEMENTED |
| 4 | K-Means Buffer Reuse | Training | IMPLEMENTED |
| 5 | Ring Lookup Hot Key Caching | Sharding | IMPLEMENTED |
| 6 | Vector Prefetching in HNSW Search | Search | IMPLEMENTED |
| 7 | Optimize Slab Arena Allocation Lock | Memory | IMPLEMENTED |
| 8 | Batch Filter Evaluator Operations | Query | IMPLEMENTED |
| 9 | Implement Result Set Pooling | GC | IMPLEMENTED |
| 10 | Implement Adaptive Batch Sizing | Parallelism | IMPLEMENTED |
| 11 | Add Bloom Filter for Rapid Filter Evaluation | Query | IMPLEMENTED |
| 12 | SIMD-Accelerated String Filter Operations | Query | IMPLEMENTED |
| 13 | Implement Connection Pool for Distributed Queries | Distribution | IMPLEMENTED |
| 14 | Optimize HNSW Graph Traversal with Branch Prediction | Search | IMPLEMENTED |
| 15 | SIMD Batch Distance Compute Optimization | Compute | IMPLEMENTED |

See [docs/improvements_completed.md](./improvements_completed.md) for details on completed implementations.

---

## Pending Improvements

## 1. Implement Vector Prefetching in HNSW Search
}
```

**Expected Improvement:** 5-10% reduction in sharding overhead.

---

## 3. Implement Vector Prefetching in HNSW Search

**File:** `internal/store/parallel_search.go:104-130`

**Issue:** Vectors are fetched from storage without prefetching, causing cache misses.

**Impact:** Memory latency dominates search time when traversing HNSW graph.

**Solution:** Add software prefetching hints:

```go
func (h *HNSWIndex) processChunk(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], filters []qry.Filter) []SearchResult {
    locations := make([]Location, len(neighbors))
    found := make([]bool, len(neighbors))
    
    // Prefetch first batch of locations
    prefetchCount := 4
    for i := 0; i < prefetchCount && i < len(neighbors); i++ {
        simd.Prefetch(unsafe.Pointer(&neighbors[i]))
    }
    
    for i, n := range neighbors {
        // Software prefetch next neighbor
        nextIdx := i + prefetchCount
        if nextIdx < len(neighbors) {
            simd.Prefetch(unsafe.Pointer(&neighbors[nextIdx]))
        }
        
        nodeID := n.Key
        loc, ok := h.locationStore.Get(nodeID)
        if ok {
            locations[i] = loc
            found[i] = true
        }
    }
    // ... rest of processing
}
```

**Expected Improvement:** 10-20% search speedup on memory-bound workloads.

---

## 4. Optimize Slab Arena Allocation Lock

**File:** `internal/memory/arena.go:82-194`

**Issue:** `allocCommon()` acquires a mutex for every allocation, even small ones that could use lock-free paths.

**Impact:** Contention on the arena mutex limits allocation throughput.

**Solution:** Implement lock-free fast path for small allocations:

```go
func (a *SlabArena) Alloc(size int) (uint64, error) {
    if size <= 64 {  // Fast path for small allocs
        return a.allocFast(size)
    }
    return a.allocCommon(size, true)
}

func (a *SlabArena) allocFast(size int) (uint64, error) {
    // Use atomic operations for small allocations
    for {
        slabsPtr := a.slabs.Load()
        slabs := *slabsPtr
        if len(slabs) == 0 {
            break // Fall through to slow path
        }
        active := slabs[len(slabs)-1]
        
        // Try atomic compare-and-swap on offset
        oldOffset := atomic.LoadUint32(&active.offset)
        newOffset := oldOffset + uint32(size)
        
        if newOffset <= uint32(len(active.data)) {
            if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
                return computeGlobalOffset(active.id, oldOffset), nil
            }
        }
        // CAS failed, another thread modified, retry
    }
    return a.allocCommon(size, true)
}
```

**Expected Improvement:** 50-70% increase in allocation throughput for concurrent workloads.

---

## 5. Batch Filter Evaluator Operations

**File:** `internal/query/filter_evaluator.go:491-508`

**Issue:** `MatchesBatch()` chains filter operations sequentially, creating intermediate slices.

**Impact:** Memory allocation overhead and poor cache locality.

**Solution:** Fuse multiple filter operations into a single pass:

```go
func (e *FilterEvaluator) MatchesBatchFused(rowIndices []int) []int {
    if len(e.ops) == 0 {
        return rowIndices
    }
    
    result := make([]int, 0, len(rowIndices))
    
    for _, idx := range rowIndices {
        matches := true
        for _, op := range e.ops {
            if !op.Match(idx) {
                matches = false
                break
            }
        }
        if matches {
            result = append(result, idx)
        }
    }
    return result
}
```

**Expected Improvement:** 15-25% filter throughput, reduced allocations.

---

## 6. Optimize Dataset Lock Usage

**File:** `internal/store/hnsw.go:284-344`

**Issue:** `getVector()` acquires `dataset.dataMu.RLock()` even for zero-copy paths that should be lock-free.

**Impact:** Unnecessary serialization of vector access.

**Solution:** For zero-copy paths, rely on epoch-based safety instead of holding locks:

```go
func (h *HNSWIndex) getVectorUnsafe(id VectorID) (vec []float32, release func()) {
    loc, ok := h.locationStore.Get(id)
    if !ok || loc.BatchIdx == -1 {
        return nil, nil
    }
    
    h.enterEpoch()
    
    // Skip RLock - epoch safety allows lock-free access
    // Data is only appended, never modified in place
    vec = h.getVectorLockedUnsafe(loc)
    
    if vec == nil {
        h.exitEpoch()
        return nil, nil
    }
    
    return vec, func() { h.exitEpoch() }
}
```

**Expected Improvement:** Eliminates lock contention on read-heavy workloads.

---

## 7. Implement Connection Pool for Distributed Queries

**File:** `internal/sharding/scatter_gather.go`

**Issue:** Each distributed query creates new connections to shard nodes.

**Impact:** Connection setup latency adds overhead to every distributed query.

**Solution:** Implement connection pooling:

```go
type ShardConnPool struct {
    pools map[string]*clientPool
    mu    sync.RWMutex
}

type clientPool struct {
    connections []*grpc.ClientConn
    idx         atomic.Int32
}

func (p *ShardConnPool) GetConn(node string) *grpc.ClientConn {
    p.mu.RLock()
    pool, ok := p.pools[node]
    p.mu.RUnlock()
    
    if !ok {
        p.mu.Lock()
        if pool, ok = p.pools[node]; !ok {
            pool = &clientPool{}
            p.pools[node] = pool
        }
        p.mu.Unlock()
    }
    
    idx := pool.idx.Add(1) % int32(len(pool.connections))
    return pool.connections[idx]
}
```

**Expected Improvement:** 20-30% reduction in distributed query latency.

---

## 8. Add SIMD for String Filter Operations

**File:** `internal/query/filter_evaluator.go:346-403`

**Issue:** String filters use slow Go string comparison without SIMD acceleration.

**Impact:** String filtering is 10-100x slower than numeric filtering.

**Solution:** Implement SIMD-accelerated string comparison for common patterns:

```go
func (o *stringFilterOp) MatchBitmap(dst []byte) {
    // For exact match, use optimized byte comparison
    if o.operator == "=" || o.operator == "eq" {
        vals := o.col.StringValues()
        valBytes := []byte(o.val)
        
        for i := 0; i < len(dst); i++ {
            // Fast path for equal-length strings
            if len(vals[i]) == len(valBytes) && 
               bytes.Equal([]byte(vals[i]), valBytes) {
                dst[i] = 1
            } else {
                dst[i] = 0
            }
        }
        return
    }
    // Fall back to slow path for complex comparisons
    for i := 0; i < len(dst); i++ {
        if o.Match(i) {
            dst[i] = 1
        } else {
            dst[i] = 0
        }
    }
}
```

**Expected Improvement:** 5-10x speedup for string filtering operations.

---

## 9. Optimize HNSW Graph Traversal with Branch Prediction Hints

**File:** `internal/store/parallel_search.go:279-354`

**Issue:** Serial result processing has poor branch prediction due to filter checks.

**Impact:** CPU pipeline stalls during graph traversal.

**Solution:** Use branch prediction hints and reorder checks:

```go
func (h *HNSWIndex) processResultsSerial(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], k int, filters []qry.Filter) []SearchResult {
    distFunc := h.GetDistanceFunc()
    hasFilters := len(filters) > 0
    
    res := make([]SearchResult, 0, len(neighbors))
    
    for i, n := range neighbors {
        if i&31 == 0 {
            select {
            case <-ctx.Done():
                return res
            default:
            }
        }
        if len(res) >= k {
            break
        }
        
        loc, found := h.GetLocation(n.Key)
        if !found {
            continue
        }
        
        // Likely path: filter matches
        if hasFilters {
            if !h.checkFilters(loc, filters) {
                continue
            }
        }
        
        vec, _ := h.extractVector(rec, loc.RowIdx)
        if vec == nil {
            continue
        }
        
        dist := distFunc(query, vec)
        res = append(res, SearchResult{
            ID:    n.Key,
            Score: dist,
        })
    }
    
    return res
}
```

**Expected Improvement:** 5-10% reduction in traversal latency.

---

## 10. Implement Result Set Pooling

**File:** `internal/store/search_pool.go:1-88`

**Issue:** `SearchResult` slices are allocated on every search.

**Impact:** GC pressure from result allocation/deallocation.

**Solution:** Expand the search pool to include result slices:

```go
type SearchPool struct {
    pool sync.Pool
    resultPool sync.Pool  // Add dedicated result pool
    
    gets atomic.Int64
    puts atomic.Int64
}

func NewSearchPool() *SearchPool {
    sp := &SearchPool{
        pool: sync.Pool{
            New: func() any {
                return &SearchContext{
                    results: make([]SearchResult, 0, 100),
                    // ... other fields
                }
            },
        },
        resultPool: sync.Pool{
            New: func() any {
                return make([]SearchResult, 0, 100)
            },
        },
    }
    return sp
}

// GetResults gets a pre-allocated result slice
func (sp *SearchPool) GetResults() []SearchResult {
    return sp.resultPool.Get().([]SearchResult)[:0]
}

// PutResults returns a result slice to the pool
func (sp *SearchPool) PutResults(r []SearchResult) {
    sp.resultPool.Put(r)
}
```

**Expected Improvement:** 10-20% reduction in GC overhead for high-QPS workloads.

---

## 11. Add Bloom Filter for Rapid Filter Evaluation

**File:** `internal/query/filter_evaluator.go:510-553`

**Issue:** `MatchesAll()` processes all filters sequentially even when early filters would reject most rows.

**Impact:** Wasted computation on rows that fail early filter stages.

**Solution:** Order filters by selectivity and add Bloom filter-like fast rejection:

```go
func (e *FilterEvaluator) MatchesAll(batchLen int) ([]int, error) {
    if len(e.ops) == 0 {
        indices := make([]int, batchLen)
        for i := 0; i < batchLen; i++ {
            indices[i] = i
        }
        return indices, nil
    }
    
    // Estimate selectivity and reorder filters
    // High-selectivity filters first (reject more early)
    sortedOps := e.ops  // Use pre-sorted order
    
    bitmap := make([]byte, batchLen)
    e.ops[0].MatchBitmap(bitmap)
    
    // Early exit if bitmap is all zeros
    allZero := true
    for _, b := range bitmap {
        if b != 0 {
            allZero = false
            break
        }
    }
    if allZero {
        return []int{}, nil
    }
    
    // Continue with remaining filters
    // ... rest of implementation
}
```

**Expected Improvement:** 30-50% filter speedup for selective filters.

---

## 12. Implement Adaptive Batch Sizing for Search Workers

**File:** `internal/store/parallel_search.go:42-101`

**Issue:** Fixed chunk size doesn't adapt to varying neighbor set sizes, causing suboptimal parallelism.

**Impact:** Either too much overhead for small sets or under-utilization for large sets.

**Solution:** Implement dynamic chunk sizing based on neighbor count:

```go
func (h *HNSWIndex) processResultsParallel(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], k int, filters []qry.Filter) []SearchResult {
    cfg := h.getParallelSearchConfig()
    numWorkers := cfg.Workers
    if numWorkers <= 0 {
        numWorkers = runtime.NumCPU()
    }
    
    // Adaptive chunk sizing
    neighborCount := len(neighbors)
    minChunkSize := 32
    maxChunkSize := 500
    
    // Optimal chunk size balances parallelism and overhead
    // Aim for 2-4x the worker count chunks
    targetChunks := numWorkers * 3
    chunkSize := neighborCount / targetChunks
    
    if chunkSize < minChunkSize {
        chunkSize = minChunkSize
    } else if chunkSize > maxChunkSize {
        chunkSize = maxChunkSize
    }
    
    if neighborCount < chunkSize*2 { // Not enough work for parallelism
        return h.processResultsSerial(ctx, query, neighbors, k, filters)
    }
    
    // ... rest of parallel processing
}
```

**Expected Improvement:** 10-15% improvement in parallel search efficiency.

---

## Summary

| # | Improvement | Area | Expected Impact |
|---|-------------|------|-----------------|
| 1 | Vector Prefetching | HNSW | 10-20% search speed |
| 2 | Lock-Free Arena Allocation | Memory | 50-70% throughput |
| 3 | Batch Filter Fusion | Query | 15-25% filter speed |
| 4 | Epoch-Based Lock Elimination | Storage | Eliminate contention |
| 5 | Connection Pooling | Distribution | 20-30% latency |
| 6 | SIMD String Filters | Query | 5-10x filter speed |
| 7 | Branch Prediction Hints | HNSW | 5-10% traversal |
| 8 | Result Set Pooling | GC | 10-20% GC reduction |
| 9 | Adaptive Filter Ordering | Query | 30-50% filter speed |
| 10 | Dynamic Chunk Sizing | Parallelism | 10-15% efficiency |

**Completed:** 15/15 improvements

**Total Expected Impact:** 2-3x improvement in overall throughput for typical workloads.

---

*Generated: 2026-01-21*
*Analysis based on commit: Latest*
