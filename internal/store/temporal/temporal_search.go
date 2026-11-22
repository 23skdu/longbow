package temporal

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/mathutil"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// SearchAsOf performs a vector search considering only data available at a specific timestamp.
func (ti *TemporalIndex) SearchAsOf(ctx context.Context, timestamp int64, k int) ([]lbtypes.SearchResult, error) {
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	if k <= 0 {
		return []lbtypes.SearchResult{}, nil
	}

	cacheKey := fmt.Sprintf("asof:%d:%d", timestamp, k)
	if results, ok := ti.cache.Get(cacheKey); ok {
		return results, nil
	}

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			// R11b: this vector is deliberately left zeroed, and it reads like a
			// missing assignment. It is not. TemporalSearchRequest carries no query
			// vector - only a dataset, search type, k and a timestamp - so there is
			// nothing to populate. As-of search is an enumeration ("k records that
			// existed as of T"), not a similarity search.
			//
			// Routing an enumeration through the vector index therefore ranks the
			// visible records by distance to the origin, i.e. by norm, and the
			// distances returned are distances to the zero vector rather than any
			// measure of relevance. That is the pinned behaviour in
			// temporal_asof_semantics_test.go, and it is why temporal results must
			// not be compared against modes that take a real query vector.
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{}

			var results []lbtypes.SearchResult
			var err error

			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				bm := segmentTree.QueryRange(0, timestamp)
				results, err = vIdx.SearchVectorsWithBitmap(ctx, queryVec, k, bm, options)
			} else {
				pred := &TemporalPredicate{
					minTs:  0,
					maxTs:  timestamp,
					shards: &ti.shards,
				}
				options.Predicate = pred
				results, err = vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			}
			if err == nil {
				ti.cache.Set(cacheKey, results, 5*time.Minute)
				return results, nil
			}
		}
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Get latest version of each unique ID before or at timestamp
	validIDs := tree.GetUniqueIDsInRange(0, timestamp)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Batch lookup versions from history
	versionMap := temporalVersionMapPool.Get().(map[uint64]VersionedVector)
	defer func() {
		clear(versionMap)
		temporalVersionMapPool.Put(versionMap)
	}()

	if ti.history != nil {
		ti.history.GetVersionsAtBatch(validIDs, timestamp, versionMap)
	}

	// 3. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		var norm float32
		var found bool

		if v, ok := versionMap[id]; ok {
			norm = v.Norm
			found = true
		}

		// Fallback to latest norm if history lookup failed or disabled
		if !found {
			shard := ti.getShard(id)
			shard.mu.RLock()
			vec, ok := shard.data[id]
			shard.mu.RUnlock()
			if ok {
				if !vec.Tombstone {
					norm = vec.Norm
					found = true
				}
			}
		}

		if found {
			offerTemporalCandidate(h, id, norm, k)
		}
	}

	searchResults := drainTemporalTopK(h)

	ti.cache.Set(cacheKey, searchResults, 5*time.Minute)
	return searchResults, nil
}

// Prewarm populates the temporal cache with common search results.
func (ti *TemporalIndex) Prewarm(ctx context.Context) error {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return nil
	}

	tree.mu.RLock()
	if len(tree.leafRefs) == 0 {
		tree.mu.RUnlock()
		return nil
	}
	latestTs := tree.maxTs
	tree.mu.RUnlock()

	// 1. Latest results
	_, _ = ti.SearchAsOf(ctx, latestTs, 100)

	// 2. Common windows (if they contain data)
	now := time.Now().UnixNano()
	windows := []time.Duration{
		5 * time.Minute,
		1 * time.Hour,
		24 * time.Hour,
	}

	for _, d := range windows {
		start := now - d.Nanoseconds()
		_, _ = ti.SearchRange(ctx, start, now, 100)
	}

	return nil
}

// SearchRange performs a vector search over data within a specific timestamp range.
func (ti *TemporalIndex) SearchRange(ctx context.Context, startTime, endTime int64, k int) ([]lbtypes.SearchResult, error) {
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	if k <= 0 || startTime > endTime {
		return []lbtypes.SearchResult{}, nil
	}

	cacheKey := fmt.Sprintf("range:%d:%d:%d:%d", startTime, endTime, k, ti.mutationSeq.Load())
	if ti.cache != nil {
		if cached, ok := ti.cache.Get(cacheKey); ok {
			return cached, nil
		}
		// Coalesce concurrent cold-cache misses so multiple workers do not
		// materialize duplicate 1M-row range allocations simultaneously.
		ti.rangeFillMu.Lock()
		defer ti.rangeFillMu.Unlock()
		if cached, ok := ti.cache.Get(cacheKey); ok {
			return cached, nil
		}
	}

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{}

			var results []lbtypes.SearchResult
			var err error

			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				bm := segmentTree.QueryRange(startTime, endTime)
				results, err = vIdx.SearchVectorsWithBitmap(ctx, queryVec, k, bm, options)
			} else {
				pred := &TemporalPredicate{
					minTs:  startTime,
					maxTs:  endTime,
					shards: &ti.shards,
				}
				options.Predicate = pred
				results, err = vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			}
			if err == nil {
				if ti.cache != nil {
					ti.cache.Set(cacheKey, results, 5*time.Minute)
				}
				return results, nil
			}
		}
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Get unique IDs in range (most recent versions within range)
	validIDs := tree.GetUniqueIDsInRange(startTime, endTime)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Batch lookup versions from history
	versionMap := temporalVersionMapPool.Get().(map[uint64]VersionedVector)
	defer func() {
		clear(versionMap)
		temporalVersionMapPool.Put(versionMap)
	}()

	if ti.history != nil {
		ti.history.GetVersionsAtBatch(validIDs, endTime, versionMap)
	}

	// 3. Select top-k using max-heap. The candidate set is not capped: the
	// result is ranked by norm, so an exact top-k requires every id whose active
	// version falls in [startTime, endTime], regardless of how many that is.
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		var norm float32
		var found bool

		if v, ok := versionMap[id]; ok && v.Timestamp >= startTime {
			norm = v.Norm
			found = true
		}

		// Fallback to latest norm if historylookup failed or disabled
		if !found {
			shard := ti.getShard(id)
			shard.mu.RLock()
			vec, ok := shard.data[id]
			shard.mu.RUnlock()
			if ok {
				if !vec.Tombstone && vec.Timestamp >= startTime && vec.Timestamp <= endTime {
					norm = vec.Norm
					found = true
				}
			}
		}

		if found {
			offerTemporalCandidate(h, id, norm, k)
		}
	}

	searchResults := drainTemporalTopK(h)
	if ti.cache != nil {
		ti.cache.Set(cacheKey, searchResults, 5*time.Minute)
	}
	return searchResults, nil
}

// SearchSlidingWindow performs a search over the last n vector updates.
func (ti *TemporalIndex) SearchSlidingWindow(ctx context.Context, windowSize int, k int) ([]lbtypes.SearchResult, error) {
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	if windowSize <= 0 || k <= 0 {
		return []lbtypes.SearchResult{}, nil
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			validIDs := tree.GetUniqueLatest(windowSize)
			if len(validIDs) == 0 {
				return []lbtypes.SearchResult{}, nil
			}
			idMap := make(map[uint64]struct{})
			for _, id := range validIDs {
				idMap[id] = struct{}{}
			}
			pred := &SlidingWindowPredicate{
				validIDs: idMap,
			}
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{
				Predicate: pred,
			}
			results, err := vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			if err == nil {
				return results, nil
			}
		}
	}

	// Fallback: heap scan over the newest windowSize unique IDs. The sorted
	// index serves the tail, so this is O(windowSize) rather than O(N) for
	// distinct timestamps.
	validIDs := tree.GetUniqueLatest(windowSize)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if ok && !vec.Tombstone {
			offerTemporalCandidate(h, id, vec.Norm, k)
		}
	}

	return drainTemporalTopK(h), nil
}

// SearchSlidingWindowByTime performs a search over vector updates from the last duration.
func (ti *TemporalIndex) SearchSlidingWindowByTime(ctx context.Context, duration time.Duration, k int) ([]lbtypes.SearchResult, error) {
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	if duration < 0 || k <= 0 {
		return []lbtypes.SearchResult{}, nil
	}

	vIdx := ti.GetVectorIndex()
	now := time.Now().UnixNano()
	start := now - duration.Nanoseconds()

	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			pred := &TemporalPredicate{
				minTs:  start,
				maxTs:  now,
				shards: &ti.shards,
			}
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{
				Predicate: pred,
			}
			results, err := vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			if err == nil {
				return results, nil
			}
		}
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Each id contributes at most one result: its active version at the window
	// end, which must itself fall inside the window. Using the distinct-id walk
	// (rather than every entry) keeps duplicate versions from filling the k
	// slots, matching SearchRange.
	validIDs := tree.GetUniqueIDsInRange(start, now)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	versionMap := temporalVersionMapPool.Get().(map[uint64]VersionedVector)
	defer func() {
		clear(versionMap)
		temporalVersionMapPool.Put(versionMap)
	}()
	if ti.history != nil {
		ti.history.GetVersionsAtBatch(validIDs, now, versionMap)
	}

	// 2. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		var norm float32
		var found bool

		if v, ok := versionMap[id]; ok && v.Timestamp >= start && v.Timestamp <= now {
			norm = v.Norm
			found = true
		}

		if !found {
			shard := ti.getShard(id)
			shard.mu.RLock()
			vec, ok := shard.data[id]
			shard.mu.RUnlock()
			if ok && !vec.Tombstone && vec.Timestamp >= start && vec.Timestamp <= now {
				norm = vec.Norm
				found = true
			}
		}

		if found {
			offerTemporalCandidate(h, id, norm, k)
		}
	}

	return drainTemporalTopK(h), nil
}

// temporalMaxHeap implements heap.Interface for top-k temporal results. It is a
// max-heap by (distance, id): the root is the worst result currently retained,
// so ties on norm are broken deterministically by keeping the smaller id.
type temporalMaxHeap []temporalScoredResult

// Len runs the len operation.
func (h temporalMaxHeap) Len() int { return len(h) }

// Less runs the less operation.
func (h temporalMaxHeap) Less(i, j int) bool {
	if h[i].distance != h[j].distance {
		return h[i].distance > h[j].distance
	}
	return h[i].id > h[j].id
}

// Swap runs the swap operation.
func (h temporalMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push runs the push operation.
func (h *temporalMaxHeap) Push(x any) {
	*h = append(*h, x.(temporalScoredResult))
}

// Pop runs the pop operation.
func (h *temporalMaxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// offerTemporalCandidate keeps the k best (smallest norm, then smallest id)
// candidates in h. k <= 0 retains nothing.
func offerTemporalCandidate(h *temporalMaxHeap, id uint64, norm float32, k int) {
	if k <= 0 {
		return
	}
	if h.Len() < k {
		heap.Push(h, temporalScoredResult{id: id, distance: norm})
		return
	}
	worst := (*h)[0]
	if norm < worst.distance || (norm == worst.distance && id < worst.id) {
		heap.Pop(h)
		heap.Push(h, temporalScoredResult{id: id, distance: norm})
	}
}

// drainTemporalTopK empties h into ascending (distance, id) order.
func drainTemporalTopK(h *temporalMaxHeap) []lbtypes.SearchResult {
	limit := h.Len()
	searchResults := make([]lbtypes.SearchResult, limit)
	for i := limit - 1; i >= 0; i-- {
		res := heap.Pop(h).(temporalScoredResult)
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(res.id), // #nosec G115
			Distance: res.distance,
			Score:    1.0 / (1.0 + res.distance),
		}
	}
	return searchResults
}
