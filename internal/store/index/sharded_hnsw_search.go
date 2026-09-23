package index

import (
	"context"
	"fmt"
	"sort"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"golang.org/x/sync/errgroup"
)

// SearchVectors finds the k-nearest neighbors using similarity search across all shards.
func (idx *ShardedHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	// 1. Optimization: Try bitmap-based filtering
	if len(filters) > 0 && idx.dataset != nil {
		var filterExpr types.FilterExpr
		if opts, ok := options.(types.SearchOptions); ok {
			filterExpr = opts.FilterExpr
		}
		bitset, err := idx.dataset.GenerateFilterBitset(filters, filterExpr)
		if err == nil && bitset != nil {
			defer bitset.Release()
			res, _ := idx.SearchVectorsWithBitmap(ctx, queryVec, k, bitset.AsRoaring(), options)
			// Sharded SearchVectorsWithBitmap already handles Local->Global mapping
			// and global bitset filtering.
			return res, nil
		}
	}

	// 2. Parallel Search across all shards (Fallback path)
	type shardResult struct {
		results  []SearchResult
		shardIdx int
	}

	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)

	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()

	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}

			res, err := shard.index.SearchVectors(ctx, queryVec, k*2, nil, options) // Oversample, evaluate filters on merge
			if err != nil {
				return err
			}
			ch <- shardResult{results: res, shardIdx: i}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	close(ch)

	// 2. Merge Results
	merged := make([]SearchResult, 0, k*len(currentShards))

	for sr := range ch {
		for _, r := range sr.results {
			// Convert LocalID to GlobalID
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
			if !ok {
				continue
			}
			r.ID = globalID

			// Re-check global filters if needed
			if len(filters) > 0 {
				_, ok := idx.locationStore.Get(globalID)
				if !ok {
					continue
				}
				// Evaluate filters here if needed.
			}

			merged = append(merged, r)
		}
	}

	// Filter Block (Redundant if shards filtered, but kept for safety/fallback)
	if len(filters) > 0 && idx.dataset != nil {
		idx.dataset.RLockData()
		if len(idx.dataset.GetRecords()) > 0 {
			// 1. Group row indices by BatchIdx
			type batchJob struct {
				rowIndices []int
				resultIdx  []int
			}
			batchJobs := make(map[int]*batchJob)

			for i, r := range merged {
				loc, ok := idx.locationStore.Get(r.ID)
				if !ok || loc.BatchIdx >= len(idx.dataset.GetRecords()) {
					continue
				}
				job, ok := batchJobs[loc.BatchIdx]
				if !ok {
					job = &batchJob{}
					batchJobs[loc.BatchIdx] = job
				}
				job.rowIndices = append(job.rowIndices, loc.RowIdx)
				job.resultIdx = append(job.resultIdx, i)
			}

			// 2. Evaluate each batch
			filteredMask := make([]bool, len(merged))
			for bIdx, job := range batchJobs {
				ev, err := query.NewFilterEvaluator(idx.dataset.GetRecords()[bIdx], filters)
				if err != nil {
					continue
				}
				matches := ev.MatchesBatch(job.rowIndices)

				// Create a quick lookup for matched row indices in this batch
				matchMap := make(map[int]struct{}, len(matches))
				for _, m := range matches {
					matchMap[m] = struct{}{}
				}

				// Mark results in the filteredMask
				for k, rowIdx := range job.rowIndices {
					if _, matched := matchMap[rowIdx]; matched {
						resIdx := job.resultIdx[k]
						filteredMask[resIdx] = true
					}
				}
			}

			// 3. Rebuild filtered results
			filtered := merged[:0]
			for i, r := range merged {
				if filteredMask[i] {
					filtered = append(filtered, r)
				}
			}
			merged = filtered
		}
		idx.dataset.RUnlockData()
	}

	// Sort and limit (ascending - lower distance/score is better)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged, nil
}

// SearchVectorsWithBitmap finds k-nearest neighbors using a bitmap filter.
func (idx *ShardedHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	type shardResult struct {
		results  []SearchResult
		shardIdx int
		err      error
	}
	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)

	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()

	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}

			// Pass nil filter to shard, filter globally
			res, err := shard.index.SearchVectorsWithBitmap(ctx, queryVec, k*2, nil, options)
			if err != nil {
				return err
			}
			ch <- shardResult{results: res, shardIdx: i}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(ch)

	// Check for errors first
	merged := make([]SearchResult, 0, k*2)
	for sr := range ch {
		for _, r := range sr.results {
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
			if !ok {
				continue
			}

			// Global Bitset Filter
			if filter != nil && !filter.Contains(uint32(globalID)) {
				continue
			}

			r.ID = globalID
			merged = append(merged, r)
		}
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged, nil
}

// SearchVectorsInRange finds all vectors within a given distance threshold.
func (idx *ShardedHNSW) SearchVectorsInRange(ctx context.Context, queryVec any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	type shardResult struct {
		results  []SearchResult
		shardIdx int
		err      error
	}
	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)

	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()

	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}

			res, err := shard.index.SearchVectorsInRange(ctx, queryVec, threshold, nil, options)
			if err != nil {
				return err
			}
			ch <- shardResult{results: res, shardIdx: i}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(ch)

	var merged []SearchResult
	for sr := range ch {
		if sr.err != nil {
			continue
		}
		for _, r := range sr.results {
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
			if !ok {
				continue
			}
			r.ID = globalID

			if len(filters) > 0 {
				_, ok := idx.locationStore.Get(globalID)
				if !ok {
					continue
				}
			}
			merged = append(merged, r)
		}
	}

	if len(filters) > 0 && idx.dataset != nil {
		idx.dataset.RLockData()
		if len(idx.dataset.GetRecords()) > 0 {
			filtered := merged[:0]
			for _, r := range merged {
				loc, ok := idx.locationStore.Get(r.ID)
				if !ok || loc.BatchIdx >= len(idx.dataset.GetRecords()) {
					continue
				}
				filtered = append(filtered, r)
			}
			merged = filtered
		}
		idx.dataset.RUnlockData()
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	return merged, nil
}

// SearchByID searches for vectors similar to the vector at the given ID.
func (idx *ShardedHNSW) SearchByID(ctx context.Context, id VectorID, k int) []VectorID {
	if k <= 0 {
		return nil
	}

	loc, ok := idx.locationStore.Get(id)
	if !ok {
		return nil
	}

	// Retrieve vector from dataset
	idx.dataset.RLockData()
	if loc.BatchIdx >= len(idx.dataset.GetRecords()) {
		idx.dataset.RUnlockData()
		return nil
	}
	rec := idx.dataset.GetRecords()[loc.BatchIdx]
	idx.dataset.RUnlockData()

	// Find vector column
	colIdx := -1
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		colIdx = 0
	}

	vec, err := ExtractVectorFromArrow(rec, loc.RowIdx, colIdx)
	if err != nil {
		return nil
	}

	// Perform global search
	results, err := idx.SearchVectors(ctx, vec, k, nil, types.SearchOptions{})
	if err != nil {
		return nil
	}

	ids := make([]VectorID, len(results))
	for i, r := range results {
		ids[i] = VectorID(r.ID)
	}
	return ids
}

// GetRawNeighbors returns the internal IDs of nearest neighbors.
func (idx *ShardedHNSW) GetRawNeighbors(id uint32) ([]uint32, error) {
	shardIdx := idx.GetShardForID(VectorID(id))

	idx.shardsMu.RLock()
	if shardIdx >= len(idx.shards) || idx.shards[shardIdx] == nil {
		idx.shardsMu.RUnlock()
		return nil, fmt.Errorf("invalid shard index")
	}
	shard := idx.shards[shardIdx]
	idx.shardsMu.RUnlock()

	loc, ok := idx.globalToLocal.Get(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector id not found in mapping")
	}
	localID := uint32(loc.BatchIdx) // #nosec G115

	// Get local neighbors
	localNeighbors, err := shard.index.GetRawNeighbors(localID)
	if err != nil {
		return nil, err
	}

	// Map to Global IDs
	globalNeighbors := make([]uint32, 0, len(localNeighbors))
	for _, ln := range localNeighbors {
		globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(ln)})
		if !ok {
			continue
		}
		globalNeighbors = append(globalNeighbors, uint32(globalID))
	}
	return globalNeighbors, nil
}

// GetNeighbors returns the k nearest neighbors for a given vector ID as SearchResults.
func (idx *ShardedHNSW) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	neighbors, err := idx.GetRawNeighbors(id)
	if err != nil {
		return nil, err
	}

	results := make([]types.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		results = append(results, types.SearchResult{
			ID: types.VectorID(neighbors[i]),
		})
	}

	return results, nil
}
