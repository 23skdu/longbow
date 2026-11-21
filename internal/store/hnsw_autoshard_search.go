package store

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
)

// SearchVectors returns the k nearest neighbors for a query vector.
func (idx *AutoShardingIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectors(ctx, q, k, filters, options)
	}

	// If no migration is active, search monolith only
	if interim == nil {
		return curr.SearchVectors(ctx, q, k, filters, options)
	}

	// Parallel Shadow Search: Query both indices concurrently
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectors(ctx, q, k, filters, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectors(ctx, q, k, filters, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1 // Return first error if both fail
	}

	return idx.mergeSearchResults(res1, res2, k), nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (idx *AutoShardingIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	if interim == nil {
		return curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	// Parallel Shadow Search
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	return idx.mergeSearchResults(res1, res2, k), nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (idx *AutoShardingIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}

	if interim == nil {
		return curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}

	// Parallel Shadow Search
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	combined := append(res1, res2...)
	return combined, nil
}

func (idx *AutoShardingIndex) mergeSearchResults(res1, res2 []SearchResult, k int) []SearchResult {
	if len(res1) == 0 {
		if len(res2) > k {
			return res2[:k]
		}
		return res2
	}
	if len(res2) == 0 {
		if len(res1) > k {
			return res1[:k]
		}
		return res1
	}

	combined := make([]SearchResult, 0, len(res1)+len(res2))
	combined = append(combined, res1...)
	combined = append(combined, res2...)

	// Sort by score ascending (lower is better for HNSW distances in Longbow)
	sort.Slice(combined, func(i, j int) bool {
		return combined[i].Score < combined[j].Score
	})

	if len(combined) > k {
		return combined[:k]
	}
	return combined
}

// IsSharded returns true if the index has been upgraded to a sharded implementation.
func (idx *AutoShardingIndex) Search(ctx context.Context, q any, k int, options any) ([]types.Candidate, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	idx.mu.RUnlock()

	if sharded || interim == nil {
		return curr.Search(ctx, q, k, options)
	}

	// Parallel Shadow Search: Query both indices concurrently
	var res1, res2 []types.Candidate
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.Search(ctx, q, k, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.Search(ctx, q, k, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	return idx.mergeCandidates(res1, res2, k), nil
}

func (idx *AutoShardingIndex) mergeCandidates(res1, res2 []types.Candidate, k int) []types.Candidate {
	if len(res1) == 0 {
		if len(res2) > k {
			return res2[:k]
		}
		return res2
	}
	if len(res2) == 0 {
		if len(res1) > k {
			return res1[:k]
		}
		return res1
	}

	combined := make([]types.Candidate, 0, len(res1)+len(res2))
	combined = append(combined, res1...)
	combined = append(combined, res2...)

	// Deduplicate by ID
	seen := make(map[uint32]struct{})
	unique := make([]types.Candidate, 0, len(combined))
	for _, cand := range combined {
		if _, ok := seen[cand.ID]; !ok {
			seen[cand.ID] = struct{}{}
			unique = append(unique, cand)
		}
	}

	// Sort by distance ascending
	sort.Slice(unique, func(i, j int) bool {
		return unique[i].Dist < unique[j].Dist
	})

	if len(unique) > k {
		return unique[:k]
	}
	return unique
}

// Warmup delegates to the current index.
func (idx *AutoShardingIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Primarily check current index
	neighbors, err := idx.current.GetRawNeighbors(id)
	if err == nil {
		return neighbors, nil
	}

	// If not found and merging, check interim
	if idx.interimIndex != nil {
		return idx.interimIndex.GetRawNeighbors(id)
	}

	return nil, err
}

// GetNeighbors returns the k nearest neighbors for a given vector ID as SearchResults.
func (idx *AutoShardingIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Primarily check current index
	res, err := idx.current.GetNeighbors(ctx, id, k)
	if err == nil {
		return res, nil
	}

	// If not found and merging, check interim
	if idx.interimIndex != nil {
		return idx.interimIndex.GetNeighbors(ctx, id, k)
	}

	return nil, err
}

// EstimateMemory implements VectorIndex.
func (idx *AutoShardingIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportState()
}

// ImportState restores the index state from a byte slice.
func (idx *AutoShardingIndex) ImportState(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ImportState(data)
}

// ExportGraph serializes the graph structure to a writer.
func (idx *AutoShardingIndex) ExportGraph(w io.Writer) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportGraph(w)
}

// ImportGraph restores the graph structure from a reader.
func (idx *AutoShardingIndex) ImportGraph(r io.Reader) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ImportGraph(r)
}

// ExportDelta returns a delta sync object since the given version.
func (idx *AutoShardingIndex) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportDelta(fromVersion)
}

// ApplyDelta applies a delta sync object to the index.
func (idx *AutoShardingIndex) ApplyDelta(delta *types.DeltaSync) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ApplyDelta(delta)
}

// GetParallelSearchConfig returns the current parallel search configuration.
func (idx *AutoShardingIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetParallelSearchConfig()
}

// SetParallelSearchConfig updates the parallel search configuration
func (idx *AutoShardingIndex) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.current.SetParallelSearchConfig(cfg)
	if idx.interimIndex != nil {
		idx.interimIndex.SetParallelSearchConfig(cfg)
	}
}

// RemapLocations updates storage locations for physical movement (compaction).
func (idx *AutoShardingIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.RemapLocations(ctx, mapping)
}
