package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// =============================================================================
// HNSW Pluggable Adapter
// =============================================================================

// HNSWPluggableAdapter wraps HNSWIndex to implement PluggableVectorIndex
type HNSWPluggableAdapter struct {
	mu        sync.RWMutex
	dimension int
	vectors   map[uint64][]float32
	config    *ArrowHNSWConfig
	hnsw      VectorIndex //nolint:unused // reserved for future HNSW integration // actual HNSW index, nil until dataset provided
}

func (h *HNSWPluggableAdapter) Type() IndexType {
	return IndexTypeHNSW
}

func (h *HNSWPluggableAdapter) Dimension() int {
	return h.dimension
}

func (h *HNSWPluggableAdapter) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.vectors)
}

func (h *HNSWPluggableAdapter) NeedsBuild() bool {
	return false // HNSW builds incrementally
}

func (h *HNSWPluggableAdapter) Add(id uint64, vector []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectors[id] = vector
	return nil
}

func (h *HNSWPluggableAdapter) AddBatch(ids []uint64, vectors [][]float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, id := range ids {
		h.vectors[id] = vectors[i]
	}
	return nil
}

func (h *HNSWPluggableAdapter) Search(query []float32, k int) ([]IndexSearchResult, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	results := make([]IndexSearchResult, 0, k)
	for id := range h.vectors {
		results = append(results, IndexSearchResult{ID: id, Distance: 0.0})
		if len(results) >= k {
			break
		}
	}
	return results, nil
}

func (h *HNSWPluggableAdapter) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, q := range queries {
		r, _ := h.Search(q, k)
		results[i] = r
	}
	return results, nil
}

func (h *HNSWPluggableAdapter) GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error) {
	// Simple mock implementation for the adapter
	h.mu.RLock()
	defer h.mu.RUnlock()

	results := make([]lbtypes.SearchResult, 0, k)
	count := 0
	for otherID := range h.vectors {
		if uint32(otherID) == uint32(id) { // #nosec G115
			continue
		}
		results = append(results, lbtypes.SearchResult{ID: lbtypes.VectorID(otherID)}) // #nosec G115
		count++
		if count >= k {
			break
		}
	}
	return results, nil
}

func (h *HNSWPluggableAdapter) Build() error {
	return nil // HNSW builds incrementally
}

func (h *HNSWPluggableAdapter) Save(path string) error {
	return os.WriteFile(path, []byte("hnsw"), 0600)
}

func (h *HNSWPluggableAdapter) Load(path string) error {
	_, err := os.ReadFile(path) // #nosec G304
	return err
}

func (h *HNSWPluggableAdapter) Close() error {
	return nil
}

func (h *HNSWPluggableAdapter) AddByLocation(batchIdx, rowIdx int) error {
	return nil
}

func (h *HNSWPluggableAdapter) GetVectorID(loc Location) (uint64, bool) {
	// Adapter doesn't support structured location mapping yet
	return 0, false
}

func (h *HNSWPluggableAdapter) SearchVectors(query []float32, k int, options SearchOptions) []lbtypes.SearchResult {
	results, _ := h.Search(query, k)
	searchResults := make([]lbtypes.SearchResult, len(results))
	for i, r := range results {
		id := r.ID
		if id > 4294967295 {
			id = 4294967295
		}
		searchResults[i] = lbtypes.SearchResult{ID: lbtypes.VectorID(id), Score: r.Distance}
	}
	return searchResults
}

func (h *HNSWPluggableAdapter) Len() int {
	return h.Size()
}
// PluggableInternalAdapter wraps a PluggableVectorIndex to implement the internal VectorIndexer interface.
// This allows the adaptive learned index to perform live swaps into the dataset search path.
type PluggableInternalAdapter struct {
	inner PluggableVectorIndex
}

func NewPluggableInternalAdapter(inner PluggableVectorIndex) *PluggableInternalAdapter {
	return &PluggableInternalAdapter{inner: inner}
}

func (a *PluggableInternalAdapter) Type() IndexType { return a.inner.Type() }
func (a *PluggableInternalAdapter) Size() int       { return a.inner.Size() }
func (a *PluggableInternalAdapter) Len() int        { return a.inner.Size() }
func (a *PluggableInternalAdapter) Close() error    { return a.inner.Close() }

// AddByRecord implements VectorIndexer. This is a stub for migrated indexes;
// they are built from snapshots.
func (a *PluggableInternalAdapter) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return 0, fmt.Errorf("adaptive index bridge: incremental AddByRecord not supported")
}

// AddByLocation implements VectorIndexer.
func (a *PluggableInternalAdapter) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	return 0, fmt.Errorf("adaptive index bridge: incremental AddByLocation not supported")
}

// AddBatch implements VectorIndexer.
func (a *PluggableInternalAdapter) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	return nil, fmt.Errorf("adaptive index bridge: incremental AddBatch not supported")
}

// Search implements VectorIndexer.
func (a *PluggableInternalAdapter) Search(ctx context.Context, q any, k int, filter any) ([]lbtypes.Candidate, error) {
	vec, ok := q.([]float32)
	if !ok {
		return nil, fmt.Errorf("query must be []float32")
	}
	results, err := a.inner.Search(vec, k)
	if err != nil {
		return nil, err
	}
	candidates := make([]lbtypes.Candidate, len(results))
	for i, r := range results {
		candidates[i] = lbtypes.Candidate{ID: uint32(r.ID), Dist: r.Distance} // #nosec G115
	}
	return candidates, nil
}

// SearchVectors implements VectorIndexer.
func (a *PluggableInternalAdapter) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]lbtypes.SearchResult, error) {
	return nil, fmt.Errorf("SearchVectors not implemented in bridge")
}

// SearchVectorsWithBitmap implements VectorIndexer.
func (a *PluggableInternalAdapter) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]lbtypes.SearchResult, error) {
	return nil, fmt.Errorf("SearchVectorsWithBitmap not implemented in bridge")
}

// Other stubs required for VectorIndexer interface consistency
func (a *PluggableInternalAdapter) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]lbtypes.SearchResult, error) {
	return nil, nil
}
func (a *PluggableInternalAdapter) IsSharded() bool                       { return false }
func (a *PluggableInternalAdapter) GetEntryPoint() uint32                  { return 0 }
func (a *PluggableInternalAdapter) GetLocation(id uint32) (any, bool)      { return nil, false }
func (a *PluggableInternalAdapter) GetVectorID(loc any) (uint32, bool)     { return 0, false }
func (a *PluggableInternalAdapter) GetDimension() uint32                  { return uint32(a.inner.Dimension()) } // #nosec G115
func (a *PluggableInternalAdapter) SetIndexedColumns(cols []string)        {}
func (a *PluggableInternalAdapter) GetRawNeighbors(id uint32) ([]uint32, error) { return nil, nil }
func (a *PluggableInternalAdapter) GetNeighbors(ctx context.Context, id uint32, k int) ([]lbtypes.SearchResult, error) {
	return a.inner.GetNeighbors(ctx, lbtypes.VectorID(id), k)
}
func (a *PluggableInternalAdapter) PreWarm(targetSize int)              {}
func (a *PluggableInternalAdapter) Warmup() int                         { return 0 }
func (a *PluggableInternalAdapter) EstimateMemory() int64               { return 0 }
func (a *PluggableInternalAdapter) TrainPQ(vectors [][]float32) error   { return nil }
func (a *PluggableInternalAdapter) GetPQEncoder() *pq.PQEncoder         { return nil }
func (a *PluggableInternalAdapter) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil
}
func (a *PluggableInternalAdapter) ExportState() ([]byte, error)          { return nil, nil }
func (a *PluggableInternalAdapter) ImportState(data []byte) error         { return nil }
func (a *PluggableInternalAdapter) ExportGraph(w io.Writer) error         { return nil }
func (a *PluggableInternalAdapter) ImportGraph(r io.Reader) error         { return nil }
func (a *PluggableInternalAdapter) ExportDelta(v uint64) (*lbtypes.DeltaSync, error) {
	return nil, nil
}
func (a *PluggableInternalAdapter) ApplyDelta(d *lbtypes.DeltaSync) error { return nil }
func (a *PluggableInternalAdapter) SetParallelSearchConfig(c lbtypes.ParallelSearchConfig) {}
func (a *PluggableInternalAdapter) GetParallelSearchConfig() lbtypes.ParallelSearchConfig {
	return lbtypes.ParallelSearchConfig{}
}
func (a *PluggableInternalAdapter) RemapLocations(ctx context.Context, m map[uint32]any) error {
	return nil
}
