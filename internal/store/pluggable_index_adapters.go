package store

import (
	"context"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

	if len(h.vectors) == 0 {
		return []IndexSearchResult{}, nil
	}

	type result struct {
		id   uint64
		dist float32
	}
	results := make([]result, 0, len(h.vectors))

	for id, vec := range h.vectors {
		var dist float32
		for i := 0; i < len(vec) && i < len(query); i++ {
			diff := vec[i] - query[i]
			dist += diff * diff
		}
		results = append(results, result{id: id, dist: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	if k > len(results) {
		k = len(results)
	}

	finalResults := make([]IndexSearchResult, k)
	for i := 0; i < k; i++ {
		finalResults[i] = IndexSearchResult{ID: results[i].id, Distance: results[i].dist}
	}
	return finalResults, nil
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
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(h.vectors)
}

func (h *HNSWPluggableAdapter) ExportState() ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(h.vectors); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *HNSWPluggableAdapter) ImportState(data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return gob.NewDecoder(bytes.NewReader(data)).Decode(&h.vectors)
}

func (h *HNSWPluggableAdapter) Load(path string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewDecoder(f).Decode(&h.vectors)
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
// AddByRecord implements VectorIndexer. Extracts vectors from Arrow record and adds to index.
func (a *PluggableInternalAdapter) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	fieldIndices := rec.Schema().FieldIndices("vector")
	if len(fieldIndices) == 0 {
		return 0, fmt.Errorf("column 'vector' not found")
	}
	col := rec.Column(fieldIndices[0])
	list, ok := col.(*array.FixedSizeList)
	if !ok {
		return 0, fmt.Errorf("column 'vector' is not a FixedSizeList")
	}
	values := list.ListValues().(*array.Float32).Float32Values()
	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * listSize
	vec := make([]float32, listSize)
	copy(vec, values[start:start+listSize])

	// Use sequential ID for simplicity
	id := uint64(a.inner.Size())
	err := a.inner.Add(id, vec)
	return uint32(id), err // #nosec G115
}

// AddByLocation implements VectorIndexer.
func (a *PluggableInternalAdapter) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	return 0, fmt.Errorf("adaptive index bridge: incremental AddByLocation not supported (requires dataset access)")
}

// AddBatch implements VectorIndexer.
func (a *PluggableInternalAdapter) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		id, err := a.AddByRecord(ctx, recs[0], rowIdxs[i], batchIdxs[i]) // Simplified: assumes all rows in same record for now
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
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
// Filters are not supported by PluggableVectorIndex - returns error if filters provided.
func (a *PluggableInternalAdapter) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]lbtypes.SearchResult, error) {
	vec, ok := q.([]float32)
	if !ok {
		if f64, ok := q.([]float64); ok {
			vec = make([]float32, len(f64))
			for i, v := range f64 {
				vec[i] = float32(v)
			}
		} else {
			return nil, fmt.Errorf("query must be []float32 or []float64")
		}
	}

	if len(filters) > 0 {
		return nil, fmt.Errorf("PluggableInternalAdapter: filters not supported")
	}

	iresults, err := a.inner.Search(vec, k)
	if err != nil {
		return nil, err
	}

	results := make([]lbtypes.SearchResult, len(iresults))
	for i, r := range iresults {
		results[i] = lbtypes.SearchResult{ID: core.VectorID(r.ID), Distance: r.Distance} // #nosec G115
	}
	return results, nil
}

// SearchVectorsWithBitmap implements VectorIndexer.
// BitMap filters are not supported by PluggableVectorIndex - returns error if bm provided.
func (a *PluggableInternalAdapter) SearchVectorsWithBitmap(ctx context.Context, q any, k int, bm *roaring.Bitmap, options any) ([]lbtypes.SearchResult, error) {
	vec, ok := q.([]float32)
	if !ok {
		if f64, ok := q.([]float64); ok {
			vec = make([]float32, len(f64))
			for i, v := range f64 {
				vec[i] = float32(v)
			}
		} else {
			return nil, fmt.Errorf("query must be []float32 or []float64")
		}
	}

	if bm != nil && !bm.IsEmpty() {
		return nil, fmt.Errorf("PluggableInternalAdapter: bitmap filters not supported")
	}

	iresults, err := a.inner.Search(vec, k)
	if err != nil {
		return nil, err
	}

	results := make([]lbtypes.SearchResult, len(iresults))
	for i, r := range iresults {
		results[i] = lbtypes.SearchResult{ID: core.VectorID(r.ID), Distance: r.Distance} // #nosec G115
	}
	return results, nil
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

func (a *PluggableInternalAdapter) GetGPUIndex() any {
	if g, ok := a.inner.(interface{ GetGPUIndex() any }); ok {
		return g.GetGPUIndex()
	}
	return nil
}
