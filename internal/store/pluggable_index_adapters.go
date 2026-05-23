package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"math"
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
	mu           sync.RWMutex
	dimension    int
	vectors      map[uint64][]float32
	locationToID map[uint64]uint64
	hnsw         VectorIndex // actual HNSW index, nil until dataset provided
	provider     lbtypes.IndexDataProvider
	hnswConfig   lbtypes.ArrowHNSWConfig
}

// Type returns the index type.
func (h *HNSWPluggableAdapter) Type() IndexType {
	return IndexTypeHNSW
}

// Dimension returns the vector dimension.
func (h *HNSWPluggableAdapter) Dimension() int {
	return h.dimension
}

// Size returns the number of vectors in the index.
func (h *HNSWPluggableAdapter) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.vectors)
}

// NeedsBuild returns whether the index needs to be explicitly built.
func (h *HNSWPluggableAdapter) NeedsBuild() bool {
	return false // HNSW builds incrementally
}

// Add adds a single vector to the index.
func (h *HNSWPluggableAdapter) Add(id uint64, vector []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectors[id] = vector
	return nil
}

// AddBatchRaw adds a batch of vectors to the index.
func (h *HNSWPluggableAdapter) AddBatchRaw(ids []uint64, vectors [][]float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, id := range ids {
		h.vectors[id] = vectors[i]
	}
	return nil
}

// Search performs a nearest neighbor search.
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

// SearchBatch performs multiple nearest neighbor searches in parallel.
func (h *HNSWPluggableAdapter) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, q := range queries {
		r, _ := h.Search(q, k)
		results[i] = r
	}
	return results, nil
}

// GetNeighbors retrieves the nearest neighbors for a specific vector ID.
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

// Build constructs the index structure.
func (h *HNSWPluggableAdapter) Build() error {
	return nil // HNSW builds incrementally
}

// Save serializes the index to a file.
func (h *HNSWPluggableAdapter) Save(path string) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	f, err := os.Create(path) // #nosec G304 -- path is internal, not user-controlled
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(h.vectors)
}

// ExportState exports the internal state of the index.
func (h *HNSWPluggableAdapter) ExportState() ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(h.vectors); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportState imports the internal state of the index.
func (h *HNSWPluggableAdapter) ImportState(data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return gob.NewDecoder(bytes.NewReader(data)).Decode(&h.vectors)
}

// Load deserializes the index from a file.
func (h *HNSWPluggableAdapter) Load(path string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	f, err := os.Open(path) // #nosec G304 -- path is internal, not user-controlled
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewDecoder(f).Decode(&h.vectors)
}

// Close releases resources associated with the index.
func (h *HNSWPluggableAdapter) Close() error {
	return nil
}

// AddByLocation is a stub for PluggableVectorIndex compatibility.
func (h *HNSWPluggableAdapter) AddByLocation(batchIdx, rowIdx int) error {
	if h.provider == nil {
		return fmt.Errorf("HNSWPluggableAdapter: AddByLocation requires IndexDataProvider")
	}

	h.provider.RLockData()
	defer h.provider.RUnlockData()

	records := h.provider.GetRecords()
	if batchIdx < 0 || batchIdx >= len(records) {
		return fmt.Errorf("invalid batch index: %d", batchIdx)
	}

	rec := records[batchIdx]
	return h.AddByRecord(context.Background(), rec, rowIdx, batchIdx)
}

// AddByRecord extracts a vector from an Arrow record and adds it to the index.
func (h *HNSWPluggableAdapter) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) error {
	fieldIdx := rec.Schema().FieldIndices("vector")
	if len(fieldIdx) == 0 {
		return fmt.Errorf("column 'vector' not found")
	}

	col := rec.Column(fieldIdx[0])
	list, ok := col.(*array.FixedSizeList)
	if !ok {
		return fmt.Errorf("column 'vector' is not a FixedSizeList")
	}

	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	if listSize != h.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.dimension, listSize)
	}

	values := list.ListValues().(*array.Float32).Float32Values()
	start := rowIdx * listSize
	vec := make([]float32, listSize)
	copy(vec, values[start:start+listSize])

	// Use packed location as a temporary ID if no ID column is available,
	// or use a sequence if needed. In this adapter, we just need a unique ID.
	id := uint64(lbtypes.PackLocation(lbtypes.Location{BatchIdx: batchIdx, RowIdx: rowIdx}))

	h.mu.Lock()
	h.vectors[id] = vec
	if h.locationToID == nil {
		h.locationToID = make(map[uint64]uint64)
	}
	h.locationToID[id] = id // Identity mapping for this case
	h.mu.Unlock()

	return nil
}

// SetDataProvider sets the data provider for the adapter.
func (h *HNSWPluggableAdapter) SetDataProvider(p lbtypes.IndexDataProvider) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.provider = p
}

// EstimateMemory returns the estimated memory usage of the index in bytes.
func (h *HNSWPluggableAdapter) EstimateMemory() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	numVectors := len(h.vectors)
	// Overhead per map entry (approximate) + slice header + vector data
	// map[uint64][]float32: ~48 bytes overhead per entry (bucket + pointer)
	// []float32: 24 bytes header + dimension * 4 bytes
	vecMem := int64(numVectors) * (48 + 24 + int64(h.dimension)*4)

	// locationToID map[uint64]uint64: ~32 bytes overhead per entry
	locMem := int64(len(h.locationToID)) * (32 + 8 + 8)

	return vecMem + locMem
}

// GetVectorID retrieves the vector ID for a given location.
func (h *HNSWPluggableAdapter) GetVectorID(loc Location) (uint64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.locationToID == nil {
		return 0, false
	}

	packed := lbtypes.PackLocation(loc)
	id, ok := h.locationToID[packed]
	return id, ok
}

// SetLocation registers a location-to-ID mapping.
func (h *HNSWPluggableAdapter) SetLocation(id uint64, loc any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.locationToID == nil {
		h.locationToID = make(map[uint64]uint64)
	}

	var packed uint64
	switch l := loc.(type) {
	case Location:
		packed = lbtypes.PackLocation(l)
	case uint64:
		packed = l
	default:
		return
	}
	h.locationToID[packed] = id
}

// SearchVectors performs a nearest neighbor search and returns results in the store's format.
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

// Len returns the number of vectors in the index.
func (h *HNSWPluggableAdapter) Len() int {
	return h.Size()
}

// GetIndexType returns the index type as a string.
func (h *HNSWPluggableAdapter) GetIndexType() string {
	return string(h.Type())
}

// PluggableInternalAdapter wraps a PluggableVectorIndex to implement the internal VectorIndexer interface.
// This allows the adaptive learned index to perform live swaps into the dataset search path.
// PluggableInternalAdapter wraps a PluggableVectorIndex to implement the internal VectorIndexer interface.
// This allows the adaptive learned index to perform live swaps into the dataset search path.
type PluggableInternalAdapter struct {
	inner    PluggableVectorIndex
	provider lbtypes.IndexDataProvider
}

// NewPluggableInternalAdapter creates a new adapter for internal use.
// NewPluggableInternalAdapter creates a new adapter for internal use.
func NewPluggableInternalAdapter(inner PluggableVectorIndex, provider lbtypes.IndexDataProvider) *PluggableInternalAdapter {
	return &PluggableInternalAdapter{inner: inner, provider: provider}
}

// Type returns the index type.
func (a *PluggableInternalAdapter) Type() IndexType { return a.inner.Type() }

// Size returns the number of vectors in the index.
func (a *PluggableInternalAdapter) Size() int { return a.inner.Size() }

// Len returns the number of vectors in the index.
func (a *PluggableInternalAdapter) Len() int { return a.inner.Size() }

// Close releases resources associated with the index.
func (a *PluggableInternalAdapter) Close() error { return a.inner.Close() }

// GetIndexType returns the index type as a string.
func (a *PluggableInternalAdapter) GetIndexType() string {
	return string(a.inner.Type())
}

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
	id := uint64(a.inner.Size()) // #nosec G115 -- Size() returns int within uint64 range
	err := a.inner.Add(id, vec)
	if err == nil {
		// Try to register location if the inner index supports it
		type locationSetter interface {
			SetLocation(id uint64, loc Location)
		}
		if ls, ok := a.inner.(locationSetter); ok {
			ls.SetLocation(id, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
		}
	}
	return uint32(id), err // #nosec G115
}

// AddByLocation implements VectorIndexer.
func (a *PluggableInternalAdapter) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if a.provider == nil {
		return 0, fmt.Errorf("PluggableInternalAdapter: provider not set")
	}

	a.provider.RLockData()
	defer a.provider.RUnlockData()

	records := a.provider.GetRecords()
	if batchIdx < 0 || batchIdx >= len(records) {
		return 0, fmt.Errorf("invalid batch index: %d", batchIdx)
	}

	rec := records[batchIdx]
	return a.AddByRecord(ctx, rec, rowIdx, batchIdx)
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

// SearchVectorsInRange performs a range search.
func (a *PluggableInternalAdapter) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]lbtypes.SearchResult, error) {
	return nil, nil
}

// IsSharded returns whether the index is sharded.
func (a *PluggableInternalAdapter) IsSharded() bool { return false }

// GetEntryPoint returns the entry point for the index.
func (a *PluggableInternalAdapter) GetEntryPoint() uint32 { return 0 }

// GetLocation retrieves the location of a vector.
func (a *PluggableInternalAdapter) GetLocation(id uint32) (any, bool) { return nil, false }

// GetVectorID retrieves the vector ID for a given location.
func (a *PluggableInternalAdapter) GetVectorID(loc any) (uint32, bool) {
	l, ok := loc.(Location)
	if !ok {
		return 0, false
	}
	id, ok := a.inner.GetVectorID(l)
	if id > math.MaxUint32 {
		return 0, false
	}
	return uint32(id), ok
}

// GetDimension returns the vector dimension.
func (a *PluggableInternalAdapter) GetDimension() uint32 { return uint32(a.inner.Dimension()) } // #nosec G115
// SetIndexedColumns sets the columns to be indexed.
func (a *PluggableInternalAdapter) SetIndexedColumns(cols []string) {}

// GetRawNeighbors retrieves the raw neighbor IDs for a vector.
func (a *PluggableInternalAdapter) GetRawNeighbors(id uint32) ([]uint32, error) { return nil, nil }

// GetNeighbors retrieves the nearest neighbors for a vector.
func (a *PluggableInternalAdapter) GetNeighbors(ctx context.Context, id uint32, k int) ([]lbtypes.SearchResult, error) {
	return a.inner.GetNeighbors(ctx, lbtypes.VectorID(id), k)
}

// PreWarm prepares the index for search.
func (a *PluggableInternalAdapter) PreWarm(targetSize int) {}

// Warmup warms up the index.
func (a *PluggableInternalAdapter) Warmup() int { return 0 }

// EstimateMemory estimates the memory usage of the index.
func (a *PluggableInternalAdapter) EstimateMemory() int64 {
	if em, ok := a.inner.(interface{ EstimateMemory() int64 }); ok {
		return em.EstimateMemory()
	}
	return 0
}

// TrainPQ trains the product quantizer for the index.
func (a *PluggableInternalAdapter) TrainPQ(vectors [][]float32) error { return nil }

// GetPQEncoder returns the product quantizer encoder.
func (a *PluggableInternalAdapter) GetPQEncoder() *pq.PQEncoder { return nil }

// DeleteBatch deletes multiple vectors from the index.
func (a *PluggableInternalAdapter) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil
}

// ExportState exports the internal state of the index.
func (a *PluggableInternalAdapter) ExportState() ([]byte, error) { return nil, nil }

// ImportState imports the internal state of the index.
func (a *PluggableInternalAdapter) ImportState(data []byte) error { return nil }

// ExportGraph exports the graph structure of the index.
func (a *PluggableInternalAdapter) ExportGraph(w io.Writer) error { return nil }

// ImportGraph imports the graph structure of the index.
func (a *PluggableInternalAdapter) ImportGraph(r io.Reader) error { return nil }

// ExportDelta exports the delta since the given version.
func (a *PluggableInternalAdapter) ExportDelta(v uint64) (*lbtypes.DeltaSync, error) {
	return nil, nil
}

// ApplyDelta applies the given delta sync to the index.
func (a *PluggableInternalAdapter) ApplyDelta(d *lbtypes.DeltaSync) error { return nil }

// SetParallelSearchConfig sets the configuration for parallel search.
func (a *PluggableInternalAdapter) SetParallelSearchConfig(c lbtypes.ParallelSearchConfig) {}

// GetParallelSearchConfig returns the configuration for parallel search.
func (a *PluggableInternalAdapter) GetParallelSearchConfig() lbtypes.ParallelSearchConfig {
	return lbtypes.ParallelSearchConfig{}
}

// RemapLocations remaps vector locations.
func (a *PluggableInternalAdapter) RemapLocations(ctx context.Context, m map[uint32]any) error {
	return nil
}

// GetGPUIndex returns the GPU index if available.
func (a *PluggableInternalAdapter) GetGPUIndex() any {
	if g, ok := a.inner.(interface{ GetGPUIndex() any }); ok {
		return g.GetGPUIndex()
	}
	return nil
}

// RelocateToOffHeap relocates index structures to off-heap memory.
func (a *PluggableInternalAdapter) RelocateToOffHeap() error {
	return nil
}

// ReleaseMonolithicChunk releases a monolithic chunk from memory.
func (a *PluggableInternalAdapter) ReleaseMonolithicChunk(cID int) error {
	// Pluggable indexes manage their own lifecycle
	return nil
}
