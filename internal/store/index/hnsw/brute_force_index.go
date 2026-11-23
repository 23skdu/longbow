package hnsw

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// BruteForceIndex represents a brute force index value.
type BruteForceIndex struct {
	mu            sync.RWMutex
	locations     []Location
	dataset       types.IndexDataProvider
	activeReaders atomic.Int64 // Track active zero-copy readers
	distFunc      simd.DistanceKernel[float32]
	dims          int
}

// NewBruteForceIndex creates a new brute force index for the given dataset.
func NewBruteForceIndex(ds types.IndexDataProvider) VectorIndex {
	return &BruteForceIndex{
		dataset:   ds,
		locations: make([]Location, 0, 64),
	}
}

// IsSharded returns true if the index is sharded. BruteForceIndex is never sharded.
func (b *BruteForceIndex) IsSharded() bool { return false }

// ReleaseMonolithicChunk releases a monolithic chunk from memory.
func (b *BruteForceIndex) ReleaseMonolithicChunk(cID int) error {
	// BruteForceIndex doesn't use the monolithic chunking model
	return nil
}

// AddByLocation adds a vector to the brute-force index using its storage location.
func (b *BruteForceIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	start := time.Now()
	b.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.GetName(), "write").Observe(time.Since(start).Seconds())
	defer b.mu.Unlock()

	id := uint32(len(b.locations)) // #nosec G115
	b.locations = append(b.locations, Location{
		BatchIdx: batchIdx,
		RowIdx:   rowIdx,
	})
	return id, nil
}

// AddByRecord adds a vector from a record batch.
func (b *BruteForceIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return b.AddByLocation(ctx, batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (b *BruteForceIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		id, _ := b.AddByRecord(ctx, recs[0], rowIdxs[i], batchIdxs[i]) // Note: simplified fallback
		ids[i] = id
	}
	return ids, nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (b *BruteForceIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	h := &bfSearchHeap{}
	heap.Init(h)

	dims := len(qF32)
	if b.distFunc == nil || b.dims != dims {
		b.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, dims)
		if b.distFunc == nil {
			b.distFunc = simd.EuclideanDistance
		}
		b.dims = dims
	}
	kernel := b.distFunc

	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		// Apply bitmap filter: skip if filter is provided and ID is not in filter
		if filter != nil && !filter.Contains(uint32(i)) {
			continue
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, err := kernel(qF32, vec)
		release()

		if err != nil {
			dist = math.MaxFloat32
		}

		if h.Len() < k {
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		} else if dist < (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		}
	}

	results := make([]SearchResult, h.Len())
	for i := len(results) - 1; i >= 0; i-- {
		item := heap.Pop(h).(bfHeapItem)
		results[i] = SearchResult{
			ID:    types.VectorID(item.id),
			Score: item.score,
		}
	}

	return results, nil
}

// GetLocation returns the physical location of a vector ID.
func (b *BruteForceIndex) GetLocation(id uint32) (any, bool) {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	if int(id) >= len(b.locations) {
		return Location{}, false
	}
	return b.locations[id], true
}

// GetDimension returns the vector dimension of the index.
// GetDimension returns the vector dimension of the index.
func (b *BruteForceIndex) GetDimension() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.dataset.GetSchema() != nil {
		for _, f := range b.dataset.GetSchema().Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				if fslType, ok := f.Type.(*arrow.FixedSizeListType); ok {
					return uint32(fslType.Len()) // #nosec G115
				}
			}
		}
	}
	return 0
}

// PreWarm pre-allocates resources for a target size. No-op for BruteForceIndex.
func (b *BruteForceIndex) PreWarm(targetSize int) {}

// Warmup is a no-op for BruteForceIndex.
func (b *BruteForceIndex) Warmup() int {
	return 0
}

// SetIndexedColumns is a no-op for BruteForceIndex.
func (b *BruteForceIndex) SetIndexedColumns(cols []string) {}

// Close releases all resources associated with the BruteForceIndex.
func (b *BruteForceIndex) Close() error {
	return nil
}

// TrainPQ is a no-op for BruteForceIndex.
func (b *BruteForceIndex) TrainPQ(vectors [][]float32) error {
	return nil
}

// GetPQEncoder returns nil as PQ is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetPQEncoder() *pq.PQEncoder {
	return nil
}

// Search performs a vector search and returns a list of candidates.
func (b *BruteForceIndex) Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error) {
	results, err := b.SearchVectors(ctx, query, k, nil, types.SearchOptions{})
	if err != nil {
		return nil, err
	}
	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{ID: uint32(r.ID), Dist: r.Distance}
	}
	return candidates, nil
}

// Size returns the number of vectors in the index.
func (b *BruteForceIndex) Size() int { return b.Len() }

// GetEntryPoint returns the entry point of the index. BruteForceIndex always returns 0.
func (b *BruteForceIndex) GetEntryPoint() uint32 { return 0 }

// GetVectorID returns the ID of a vector given its location.
func (b *BruteForceIndex) GetVectorID(loc any) (uint32, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	l, ok := loc.(Location)
	if !ok {
		return 0, false
	}
	for i, existing := range b.locations {
		if existing == l {
			return uint32(i), true
		}
	}
	return 0, false
}

// DeleteBatch removes a batch of vectors from the index. No-op for BruteForceIndex.
func (b *BruteForceIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil
}

// ExportState serializes the current index state.
func (b *BruteForceIndex) ExportState() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	state := types.SyncState{
		Version:   uint64(len(b.locations)),
		Dims:      int(b.GetDimension()),
		Locations: b.locations,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportState restores the index state from a byte array.
func (b *BruteForceIndex) ImportState(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var state types.SyncState
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&state); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.locations = make([]Location, len(state.Locations))
	copy(b.locations, state.Locations)
	return nil
}

// ExportGraph serializes the index graph structure. No-op for BruteForceIndex.
func (b *BruteForceIndex) ExportGraph(w io.Writer) error { return nil }

// ImportGraph restores the index graph from a reader. No-op for BruteForceIndex.
func (b *BruteForceIndex) ImportGraph(r io.Reader) error { return nil }

// ExportDelta returns the changes since a given version.
func (b *BruteForceIndex) ExportDelta(fromVersion uint64) (*DeltaSync, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := uint64(len(b.locations))
	if fromVersion >= total {
		return &DeltaSync{
			FromVersion: fromVersion,
			ToVersion:   total,
		}, nil
	}

	delta := &DeltaSync{
		FromVersion:  fromVersion,
		ToVersion:    total,
		StartIndex:   int(fromVersion), // #nosec G115
		NewLocations: make([]Location, total-fromVersion),
	}
	copy(delta.NewLocations, b.locations[fromVersion:total])
	return delta, nil
}

// ApplyDelta applies incremental changes to the index.
func (b *BruteForceIndex) ApplyDelta(delta *DeltaSync) error {
	if delta == nil || len(delta.NewLocations) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if delta.StartIndex == len(b.locations) {
		b.locations = append(b.locations, delta.NewLocations...)
	} else if delta.StartIndex < len(b.locations) {
		newLocs := b.locations[:delta.StartIndex]
		b.locations = append(newLocs, delta.NewLocations...)
	} else {
		expanded := make([]Location, delta.StartIndex+len(delta.NewLocations))
		copy(expanded, b.locations)
		copy(expanded[delta.StartIndex:], delta.NewLocations)
		b.locations = expanded
	}
	return nil
}

// SetParallelSearchConfig updates the configuration for parallel searches.
func (b *BruteForceIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {}

// GetParallelSearchConfig returns the current parallel search configuration.
func (b *BruteForceIndex) GetParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{}
}

// RemapLocations updates the mapping of vector IDs to storage locations.
func (b *BruteForceIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	return nil
}

// GetGPUIndex returns the underlying GPU index if available.
func (b *BruteForceIndex) GetGPUIndex() any { return nil }

// EstimateMemory returns an estimate of the memory used by the index.
func (b *BruteForceIndex) EstimateMemory() int64 {
	return int64(len(b.locations) * 8)
}

// GetRawNeighbors is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	return nil, errors.New("GetRawNeighbors not supported for BruteForceIndex")
}

// GetNeighbors is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]SearchResult, error) {
	return nil, errors.New("GetNeighbors not supported for BruteForceIndex")
}

// RelocateToOffHeap finds k-nearest neighbors using a full linear scan.
func (b *BruteForceIndex) RelocateToOffHeap() error {
	return nil
}
