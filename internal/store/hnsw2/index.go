package hnsw2

import (
	"context"
	"math"
	"math/rand"
	
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"
)

// Ensure ArrowHNSW implements store.VectorIndex
var _ store.VectorIndex = (*ArrowHNSW)(nil)

// AddByLocation implements store.VectorIndex.
// It assigns a new VectorID, records the location, and inserts into the graph.
func (h *ArrowHNSW) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	// 1. Generate new VectorID AND Store location
	// ChunkedLocationStore.Append handles atomic ID generation and storage
	vecID := h.locationStore.Append(store.Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	id := uint32(vecID)

	// 2. (Location stored in step 1)

	// 3. Generate Random Level
	level := h.generateLevel()

	// 4. Insert into Graph
	if err := h.Insert(id, level); err != nil {
		return 0, err
	}

	return id, nil
}

// generateLevel chooses a random level for a new node.
func (h *ArrowHNSW) generateLevel() int {
	// -ln(uniform(0,1)) * ml
	return int(math.Floor(-math.Log(rand.Float64()) * h.ml))
}

// AddByRecord implements store.VectorIndex.
func (h *ArrowHNSW) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return h.AddByLocation(batchIdx, rowIdx)
}

// AddBatch implements store.VectorIndex.
func (h *ArrowHNSW) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	n := len(rowIdxs)
	ids := make([]uint32, n)
	
	g, _ := errgroup.WithContext(context.Background())
	// ArrowHNSW has internal sharded locks, so we can process in parallel.
	// For very large batches, we might want to group by lock ID, 
	// but the OS scheduler and Go runtime should handle this reasonably well.
	
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			id, err := h.AddByLocation(batchIdxs[i], rowIdxs[i])
			if err != nil {
				return err
			}
			ids[i] = id
			return nil
		})
	}
	
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return ids, nil
}

// SearchVectors implements store.VectorIndex.
func (h *ArrowHNSW) SearchVectors(query []float32, k int, filters []store.Filter) ([]store.SearchResult, error) {
	// Filter support TODO: Convert general filters to bitset?
	// For now, only basic search supported via this interface.
	
	ef := k + 100
	// Use configured ef if larger (or standard search ef separation)
	// Usually for search we might want a different dynamic ef.
	// We'll stick to a heuristic k + 100 for now.
	
	// Pass nil filter. Search signature updated in Step 2554.
	return h.Search(query, k, ef, nil)
}

// SearchVectorsWithBitmap implements store.VectorIndex.
func (h *ArrowHNSW) SearchVectorsWithBitmap(query []float32, k int, filter *store.Bitset) []store.SearchResult {
	ef := k + 100
	// Calls h.Search which returns ([]SearchResult, error)
	// The interface signature returns only []SearchResult
	res, _ := h.Search(query, k, ef, filter)
	return res
}

// GetLocation implements store.VectorIndex.
func (h *ArrowHNSW) GetLocation(id store.VectorID) (store.Location, bool) {
	return h.locationStore.Get(id)
}

// Len implements store.VectorIndex.
func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// GetDimension implements store.VectorIndex.
func (h *ArrowHNSW) GetDimension() uint32 {
	return uint32(h.dims)
}

// Warmup implements store.VectorIndex.
func (h *ArrowHNSW) Warmup() int {
	// Could implement pre-fetching of all pages/nodes
	return h.Size()
}

// SetIndexedColumns implements store.VectorIndex.
func (h *ArrowHNSW) SetIndexedColumns(cols []string) {
	// No-op for now
}

// EstimateMemory implements store.VectorIndex.
func (h *ArrowHNSW) EstimateMemory() int64 {
	// Rough estimation: Nodes * (Levels * M * 4 bytes + VectorPtr + Overhead)
	// + LocationStore
	// + Vectors if copied (they are pointers for us)
	
	nodeCount := int64(h.Size())
	// Base node overhead
	mem := nodeCount * 64 
	
	// Graph edges: Average connections approx M * nodeCount * 4 bytes
	mem += nodeCount * int64(h.m) * 4
	
	return mem
}

// Close implements store.VectorIndex.
func (h *ArrowHNSW) Close() error {
	// TODO: Clean up resources
	return nil
}
