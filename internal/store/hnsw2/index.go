package hnsw2

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"

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
	if n == 0 {
		return nil, nil
	}
	
	// 1. Prepare Locations and Reserve IDs
	// Use BatchAppend to lock once and reserve a contiguous block of IDs
	locs := make([]store.Location, n)
	for i := 0; i < n; i++ {
		locs[i] = store.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}
	}
	
	startID := h.locationStore.BatchAppend(locs)
	
	// 2. Pre-grow graph to ensure capacity for all new items
	// This avoids "Stop-the-World" pauses during parallel insertion
	// finalSize must accommodate (startID + n - 1)
	finalSize := int(startID) + n
	h.Grow(finalSize)
	
	ids := make([]uint32, n)
	
	// 3. Parallel Insert
	// We bypass AddByLocation as we already have ID and Capacity
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	
	for i := 0; i < n; i++ {
		i := i
		// Calculate pre-assigned ID
		id := uint32(startID) + uint32(i)
		ids[i] = id
		
		g.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			
			// Generate random level for the node
			// Note: Global rand is thread-safe but contended. 
			// For extremely high throughput, thread-local rand is better.
			level := h.generateLevel()
			
			// Insert into the graph
			// Insert is thread-safe and now mostly lock-free (except for sharded locks)
			if err := h.Insert(id, level); err != nil {
				return err
			}
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

// GetNeighbors returns the nearest neighbors for a given vector ID from the graph
// at the base layer (Layer 0).
func (h *ArrowHNSW) GetNeighbors(id store.VectorID) ([]store.VectorID, error) {
	data := h.data.Load()
	if int(id) >= int(h.nodeCount.Load()) {
		return nil, fmt.Errorf("vector ID %d out of range", id)
	}

	// Layer 0 neighbors
	// Layer 0 neighbors
	layer := 0
	
	cID := chunkID(uint32(id))
	cOff := chunkOffset(uint32(id))
	
	count := int(atomic.LoadInt32(&data.Counts[layer][cID][cOff]))
	baseIdx := int(cOff) * MaxNeighbors
	neighborsChunk := data.Neighbors[layer][cID]

	results := make([]store.VectorID, count)
	for i := 0; i < count; i++ {
		results[i] = store.VectorID(neighborsChunk[baseIdx+i])
	}

	return results, nil
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
	data := h.data.Load()
	if data == nil {
		return 0
	}

	capacity := int64(data.Capacity)
	dims := int64(h.dims)

	// memory = capacity * (Level[1] + Vector[dims*4] + Neighbors[MaxLayers*MaxNeighbors*4] + Counts[MaxLayers*4] + Versions[MaxLayers*4])
	// Neighbors per layer: ChunkSize * MaxNeighbors * 4 bytes
	// Neighborhood overhead for all layers
	neighborhoodMem := capacity * MaxLayers * MaxNeighbors * 4

	// Metadata: Levels (1 byte), Counts (4 bytes), Versions (4 bytes) per layer
	metadataMem := capacity * (1 + MaxLayers*4 + MaxLayers*4)

	// Vectors: dims * 4 bytes
	vectorMem := capacity * dims * 4

	// Sharded locks overhead (1024 * size of Mutex)
	locksMem := int64(1024 * 64) // Approximation for sync.Mutex

	// Bitset memory (deleted nodes)
	bitsetMem := capacity / 8

	return neighborhoodMem + metadataMem + vectorMem + locksMem + bitsetMem
}

// Close implements store.VectorIndex.
func (h *ArrowHNSW) Close() error {
	// TODO: Clean up resources
	return nil
}
