package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
)

// Ensure ArrowHNSW implements VectorIndex
var _ VectorIndex = (*ArrowHNSW)(nil)

// AddByLocation implements VectorIndex.
// It assigns a new VectorID, records the location, and inserts into the graph.
func (h *ArrowHNSW) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	// 1. Generate new VectorID AND Store location
	// ChunkedLocationStore.Append handles atomic ID generation and storage
	vecID := h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
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

// AddByRecord implements VectorIndex.
func (h *ArrowHNSW) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return h.AddByLocation(batchIdx, rowIdx)
}

// NewArrowHNSW creates a new HNSW index backed by Arrow records.
// If locationStore is nil, a new one is created.
func NewArrowHNSW(dataset *Dataset, config ArrowHNSWConfig, locStore *ChunkedLocationStore) *ArrowHNSW {
	if config.M == 0 {
		config = DefaultArrowHNSWConfig()
	}

	h := &ArrowHNSW{
		dataset:        dataset,
		config:         config,
		m:              config.M,
		mMax:           config.MMax,
		mMax0:          config.MMax0,
		efConstruction: config.EfConstruction,
		ml:             1.0 / math.Log(float64(config.M)),
		deleted:        NewBitset(), // Initial capacity, grows
	}

	if locStore != nil {
		h.locationStore = locStore
	} else {
		h.locationStore = NewChunkedLocationStore()
	}

	// Initialize with empty graph data
	// Start with reasonable capacity
	initialCap := config.InitialCapacity
	if initialCap <= 0 {
		initialCap = 1024
	}
	h.data.Store(NewGraphData(initialCap, 0, config.SQ8Enabled, config.PQEnabled)) // Dim 0 initially, updated on first insert

	// Pre-allocate pools
	h.searchPool = NewArrowSearchContextPool()
	h.batchComputer = NewBatchDistanceComputer(memory.DefaultAllocator, h.dims)

	// Detect vector column index
	if dataset != nil && dataset.Schema != nil {
		for i, field := range dataset.Schema.Fields() {
			if field.Name == "vector" {
				h.vectorColIdx = i
				break
			}
		}
	}

	return h
}

// AddBatch implements VectorIndex.
func (h *ArrowHNSW) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	n := len(rowIdxs)
	if n == 0 {
		return nil, nil
	}

	// 1. Prepare Locations and Reserve IDs
	// Use BatchAppend to lock once and reserve a contiguous block of IDs
	locs := make([]Location, n)
	for i := 0; i < n; i++ {
		locs[i] = Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}
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

// SearchVectors implements VectorIndex.
func (h *ArrowHNSW) SearchVectors(query []float32, k int, filters []Filter) ([]SearchResult, error) {
	// Filter support TODO: Convert general filters to bitset?
	// For now, only basic search supported via this interface.

	ef := k + 100
	// Use configured ef if larger (or standard search ef separation)
	// Usually for search we might want a different dynamic ef.
	// We'll stick to a heuristic k + 100 for now.

	// Pass nil filter. Search signature updated in Step 2554.
	return h.Search(query, k, ef, nil)
}

// SearchVectorsWithBitmap implements VectorIndex.
func (h *ArrowHNSW) SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {
	ef := k + 100
	// Calls h.Search which returns ([]SearchResult, error)
	// The interface signature returns only []SearchResult
	res, _ := h.Search(query, k, ef, filter)
	return res
}

// GetLocation implements VectorIndex.
func (h *ArrowHNSW) GetLocation(id VectorID) (Location, bool) {
	return h.locationStore.Get(id)
}

// GetNeighbors returns the nearest neighbors for a given vector ID from the graph
// at the base layer (Layer 0).
func (h *ArrowHNSW) GetNeighbors(id VectorID) ([]VectorID, error) {
	data := h.data.Load()
	if int(id) >= int(h.nodeCount.Load()) {
		return nil, fmt.Errorf("vector ID %d out of range", id)
	}

	// Layer 0 neighbors
	// Layer 0 neighbors
	layer := 0

	cID := chunkID(uint32(id))
	cOff := chunkOffset(uint32(id))

	count := int(atomic.LoadInt32(&(*data.Counts[layer][cID])[cOff]))
	baseIdx := int(cOff) * MaxNeighbors
	neighborsChunk := (*data.Neighbors[layer][cID])

	results := make([]VectorID, count)
	for i := 0; i < count; i++ {
		results[i] = VectorID(neighborsChunk[baseIdx+i])
	}

	return results, nil
}

// Len implements VectorIndex.
func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// GetDimension implements VectorIndex.
func (h *ArrowHNSW) GetDimension() uint32 {
	return uint32(h.dims)
}

// Warmup implements VectorIndex.
func (h *ArrowHNSW) Warmup() int {
	// Could implement pre-fetching of all pages/nodes
	return h.Size()
}

// SetIndexedColumns implements VectorIndex.
func (h *ArrowHNSW) SetIndexedColumns(cols []string) {
	// No-op for now
}

// EstimateMemory implements VectorIndex.
func (h *ArrowHNSW) EstimateMemory() int64 {
	data := h.data.Load()
	if data == nil {
		return 0
	}

	capacity := int64(data.Capacity)
	dims := int64(h.dims)

	// memory = capacity * (Level[1] + Vector[dims*4] + Neighbors[ArrowMaxLayers*MaxNeighbors*4] + Counts[ArrowMaxLayers*4] + Versions[ArrowMaxLayers*4])
	// Neighbors per layer: ChunkSize * MaxNeighbors * 4 bytes
	// Neighborhood overhead for all layers
	neighborhoodMem := capacity * ArrowMaxLayers * MaxNeighbors * 4

	// Metadata: Levels (1 byte), Counts (4 bytes), Versions (4 bytes) per layer
	metadataMem := capacity * (1 + ArrowMaxLayers*4 + ArrowMaxLayers*4)

	// Vectors: dims * 4 bytes
	vectorMem := capacity * dims * 4

	// Sharded locks overhead (1024 * size of Mutex)
	locksMem := int64(1024 * 64) // Approximation for sync.Mutex

	// Bitset memory (deleted nodes)
	bitsetMem := capacity / 8

	return neighborhoodMem + metadataMem + vectorMem + locksMem + bitsetMem
}

// Close implements VectorIndex.
func (h *ArrowHNSW) Close() error {
	// TODO: Clean up resources
	return nil
}
