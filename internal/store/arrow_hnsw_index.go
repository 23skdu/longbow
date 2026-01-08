package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
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
func NewArrowHNSW(dataset *Dataset, config ArrowHNSWConfig, locStore *ChunkedLocationStore) *ArrowHNSW { //nolint:gocritic
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
		deleted:        query.NewBitset(), // Initial capacity, grows
		metric:         config.Metric,
		vectorColIdx:   -1,
	}

	h.distFunc = h.resolveDistanceFunc()
	h.batchDistFunc = h.resolveBatchDistanceFunc()

	h.shardedLocks = make([]sync.Mutex, ShardedLockCount)
	for i := 0; i < len(h.shardedLocks); i++ {
		h.shardedLocks[i] = sync.Mutex{}
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

	gd := NewGraphData(initialCap, 0, config.SQ8Enabled, config.PQEnabled, config.BQEnabled)
	h.data.Store(gd) // Dim 0 initially, updated on first insert
	h.backend.Store(gd)

	// Pre-allocate pools
	h.searchPool = NewArrowSearchContextPool()
	// h.dims is 0 here usually, unless passed differently? No, NewArrowHNSW signature doesn't take dims.
	// But it might be updated later? BatchDistanceComputer needs fixed dims?
	// It copies dims. If dims is 0, it might need re-init later?
	// For now, just load it.
	h.batchComputer = NewBatchDistanceComputer(memory.DefaultAllocator, int(h.dims.Load()))

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
	h.Grow(finalSize, 0)

	ids := make([]uint32, n)

	// 3. Parallel Insert
	// Note: When SQ8 is enabled, we serialize insertions to avoid race conditions
	// on shared memory writes to the vector chunks
	// 3. Parallel Insert
	// Note: When SQ8 or BQ is enabled, we serialize insertions to avoid race conditions
	// on shared memory writes to the vector chunks
	if h.config.SQ8Enabled || h.config.BQEnabled {
		// Serial insertion for SQ8/BQ to ensure chunks are safely written
		for i := 0; i < n; i++ {
			id := uint32(startID) + uint32(i)
			ids[i] = id

			level := h.generateLevel()
			if err := h.Insert(id, level); err != nil {
				return nil, err
			}
		}
		return ids, nil
	}

	// Parallel insertion for non-SQ8 indices
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
// SearchVectors implements VectorIndex.
func (h *ArrowHNSW) SearchVectors(queryVec []float32, k int, filters []query.Filter) ([]SearchResult, error) {
	// Post-filtering with Adaptive Expansion
	initialFactor := 10
	retryFactor := 50
	maxRetries := 1

	q := queryVec // Alias to avoid shadowing package query

	limit := k
	if len(filters) > 0 {
		limit = k * initialFactor
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Use ef = limit + 100 heuristic
		ef := limit + 100

		// Pass nil filter to get raw candidates from graph
		candidates, err := h.Search(q, limit, ef, nil)
		if err != nil {
			return nil, err
		}

		// Filter candidates
		var res []SearchResult
		if len(filters) > 0 {
			// TODO: Handle multiple batches. Currently binding to Batch 0.
			if h.dataset == nil || len(h.dataset.Records) == 0 {
				return nil, fmt.Errorf("dataset empty during filter")
			}

			var evaluator *query.FilterEvaluator
			var err error
			evaluator, err = query.NewFilterEvaluator(h.dataset.Records[0], filters)
			if err != nil {
				return nil, err
			}

			res = make([]SearchResult, 0, len(candidates))

			// 1. Gather valid candidates and their row indices for batch processing
			// We only filter candidates that are in Batch 0 (current limit)
			validCandidates := make([]SearchResult, 0, len(candidates))
			rowIndices := make([]int, 0, len(candidates))

			for _, candle := range candidates {
				loc, ok := h.locationStore.Get(VectorID(candle.ID))
				if !ok || loc.BatchIdx != 0 {
					continue
				}
				rowIndices = append(rowIndices, loc.RowIdx)
				validCandidates = append(validCandidates, candle)
			}

			// 2. Vectorized Filter Evaluation
			// MatchesBatch uses SIMD/Gather where possible to filter indices efficiently
			matchedIndices := evaluator.MatchesBatch(rowIndices)

			// 3. Reconstruct Results
			// matchedIndices is a subsequence of rowIndices. validCandidates aligns with rowIndices.
			// We match them up.
			matchIdx := 0
			for i, rowIdx := range rowIndices {
				if matchIdx < len(matchedIndices) && rowIdx == matchedIndices[matchIdx] {
					res = append(res, validCandidates[i])
					matchIdx++
				}
			}
		} else {
			res = candidates
		}

		if len(res) >= k || attempt == maxRetries || len(filters) == 0 {
			// Truncate to k
			if len(res) > k {
				res = res[:k]
			}
			return res, nil
		}

		// Retry with larger limit
		limit = k * retryFactor
	}

	return nil, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (h *ArrowHNSW) SearchVectorsWithBitmap(q []float32, k int, filter *query.Bitset) []SearchResult {
	ef := k + 100
	// Calls h.Search which returns ([]SearchResult, error)
	// The interface signature returns only []SearchResult
	res, _ := h.Search(q, k, ef, filter)
	return res
}

// GetLocation implements VectorIndex.
func (h *ArrowHNSW) GetLocation(id VectorID) (Location, bool) {
	return h.locationStore.Get(id)
}

// GetVectorID implements VectorIndex.
// It returns the ID for a given location using the reverse index.
func (h *ArrowHNSW) GetVectorID(loc Location) (VectorID, bool) {
	return h.locationStore.GetID(loc)
}

// SetLocation allows manually setting the location for a vector ID.
// This is used by ShardedHNSW to populate shard-local location stores for filtering.
func (h *ArrowHNSW) SetLocation(id VectorID, loc Location) {
	h.locationStore.EnsureCapacity(id)
	h.locationStore.Set(id, loc)
	h.locationStore.UpdateSize(id)
}

// GetNeighbors returns the nearest neighbors for a given vector ID from the graph
// at the base layer (Layer 0).
func (h *ArrowHNSW) GetNeighbors(id VectorID) ([]VectorID, error) {
	data := h.data.Load()
	if int(id) >= int(h.nodeCount.Load()) {
		return nil, fmt.Errorf("vector ID %d out of range", id)
	}

	// Layer 0 neighbors
	layer := 0

	cID := chunkID(uint32(id))
	cOff := chunkOffset(uint32(id))

	countsChunk := data.GetCountsChunk(layer, cID)
	if countsChunk == nil {
		return nil, fmt.Errorf("chunk %d not allocated", cID)
	}
	count := int(atomic.LoadInt32(&(*countsChunk)[cOff]))
	if count == 0 {
		return []VectorID{}, nil
	}

	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if neighborsChunk == nil {
		return nil, fmt.Errorf("neighbors chunk %d missing", cID)
	}
	baseIdx := int(cOff) * MaxNeighbors

	results := make([]VectorID, count)
	chunk := *neighborsChunk
	for i := 0; i < count; i++ {
		results[i] = VectorID(chunk[baseIdx+i])
	}

	return results, nil
}

// Len implements VectorIndex.
func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// GetDimension implements VectorIndex.
func (h *ArrowHNSW) GetDimension() uint32 {
	return uint32(h.dims.Load())
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
	dims := int64(h.dims.Load())

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
	// Release GraphData
	if data := h.data.Swap(nil); data != nil {
		_ = data.Close()
	}
	if backend := h.backend.Swap(nil); backend != nil {
		_ = backend.Close()
	}

	// Release Bitsets (Roaring Bitmaps are pooled)
	if h.deleted != nil {
		h.deleted.Release()
		h.deleted = nil
	}

	// Clear pools and stores
	h.searchPool = nil
	h.batchComputer = nil

	// Break references to dataset and locationStore
	h.dataset = nil
	h.locationStore = nil

	return nil
}

// resolveDistanceFunc returns the distance function based on configuration.
func (h *ArrowHNSW) resolveDistanceFunc() func(a, b []float32) float32 {
	switch h.metric {
	case MetricCosine:
		return simd.CosineDistance
	case MetricDotProduct:
		// For HNSW minimization, negate Dot Product
		return func(a, b []float32) float32 {
			return -simd.DotProduct(a, b)
		}
	default: // Euclidean
		return simd.EuclideanDistance
	}
}

// resolveBatchDistanceFunc returns the batch distance function.
func (h *ArrowHNSW) resolveBatchDistanceFunc() func(query []float32, vectors [][]float32, results []float32) {
	switch h.metric {
	case MetricCosine:
		return simd.CosineDistanceBatch
	case MetricDotProduct:
		return func(query []float32, vectors [][]float32, results []float32) {
			simd.DotProductBatch(query, vectors, results)
			for i := range results {
				results[i] = -results[i]
			}
		}
	default:
		return simd.EuclideanDistanceBatch
	}
}
