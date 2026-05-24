package index

import (
	"context"
	"fmt" // // // // //
	// // // // //
	"strconv"
	"sync/atomic"
	"time" // // // // //

	// // // // //
	// // // // //

	"github.com/23skdu/longbow/internal/metrics" // // // // //
	// // // // //
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow" // // // // //
	// // // // //
)

// Search finds the k-closest neighbors to the query vector.
func (h *ArrowHNSW) Search(ctx context.Context, queryVal any, k int, filter any) ([]types.Candidate, error) {
	start := time.Now()

	meta := h.metadataRegistry.Load()
	if meta.NodeCount == 0 {
		return []types.Candidate{}, nil
	}

	// Perform search to find closest neighbors
	results, err := h.SearchVectorsWithBitmap(ctx, queryVal, k, nil, nil)

	// Record search metrics
	duration := time.Since(start).Seconds()
	typeStr := h.config.DataType.String()
	dimStr := strconv.Itoa(int(h.dims.Load()))
	metrics.HNSWSearchLatencyByType.WithLabelValues(typeStr).Observe(duration)
	metrics.HNSWSearchLatencyByDim.WithLabelValues(dimStr).Observe(duration)

	if err != nil {
		return nil, err
	}

	// Convert []types.SearchResult to []types.Candidate
	typeResults := make([]types.Candidate, len(results))
	for i, r := range results {
		typeResults[i] = types.Candidate{
			ID:   uint32(r.ID),
			Dist: r.Distance,
		}
	}

	return typeResults, nil
}

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	return h.GetNodeCount()
}

// Navigate performs a graph navigation query
func (h *ArrowHNSW) Navigate(ctx context.Context, navQuery NavigatorQuery) (*NavigatorPath, error) {
	if h.navigator == nil {
		return nil, fmt.Errorf("graph navigator not initialized")
	}
	return h.navigator.FindPath(ctx, navQuery)
}

// GetDimension returns the vector dimensionality of the index.
func (h *ArrowHNSW) GetDimension() uint32 {
	dims := h.GetDims()
	if dims > 0 {
		return uint32(dims)
	}
	if h.dataset != nil && h.dataset.GetSchema() != nil {
		for _, f := range h.dataset.GetSchema().Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				if fslType, ok := f.Type.(*arrow.FixedSizeListType); ok {
					return uint32(fslType.Len()) // #nosec G115
				}
			}
		}
	}
	return 0
}

// GetM returns the M parameter (connections per layer)
func (h *ArrowHNSW) GetM() int {
	return int(h.m.Load())
}

// GetMMax returns the MMax parameter (max connections)
func (h *ArrowHNSW) GetMMax() int {
	return int(h.mMax.Load())
}

// GetMMax0 returns the MMax0 parameter (max connections in layer 0)
func (h *ArrowHNSW) GetMMax0() int {
	return int(h.mMax0.Load())
}

// GetEfConstruction returns the efConstruction parameter
func (h *ArrowHNSW) GetEfConstruction() int32 {
	return h.efConstruction.Load()
}

// GetNodeCount returns the current number of nodes
func (h *ArrowHNSW) GetNodeCount() int {
	meta := h.GetMetadataSnapshot()
	return int(meta.NodeCount)
}

// GetMaxLevel returns the maximum level in the graph
func (h *ArrowHNSW) GetMaxLevel() int32 {
	meta := h.GetMetadataSnapshot()
	return meta.MaxLevel
}

// GetEntryPoint returns the entry point node ID
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	meta := h.GetMetadataSnapshot()
	return meta.EntryPoint
}

// GetDims returns the vector dimensionality
func (h *ArrowHNSW) GetDims() int32 {
	return h.dims.Load()
}

// IsDeleted returns whether the given vector ID is marked as deleted.
func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	if h.deleted == nil {
		return false
	}
	return h.deleted.Contains(id)
}

// Warmup triggers the loading of index data into memory.
func (h *ArrowHNSW) Warmup() int {
	return h.GetNodeCount()
}

// GetIndexType returns "hnsw".
func (h *ArrowHNSW) GetIndexType() string {
	return "hnsw"
}

// Len returns the current size of the index.
func (h *ArrowHNSW) Len() int {
	return h.Size()
}

func (h *ArrowHNSW) ensureReady() {
	if h.searchPool == nil {
		h.initMu.Lock()
		if h.searchPool == nil {
			h.searchPool = NewArrowSearchContextPool()
		}
		if h.deleted == nil {
			h.deleted = roaring.New()
		}
		if h.locationStore == nil {
			h.locationStore = NewChunkedLocationStore()
		}

		h.initMu.Unlock()
	}
}

// SearchVectorsWithBitmap performs k-NN search with a roaring bitmap filter.

// searchLayerFloat32 is a monomorphic specialization of searchLayer for the
// float32-query/float32-data case. It avoids DistanceComputer interface dispatch
// by calling *float32ToFloat32Computer methods directly, and skips the fallback
// type-switch closure path entirely.

// SearchVectors performs a search with multiple filters and options.

// SearchVectorsInRange finds all vectors within a certain distance threshold.

// ProcessResultsParallel processes search candidates in parallel to compute final search results.
// It applies filters and thresholds while maintaining top-K ordering.

// ExtractVectorToBufferForParallel extracts a vector directly into a destination buffer.

// ExtractVectorF64ToBufferForParallel extracts a float64 vector into a destination buffer.

// ExtractVectorByIDToBufferForParallel extracts a vector by ID into a float32 buffer.

// ExtractVectorF64ByIDToBufferForParallel extracts a vector by ID into a float64 buffer.

// flushSearchMetrics handles the efficient emission of search-layer metrics,
// including sampling logic for Histogram metrics to avoid overhead.
func (h *ArrowHNSW) flushSearchMetrics(ctx *ArrowSearchContext) {
	if ctx == nil {
		return
	}

	// Always increment global distance counter (low overhead atomic)
	if ctx.distComputeCount > 0 {
		metrics.HnswDistanceCalculations.Add(float64(ctx.distComputeCount))
	}

	// Sampling for Histogram metrics (e.g. nodes visited)
	if h.config.SearchLayerSampleRate > 0 {
		count := h.metricsSampleCounter.Add(1)
		interval := uint64(1.0 / h.config.SearchLayerSampleRate)
		if interval == 0 {
			interval = 1
		}

		if count%interval == 0 {
			metrics.HnswNodesVisited.WithLabelValues(h.name).Observe(float64(ctx.nodesVisitedCount))
		}
	}
}

// MinCandidateHeap for exploration (closest first)
// Uses store.Candidate (ID, Dist) to match ArrowSearchContext
// GetLayerNeighbors returns internal neighbor IDs for a specific layer
func (h *ArrowHNSW) GetLayerNeighbors(id uint32, layer int) ([]uint32, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data is nil")
	}

	maxLevel := h.GetMaxLevel()
	meta := h.GetMetadataSnapshot()
	if int64(id) >= meta.NodeCount {
		return nil, fmt.Errorf("%w: id=%d", ErrVectorNotFound, id)
	}
	if meta.MaxLevel < 0 {
		return nil, nil
	}

	if layer < 0 || int32(layer) > maxLevel { // #nosec G115
		return nil, fmt.Errorf("invalid layer: %d", layer)
	}

	// 1. Try PackedNeighbors (Lock-Free)
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		if neighbors, ok := data.PackedNeighbors[layer].GetNeighbors(id); ok {
			return neighbors, nil
		}
	}

	// 2. Fallback to Legacy Chunks
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	neighborhood := data.GetNeighborsChunk(layer, cID)
	counts := data.GetCountsChunk(layer, cID)
	if neighborhood == nil || counts == nil {
		return nil, nil
	}

	count := atomic.LoadInt32(&counts[cOff])
	if count == 0 {
		return nil, nil
	}

	neighbors := make([]uint32, count)
	startIdx := int(cOff) * types.MaxNeighbors                  // #nosec G115
	copy(neighbors, neighborhood[startIdx:startIdx+int(count)]) // #nosec G115

	return neighbors, nil
}

// GetRawNeighbors implements the VectorIndexer interface
func (h *ArrowHNSW) GetRawNeighbors(id uint32) ([]uint32, error) {
	return h.GetLayerNeighbors(id, 0)
}

// GetNeighbors retrieves the k-nearest neighbors for a given vector ID.
func (h *ArrowHNSW) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	neighbors, err := h.GetLayerNeighbors(id, 0)
	if err != nil || len(neighbors) == 0 {
		return nil, err
	}

	// 1. Get query vector
	qVecAny, err := h.GetVector(id)
	if err != nil {
		return nil, err
	}
	qVec, ok := qVecAny.([]float32)
	if !ok {
		// If not float32, we can't easily compute distances here for now
		// but we still return the neighbors without distances or with 0
		results := make([]types.SearchResult, 0, min(k, len(neighbors)))
		for i := 0; i < len(neighbors) && i < k; i++ {
			results = append(results, types.SearchResult{
				ID: types.VectorID(neighbors[i]),
			})
		}
		return results, nil
	}

	results := make([]types.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		nID := neighbors[i]
		nVecAny, err := h.GetVector(nID)
		if err != nil || nVecAny == nil {
			continue
		}

		dist := float32(0.0)
		if nVec, ok := nVecAny.([]float32); ok {
			dist, _ = h.distFunc(qVec, nVec)
		}

		results = append(results, types.SearchResult{
			ID:       types.VectorID(nID),
			Distance: dist,
			Score:    dist,
		})
	}

	return results, nil
}

// SearchForParallel performs a search for parallel processing.

// SearchWithArena performs k-NN search using an arena allocator for results.
func (h *ArrowHNSW) SearchWithArena(queryVec []float32, k int, arena any) []types.VectorID {
	// Fallback to standard search if no arena
	if arena == nil {
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]types.VectorID, len(results))
		for i, r := range results {
			ids[i] = types.VectorID(r.ID)
		}
		return ids
	}

	searchArena, ok := arena.(*SearchArena)
	if !ok {
		// Try casting if it's passed as interface
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]types.VectorID, len(results))
		for i, r := range results {
			ids[i] = types.VectorID(r.ID)
		}
		return ids
	}

	results, err := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
	if err != nil || len(results) == 0 {
		return nil
	}

	ids := searchArena.AllocVectorIDSlice(len(results))
	if ids == nil {
		// Fallback to heap if arena exhausted
		ids = make([]types.VectorID, len(results))
	}

	for i, r := range results {
		ids[i] = types.VectorID(r.ID)
	}
	return ids
}

// GetVector retrieves the vector for the given ID, checking memory and disk caches.

// GetVectorAny returns the vector with the given ID as an interface{}.

// mustGetVectorFromData retrieves a vector from the given data snapshot.
