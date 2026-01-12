package store

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"runtime"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(q []float32, k, ef int, filter *query.Bitset) ([]SearchResult, error) {
	// Lock-free access: load backend
	backend := h.backend.Load()
	if backend == nil || h.nodeCount.Load() == 0 {
		return []SearchResult{}, nil
	}
	graph := backend

	if k <= 0 {
		return []SearchResult{}, nil
	}

	// Determine refinement parameters
	targetK := k
	useRefinement := (h.config.SQ8Enabled || h.config.PQEnabled || h.config.BQEnabled) && h.config.RefinementFactor > 1.0
	if useRefinement {
		targetK = int(float64(k) * h.config.RefinementFactor)
		if targetK > graph.Size() {
			targetK = graph.Size()
		}
	}

	if ef <= 0 {
		ef = k * 2 // Default ef to 2*k
	}
	// Ensure ef is at least as large as targetK
	if ef < targetK {
		ef = targetK
	}

	// Get search context from pool
	metrics.HNSWSearchPoolGetTotal.Inc()
	ctx := h.searchPool.Get().(*ArrowSearchContext)
	defer func() {
		metrics.HNSWSearchPoolPutTotal.Inc()
		h.searchPool.Put(ctx)
	}()

	// Encode query if SQ8 enabled
	dims := int(h.dims.Load())
	metrics.HNSWSearchQueriesTotal.WithLabelValues(strconv.Itoa(dims)).Inc()

	// 1. Finding entry points in upper layers
	data := graph
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	totalVisited := 0

	// Start timer for polymorphic latency
	start := time.Now()
	typeLabel := "float32"
	vecBytes := dims * 4 // Default float32

	if h.config.Float16Enabled {
		typeLabel = "float16"
		vecBytes = dims * 2
	} else {
		// Map internal type to string
		switch data.Type {
		case VectorTypeComplex64:
			typeLabel = "complex64"
			// dims is total float32 elements. Complex64 is 2x float32 size, so bytes = dims * 4
		case VectorTypeComplex128:
			typeLabel = "complex128"
			// dims is total float32 elements. Complex128 is 16 bytes.
			// One complex128 corresponds to 2 float32s (8 bytes). So size is double.
			vecBytes = dims * 8
		case VectorTypeUint8:
			typeLabel = "uint8"
			vecBytes = dims
		case VectorTypeInt8:
			typeLabel = "int8"
			vecBytes = dims
			// add others as needed, default float32
		}
	}
	// Check for optimizations
	if h.config.SQ8Enabled {
		typeLabel = "sq8"
		vecBytes = dims
	} else if h.config.PQEnabled {
		typeLabel = "pq"
		vecBytes = data.PQDims
	}

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.HNSWPolymorphicLatency.WithLabelValues(typeLabel).Observe(duration)

		// Throughput: total bytes processed
		totalBytes := float64(totalVisited) * float64(vecBytes)
		metrics.HNSWPolymorphicThroughput.WithLabelValues(typeLabel).Add(totalBytes)
	}()

	// Resolve Distance Computer
	computer := h.resolveHNSWComputer(data, ctx, q)

	for l := maxL; l > 0; l-- {
		ctx.Reset()
		ep = h.searchLayer(computer, ep, 1, l, ctx, data, nil)
		totalVisited += ctx.VisitedCount()
	}

	// 2. Final Search at Layer 0
	ctx.Reset()

	_ = h.searchLayer(computer, ep, ef, 0, ctx, data, filter)
	totalVisited += ctx.VisitedCount()

	// Results are in resultSet
	resultSet := ctx.resultSet
	results := make([]SearchResult, 0, targetK)

	// Peek multiple results from max-heap (not efficient but we only do it once)
	// Actually results is a slice of SearchResult. We need to pop everything from resultSet
	tempResults := make([]Candidate, 0, resultSet.Len())
	for resultSet.Len() > 0 {
		c, _ := resultSet.Pop()
		tempResults = append(tempResults, c)
	}

	// Sort by distance ascending (nearest first)
	sort.Slice(tempResults, func(i, j int) bool {
		return tempResults[i].Dist < tempResults[j].Dist
	})

	// Add to results up to targetK
	for i := 0; i < len(tempResults) && i < targetK; i++ {
		results = append(results, SearchResult{
			ID:    VectorID(tempResults[i].ID),
			Score: tempResults[i].Dist,
		})
	}

	// 3. Post-Refinement
	if useRefinement && len(results) > 0 {
		for i := range results {
			// Compute exact distance
			vec := h.mustGetVectorFromData(data, uint32(results[i].ID))
			if vec != nil {
				results[i].Score = simd.EuclideanDistance(q, vec)
			}
		}
		// Resort
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score < results[j].Score
		})
		// Truncate to k
		if len(results) > k {
			results = results[:k]
		}
	}

	return results, nil
}

func (h *ArrowHNSW) searchLayer(computer HNSWDistanceComputer, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *GraphData, filter *query.Bitset) (candidate uint32) {
	ctx.ResetVisited()
	ctx.candidates.Clear()

	useF16 := h.config.Float16Enabled // Still needed for PackedAdjacency selection

	// Prepare result buffer
	// Note: ctx.candidates is FixedHeap(400). If ef > 400, results might be truncated.
	// logic continues...

	// Calculate entry point distance
	entryDist := computer.ComputeSingle(entryPoint)

	ctx.candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.Visit(entryPoint)

	resultSet := ctx.resultSet
	if resultSet.cap < ef {
		resultSet = NewMaxHeap(ef)
		ctx.resultSet = resultSet
	}
	resultSet.Clear()
	if !h.IsDeleted(entryPoint) {
		resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	}

	closest := entryPoint
	closestDist := entryDist

	for ctx.candidates.Len() > 0 {
		curr, ok := ctx.candidates.Pop()
		if !ok {
			break
		}

		if resultSet.Len() >= ef {
			worst, ok := resultSet.Peek()
			if ok && curr.Dist > worst.Dist {
				break
			}
		}

		if curr.Dist < closestDist {
			closest = curr.ID
			closestDist = curr.Dist
		}

		// 1. Collect unvisited neighbors
		ctx.scratchIDs = ctx.scratchIDs[:0]
		collected := false

		// Priority: PackedAdjacency (Thread-safe, pre-packed)
		if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
			pn := data.PackedNeighbors[layer]
			var neighborIDs []uint32
			var found bool
			if useF16 {
				neighborIDs, _, found = pn.GetNeighborsF16(curr.ID)
			} else {
				neighborIDs, found = pn.GetNeighbors(curr.ID)
			}
			if found {
				for _, nid := range neighborIDs {
					if !ctx.visited.IsSet(nid) {
						ctx.Visit(nid)
						ctx.Visit(nid)
						ctx.scratchIDs = append(ctx.scratchIDs, nid)
						computer.Prefetch(nid)
					}
				}
				collected = true
			}
		}

		// Fallback: Legacy Chunked Storage (Seqlock protected)
		if !collected {
			for {
				ctx.scratchIDs = ctx.scratchIDs[:0]
				versionsChunk := data.GetVersionsChunk(layer, chunkID(curr.ID))
				if versionsChunk == nil {
					break
				}
				verAddr := &versionsChunk[chunkOffset(curr.ID)]
				ver := atomic.LoadUint32(verAddr)
				if ver%2 != 0 {
					runtime.Gosched()
					continue
				}

				countsChunk := data.GetCountsChunk(layer, chunkID(curr.ID))
				neighborsChunk := data.GetNeighborsChunk(layer, chunkID(curr.ID))
				if countsChunk == nil || neighborsChunk == nil {
					break
				}
				count := int(atomic.LoadInt32(&countsChunk[chunkOffset(curr.ID)]))
				if count > MaxNeighbors {
					count = MaxNeighbors
				}
				baseIdx := int(chunkOffset(curr.ID)) * MaxNeighbors

				// Collect neighbors into a temporary local buffer to avoid contaminating visited set
				var localNeighbors [MaxNeighbors]uint32
				for i := 0; i < count; i++ {
					localNeighbors[i] = atomic.LoadUint32(&neighborsChunk[baseIdx+i])
				}

				if atomic.LoadUint32(verAddr) == ver {
					// Consistency check passed, now we can safely Visit and append
					for i := 0; i < count; i++ {
						nid := localNeighbors[i]
						if !ctx.visited.IsSet(nid) {
							ctx.Visit(nid)
							ctx.scratchIDs = append(ctx.scratchIDs, nid)
							ctx.scratchIDs = append(ctx.scratchIDs, nid)
							computer.Prefetch(nid)
						}
					}
					collected = true
					break
				}
				runtime.Gosched()
			}
		}

		if !collected || len(ctx.scratchIDs) == 0 {
			continue
		}

		// 2. Compute Distances and Update resultSet
		batchCount := len(ctx.scratchIDs)
		if cap(ctx.scratchDists) < batchCount {
			ctx.scratchDists = make([]float32, batchCount*2)
		}
		dists := ctx.scratchDists[:batchCount]

		// Compute Distances
		computer.Compute(ctx.scratchIDs, dists)

		for i, nid := range ctx.scratchIDs {
			dist := dists[i]
			if filter != nil && !filter.Contains(int(nid)) {
				continue
			}

			// Always push to candidates for further exploration if within reach
			// Candidates is a min-heap
			if resultSet.Len() < ef {
				ctx.candidates.Push(Candidate{ID: nid, Dist: dist})
				if !h.IsDeleted(nid) {
					resultSet.Push(Candidate{ID: nid, Dist: dist})
				}
			} else {
				worst, _ := resultSet.Peek()
				if dist < worst.Dist {
					ctx.candidates.Push(Candidate{ID: nid, Dist: dist})
					if !h.IsDeleted(nid) {
						resultSet.Pop()
						resultSet.Push(Candidate{ID: nid, Dist: dist})
					}
				}
			}
		}
	}

	return closest
}

func (h *ArrowHNSW) distance(q []float32, id uint32, data *GraphData, _ *ArrowSearchContext) float32 {
	v := h.mustGetVectorFromData(data, id)
	if v == nil {
		return math.MaxFloat32
	}
	// Use static dispatch function
	return simd.DistFunc(q, v)
}

func (h *ArrowHNSW) distanceF16(q []float16.Num, id uint32, data *GraphData, _ *ArrowSearchContext) float32 {
	v := data.GetVectorF16(id)
	if v == nil {
		return math.MaxFloat32
	}
	return simd.EuclideanDistanceF16(q, v)
}

func (h *ArrowHNSW) distanceComplex64(q []complex64, id uint32, data *GraphData, _ *ArrowSearchContext) float32 {
	v := data.GetVectorComplex64(id)
	if v == nil {
		return math.MaxFloat32
	}
	metrics.HNSWComplexOpsTotal.WithLabelValues("complex64").Inc()
	return simd.EuclideanDistanceComplex64(q, v)
}

func (h *ArrowHNSW) distanceComplex128(q []complex128, id uint32, data *GraphData, _ *ArrowSearchContext) float32 {
	v := data.GetVectorComplex128(id)
	if v == nil {
		return math.MaxFloat32
	}
	metrics.HNSWComplexOpsTotal.WithLabelValues("complex128").Inc()
	return simd.EuclideanDistanceComplex128(q, v)
}

func (h *ArrowHNSW) getVectorF16(id uint32) ([]float16.Num, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("no graph data loaded")
	}
	v := data.GetVectorF16(id)
	if v != nil {
		return v, nil
	}

	// Fallback to dataset (cold storage)
	v32, err := h.getVector(id)
	if err != nil {
		return nil, fmt.Errorf("vector f16 not found (and fallback failed: %v): %d", err, id)
	}

	// Convert F32 -> F16
	v16 := make([]float16.Num, len(v32))
	for i, f := range v32 {
		v16[i] = float16.New(f)
	}
	return v16, nil
}

func (h *ArrowHNSW) getVector(id uint32) ([]float32, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("no graph data loaded")
	}

	// 1. Check Hot Storage (GraphData)
	// Only return from GraphData if the ID is already committed ( < nodeCount ).
	// Memory might be allocated (capacity) for future IDs (pre-grown) but contain zeros.
	if int64(id) < h.nodeCount.Load() {
		v := h.mustGetVectorFromData(data, id)
		if v != nil {
			return v, nil
		}
	}

	// 2. Fallback to Cold Storage (Dataset Records via LocationStore)
	if h.locationStore == nil || h.dataset == nil {
		return nil, fmt.Errorf("vector not found in graph data and no fallback available: %d", id)
	}

	loc, ok := h.locationStore.Get(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector location not found: %d", id)
	}

	h.dataset.dataMu.RLock()
	defer h.dataset.dataMu.RUnlock()

	if loc.BatchIdx < 0 || loc.BatchIdx >= len(h.dataset.Records) {
		return nil, fmt.Errorf("invalid batch index %d for vector %d", loc.BatchIdx, id)
	}

	rec := h.dataset.Records[loc.BatchIdx]
	return ExtractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
}

func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := data.Dims
	// paddedDims removal fixed lint error

	if dims <= 0 {
		return nil
	}

	// 1. Try Generic Accessor (F32, F16, Complex)
	// This handles all exact-representation or simple-cast types.
	if vec, err := data.GetVectorAsFloat32(id); err == nil {
		return vec
	}

	// 2. Try SQ8 (Lossy)
	sq8Chunk := data.GetVectorsSQ8Chunk(cID)
	if sq8Chunk != nil && h.quantizer != nil {
		sq8Stride := (dims + 63) & ^63
		start := int(cOff) * sq8Stride
		sq8s := sq8Chunk[start : start+dims]
		return h.quantizer.Decode(sq8s)
	}

	// 3. Try BQ (Lossy)
	bqChunk := data.GetVectorsBQChunk(cID)
	if bqChunk != nil && h.bqEncoder != nil {
		numWords := h.bqEncoder.CodeSize()
		off := int(cOff) * numWords
		return h.bqEncoder.Decode(bqChunk[off : off+numWords])
	}

	// 4. Try PQ (Lossy)
	pqChunk := data.GetVectorsPQChunk(cID)
	if pqChunk != nil && h.pqEncoder != nil {
		pqM := h.config.PQM
		if pqM > 0 {
			off := int(cOff) * pqM
			vec, err := h.pqEncoder.Decode(pqChunk[off : off+pqM])
			if err == nil {
				return vec
			}
		}
	}

	// 5. Sentinel Fallback (Data Miss / Race Condition)
	// If we reached here, the vector ID is valid in the graph but data is missing.
	// This can happen during high-concurrency ingestion where the graph link is
	// visible before the data is fully committed/copied.
	// Returning a sentinel avoids panics in the hot path.
	metrics.VectorSentinelHitTotal.Inc()
	return h.getSentinelVector(dims)
}

// getSentinelVector returns a zero-filled vector of the correct dimension.
// It uses a cached slice if available or allocates one.
// Since this is an error path fallback, allocation is acceptable, but we try to be cheap.
func (h *ArrowHNSW) getSentinelVector(dims int) []float32 {
	// TODO: Use a pool or pre-allocated global if this happens frequently.
	// For now, simple allocation is fine as this should be rare (0 in steady state).
	return make([]float32, dims)
}

func (h *ArrowHNSW) prefetchNode(id uint32, data *GraphData, useSQ8, usePQ bool) {
	switch {
	case useSQ8:
		cID := chunkID(id)
		if vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID); vecSQ8Chunk != nil {
			cOff := chunkOffset(id)
			dims := int(h.dims.Load())
			off := int(cOff) * dims
			if off < len(vecSQ8Chunk) {
				simd.Prefetch(unsafe.Pointer(&vecSQ8Chunk[off]))
				metrics.PrefetchOperationsTotal.Inc()
			}
		}
	case usePQ:
		cID := chunkID(id)
		if vecPQChunk := data.GetVectorsPQChunk(cID); vecPQChunk != nil {
			cOff := chunkOffset(id)
			pqM := data.PQDims
			off := int(cOff) * pqM
			if off < len(vecPQChunk) {
				simd.Prefetch(unsafe.Pointer(&vecPQChunk[off]))
				metrics.PrefetchOperationsTotal.Inc()
			}
		}
	default:
		cID := chunkID(id)
		if vecChunk := data.GetVectorsChunk(cID); vecChunk != nil {
			cOff := chunkOffset(id)
			dims := int(h.dims.Load())
			off := int(cOff) * dims
			if off < len(vecChunk) {
				simd.Prefetch(unsafe.Pointer(&vecChunk[off]))
				metrics.PrefetchOperationsTotal.Inc()
			}
		}
	}
}
