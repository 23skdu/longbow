package store

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"runtime"
	"sort"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(q any, k, ef int, filter *query.Bitset) ([]SearchResult, error) {
	// Encode query if SQ8 enabled
	dims := int(h.dims.Load())

	// Validate Query Dimensions
	if dims > 0 {
		var qLen, expectedLen int
		var qType string
		// Lock-free access: load backend for Type check
		backend := h.backend.Load()
		// If backend is nil, we can't check Type properly unless configs are used.
		// h.config.DataType is available.
		targetType := h.config.DataType
		if backend != nil {
			targetType = backend.Type
		}

		switch v := q.(type) {
		case []float32:
			qLen = len(v)
			qType = "float32"
			expectedLen = dims
			if targetType == VectorTypeComplex64 || targetType == VectorTypeComplex128 {
				expectedLen = dims * 2
			}
		case []float64:
			qLen = len(v)
			qType = "float64"
			expectedLen = dims
			if targetType == VectorTypeComplex128 {
				expectedLen = dims * 2
			}
		case []complex128:
			qLen = len(v)
			qType = "complex128"
			expectedLen = dims
		case []complex64:
			qLen = len(v)
			qType = "complex64"
			expectedLen = dims
		case []float16.Num:
			qLen = len(v)
			qType = "float16"
			expectedLen = dims
		}

		if qLen > 0 && qLen != expectedLen {
			return nil, fmt.Errorf("dimension mismatch: index expects %d elements (logical dims=%d), got query len %d (type=%s, index_type=%s)",
				expectedLen, dims, qLen, qType, targetType)
		}
	}

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

	metrics.HNSWSearchQueriesTotal.WithLabelValues(strconv.Itoa(dims)).Inc()
	metrics.HnswSearchThroughputDims.WithLabelValues(strconv.Itoa(dims)).Inc()

	dsName := h.dataset.Name

	// 1. Finding entry points in upper layers
	data := graph
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	// Start timer for polymorphic latency
	start := time.Now()
	typeLabel := "float32"

	totalVisited := 0
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
		metrics.HNSWSearchLatencyByType.WithLabelValues(typeLabel).Observe(duration)
		metrics.HNSWSearchLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)

		// Throughput: total bytes processed
		totalBytes := float64(totalVisited) * float64(vecBytes)
		metrics.HNSWPolymorphicThroughput.WithLabelValues(typeLabel).Add(totalBytes)
	}()

	// Resolve Distance Computer
	computer := h.resolveHNSWComputer(data, ctx, q, false)

	traversalStart := time.Now()
	for l := maxL; l > 0; l-- {
		ctx.Reset()
		ep = h.searchLayer(computer, ep, 1, l, ctx, data, nil)
		totalVisited += ctx.VisitedCount()
	}
	metrics.HNSWSearchPhaseDurationSeconds.WithLabelValues(dsName, "traversal").Observe(time.Since(traversalStart).Seconds())

	// 2. Final Search at Layer 0
	ctx.Reset()

	layer0Start := time.Now()
	_ = h.searchLayer(computer, ep, ef, 0, ctx, data, filter)
	totalVisited += ctx.VisitedCount()
	metrics.HNSWSearchPhaseDurationSeconds.WithLabelValues(dsName, "layer0").Observe(time.Since(layer0Start).Seconds())

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
		refineStart := time.Now()
		metrics.HNSWRefineThroughput.WithLabelValues(typeLabel).Add(float64(len(results)))

		// Create a high-precision computer for refinement (usually Float32)
		// We use the same context but ignore SQ8/PQ/BQ settings
		refineComputer := h.resolveHNSWComputer(data, ctx, q, false)
		// If refineComputer is SQ8, we need to force it to Float32/Primary
		// Actually resolveHNSWComputer uses h.config.SQ8Enabled.
		// For refinement, we want to bypass quantized computers.

		// TODO: Refactor resolveHNSWComputer to accept a 'forceFullPrecision' flag.
		// For now, let's manually build a float32 computer if possible.
		if _, isSQ8 := refineComputer.(*sq8Computer); isSQ8 {
			if qF32, ok := q.([]float32); ok {
				refineComputer = &float32Computer{
					data:       data,
					q:          qF32,
					dims:       data.Dims,
					paddedDims: data.GetPaddedDims(),
					distFunc:   h.distFunc,
				}
			}
		}

		for i := range results {
			results[i].Score = refineComputer.ComputeSingle(uint32(results[i].ID))
		}
		// Resort
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score < results[j].Score
		})
		// Truncate to k
		if len(results) > k {
			results = results[:k]
		}
		metrics.HNSWSearchPhaseDurationSeconds.WithLabelValues(dsName, "refinement").Observe(time.Since(refineStart).Seconds())
	}

	return results, nil
}

// minCandidate returns the candidate with the smaller distance.
// This small function encourages the compiler to use CMOV instructions.
func minCandidate(a, b Candidate) Candidate {
	if b.Dist < a.Dist {
		return b
	}
	return a
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

	// Ensure heaps are large enough for this ef
	if ctx.candidates.cap < ef {
		ctx.candidates.Grow(ef)
	}
	resultSet := ctx.resultSet
	if resultSet.cap < ef {
		resultSet = NewMaxHeap(ef)
		ctx.resultSet = resultSet
	}
	resultSet.Clear()

	ctx.candidates.Clear()
	ctx.candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.Visit(entryPoint)

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

		// Optimize closest update with potentially branchless minCandidate
		best := minCandidate(Candidate{ID: closest, Dist: closestDist}, curr)
		closest = best.ID
		closestDist = best.Dist

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

		// Fallback: Legacy Chunked Storage (Seqlock protected) or DiskGraph
		if !collected {
			// Hybrid Strategy: Check Mutable GraphData first, then DiskGraph
			usedDisk := false

			// 1. Check Mutable GraphData
			// We optimize by checking if chunk exists AND has neighbors
			var neighborsChunk []uint32
			var count int
			var verAddr *uint32
			var ver uint32

			cID := chunkID(curr.ID)
			cOff := chunkOffset(curr.ID)
			versionsChunk := data.GetVersionsChunk(layer, cID)

			if versionsChunk != nil {
				countsChunk := data.GetCountsChunk(layer, cID)
				neighborsChunk = data.GetNeighborsChunk(layer, cID)

				if countsChunk != nil && neighborsChunk != nil {
					// Check count to see if populated
					count = int(atomic.LoadInt32(&countsChunk[cOff]))
					if count > 0 {
						// Mutable version exists
						verAddr = &versionsChunk[cOff]
					}
				}
			}

			// 2. If not in Mutable, Check DiskGraph
			if count == 0 {
				disk := h.diskGraph.Load()
				if disk != nil && int(curr.ID) < disk.Size() {
					// Retrieve from Disk
					// reuse scratchNeighbors from context to avoid allocation
					// We need to cast or copy? GetNeighbors returns []uint32
					// DiskGraph.GetNeighbors takes buffer
					diskNeighbors := disk.GetNeighbors(layer, curr.ID, ctx.scratchNeighbors[:0])
					if len(diskNeighbors) > 0 {
						for _, nid := range diskNeighbors {
							if !ctx.visited.IsSet(nid) {
								ctx.Visit(nid)
								ctx.scratchIDs = append(ctx.scratchIDs, nid)
								computer.Prefetch(nid)
							}
						}
						collected = true
						usedDisk = true
					}
				}
			}

			// 3. Process Mutable Neighbors (if found and not using disk)
			if !usedDisk && count > 0 {
				// Retry loop for seqlock
				for retries := 0; retries <= 1000; retries++ {
					ver = atomic.LoadUint32(verAddr)
					if ver%2 != 0 {
						runtime.Gosched()
						continue
					}

					if count > MaxNeighbors {
						count = MaxNeighbors
					}
					baseIdx := int(cOff) * MaxNeighbors

					// Collect neighbors into a temporary local buffer
					var localNeighbors [MaxNeighbors]uint32
					for i := 0; i < count; i++ {
						localNeighbors[i] = atomic.LoadUint32(&neighborsChunk[baseIdx+i])
					}

					if atomic.LoadUint32(verAddr) == ver {
						for i := 0; i < count; i++ {
							nid := localNeighbors[i]
							if !ctx.visited.IsSet(nid) {
								ctx.Visit(nid)
								ctx.scratchIDs = append(ctx.scratchIDs, nid)
								computer.Prefetch(nid)
							}
						}
						collected = true
						break
					}
					// If failed, reload count/chunk? Usually stable within Search call scope.
					// Just retry verify.
					runtime.Gosched()
				}
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

func (h *ArrowHNSW) distanceF16(q []float16.Num, id uint32, data *GraphData, _ *ArrowSearchContext) float32 {
	v := data.GetVectorF16(id)
	if v == nil {
		return math.MaxFloat32
	}
	return simd.EuclideanDistanceF16(q, v)
}

func (h *ArrowHNSW) getVectorAny(id uint32) (any, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("no graph data loaded")
	}

	// 1. Check Hot Storage (GraphData)
	if int64(id) < h.nodeCount.Load() {
		// mustGetVectorFromData returns 'any'
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
	return ExtractVectorAny(rec, loc.RowIdx, h.vectorColIdx)
}

func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) any {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := data.Dims
	// paddedDims removal fixed lint error

	if dims <= 0 {
		return nil
	}

	// 1. Try Generic Accessor (F32, F16, Complex)
	// This handles all exact-representation types natively.
	if vec, err := data.GetVector(id); err == nil {
		return vec
	}

	// 1b. Try Float32 (Legacy / Conversion path)
	// If GetVector failed (maybe type mismatch?), try generic float32 conversion.
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

	// 5. DiskGraph Fallback
	if disk := h.diskGraph.Load(); disk != nil && int(id) < disk.Size() {
		// Try SQ8 from Disk
		if h.config.SQ8Enabled && h.quantizer != nil {
			if vecBytes := disk.GetVectorSQ8(id); vecBytes != nil {
				if len(vecBytes) == dims {
					return h.quantizer.Decode(vecBytes)
				}
			}
		}

		// Try PQ from Disk
		if h.config.PQEnabled && h.pqEncoder != nil {
			if vecBytes := disk.GetVectorPQ(id); vecBytes != nil {
				if vec, err := h.pqEncoder.Decode(vecBytes); err == nil {
					return vec
				}
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
