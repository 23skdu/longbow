package store

import (
	"fmt"
	"math"

	"runtime"
	"slices"
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
	useRefinement := (h.config.SQ8Enabled || h.config.PQEnabled) && h.config.RefinementFactor > 1.0
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

	// SQ8 Setup
	// Check if backend supports SQ8?
	// We assume if config.SQ8Enabled is true, backend supports it.

	// ... (Rest of setup)

	// Encode query if SQ8 enabled
	dims := int(h.dims.Load())
	if h.quantizer != nil {
		if cap(ctx.querySQ8) < dims {
			ctx.querySQ8 = make([]byte, dims)
		}
		ctx.querySQ8 = ctx.querySQ8[:dims]
		h.quantizer.Encode(q, ctx.querySQ8)
	}

	// Encode query if BQ enabled
	if h.bqEncoder != nil {
		// Calculate BQ size
		numWords := h.bqEncoder.CodeSize()
		if cap(ctx.queryBQ) < numWords {
			ctx.queryBQ = make([]uint64, numWords)
		}
		ctx.queryBQ = ctx.queryBQ[:numWords]
		copy(ctx.queryBQ, h.bqEncoder.Encode(q))
	}

	// Encode query if Float16 enabled
	if h.config.Float16Enabled {
		if cap(ctx.queryF16) < dims {
			ctx.queryF16 = make([]float16.Num, dims)
		}
		ctx.queryF16 = ctx.queryF16[:dims]
		for i, v := range q {
			ctx.queryF16[i] = float16.New(v)
		}
	}

	// Setup PQ / ADC
	if h.config.PQEnabled && h.pqEncoder != nil {
		table, err := h.pqEncoder.BuildADCTable(q)
		if err == nil {
			if cap(ctx.adcTable) < len(table) {
				ctx.adcTable = make([]float32, len(table))
			}
			ctx.adcTable = ctx.adcTable[:len(table)]
			copy(ctx.adcTable, table)
		}
	}

	// Ensure visited bitset is large enough
	// Reuse or create visited bitset
	nodeCount := int(h.nodeCount.Load())
	if ctx.visited == nil {
		ctx.visited = NewArrowBitset(nodeCount + 1000)
	} else if ctx.visited.Size() < nodeCount {
		ctx.visited.Grow(nodeCount + 1000)
		// Grow clears new bits, ResetVisited handles old ones
	}
	// We do NOT clear set here, because searchLayer calls ResetVisited()

	// Start from entry point (atomic load)
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	data := h.data.Load()

	// Search from top layer to layer 1
	for level := maxL; level > 0; level-- {
		ep = h.searchLayer(q, ep, 1, level, ctx, data, nil)
	}

	// Search layer 0 with ef candidates
	h.searchLayer(q, ep, ef, 0, ctx, data, filter)

	// Extract results
	results := make([]SearchResult, 0, targetK)

	// Extract results from resultSet (MaxHeap returns Worst first)
	// We pop all, then reverse to get Best -> Worst
	// Or if using refinement, we'll sort anyway.

	for ctx.resultSet.Len() > 0 {
		cand, ok := ctx.resultSet.Pop()
		if !ok {
			break
		}

		id := cand.ID
		score := cand.Dist

		// If Re-ranking, recompute score
		if useRefinement {
			vec := h.mustGetVectorFromData(data, id)
			if len(vec) == dims {
				// Compute exact distance using configured metric
				score = h.distFunc(q, vec)
			}
		}

		results = append(results, SearchResult{
			ID:    VectorID(id),
			Score: score,
		})
	}

	// Reverse results (since MaxHeap Pop gave Worst...Best)
	// Unless we re-rank and sort later.
	if !useRefinement {
		slices.Reverse(results)
		if len(results) > k {
			results = results[:k]
		}
	}

	// If refinement used, sort and truncate
	if useRefinement {
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score < results[j].Score // Ascending distance (smallest first)
		})

		if len(results) > k {
			results = results[:k]
		}
	}

	return results, nil
}

// searchLayer performs greedy search at a specific layer.
// Returns the closest node found.
func (h *ArrowHNSW) searchLayer(q []float32, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *GraphData, filter *query.Bitset) (candidate uint32) {
	// Optimistic Clearing: Use sparse list instead of O(N) memset
	ctx.ResetVisited()
	ctx.candidates.Clear()

	// Initialize with entry point
	entryDist := h.distance(q, entryPoint, data, ctx)
	if h.config.Float16Enabled {
		entryDist = h.distanceF16(ctx.queryF16, entryPoint, data, ctx)
	}

	ctx.candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.Visit(entryPoint)

	// W: result set (max-heap to track furthest result)
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

	// SQ8 Setup
	useSQ8 := h.quantizer != nil && len(data.VectorsSQ8) > 0
	usePQ := h.config.PQEnabled && h.pqEncoder != nil && len(data.VectorsPQ) > 0
	useF16 := h.config.Float16Enabled
	adcTable := ctx.adcTable
	_ = useF16

	var useBatchCompute bool

	noImprovementIterations := 0
	maxNoImprovement := ef * 3 // More conservative threshold
	const epsilon = 1e-5

	// Greedy search
	for ctx.candidates.Len() > 0 {
		// Adaptive Early Termination: Stop if we are not improving significantly
		if h.config.AdaptiveEf && noImprovementIterations >= maxNoImprovement {
			if h.metricEarlyTermination != nil {
				h.metricEarlyTermination.WithLabelValues(h.getDatasetName(), "convergence").Inc()
			}
			break
		}

		// Get nearest candidate (min-heap)
		curr, ok := ctx.candidates.Pop()
		if !ok {
			break
		}

		if curr.Dist < closestDist {
			if curr.Dist < closestDist-epsilon {
				noImprovementIterations = 0
			}
			closestDist = curr.Dist
			closest = curr.ID
		} else {
			noImprovementIterations++
		}

		// Stop if current is farther than furthest result (and result set is full)
		if resultSet.Len() >= ef {
			worst, ok := resultSet.Peek()
			if ok && curr.Dist > worst.Dist {
				break
			}
		}

		// Explore neighbors

		// 1. Collect unvisited neighbors - Batching Phase
		for {
			ctx.scratchIDs = ctx.scratchIDs[:0]

			// Seqlock read start
			versionsChunk := data.GetVersionsChunk(layer, chunkID(curr.ID))
			if versionsChunk == nil {
				// Should not happen, but safeguard
				runtime.Gosched()
				continue
			}
			verAddr := &versionsChunk[chunkOffset(curr.ID)]
			ver := atomic.LoadUint32(verAddr)

			if ver%2 != 0 {
				runtime.Gosched()
				continue
			}

			// Get neighbors
			cID := chunkID(curr.ID)
			cOff := chunkOffset(curr.ID)

			// Check if chunk exists
			countsChunk := data.GetCountsChunk(layer, cID)
			if countsChunk == nil {
				continue
			}

			// 1. Get number of neighbors
			count := atomic.LoadInt32(&countsChunk[cOff])
			neighborCount := int(count)
			baseIdx := int(cOff) * MaxNeighbors

			neighborsChunk := data.GetNeighborsChunk(layer, cID)
			if neighborsChunk == nil {
				continue
			}

			for i := 0; i < neighborCount; i++ {
				// Atomic load to satisfy race detector
				neighborID := atomic.LoadUint32(&neighborsChunk[baseIdx+i])
				if !ctx.visited.IsSet(neighborID) {
					// Speculative add - do not Set visited yet
					ctx.scratchIDs = append(ctx.scratchIDs, neighborID)

					// Software Prefetching: Hint CPU to fetch neighbor's vector data
					// Determine address based on configuration (SQ8, BQ, or Float32)
					// This is best-effort optimization.
					switch {
					case useSQ8:
						cID := chunkID(neighborID)
						if vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID); vecSQ8Chunk != nil {
							cOff := chunkOffset(neighborID)
							dims := int(h.dims.Load())
							off := int(cOff) * dims
							if off < len(vecSQ8Chunk) {
								simd.Prefetch(unsafe.Pointer(&vecSQ8Chunk[off]))
								metrics.PrefetchOperationsTotal.Inc()
							}
						}
					case usePQ:
						// PQ Prefetch
						cID := chunkID(neighborID)
						if vecPQChunk := data.GetVectorsPQChunk(cID); vecPQChunk != nil {
							cOff := chunkOffset(neighborID)
							pqM := data.PQDims
							off := int(cOff) * pqM
							if off < len(vecPQChunk) {
								simd.Prefetch(unsafe.Pointer(&vecPQChunk[off]))
								metrics.PrefetchOperationsTotal.Inc()
							}
						}
					case h.config.BQEnabled && h.bqEncoder != nil:
						// BQ Prefetch
						if vecBQ := data.GetVectorBQ(neighborID); len(vecBQ) > 0 {
							simd.Prefetch(unsafe.Pointer(&vecBQ[0]))
							metrics.PrefetchOperationsTotal.Inc()
						}
					default:
						// Float32 Prefetch (Chunked)
						cID := chunkID(neighborID)
						if vecChunk := data.GetVectorsChunk(cID); vecChunk != nil {
							cOff := chunkOffset(neighborID)
							dims := int(h.dims.Load())
							off := int(cOff) * dims
							if off < len(vecChunk) {
								simd.Prefetch(unsafe.Pointer(&vecChunk[off]))
								metrics.PrefetchOperationsTotal.Inc()
							}
						}
					}
				}
			}

			// Verify concurrency
			if atomic.LoadUint32(verAddr) != ver {
				continue
			}

			// Commit visited state
			for _, nid := range ctx.scratchIDs {
				ctx.Visit(nid)
			}

			batchCount := len(ctx.scratchIDs)
			if batchCount == 0 {
				break
			}

			// 2. Compute Distances - Batch Processing Phase
			var dists []float32
			if useF16 {
				if cap(ctx.scratchDists) < batchCount {
					ctx.scratchDists = make([]float32, batchCount*2)
				}
				dists = ctx.scratchDists[:batchCount]
				for i, nid := range ctx.scratchIDs {
					dists[i] = h.distanceF16(ctx.queryF16, nid, data, ctx)
				}
			} else {
				if cap(ctx.scratchDists) < batchCount {
					ctx.scratchDists = make([]float32, batchCount*2)
				}
				dists = ctx.scratchDists[:batchCount]

				if bc, ok := h.batchComputer.(interface{ ShouldUseBatchCompute(int) bool }); ok {
					useBatchCompute = bc.ShouldUseBatchCompute(batchCount)
				}
			}

			// Redefine useBatchCompute more broadly if needed, but it's only used below.
			// Let's ensure it's accessible.
			_ = useBatchCompute

			if usePQ && len(adcTable) > 0 {
				// PQ/ADC Path
				pqM := data.PQDims
				if cap(ctx.scratchPQCodes) < batchCount*pqM {
					ctx.scratchPQCodes = make([]byte, batchCount*pqM)
				}
				ctx.scratchPQCodes = ctx.scratchPQCodes[:batchCount*pqM]

				for i, nid := range ctx.scratchIDs {
					cID := chunkID(nid)
					cOff := chunkOffset(nid)
					off := int(cOff) * pqM
					vecPQChunk := data.GetVectorsPQChunk(cID)
					if vecPQChunk != nil && off+pqM <= len(vecPQChunk) {
						copy(ctx.scratchPQCodes[i*pqM:(i+1)*pqM], vecPQChunk[off:off+pqM])
					} else {
						// Fill with zero or handle error
						for j := 0; j < pqM; j++ {
							ctx.scratchPQCodes[i*pqM+j] = 0
						}
					}
				}

				// Compute ADCDistances
				if h.pqEncoder != nil {
					if err := h.pqEncoder.ADCDistanceBatch(adcTable, ctx.scratchPQCodes, dists); err != nil {
						for i := range dists {
							dists[i] = math.MaxFloat32
						}
					}
				} else {
					for i := range dists {
						dists[i] = math.MaxFloat32
					}
				}
			} else if useSQ8 && h.metric == MetricEuclidean {
				// Ensure querySQ8 is correctly set in context for useSQ8 path
				if len(ctx.querySQ8) == 0 {
					ctx.querySQ8 = h.quantizer.Encode(q, nil)
				}

				for i, nid := range ctx.scratchIDs {
					cID := chunkID(nid)
					cOff := chunkOffset(nid)
					dims := int(h.dims.Load())
					off := int(cOff) * dims

					vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
					if vecSQ8Chunk != nil && off+dims <= len(vecSQ8Chunk) {
						d := simd.EuclideanDistanceSQ8(ctx.querySQ8, vecSQ8Chunk[off:off+dims])
						dists[i] = float32(d)
					} else {
						dists[i] = math.MaxFloat32
						dists[i] = math.MaxFloat32
					}
				}
			} else if useBQ := h.config.BQEnabled && h.bqEncoder != nil; useBQ {
				// BQ Path
				if len(ctx.queryBQ) == 0 {
					ctx.queryBQ = h.bqEncoder.Encode(q)
				}

				for i, nid := range ctx.scratchIDs {
					vec := data.GetVectorBQ(nid)
					if vec != nil {
						// Compute Hamming Distance
						hamming := h.bqEncoder.HammingDistance(ctx.queryBQ, vec)
						// Convert to float score (or keep as distance? HNSW uses dists)
						// Hamming is a distance. Lower is better.
						// Just cast to float32.
						dists[i] = float32(hamming)
					} else {
						dists[i] = math.MaxFloat32
					}
				}
			} else if useBatchCompute && h.metric == MetricEuclidean {
				// Vectorized batch distance computation using Arrow compute
				// Gather vectors for batch processing
				if cap(ctx.scratchVecs) < batchCount {
					ctx.scratchVecs = make([][]float32, batchCount*2)
				}
				vecs := ctx.scratchVecs[:batchCount]

				allValid := true
				for i, nid := range ctx.scratchIDs {
					v := h.mustGetVectorFromData(data, nid)
					if v == nil {
						dists[i] = math.MaxFloat32
						vecs[i] = nil
						allValid = false
					} else {
						vecs[i] = v
					}
				}

				if allValid {
					// Use batch computer for vectorized distances
					if bc, ok := h.batchComputer.(interface {
						ComputeL2Distances(query []float32, vectors [][]float32) ([]float32, error)
					}); ok {
						batchDists, err := bc.ComputeL2Distances(q, vecs)
						if err != nil {
							// Fallback to SIMD on error
							simd.EuclideanDistanceBatch(q, vecs, dists)
						} else {
							copy(dists, batchDists)
						}
					} else {
						simd.EuclideanDistanceBatch(q, vecs, dists)
					}
				} else {
					// Handle nil vectors by computing individually for non-nil
					for i := 0; i < batchCount; i++ {
						if vecs[i] != nil {
							h.batchDistFunc(q, vecs[i:i+1], dists[i:i+1])
						}
					}
				}
			} else {
				// Float32 Path (SIMD for small batches)
				if cap(ctx.scratchVecs) < batchCount {
					ctx.scratchVecs = make([][]float32, batchCount*2)
				}
				vecs := ctx.scratchVecs[:batchCount]

				allValid := true
				for i, nid := range ctx.scratchIDs {
					v := h.mustGetVectorFromData(data, nid)
					if v == nil {
						dists[i] = math.MaxFloat32
						vecs[i] = nil
						allValid = false
					} else {
						vecs[i] = v
					}
				}
				if allValid {
					h.batchDistFunc(q, vecs, dists)
				} else {
					for i := 0; i < batchCount; i++ {
						if vecs[i] != nil {
							h.batchDistFunc(q, vecs[i:i+1], dists[i:i+1])
						}
					}
				}
			}

			// 3. Process Results
			// fmt.Printf("DEBUG: Post-Compute\n")
			for i, neighborID := range ctx.scratchIDs {
				dist := dists[i]

				// Update closest
				if dist < closestDist {
					closest = neighborID
					closestDist = dist
				}

				// Check filter if provided
				// NOTE: We check type first as imported query.Bitset logic
				if filter != nil && !filter.Contains(int(neighborID)) {
					continue
				}

				// Update result set - only add if not deleted
				isDeleted := h.IsDeleted(neighborID)
				shouldAdd := !isDeleted && resultSet.Len() < ef
				if !isDeleted && !shouldAdd {
					worst, ok := resultSet.Peek()
					if ok && dist < worst.Dist {
						shouldAdd = true
					}
				}

				if shouldAdd {
					if resultSet.Len() >= ef {
						resultSet.Pop()
					}
					resultSet.Push(Candidate{ID: neighborID, Dist: dist})
				}

				// Always add to candidates to maintain graph navigability
				ctx.candidates.Push(Candidate{ID: neighborID, Dist: dist})

			}
		}

		// For layer 0, we leave results in resultSet for the caller to extract.

	}

	return closest
}

func (h *ArrowHNSW) distance(q []float32, id uint32, data *GraphData, ctx *ArrowSearchContext) float32 {
	metrics.HnswDistanceCalculations.Inc()
	_ = ctx
	// BQ Check
	if h.config.BQEnabled && h.bqEncoder != nil && len(data.VectorsBQ) > 0 {
		vec := data.GetVectorBQ(id)
		if vec != nil {
			// Need quantized query.
			// Ideally passed in or cached.
			qBQ := h.bqEncoder.Encode(q)
			dist := h.bqEncoder.HammingDistance(qBQ, vec)
			return float32(dist)
		}
	}

	// PQ Check
	if h.config.PQEnabled && h.pqEncoder != nil && len(data.VectorsPQ) > 0 {
		vec := data.GetVectorPQ(id)
		if vec != nil {
			// Use ADC table if available in context
			if len(ctx.adcTable) > 0 {
				dist, err := h.pqEncoder.ADCDistance(ctx.adcTable, vec)
				if err == nil {
					return dist
				}
			}
			// Fallback: build table on fly for this single check
			table, err := h.pqEncoder.BuildADCTable(q)
			if err == nil {
				dist, _ := h.pqEncoder.ADCDistance(table, vec)
				return dist
			}
		}
	}

	// SQ8 Check
	if h.metric == MetricEuclidean && h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		cID := chunkID(id)
		cOff := chunkOffset(id)
		dims := int(h.dims.Load())
		off := int(cOff) * dims

		// Helper handles nil/bounds
		sq8Chunk := data.GetVectorsSQ8Chunk(cID)
		if sq8Chunk != nil && off+dims <= len(sq8Chunk) {
			// We need query to be quantized too.
			// Just quantize on fly for single distance check (negligible for entry point).
			qSQ8 := h.quantizer.Encode(q, nil)
			d := simd.EuclideanDistanceSQ8(qSQ8, sq8Chunk[off:off+dims])
			return float32(d)
		}
	}

	// Optimization: Check for cached vector pointer (avoids Arrow overhead)
	// This is safe because VectorPtr is pinned to the Arrow RecordBatch which is kept alive by Dataset
	// Optimization: Check for dense vector storage (avoids Arrow overhead)
	cID := chunkID(id)

	vecChunk := data.GetVectorsChunk(cID)
	if vecChunk != nil {
		cOff := chunkOffset(id)
		dims := int(h.dims.Load())
		start := int(cOff) * dims
		if start+dims <= len(vecChunk) {
			vec := vecChunk[start : start+dims]
			return h.distFunc(q, vec)
		}
	}

	// Fallback: Get vector from Arrow storage (zero-copy)
	vec, err := h.getVector(id)
	if err != nil {
		return float32(math.Inf(1))
	}

	// Use SIMD-optimized distance calculation
	return h.distFunc(q, vec)
}

func (h *ArrowHNSW) distanceF16(q []float16.Num, id uint32, data *GraphData, ctx *ArrowSearchContext) float32 {
	metrics.HNSWDistanceCalculationsF16Total.Inc()
	_ = ctx
	vec := data.GetVectorF16(id)
	if vec == nil {
		// Fallback to cold storage if needed (unlikely in hot path)
		_, err := h.getVector(id)
		if err != nil {
			return float32(math.Inf(1))
		}
		return 0
	}
	return h.distFuncF16(q, vec)
}

// l2Distance computes Euclidean (L2) distance between two vectors.
// Kept for testing and as fallback.
func l2Distance(a, b []float32) float32 {
	return simd.EuclideanDistance(a, b)
}

// getVector retrieves a vector from Arrow storage using zero-copy access.
// This uses the Dataset's Index locationStore to map VectorID to (BatchIdx, RowIdx).
func (h *ArrowHNSW) getVector(id uint32) ([]float32, error) {
	// Relaxed check: Rely on dataset to validation ID presence
	// if int(id) >= int(h.nodeCount.Load()) { ... }

	// Try internal location store first (primary path for ArrowHNSW)
	if h.locationStore != nil {
		if loc, ok := h.locationStore.Get(VectorID(id)); ok {
			if h.dataset == nil {
				return nil, fmt.Errorf("dataset is nil for vector %d", id)
			}
			rec, ok := h.dataset.GetRecord(loc.BatchIdx)
			if !ok {
				return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
			}
			return ExtractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
		}
	}

	// Fallback to dataset index (legacy/hybrid mode)
	if h.dataset == nil {
		return nil, fmt.Errorf("dataset is nil and vector %d not in internal store", id)
	}

	idx := h.dataset.Index
	if idx == nil {
		return nil, fmt.Errorf("dataset index is nil and vector %d not in internal store", id)
	}

	// Get location from index
	loc, ok := idx.GetLocation(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector %d not found in locationStore", id)
	}

	// Get the Arrow record batch
	rec, ok := h.dataset.GetRecord(loc.BatchIdx)
	if !ok {
		return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
	}
	// Extract vector using zero-copy Arrow access
	return ExtractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
}

// getVectorF16 retrieves a vector from Arrow storage as Float16 (Zero-Copy).
func (h *ArrowHNSW) getVectorF16(id uint32) ([]float16.Num, error) {
	if h.locationStore != nil {
		if loc, ok := h.locationStore.Get(VectorID(id)); ok {
			if h.dataset == nil {
				return nil, fmt.Errorf("dataset is nil for vector %d", id)
			}
			rec, ok := h.dataset.GetRecord(loc.BatchIdx)
			if !ok {
				return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
			}
			return ExtractVectorF16FromArrow(rec, loc.RowIdx, h.vectorColIdx)
		}
	}

	// Fallback to dataset index
	if h.dataset == nil || h.dataset.Index == nil {
		return nil, fmt.Errorf("dataset index is nil and vector %d not in internal store", id)
	}
	loc, ok := h.dataset.Index.GetLocation(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector %d not found in locationStore", id)
	}
	rec, ok := h.dataset.GetRecord(loc.BatchIdx)
	if !ok {
		return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
	}
	return ExtractVectorF16FromArrow(rec, loc.RowIdx, h.vectorColIdx)
}
func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32 {
	// Try getting from GraphData (hot storage) first if available
	if data != nil && data.Dims > 0 {
		if h.config.Float16Enabled {
			cID := chunkID(id)

			chunk := data.GetVectorsF16Chunk(cID)
			if chunk != nil {
				cOff := chunkOffset(id)
				start := int(cOff) * data.Dims
				if start+data.Dims <= len(chunk) {
					res := make([]float32, data.Dims)
					src := chunk[start : start+data.Dims]
					for i := range res {
						res[i] = src[i].Float32()
					}
					return res
				}
			}
		}

		cID := chunkID(id)
		chunk := data.GetVectorsChunk(cID)
		if chunk != nil {
			cOff := chunkOffset(id)
			start := int(cOff) * data.Dims
			if start+data.Dims <= len(chunk) {
				return chunk[start : start+data.Dims]
			}
		}
	}

	// Check Disk Store
	if data != nil && data.DiskStore != nil {
		metrics.DiskStoreReadBytesTotal.WithLabelValues(h.getDatasetName()).Add(float64(data.Dims * 4))
		vec, err := data.DiskStore.Get(id)
		if err == nil {
			return vec
		}
	}

	// Check Disk Store
	if data != nil && data.DiskStore != nil {
		metrics.DiskStoreReadBytesTotal.WithLabelValues(h.getDatasetName()).Add(float64(data.Dims * 4))
		vec, err := data.DiskStore.Get(id)
		if err == nil {
			return vec
		}
	}

	// Fallback to LocationStore/Dataset (cold storage)
	vec, err := h.getVector(id)
	if err != nil {
		// Panic or log? In HNSW, missing vector during search/insert is critical.
		// We return a zero-length slice or nil to avoid crash, but this method implies "must".
		// For now, return nil which might panic downstream or check usage.
		// panic(err)
		return nil
	}
	return vec
}
