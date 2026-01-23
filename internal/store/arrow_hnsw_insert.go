package store

import (
	"context"
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"math/rand"
	"slices"
	"sync"

	"time"

	"github.com/23skdu/longbow/internal/metrics"

	"github.com/23skdu/longbow/internal/simd"
)

// Grow implementation moved to graph.go

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// Zero-Copy Ingestion Path
	// Get vector for distance calculations (and caching)
	// We use generic getVectorAny to support all types.
	vec, err := h.getVectorAny(id)
	if err != nil {
		return err
	}
	return h.InsertWithVector(id, vec, level)
}

// ensureTrained checks if SQ8 training is needed and performs it if sufficient data is accumulated.
// limitID specifies the max ID to backfill (inclusive).
func (h *ArrowHNSW) ensureTrained(limitID int, extraSamples [][]float32) {
	if h.sq8Ready.Load() {
		return
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	if h.sq8Ready.Load() {
		return
	}

	dims := int(h.dims.Load())
	if h.quantizer == nil {
		h.quantizer = NewScalarQuantizer(dims)
	}

	if h.quantizer.IsTrained() {
		h.sq8Ready.Store(true)
		return
	}

	// Buffer vectors for training
	for _, v := range extraSamples {
		vecCopy := make([]float32, len(v))
		copy(vecCopy, v)
		h.sq8TrainingBuffer = append(h.sq8TrainingBuffer, vecCopy)
	}

	// Check if threshold reached
	threshold := h.config.SQ8TrainingThreshold
	if threshold <= 0 {
		threshold = 1000
	}

	if len(h.sq8TrainingBuffer) >= threshold {
		// Train!
		h.quantizer.Train(h.sq8TrainingBuffer)

		// Backfill existing vectors
		if limitID >= 0 {
			currentData := h.data.Load()
			for i := uint32(0); i <= uint32(limitID); i++ {
				cID := chunkID(i)
				cOff := chunkOffset(i)

				vecChunk := currentData.GetVectorsChunk(cID)
				if vecChunk == nil {
					continue
				}

				f32Stride := currentData.GetPaddedDims()
				sq8Stride := (dims + 63) & ^63

				f32Off := int(cOff) * f32Stride
				if f32Off+dims > len(vecChunk) {
					continue
				}
				srcVec := vecChunk[f32Off : f32Off+dims]

				// Encode to SQ8 chunk
				sq8Chunk := currentData.GetVectorsSQ8Chunk(cID)
				if sq8Chunk != nil {
					sq8Off := int(cOff) * sq8Stride
					if sq8Off+dims <= len(sq8Chunk) {
						dest := sq8Chunk[sq8Off : sq8Off+dims]
						h.quantizer.Encode(srcVec, dest)
					}
				}
			}
		}

		// Clear buffer
		h.sq8TrainingBuffer = nil
		h.sq8Ready.Store(true)
	}
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *GraphData) ([]Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, false)
	_, err := h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, nil)
	if err != nil {
		return nil, err
	}

	// Transfer results from resultSet to a slice
	res := make([]Candidate, ctx.resultSet.Len())
	for i := len(res) - 1; i >= 0; i-- {
		c, _ := ctx.resultSet.Pop()
		res[i] = c
	}
	return res, nil
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
	if len(candidates) <= m {
		return candidates
	}

	// Optimization: Limit the scope of the diversity check
	// If limit is set, we only consider the top K candidates for diversity.
	// This avoids O(M * Ef) complexity when Ef is large.
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		// candidates are already sorted by distance (closest first)
		candidates = candidates[:limit]
	}

	// RobustPrune heuristic: select diverse neighbors
	// Use scratch in context or allocate (fallback)
	var selected []Candidate //nolint:prealloc // conditional allocation
	var remaining []Candidate

	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m*2)
		}
		selected = ctx.scratchSelected[:0]

		// Initialize/Reset Request Batches
		if ctx.selectedBatch == nil {
			ctx.selectedBatch = h.newVectorBatch(data)
		} else {
			ctx.selectedBatch.Reset()
		}
		if ctx.remainingBatch == nil {
			ctx.remainingBatch = h.newVectorBatch(data)
		} else {
			ctx.remainingBatch.Reset()
		}

		if cap(ctx.scratchRemaining) < len(candidates) {
			ctx.scratchRemaining = make([]Candidate, len(candidates)*2)
		}
		remaining = ctx.scratchRemaining[:len(candidates)]

		// Map variables to batches for clarity (optional, or just use ctx.X)
	} else {
		// Fallback for tests/cases without context
		selected = make([]Candidate, 0, m)
		remaining = make([]Candidate, len(candidates))
		// We need transient batches here too.
		// Since we don't have context, we creating new batches implies allocation.
		// This path is for tests mostly.
		ctx = &ArrowSearchContext{
			selectedBatch:  h.newVectorBatch(data),
			remainingBatch: h.newVectorBatch(data),
		}
	}
	copy(remaining, candidates)

	// Pre-fetch vectors once
	for i := range candidates {
		// Add returns bool, but here we just want to ensure it's added.
		// If fetch fails, we might just have nil inside the batch (handled by implementation)
		// but VectorBatch interface `Add` returns false on failure.
		// How to handle partial failure in batch?
		// float32VectorBatch.Add appends. If it fails (nil), it returns false and DOES NOT append.
		// So batch size < candidates size.
		// This will desync `remaining` (Candidates) and `remainingBatch` (Vectors).
		// We must ensure sync.
		added := ctx.remainingBatch.Add(candidates[i].ID)
		if !added {
			// If we fail to get vector, we should probably treat it as a "nil" vector
			// or remove the candidate?
			// Existing logic set `remainingVecs[i] = nil`.
			// To maintain sync with `remaining` index, `VectorBatch` should probably support adding nil?
			// Or we manually add "nil" or "zero" representation?
			// My `Add` implementation doesn't support adding nil.
			// Let's modify `Add` to append nil placehoder or simply use AddVec(nil)?
			ctx.remainingBatch.AddVec(nil)
		}
	}

	var discarded []Candidate
	if ctx != nil {
		discarded = ctx.scratchDiscarded[:0]
	} else {
		discarded = make([]Candidate, 0, m)
	}

	alpha := h.config.Alpha
	if alpha < 1.0 {
		alpha = 1.0
	}

	// Use scratchSelectedIdxs to track which candidates are still "remaining"
	// but instead of removing from slices, we'll mark them.
	// Actually, the current "remaining" logic is O(M * remaining).
	// Let's keep it but ensure NO ALLOCATIONS.

	for len(selected) < m && len(remaining) > 0 {
		// Find the closest remaining candidate
		bestIdx := 0
		bestDist := remaining[0].Dist

		for i := 1; i < len(remaining); i++ {
			if remaining[i].Dist < bestDist {
				bestDist = remaining[i].Dist
				bestIdx = i
			}
		}

		// Add to selected
		selCand := remaining[bestIdx]
		selVecAny := ctx.remainingBatch.Get(bestIdx)

		selected = append(selected, selCand)
		ctx.selectedBatch.AddVec(selVecAny)

		// Remove from remaining (O(1) swap and pop)
		lastIdx := len(remaining) - 1
		remaining[bestIdx] = remaining[lastIdx]
		remaining = remaining[:lastIdx]
		ctx.remainingBatch.Swap(bestIdx, lastIdx)
		ctx.remainingBatch.Pop()

		// Prune remaining candidates that are too close to the selected one
		if len(selected) < m && len(remaining) > 0 {
			// Batch optimization: Compute distances to selected neighbor
			count := len(remaining)
			if cap(ctx.scratchDists) < count {
				ctx.scratchDists = make([]float32, count*2)
			}
			dists := ctx.scratchDists[:count]

			if h.metric == MetricEuclidean && h.quantizer != nil && h.quantizer.IsTrained() && len(data.VectorsSQ8) > 0 {
				// SQ8 Path
				cID := chunkID(selCand.ID)
				cOff := chunkOffset(selCand.ID)
				dims := int(h.dims.Load())
				paddedDims := (dims + 63) & ^63
				offSel := int(cOff) * paddedDims

				vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
				if vecSQ8Chunk != nil && offSel+dims <= len(vecSQ8Chunk) {
					vecSel := vecSQ8Chunk[offSel : offSel+dims]
					// Batch SIMD
					if cap(ctx.scratchVecsSQ8) < count {
						ctx.scratchVecsSQ8 = make([][]byte, count*2)
					}
					vecsSQ8 := ctx.scratchVecsSQ8[:count]
					hasInvalid := false

					for i := range remaining {
						nCID := chunkID(remaining[i].ID)
						nCOff := chunkOffset(remaining[i].ID)
						offRem := int(nCOff) * paddedDims

						nVecSQ8Chunk := data.GetVectorsSQ8Chunk(nCID)
						if nVecSQ8Chunk != nil && offRem+dims <= len(nVecSQ8Chunk) {
							vecsSQ8[i] = nVecSQ8Chunk[offRem : offRem+dims]
						} else {
							vecsSQ8[i] = vecSel // Safe dummy
							hasInvalid = true
						}
					}

					if err := simd.EuclideanDistanceSQ8Batch(vecSel, vecsSQ8, dists); err != nil {
						// Fallback to single compute if batch fails (unlikely)
						for i := range dists {
							d, _ := simd.EuclideanDistanceSQ8(vecSel, vecsSQ8[i])
							dists[i] = float32(d)
						}
					}

					if hasInvalid {
						for i := range remaining {
							nCID := chunkID(remaining[i].ID)
							nVecSQ8Chunk := data.GetVectorsSQ8Chunk(nCID)
							if nVecSQ8Chunk == nil {
								dists[i] = math.MaxFloat32
								continue
							}
							nCOff := chunkOffset(remaining[i].ID)
							offRem := int(nCOff) * paddedDims
							if offRem+dims > len(nVecSQ8Chunk) {
								dists[i] = math.MaxFloat32
							}
						}
					}
				} else {
					for i := range dists {
						dists[i] = math.MaxFloat32
					}
				}
			} else {
				// Float32 Path / Default Path
				// Replaced h.batchComputer / h.batchDistFunc with VectorBatch.ComputeDistances
				ctx.remainingBatch.ComputeDistances(selVecAny, dists)
			}

			// Filter remaining
			filteredCount := 0
			for i, cand := range remaining {
				if dists[i]*alpha > cand.Dist {
					if i != filteredCount {
						remaining[filteredCount] = cand
						ctx.remainingBatch.Swap(i, filteredCount)
					}
					filteredCount++
				} else {
					discarded = append(discarded, cand)
				}
			}
			remaining = remaining[:filteredCount]
			// Pop truncated elements from batch
			currentBatchLen := ctx.remainingBatch.Len()
			for k := 0; k < currentBatchLen-filteredCount; k++ {
				ctx.remainingBatch.Pop()
			}
		}
	}

	// Update context's discarded for return if needed (heuristic keepPruned)
	if ctx != nil {
		ctx.scratchDiscarded = discarded
	}

	// If we haven't filled M, backfill from discarded (keepPrunedConnections logic)
	// This ensures we maintain connectivity even if heuristic prunes aggressively
	if h.config.KeepPrunedConnections && len(selected) < m && len(discarded) > 0 {
		// Sort discarded by distance (closest first)
		// Use slices.SortFunc to avoid reflection overhead of sort.Slice
		slices.SortFunc(discarded, func(a, b Candidate) int {
			if a.Dist < b.Dist {
				return -1
			}
			if a.Dist > b.Dist {
				return 1
			}
			return 0
		})

		for _, cand := range discarded {
			if len(selected) >= m {
				break
			}
			selected = append(selected, cand)
		}
	}

	// Return a COPY to avoid shared scratch corruption during re-entrant calls (e.g. symmetry/pruning)
	res := make([]Candidate, len(selected))
	copy(res, selected)
	return res
}

// AddConnection adds a directed edge from source to target at the given layer.

// LevelGenerator generates random levels for new nodes.
type LevelGenerator struct {
	ml  float64
	rng *rand.Rand
	mu  sync.Mutex
}

// NewLevelGenerator creates a new level generator.
func NewLevelGenerator(ml float64) *LevelGenerator {
	return &LevelGenerator{
		ml:  ml,
		rng: rand.New(rand.NewSource(42)), // Fixed seed for reproducibility
	}
}

// Generate returns a random level using exponential decay.
func (lg *LevelGenerator) Generate() int {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// Use exponential distribution: -ln(uniform(0,1)) * ml
	uniform := lg.rng.Float64()
	level := int(-math.Log(uniform) * lg.ml)

	// Cap at ArrowMaxLayers - 1
	if level >= ArrowMaxLayers {
		level = ArrowMaxLayers - 1
	}

	return level
}

// InsertWithVectorF16 inserts a Float16 vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVectorF16(id uint32, vec []float16.Num, level int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWInsertDurationSeconds.Observe(time.Since(start).Seconds())
		metrics.HNSWNodesTotal.WithLabelValues("default").Inc()
	}()

	dims := int(h.dims.Load())
	data := h.data.Load()

	// Ensure Capacity & Chunks
	if data.Capacity <= int(id) || (dims > 0 && data.VectorsF16 == nil) {
		h.Grow(int(id)+1, dims)
		data = h.data.Load()
	}

	cID := chunkID(id)
	cOff := chunkOffset(id)
	data = h.ensureChunk(data, cID, cOff, dims)

	// Acquire search context
	metrics.HNSWInsertPoolGetTotal.Inc()
	ctx := h.searchPool.Get().(*ArrowSearchContext)
	ctx.Reset()
	defer func() {
		metrics.HNSWInsertPoolPutTotal.Inc()
		h.searchPool.Put(ctx)
	}()

	// Initialize new node level
	levelsChunk := data.GetLevelsChunk(cID)
	levelsChunk[cOff] = uint8(level)

	// Cache dimensions on first insert
	if dims == 0 && len(vec) > 0 {
		h.initMu.Lock()
		dims = int(h.dims.Load())
		if dims == 0 {
			newDims := len(vec)
			h.Grow(data.Capacity, newDims)
			data = h.data.Load()
			if int(id) >= data.Capacity {
				lockStart := time.Now()
				h.growMu.Lock()
				h.metricLockWait.WithLabelValues("grow").Observe(time.Since(lockStart).Seconds())
				// Double check capacity under lock
				data = h.data.Load()
				if int(id) >= data.Capacity {
					h.Grow(int(id)+1, dims)
					data = h.data.Load()
				}
				h.growMu.Unlock()
			}
			h.dims.Store(int32(newDims))
			dims = newDims
			data = h.ensureChunk(data, cID, cOff, dims)
		} else {
			data = h.data.Load()
		}
		h.initMu.Unlock()
	}

	// Store FP16 Vector (Zero Copy / Memcpy)
	if len(vec) > 0 && dims > 0 {
		f16Chunk := data.GetVectorsF16Chunk(cID)
		if f16Chunk != nil {
			dest := f16Chunk[int(cOff)*dims : (int(cOff)+1)*dims]
			copy(dest, vec)
		}
	}

	// Setup Context Query for Search
	if cap(ctx.queryF16) < len(vec) {
		ctx.queryF16 = make([]float16.Num, len(vec))
	}
	ctx.queryF16 = ctx.queryF16[:len(vec)]
	copy(ctx.queryF16, vec)

	// Logic for BQ/PQ/SQ8 if enabled would go here (omitted for brevity/focus on F16 path)
	// Generally F16 path implies we use F16 for distance.

	// Search and Connect
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	if int(ep) >= data.Capacity {
		data = h.data.Load()
		data = h.ensureChunk(data, cID, cOff, dims)
	}

	// Descent
	for lc := maxL; lc > level; lc-- {
		neighbors := h.searchLayerForInsertF16(ctx, ctx.queryF16, ep, 1, lc, data)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	// Insertion
	ef := int(h.efConstruction.Load())
	if h.config.AdaptiveEf {
		ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
	}

	for lc := level; lc >= 0; lc-- {
		candidates := h.searchLayerForInsertF16(ctx, ctx.queryF16, ep, ef, lc, data)

		m := h.m
		if lc == 0 {
			m = h.m * 2
		}
		maxConn := h.mMax
		if lc > 0 {
			maxConn = h.mMax0
		}
		if m > maxConn {
			m = maxConn
		}

		neighbors := h.selectNeighbors(ctx, candidates, m, data)

		for _, neighbor := range neighbors {
			h.AddConnection(ctx, data, id, neighbor.ID, lc, maxConn, neighbor.Dist)
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn, neighbor.Dist)
			h.pruneIfNecessary(ctx, data, neighbor.ID, lc, maxConn)
		}

		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	if level > maxL {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	}
	h.nodeCount.Add(1)

	return nil
}

// searchLayerForInsertF16 performs search using FP16 query.
func (h *ArrowHNSW) searchLayerForInsertF16(ctx *ArrowSearchContext, query []float16.Num, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	maxID := data.Capacity + 1000
	if ctx.visited == nil {
		ctx.visited = NewArrowBitset(maxID)
	} else if ctx.visited.Size() < maxID {
		ctx.visited.Grow(maxID)
	}
	ctx.ResetVisited()

	if ctx.candidates == nil {
		ctx.candidates = NewFixedHeap(ef * 2)
	} else {
		ctx.candidates.Grow(ef * 2)
		ctx.candidates.Clear()
	}
	candidates := ctx.candidates

	if ctx.resultSet == nil {
		ctx.resultSet = NewMaxHeap(ef * 2)
	} else {
		ctx.resultSet.Grow(ef * 2)
		ctx.resultSet.Clear()
	}
	resultSet := ctx.resultSet

	// Entry Distance (F16)
	entryDist := h.distanceF16(query, entryPoint, data, ctx)

	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.Visit(entryPoint)

	ctx.scratchResults = ctx.scratchResults[:0]
	ctx.scratchResults = append(ctx.scratchResults, Candidate{ID: entryPoint, Dist: entryDist})
	results := ctx.scratchResults // To return at end if we break early? No, we return sorted copy.

	iterations := 0
	maxOps := h.nodeCount.Load() * 2
	if maxOps < 10000 {
		maxOps = 10000
	}

	for candidates.Len() > 0 {
		iterations++
		if int64(iterations) > maxOps {
			break
		}
		curr, ok := candidates.Pop()
		if !ok {
			break
		}

		if best, ok := resultSet.Peek(); ok && resultSet.Len() >= ef && curr.Dist > best.Dist {
			break
		}

		// Neighbors
		ctx.scratchNeighbors = ctx.scratchNeighbors[:0]
		// Need cap check?
		if cap(ctx.scratchNeighbors) < MaxNeighbors {
			ctx.scratchNeighbors = make([]uint32, MaxNeighbors)
		}
		// Assuming GetNeighbors fills it
		rawNeighbors := data.GetNeighbors(layer, curr.ID, ctx.scratchNeighbors)

		ctx.scratchIDs = ctx.scratchIDs[:0]
		for _, nid := range rawNeighbors {
			if !ctx.visited.IsSet(nid) {
				ctx.scratchIDs = append(ctx.scratchIDs, nid)
				ctx.Visit(nid) // Sparse mark
			}
		}
		unvisited := ctx.scratchIDs
		count := len(unvisited)
		if count == 0 {
			continue
		}

		// Calculate Distances (F16 Batch)
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists := ctx.scratchDists[:count]

		// Batched F16 Distance
		// We can loop or use SIMD if available.
		// For now simple loop calling distanceF16 which uses SIMD.
		for i, nid := range unvisited {
			dists[i] = h.distanceF16(query, nid, data, ctx)
		}

		for i, nid := range unvisited {
			dist := dists[i]
			best, _ := resultSet.Peek()
			if resultSet.Len() < ef || dist < best.Dist {
				resultSet.Push(Candidate{ID: nid, Dist: dist})
				if resultSet.Len() > ef {
					resultSet.Pop()
				}
				candidates.Push(Candidate{ID: nid, Dist: dist})
			}
		}
	}

	results = results[:0]
	for resultSet.Len() > 0 {
		cand, _ := resultSet.Pop()
		results = append(results, cand)
	}

	slices.SortFunc(results, func(a, b Candidate) int {
		if a.Dist < b.Dist {
			return -1
		} else if a.Dist > b.Dist {
			return 1
		}
		return 0
	})

	ctx.scratchResults = results
	return results
}
