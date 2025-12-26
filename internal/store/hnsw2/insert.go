package hnsw2

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"
	
	"github.com/23skdu/longbow/internal/simd"
)

// Grow implementation moved to graph.go

// TrainPQ trains the PQ encoder on the provided sample vectors and enables PQ.
func (h *ArrowHNSW) TrainPQ(vectors [][]float32) error {
	// Deprecated in favor of ScalarQuantizer?
	// Leaving as no-op or TODO for now to avoid breaking existing code if any.
	return nil
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// 1. Acquire Shared Lock for reading properties
	h.resizeMu.RLock()
	
	// 2. Check Capacity & SQ8 Status
	// Note: h.data.Load() is atomic, but we need consistency throughout insert
	data := h.data.Load()
	sq8Missing := h.config.SQ8Enabled && len(data.VectorsSQ8) == 0 && h.dims > 0
	
	if data.Capacity <= int(id) || sq8Missing {
		// Need to grow or re-alloc. Release shared lock.
		h.resizeMu.RUnlock()
		
		// Grow (acquires exclusive lock)
		// If just SQ8 missing, we pass current capacity to force check
		targetCap := int(id) + 1
		if sq8Missing && targetCap < data.Capacity {
			targetCap = data.Capacity
		}
		h.Grow(targetCap)
		
		// Re-acquire shared lock
		h.resizeMu.RLock()
		
		// Reload data after potential resize
		data = h.data.Load()
	}
	defer h.resizeMu.RUnlock()
	
	// Get current graph data (refresh not needed as Load() is atomic and pointer swap implies strictly newer data, but we already have `data` from above.)
	// Actually, wait. Insert flow:
	// If we resized, `data` was updated at line 119.
	// If we didn't resize, `data` is from line 107.
	// So `data` is current. We don't need to reload it.
	// Or if we do, just assign it.
	
	// Initialize the new node
	data.Levels[id] = uint8(level)

	// Get vector for distance calculations (and caching)
	vec, err := h.getVector(id)
	if err != nil {
		return err
	}
	
	// Cache dimensions on first insert
	if h.dims == 0 && len(vec) > 0 {
		h.dims = len(vec)
	}
	
	// Cache unsafe pointer
	if len(vec) > 0 {
		atomic.StorePointer(&data.VectorPtrs[id], unsafe.Pointer(&vec[0]))
	}
	
	// Check if this is the first node
	if h.nodeCount.Load() == 0 {
		h.initMu.Lock()
		if h.nodeCount.Load() == 0 {
			h.entryPoint.Store(id)
			h.maxLevel.Store(int32(level))
			h.nodeCount.Store(1)
			h.initMu.Unlock()
			return nil
		}
		h.initMu.Unlock()
	}
	
	// SQ8 Encoding
	if h.quantizer == nil && h.config.SQ8Enabled && h.dims > 0 {
		h.quantizer = NewScalarQuantizer(h.dims)
	}
	
	if h.quantizer != nil {
		// Auto-train on first vector if needed?
		// For now assume quantizer is pre-configured or defaults are used.
		// Ensure capacity
		offset := int(id) * h.dims
		if len(data.VectorsSQ8) < offset+h.dims {
			return fmt.Errorf("SQ8 storage insufficient for vector %d", id)
		}
		if offset+h.dims <= len(data.VectorsSQ8) {
			qVec := h.quantizer.Encode(vec)
			copy(data.VectorsSQ8[offset:], qVec)
		}
	}
	
	// Search for nearest neighbors at each layer
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	
	// Search from top layer down to level+1
	
	// Use search pool to avoid allocations
	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)
	
	for lc := maxL; lc > level; lc-- {
		// Find closest point at this layer
		neighbors := h.searchLayerForInsert(ctx, vec, ep, 1, lc, data)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Insert at layers 0 to level
	for lc := level; lc >= 0; lc-- {
		// Find M nearest neighbors at this layer
		candidates := h.searchLayerForInsert(ctx, vec, ep, h.efConstruction, lc, data)
		
		// Select M neighbors (Target)
		m := h.m
		if lc == 0 {
			m = h.m * 2 // Layer 0 usually denser
		}
		// Ensure m <= Max capacity
		maxCap := h.mMax
		if lc > 0 {
			maxCap = h.mMax0
		}
		if m > maxCap {
			m = maxCap
		}
		
		neighbors := h.selectNeighbors(ctx, candidates, m, data)
		
		// Add connections
		maxConn := h.mMax
		if lc > 0 {
			maxConn = h.mMax0
		}
		
		for _, neighbor := range neighbors {
			h.addConnection(ctx, data, id, neighbor.ID, lc, maxConn)
			h.addConnection(ctx, data, neighbor.ID, id, lc, maxConn)
			
			// Prune neighbor if needed (still useful to keep strict M_max if buffer wasn't hit)
			// Read count atomically
			count := atomic.LoadInt32(&data.Counts[lc][neighbor.ID])
			if int(count) > maxConn {
				h.pruneConnections(ctx, data, neighbor.ID, maxConn, lc)
			}
		}
		
		// Update entry point
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Update max level if needed
	if level > maxL {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	}
	
	h.nodeCount.Add(1)
	return nil
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(ctx *SearchContext, query []float32, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	
	// Ensure visited bitset is large enough
	// Use maxID from location store to cover all possible IDs, including the one currently being inserted
	// Add safety margin
	maxID := int(h.locationStore.MaxID()) + 1000
	if ctx.visited.Size() < maxID {
		ctx.visited = NewBitset(maxID)
	}

	// Ensure candidates heap is large enough
	if ctx.candidates.cap < ef*2 {
		// If pool heap is too small, allocate a temporary one or resize
		// For now, just allocate a new one to be safe, though pool should usually suffice
		ctx.candidates = NewFixedHeap(ef * 2)
	}
	
	visited := ctx.visited
	candidates := ctx.candidates
	
	// Initialize with entry point
	var entryDist float32
	var querySQ8 []byte
	
	if h.quantizer != nil {
		querySQ8 = h.quantizer.Encode(query)
		// Access SQ8 data for entryPoint
		off := int(entryPoint) * h.dims
		if off+h.dims <= len(data.VectorsSQ8) {
			dSQ8 := simd.EuclideanDistanceSQ8(querySQ8, data.VectorsSQ8[off:off+h.dims])
			entryDist = float32(dSQ8)
		} else {
			// Fallback if not quantized yet? Should not happen if strictly maintained.
			entryDist = 0 // Or MaxFloat
		}
	} else {
		entryDist = simd.EuclideanDistance(query, h.mustGetVectorFromData(data, entryPoint))
	}

	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	visited.Set(entryPoint)
	
	results := make([]Candidate, 0, ef)
	results = append(results, Candidate{ID: entryPoint, Dist: entryDist})
	
	// Circuit breaker for traversal
	iterations := 0
	maxOps := h.nodeCount.Load() * 2 // Generous limit based on graph size
	if maxOps < 10000 { maxOps = 10000 }
	
	for candidates.Len() > 0 {
		iterations++
		if uint32(iterations) > maxOps {
			// Safety break to prevent infinite loops if visited set fails
			break
		}
		curr, ok := candidates.Pop()
		if !ok {
			break
		}
		
		// Stop if current is farther than furthest result
		if len(results) >= ef && curr.Dist > results[len(results)-1].Dist {
			break
		}
		
		// Explore neighbors
		neighborCount := int(atomic.LoadInt32(&data.Counts[layer][curr.ID]))
		baseIdx := int(curr.ID) * MaxNeighbors
		
		// Batch Distance Calculation for Neighbors
		// Collect unvisited neighbors
		unvisitedIDs := ctx.scratchIDs[:0]
		neighbors := data.Neighbors[layer][baseIdx : baseIdx+neighborCount]
		for _, nid := range neighbors {
			if !visited.IsSet(nid) {
				visited.Set(nid)
				unvisitedIDs = append(unvisitedIDs, nid)
			}
		}
		
		count := len(unvisitedIDs)
		if count == 0 {
			continue
		}
		
		var dists []float32
		if ctx.candidates.cap < count {
			// Reuse some buffer?
			dists = make([]float32, count)
		} else {
			// Reuse scratch?
			if cap(ctx.scratchDists) < count { ctx.scratchDists = make([]float32, count*2) }
			dists = ctx.scratchDists[:count]
		}
		
		if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
			// SQ8 Path
			// Serialize loop for now, TODO: Batch SIMD
			for i, nid := range unvisitedIDs {
				off := int(nid) * h.dims
				// Bounds check?
				d := simd.EuclideanDistanceSQ8(querySQ8, data.VectorsSQ8[off:off+h.dims])
				dists[i] = float32(d)
			}
		} else {
			if cap(ctx.scratchVecs) < count {
				ctx.scratchVecs = make([][]float32, count*2)
			}
			vecs := ctx.scratchVecs[:count]
			for i, nid := range unvisitedIDs {
				vecs[i] = h.mustGetVectorFromData(data, nid)
			}
			simd.EuclideanDistanceBatch(query, vecs, dists)
		}
		
		// Process results
		for i, nid := range unvisitedIDs {
			dist := dists[i]
			if len(results) < ef || dist < results[len(results)-1].Dist {
				candidates.Push(Candidate{ID: nid, Dist: dist})
				results = append(results, Candidate{ID: nid, Dist: dist})
				
				if len(results) > ef {
					// Simple bubble sort / insertion
					for k := len(results) - 1; k > 0; k-- {
						if results[k].Dist < results[k-1].Dist {
							results[k], results[k-1] = results[k-1], results[k]
						} else {
							break
						}
					}
					results = results[:ef]
				}
			}
		}
	}
	
	return results
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *SearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
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
	var selected []Candidate
	var remaining []Candidate
	
	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m*2)
		}
		selected = ctx.scratchSelected[:0]
		
		if cap(ctx.scratchRemaining) < len(candidates) {
			ctx.scratchRemaining = make([]Candidate, len(candidates)*2)
		}
		remaining = ctx.scratchRemaining[:len(candidates)]
	} else {
		selected = make([]Candidate, 0, m)
		remaining = make([]Candidate, len(candidates))
	}
	copy(remaining, candidates)
	
	// Use scratch buffer for discarded candidates
	var discarded []Candidate
	if ctx != nil {
		if cap(ctx.scratchDiscarded) < len(candidates) {
			ctx.scratchDiscarded = make([]Candidate, 0, len(candidates)*2)
		}
		ctx.scratchDiscarded = ctx.scratchDiscarded[:0]
		discarded = ctx.scratchDiscarded
	} else {
		discarded = make([]Candidate, 0, len(candidates))
	}
	
	// Alpha parameter for diversity (1.0 = strict, >1.0 = relaxed)
	// Relaxing alpha improves recall at cost of graph sparsity
	alpha := h.config.Alpha
	if alpha < 1.0 {
		alpha = 1.0
	}
	
	// Circuit breaker: prevent infinite loops
	maxIterations := len(candidates) * 2
	iterations := 0
	
	for len(selected) < m && len(remaining) > 0 {
		iterations++
		if iterations > maxIterations {
			// Safety: break out if we've iterated too many times
			break
		}
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
		selected = append(selected, remaining[bestIdx])
		
		// Remove from remaining
		remaining[bestIdx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
		
		// Prune remaining candidates that are too close to the selected one
		if len(selected) < m && len(remaining) > 0 {
			selectedVec := h.mustGetVectorFromData(data, selected[len(selected)-1].ID)
			
			// Batch optimization: Compute distances to selected neighbor
			count := len(remaining)
			var dists []float32
			
			if ctx != nil {
				if cap(ctx.scratchDists) < count {
					ctx.scratchDists = make([]float32, count*2)
				}
				dists = ctx.scratchDists[:count]
			} else {
				dists = make([]float32, count)
			}
			
			if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
				// SQ8 Path
				selID := selected[len(selected)-1].ID
				offSel := int(selID) * h.dims
				
				// SQ8 Vector of selected neighbor
				// Check bounds - if out of bounds, skip diversity check entirely
				if offSel+h.dims > len(data.VectorsSQ8) {
					// Can't compute distances, skip filtering
					continue
				}
				vecSel := data.VectorsSQ8[offSel : offSel+h.dims]
				
				for i := range remaining {
					// Check bounds for remaining
					offRem := int(remaining[i].ID) * h.dims
					if offRem+h.dims > len(data.VectorsSQ8) {
						// Out of bounds, use max distance to keep candidate
						dists[i] = math.MaxFloat32
						continue
					}
					
					d := simd.EuclideanDistanceSQ8(vecSel, data.VectorsSQ8[offRem : offRem+h.dims])
					dists[i] = float32(d)
				}
			} else {
				// Standard Float32 Path
				if ctx != nil && cap(ctx.scratchVecs) < count {
					ctx.scratchVecs = make([][]float32, count*2)
				}
				var remVecs [][]float32
				if ctx != nil {
					remVecs = ctx.scratchVecs[:count]
				} else {
					remVecs = make([][]float32, count)
				}
				
				for i := range remaining {
					remVecs[i] = h.mustGetVectorFromData(data, remaining[i].ID)
				}
				
				if h.batchComputer != nil {
					_, _ = h.batchComputer.ComputeL2DistancesInto(selectedVec, remVecs, dists)
				} else {
					simd.EuclideanDistanceBatch(selectedVec, remVecs, dists)
				}
			}
			
			// Filter remaining to remove candidates too close to selected
			filtered := remaining[:0]
			for i, cand := range remaining {
				distToSelected := dists[i]
				
				// Keep candidate if it's not too close to the selected neighbor
				// (i.e., distance to selected * alpha > distance to query)
				if distToSelected * alpha > cand.Dist {
					filtered = append(filtered, cand)
				} else {
					discarded = append(discarded, cand)
				}
			}
			remaining = filtered
		}
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
	
	return selected
}

// addConnection adds a directed edge from source to target at the given layer.
func (h *ArrowHNSW) addConnection(ctx *SearchContext, data *GraphData, source, target uint32, layer int, maxConn int) {
	// Acquire lock for the specific node (shard)
	lockID := source % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	countAddr := &data.Counts[layer][source]
	count := int(atomic.LoadInt32(countAddr))
	baseIdx := int(source) * MaxNeighbors
	
	// Check if already connected
	for i := 0; i < count; i++ {
		if data.Neighbors[layer][baseIdx+i] == target {
			return // Already connected
		}
	}
	
	// Create space if needed
	if count >= MaxNeighbors {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
		count = int(atomic.LoadInt32(countAddr))
	}
	
	// Add connection if space available (should be now)
	if count < MaxNeighbors {
		data.Neighbors[layer][baseIdx+count] = target
		atomic.StoreInt32(countAddr, int32(count+1))
	}
}

// pruneConnections reduces the number of connections to maxConn using the heuristic.
func (h *ArrowHNSW) pruneConnections(ctx *SearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	// Acquire lock for the specific node
	lockID := nodeID % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	h.pruneConnectionsLocked(ctx, data, nodeID, maxConn, layer)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *SearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	countAddr := &data.Counts[layer][nodeID]
	count := int(atomic.LoadInt32(countAddr))
	
	if count <= maxConn {
		return
	}
	
	// Collect all current neighbors as candidates
	baseIdx := int(nodeID) * MaxNeighbors
	candidates := make([]Candidate, count)
	
	nodeVec := h.mustGetVectorFromData(data, nodeID)
    
	// Batch processing optimization:
	var dists []float32
	if ctx != nil {
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists = ctx.scratchDists[:count]
	} else {
		dists = make([]float32, count)
	}

	// Logic Switch: SQ8 vs Float32
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		// SQ8 Path
		// Node itself is in VectorsSQ8
		offNode := int(nodeID) * h.dims
		if offNode+h.dims <= len(data.VectorsSQ8) {
			nodeSQ8 := data.VectorsSQ8[offNode : offNode+h.dims]
			
			for i := 0; i < count; i++ {
				neighborID := data.Neighbors[layer][baseIdx+i]
				offRem := int(neighborID) * h.dims
				
				// Bounds check (paranoia)
				if offRem+h.dims <= len(data.VectorsSQ8) {
					d := simd.EuclideanDistanceSQ8(nodeSQ8, data.VectorsSQ8[offRem : offRem+h.dims])
					dists[i] = float32(d)
				} else {
					dists[i] = math.MaxFloat32 // Push to end
				}
			}
		} else {
			// Fallback (e.g. quantization failed for this node)
			// Should strictly not happen if Insert works.
			for i := 0; i < count; i++ { dists[i] = math.MaxFloat32 }
		}
		
	} else {
		// Float32 Path
		// Use scratch buffers from context to avoid allocations
		if ctx != nil && cap(ctx.scratchVecs) < count {
			ctx.scratchVecs = make([][]float32, count*2)
		}
		
		var neighborVecs [][]float32
		if ctx != nil {
			neighborVecs = ctx.scratchVecs[:count]
		} else {
			neighborVecs = make([][]float32, count)
		}
		
		for i := 0; i < count; i++ {
			neighborID := data.Neighbors[layer][baseIdx+i]
			neighborVecs[i] = h.mustGetVectorFromData(data, neighborID)
		}
		
		if h.batchComputer != nil {
			_, _ = h.batchComputer.ComputeL2DistancesInto(nodeVec, neighborVecs, dists)
		} else {
			simd.EuclideanDistanceBatch(nodeVec, neighborVecs, dists)
		}
	}
	
	for i := 0; i < count; i++ {
		neighborID := data.Neighbors[layer][baseIdx+i]
		candidates[i] = Candidate{ID: neighborID, Dist: dists[i]}
	}
	
	// Run heuristic to select best M neighbors
	// Prevent infinite recursion by limiting depth
	if ctx != nil {
		ctx.pruneDepth++
		defer func() { ctx.pruneDepth-- }()
		
		// Circuit breaker: if we're too deep in recursion, just return current neighbors
		if ctx.pruneDepth > 5 {
			// Too deep, return current neighbors without pruning
			return
		}
	}
	
	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	
	// Write back
	newCount := len(selected)
	for i := 0; i < newCount; i++ {
		data.Neighbors[layer][baseIdx+i] = selected[i].ID
	}
	atomic.StoreInt32(countAddr, int32(newCount))
}

func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32 {
	// Optimization: Check for cached vector pointer
	if int(id) < len(data.VectorPtrs) {
		ptr := atomic.LoadPointer(&data.VectorPtrs[id])
		if ptr != nil && h.dims > 0 {
			return unsafe.Slice((*float32)(ptr), h.dims)
		}
	}
	
	// Fallback
	vec, err := h.getVector(id)
	if err != nil {
		return []float32{}
	}
	return vec
}


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
	
	// Cap at MaxLayers - 1
	if level >= MaxLayers {
		level = MaxLayers - 1
	}
	
	return level
}
