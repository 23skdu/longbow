package hnsw2

import (
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"
	
	"github.com/23skdu/longbow/internal/simd"
)

// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Copy-On-Resize strategy: a new GraphData is allocated, data copied, and atomically swapped.
func (h *ArrowHNSW) Grow(minCap int) {
	// Acquire exclusive lock for resizing
	h.resizeMu.Lock()
	defer h.resizeMu.Unlock()
	
	// Double-check capacity under lock
	oldData := h.data.Load()
	if oldData.Capacity > minCap {
		return
	}

	// Calculate new capacity
	newCap := oldData.Capacity * 2
	if newCap < minCap {
		newCap = minCap
	}
	if newCap < 1000 {
		newCap = 1000
	}

	// Allocate new data
	newData := NewGraphData(newCap)

	// Copy existing data
	// 1. Levels
	copy(newData.Levels, oldData.Levels)
	
	// 2. VectorPtrs
	copy(newData.VectorPtrs, oldData.VectorPtrs)
	
	// 2b. QuantizedVectors
	if len(oldData.QuantizedVectors) > 0 {
		// Calculate M from current ratio
		// capacity * M = len
		m := len(oldData.QuantizedVectors) / oldData.Capacity
		newData.QuantizedVectors = make([]byte, newCap*m)
		copy(newData.QuantizedVectors, oldData.QuantizedVectors)
	} else if h.pqEncoder != nil {
		// If encoder exists but no vectors yet, init
		newData.QuantizedVectors = make([]byte, newCap*h.pqEncoder.config.M)
	}
	
	// 3. Neighbors and Counts for each layer
	for i := 0; i < MaxLayers; i++ {
		copy(newData.Neighbors[i], oldData.Neighbors[i])
		copy(newData.Counts[i], oldData.Counts[i])
	}
	
	// Atomically switch to new data
	h.data.Store(newData)
}

// TrainPQ trains the PQ encoder on the provided sample vectors and enables PQ.
func (h *ArrowHNSW) TrainPQ(vectors [][]float32) error {
	h.resizeMu.Lock()
	defer h.resizeMu.Unlock()
	
	enc, err := TrainPQEncoder(h.config.PQ, vectors)
	if err != nil {
		return err
	}
	h.pqEncoder = enc
	
	// Initialize storage if needed
	data := h.data.Load()
	if len(data.QuantizedVectors) == 0 {
		// Re-allocate data with PQ storage?
		// Or just allocate the slice
		// data is pointer, we can modify slice header if capacity allows?
		// No, better to allocate new slice.
		// NOTE: NewGraphData usually called in Grow.
		// If we enable PQ *after* some inserts, we should backfill?
		// For now assume "Train before Insert".
		
		// But Grow uses capacity.
		targetLen := data.Capacity * h.config.PQ.M
		dst := make([]byte, targetLen)
		data.QuantizedVectors = dst
	}
	
	return nil
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// 1. Acquire Shared Lock for reading properties
	h.resizeMu.RLock()
	
	// 2. Check Capacity
	// Note: h.data.Load() is atomic, but we need consistency throughout insert
	data := h.data.Load()
	if data.Capacity <= int(id) {
		// Need to grow. Release shared lock.
		h.resizeMu.RUnlock()
		
		// Grow (acquires exclusive lock)
		h.Grow(int(id) + 1)
		
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
	
	// Check if this is the first node
	if h.nodeCount.Load() == 0 {
		h.entryPoint.Store(id)
		h.maxLevel.Store(int32(level))
		h.nodeCount.Add(1)
		return nil
	}
	
	// Get vector for distance calculations
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
		data.VectorPtrs[id] = unsafe.Pointer(&vec[0])
	}
	
	// PQ Encoding
	if h.pqEncoder != nil {
		// Ensure capacity happens in Grow, but we might need to init slice if first time
		// (handled in TrainPQ or Grow).
		
		m := h.pqEncoder.config.M
		offset := int(id) * m
		if offset+m <= len(data.QuantizedVectors) {
			h.pqEncoder.EncodeInto(vec, data.QuantizedVectors[offset:offset+m])
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
		for _, neighbor := range neighbors {
			h.addConnection(data, id, neighbor.ID, lc)
			h.addConnection(data, neighbor.ID, id, lc)
			
			// Prune neighbor if needed
			maxConn := h.mMax
			if lc > 0 {
				maxConn = h.mMax0
			}
			
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
func (h *ArrowHNSW) searchLayerForInsert(ctx *SearchContext, query []float32, entryPoint uint32, ef int, layer int, data *GraphData) []Candidate {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	
	// Ensure visited bitset is large enough
	nodeCount := int(h.nodeCount.Load())
	if ctx.visited.Size() < nodeCount {
		ctx.visited = NewBitset(nodeCount)
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
	entryDist := simd.EuclideanDistance(query, h.mustGetVectorFromData(data, entryPoint))
	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	visited.Set(entryPoint)
	
	results := make([]Candidate, 0, ef)
	results = append(results, Candidate{ID: entryPoint, Dist: entryDist})
	
	// PQ Precomputation
	var pqTable []float32
	var usePQ bool
	if h.pqEncoder != nil && len(data.QuantizedVectors) > 0 {
		usePQ = true
		pqTable = h.pqEncoder.ComputeTableFlat(query) // TODO: Scratch
	}
	
	for candidates.Len() > 0 {
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
		// NOTE: Original code did loop with single distanceSIMD.
		// We should batch this for PQ/SIMD efficiency.
		var unvisitedIDs []uint32
		// Pre-allocate decent size?
		unvisitedIDs = make([]uint32, 0, neighborCount) 
		
		for i := 0; i < neighborCount; i++ {
			neighborID := data.Neighbors[layer][baseIdx+i]
			if !visited.IsSet(neighborID) {
				visited.Set(neighborID)
				unvisitedIDs = append(unvisitedIDs, neighborID)
			}
		}
		
		if len(unvisitedIDs) == 0 {
			continue
		}
		
		count := len(unvisitedIDs)
		var dists []float32
		if ctx.candidates.cap < count {
			// Reuse some buffer?
			dists = make([]float32, count)
		} else {
			// Reuse scratch?
			if cap(ctx.scratchDists) < count { ctx.scratchDists = make([]float32, count*2) }
			dists = ctx.scratchDists[:count]
		}
		
		if usePQ {
			m := h.pqEncoder.config.M
			flatCodes := make([]byte, count*m) // Temp
			for i, nid := range unvisitedIDs {
				offset := int(nid) * m
				copy(flatCodes[i*m:], data.QuantizedVectors[offset:offset+m])
			}
			simd.ADCDistanceBatch(pqTable, flatCodes, m, dists)
		} else {
			vecs := make([][]float32, count)
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
	
	// RobustPrune heuristic: select diverse neighbors
	selected := make([]Candidate, 0, m)
	remaining := make([]Candidate, len(candidates))
	copy(remaining, candidates)
	
	// Use scratch buffer for discarded candidates
	var discarded []Candidate
	if ctx != nil {
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
			
			if h.pqEncoder != nil && len(data.QuantizedVectors) > 0 {
				// PQ Path (SDC)
				m := h.pqEncoder.config.M
				
				// Get code for selectedVec (which is selected[...].ID)
				selID := selected[len(selected)-1].ID
				offSel := int(selID) * m
				codeA := data.QuantizedVectors[offSel : offSel+m]
				
				
				// Collect remaining codes (TODO: Scratch)
				flatCodes := make([]byte, count*m) 
				for i := range remaining {
					offset := int(remaining[i].ID) * m
					copy(flatCodes[i*m:], data.QuantizedVectors[offset:offset+m])
				}
				
				var scratch []float32
				if ctx != nil {
					scratch = ctx.scratchPQTable
				}
				h.pqEncoder.SDCDistanceOneBatch(codeA, flatCodes, m, dists, scratch)
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
				simd.EuclideanDistanceBatch(selectedVec, remVecs, dists)
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
func (h *ArrowHNSW) addConnection(data *GraphData, source, target uint32, layer int) {
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
	
	// Add connection if space available
	if count < MaxNeighbors {
		data.Neighbors[layer][baseIdx+count] = target
		atomic.StoreInt32(countAddr, int32(count+1))
	}
}

// pruneConnections reduces the number of connections to maxConn using the heuristic.
func (h *ArrowHNSW) pruneConnections(ctx *SearchContext, data *GraphData, nodeID uint32, maxConn int, layer int) {
	// Acquire lock for the specific node
	lockID := nodeID % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

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

	// Logic Switch: PQ vs Float32
	if h.pqEncoder != nil && len(data.QuantizedVectors) > 0 {
		// PQ Path (ADC)
		// We use ADC here because getting ID for nodeVec is tricky (passed as slice).
		// Overhead of ComputeTableFlat is acceptable for rare prune calls (overflow only).
		table := h.pqEncoder.ComputeTableFlat(nodeVec) 
		
		m := h.pqEncoder.config.M
		flatCodes := make([]byte, count * m) // TODO: Scratch
		for i := 0; i < count; i++ {
			neighborID := data.Neighbors[layer][baseIdx+i]
			offset := int(neighborID) * m
			copy(flatCodes[i*m:], data.QuantizedVectors[offset:offset+m])
		}
		
		simd.ADCDistanceBatch(table, flatCodes, m, dists)
		
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
		
		simd.EuclideanDistanceBatch(nodeVec, neighborVecs, dists)
	}
	
	for i := 0; i < count; i++ {
		neighborID := data.Neighbors[layer][baseIdx+i]
		candidates[i] = Candidate{ID: neighborID, Dist: dists[i]}
	}
	
	// Run heuristic to select best M neighbors
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
		ptr := data.VectorPtrs[id]
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
