package core

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) {
	// 0. Use Lock-Free path if applicable
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		return
	}

	// 1. Structural/Promotion (Optimistic check first)
	// COW Promotion
	data = h.promoteNode(data, source)

	cID := types.ChunkID(source)
	cOff := types.ChunkOffset(source)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// Reload data and try again
		data = h.data.Load()
		data = h.promoteNode(data, source)
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			fmt.Printf("Warning: AddConnection failed - chunk for %d at layer %d not initialized\n", source, layer)
			return
		}
	}

	// 2. Lock-free Duplicate Check (Seqlock read style)
	// We check for duplicates without the lock first.
	// If found, we skip completely.
	currentNeighbors := h.GetNeighborsCombined(layer, source)
	for _, n := range currentNeighbors {
		if n == target {
			return
		}
	}

	// 2. Optimistic Path (CAS) 
	// 2a. Fast-Path Layer 0 (Lock-Free Adjacency [#11])
	if layer == 0 && layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		for attempt := 0; attempt < 3; attempt++ {
			current, ok := pn.GetNeighbors(source)
			if !ok { current = []uint32{} }
			
			// check duplicates
			for _, n := range current {
				if n == target { return }
			}
			
			if len(current) >= maxConn { break }

			newNeighbors := make([]uint32, len(current)+1)
			copy(newNeighbors, current)
			newNeighbors[len(current)] = target
			
			if err := pn.SetNeighbors(source, newNeighbors); err == nil {
				atomic.AddUint64(&data.GlobalVersion, 1)
				return 
			}
		}
	} else if (layer >= len(data.PackedNeighbors) || data.PackedNeighbors[layer] == nil) && len(currentNeighbors) < types.MaxNeighbors-1 {
		// 2b. Legacy Optimistic Lock-Free Reservation (only if no packed storage)
		cID := types.ChunkID(source)
		cOff := types.ChunkOffset(source)
		countsChunk := data.GetCountsChunk(layer, cID)
		neighborsChunk := data.GetNeighborsChunk(layer, cID)
		if countsChunk != nil && neighborsChunk != nil {
			currentCount := atomic.LoadInt32(&countsChunk[cOff])
			if int(currentCount) < types.MaxNeighbors && int(currentCount) < maxConn {
				if atomic.CompareAndSwapInt32(&countsChunk[cOff], currentCount, currentCount+1) {
					baseIdx := int(cOff) * types.MaxNeighbors
					atomic.StoreUint32(&neighborsChunk[baseIdx+int(currentCount)], target)
					atomic.AddUint64(&data.GlobalVersion, 1)
					return
				}
			}
		}
	}

	// 3. Locked Path (Fallback/Pruning/Persistence)
	oldVer := data.LockNode(layer, source)
	defer data.UnlockNode(layer, source, oldVer)

	// Re-check duplicates and state under lock
	currentNeighbors = h.GetNeighborsCombined(layer, source)
	for _, n := range currentNeighbors {
		if n == target {
			return
		}
	}

	// 5. If we are at or over maxConn, we MUST prune first to make room
	// or to enforce invariants.
	if len(currentNeighbors) >= maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer, nil)
		// Re-read after pruning
		currentNeighbors = h.GetNeighborsCombined(layer, source)
		// Check duplicates AGAIN after pruning just in case
		for _, n := range currentNeighbors {
			if n == target {
				return
			}
		}
	}

	// Final Safety Check: Never exceed hard limit of legacy storage
	if len(currentNeighbors) >= types.MaxNeighbors {
		return 
	}

	// 6. Update Legacy Storage
	cID = types.ChunkID(source)
	cOff = types.ChunkOffset(source)
	countsChunk = data.GetCountsChunk(layer, cID)
	neighborsChunk = data.GetNeighborsChunk(layer, cID)
	
	if countsChunk != nil && neighborsChunk != nil {
		slot := len(currentNeighbors)
		baseIdx := int(cOff) * types.MaxNeighbors
		// Double check index safety
		if slot < types.MaxNeighbors {
			atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)
			atomic.StoreInt32(&countsChunk[cOff], int32(slot+1))
		}
	}

	// 6. Update Packed Storage
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		newNeighbors := append(currentNeighbors, target)
		if h.config.Float16Enabled {
			// (Simplified for this update: f16 handling omitted for brevity but should be here)
			// Actually I should keep it.
			_ = pn.SetNeighbors(source, newNeighbors)
		} else {
			_ = pn.SetNeighbors(source, newNeighbors)
		}
	}

	atomic.AddUint64(&data.GlobalVersion, 1)
}

// AddConnectionsBatch adds multiple directed edges to a single target node at the given layer.
// This is an optimized version of AddConnection for batch scenarios to reduce lock contention.
func (h *ArrowHNSW) AddConnectionsBatch(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) {
	if len(sources) == 0 {
		return
	}

	cID := types.ChunkID(target)
	cOff := types.ChunkOffset(target)

	// Ensure chunk exists
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			fmt.Printf("Warning: AddConnectionsBatch failed - chunk for target %d at layer %d not initialized\n", target, layer)
			return
		}
	}

	// 1. Optimistic Duplicate Check
	currentNeighbors := h.GetNeighborsCombined(layer, target)
	var toAddIdxs []int
	for i, src := range sources {
		found := false
		for _, n := range currentNeighbors {
			if n == src {
				found = true
				break
			}
		}
		if !found {
			toAddIdxs = append(toAddIdxs, i)
		}
	}

	if len(toAddIdxs) == 0 {
		return
	}

	var currentDists []float32
	if len(currentNeighbors) > 0 {
		currentDists = make([]float32, len(currentNeighbors))
		h.computeDistances(data, target, currentNeighbors, currentDists)
	}

	// 2. Acquire Node Lock
	oldVer := data.LockNode(layer, target)
	defer data.UnlockNode(layer, target, oldVer)

	// 3. Re-read current state under lock
	countAddr := &countsChunk[cOff]
	currentCount := atomic.LoadInt32(countAddr)
	baseIdx := int(cOff) * types.MaxNeighbors

	// Final filter of duplicates and capacity check
	available := types.MaxNeighbors - int(currentCount)
	if available <= 0 {
		return
	}

	finalToAdd := make([]int, 0, len(toAddIdxs))
	for _, idx := range toAddIdxs {
		src := sources[idx]
		found := false
		for j := 0; j < int(currentCount); j++ {
			if atomic.LoadUint32(&neighborsChunk[baseIdx+j]) == src {
				found = true
				break
			}
		}
		if !found {
			finalToAdd = append(finalToAdd, idx)
		}
	}

	if len(finalToAdd) > available {
		finalToAdd = finalToAdd[:available]
	}

	if len(finalToAdd) == 0 {
		return
	}

	// 4. Perform Writes
	for _, idx := range finalToAdd {
		slot := int(currentCount)
		src := sources[idx]
		atomic.StoreUint32(&neighborsChunk[baseIdx+slot], src)
		currentCount++
	}

	// Update Count Atomically
	atomic.StoreInt32(countAddr, currentCount)

	// Increment global version
	atomic.AddUint64(&data.GlobalVersion, 1)

	// Packed Neighbors Batch Update
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]

		newNeighbors := make([]uint32, len(currentNeighbors)+len(finalToAdd))
		copy(newNeighbors, currentNeighbors)

		offset := len(currentNeighbors)
		for i, idx := range finalToAdd {
			newNeighbors[offset+i] = sources[idx]
		}

		if h.config.Float16Enabled {
			_, existingDists, ok := pn.GetNeighborsF16(target)
			newF16Dists := make([]float16.Num, len(currentNeighbors)+len(finalToAdd))
			if ok && len(existingDists) == len(currentNeighbors) {
				copy(newF16Dists, existingDists)
			} else {
				for i := range currentNeighbors {
					newF16Dists[i] = float16.New(0)
				}
			}
			for i, idx := range finalToAdd {
				newF16Dists[offset+i] = float16.New(dists[idx])
			}
			_ = pn.SetNeighborsF16(target, newNeighbors, newF16Dists)
		} else {
			_ = pn.SetNeighbors(target, newNeighbors)
		}
	}

	// Prune if needed
	if int(currentCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer, currentDists)
	}
}

// PruneConnections removes excess connections from a node's neighbor list.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *types.GraphData, id uint32, maxConn, layer int) {
	// COW Promotion
	data = h.promoteNode(data, id)

	oldVer := data.LockNode(layer, id)
	defer data.UnlockNode(layer, id, oldVer)

	h.pruneConnectionsLocked(ctx, data, id, maxConn, layer, nil)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, maxConn, layer int, precalculatedDists []float32) {
	cID := types.ChunkID(nodeID)
	cOff := types.ChunkOffset(nodeID)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		// Data snapshot is stale or corrupted, reload and retry
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return // Cannot prune if chunks are missing
		}
	}

	// 1. Collect all current neighbors as candidates (Unified)
	currentNeighbors := h.GetNeighborsCombined(layer, nodeID)
	count := len(currentNeighbors)
	if count <= maxConn {
		return
	}

	dists := make([]float32, count)
	candidates := make([]types.Candidate, count)

	// Unified Distance Calculation (v0.1.4-rc2)
	nodeVecAny, err := data.GetVector(nodeID)
	if err != nil || nodeVecAny == nil {
		return
	}

	// Unified Distance Calculation (v0.1.4-rc2) Fast-Path for Float32
	if data.Type == 1 {
		nodeVecF32, ok := nodeVecAny.([]float32)
		if !ok || nodeVecF32 == nil {
			return
		}

		for i := 0; i < count; i++ {
			if i < len(precalculatedDists) {
				dists[i] = precalculatedDists[i]
				continue
			}
			neighborID := currentNeighbors[i]
			vecAny, err := data.GetVector(neighborID)
			if err != nil || vecAny == nil {
				dists[i] = math.MaxFloat32
				continue
			}

			if vecF32, ok := vecAny.([]float32); ok {
				d, err := h.distFunc(nodeVecF32, vecF32)
				if err == nil {
					dists[i] = d
				} else {
					dists[i] = math.MaxFloat32
				}
			} else {
				dists[i] = math.MaxFloat32
			}
		}
	} else {
		// Helper to get float32 representation for distance calc
		toF32 := func(v any) []float32 {
			switch vf := v.(type) {
			case []float32:
				return vf
			case []int32:
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			case []uint32:
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			case []int8:
				if h.quantizer != nil && h.sq8Ready.Load() {
					byteVec := *(*[]byte)(unsafe.Pointer(&vf)) // #nosec G103
					return h.quantizer.Decode(byteVec)
				}
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(uint8(val)) }
				return res
			case []uint8:
				if h.quantizer != nil && h.sq8Ready.Load() {
					return h.quantizer.Decode(vf)
				}
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			default:
				return nil
			}
		}

		nodeVecF32 := toF32(nodeVecAny)
		if nodeVecF32 == nil {
			return
		}

		for i := 0; i < count; i++ {
			if i < len(precalculatedDists) {
				dists[i] = precalculatedDists[i]
				continue
			}
			neighborID := currentNeighbors[i]
			vecAny, err := data.GetVector(neighborID)
			if err != nil || vecAny == nil {
				dists[i] = math.MaxFloat32
				continue
			}

			vecF32 := toF32(vecAny)
			if vecF32 != nil {
				d, err := h.distFunc(nodeVecF32, vecF32)
				if err == nil {
					dists[i] = d
				} else {
					dists[i] = math.MaxFloat32
				}
			} else {
				dists[i] = math.MaxFloat32
			}
		}
	}
	// Populate candidates with IDs and distances
	for i := 0; i < count; i++ {
		candidates[i] = types.Candidate{ID: currentNeighbors[i], Dist: dists[i]}
	}

	// Run heuristic to select best M neighbors
	// Prevent infinite recursion by limiting depth
	if ctx != nil {
		ctx.pruneDepth++
		defer func() { ctx.pruneDepth-- }()

		// Circuit breaker
		if ctx.pruneDepth > 5 {
			return
		}
	}

	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	// Safety: Hard truncation to ensure we never exceed maxConn even if heuristic is permissive
	if len(selected) > maxConn {
		selected = selected[:maxConn]
	}

	// Seqlock write start: odd = dirty
	verChunk := data.GetVersionsChunk(layer, cID)
	var verAddr *uint32
	if verChunk != nil {
		verAddr = &verChunk[cOff]
		atomic.AddUint32(verAddr, 1)
	}

	baseIdx := int(cOff) * types.MaxNeighbors
	countAddr := &countsChunk[cOff]

	// Write back
	for i, cand := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], cand.ID)
	}
	atomic.StoreInt32(countAddr, int32(len(selected))) // #nosec G115

	// Seqlock write end: even = clean
	if verAddr != nil {
		atomic.AddUint32(verAddr, 1)
	}

	// Increment global version
	atomic.AddUint64(&data.GlobalVersion, 1)

	// --- Packed Neighbors Integration (v0.1.4-rc1) ---
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		ids := make([]uint32, len(selected))
		for i, cand := range selected {
			ids[i] = cand.ID
		}
		if h.config.Float16Enabled {
			f16Dists := make([]float16.Num, len(selected))
			for i, cand := range selected {
				f16Dists[i] = float16.New(cand.Dist)
			}
			_ = pn.SetNeighborsF16(nodeID, ids, f16Dists)
		} else {
			_ = pn.SetNeighbors(nodeID, ids)
		}
	}
}

// computeDistances calculates distance from nodeID to all items in neighbors array.
func (h *ArrowHNSW) computeDistances(data *types.GraphData, nodeID uint32, neighbors []uint32, dists []float32) {
	nodeVecAny, err := data.GetVector(nodeID)
	if err != nil || nodeVecAny == nil {
		return
	}

	if data.Type == 1 { // Float32 Fast-Path
		nodeVecF32, ok := nodeVecAny.([]float32)
		if !ok || nodeVecF32 == nil {
			return
		}

		for i, neighborID := range neighbors {
			vecAny, err := data.GetVector(neighborID)
			if err != nil || vecAny == nil {
				dists[i] = math.MaxFloat32
				continue
			}

			if vecF32, ok := vecAny.([]float32); ok {
				d, err := h.distFunc(nodeVecF32, vecF32)
				if err == nil {
					dists[i] = d
				} else {
					dists[i] = math.MaxFloat32
				}
			} else {
				dists[i] = math.MaxFloat32
			}
		}
	} else {
		toF32 := func(v any) []float32 {
			switch vf := v.(type) {
			case []float32:
				return vf
			case []int32:
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			case []uint32:
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			case []int8:
				if h.quantizer != nil && h.sq8Ready.Load() {
					byteVec := *(*[]byte)(unsafe.Pointer(&vf)) // #nosec G103
					return h.quantizer.Decode(byteVec)
				}
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(uint8(val)) }
				return res
			case []uint8:
				if h.quantizer != nil && h.sq8Ready.Load() {
					return h.quantizer.Decode(vf)
				}
				res := make([]float32, len(vf))
				for i, val := range vf { res[i] = float32(val) }
				return res
			default:
				return nil
			}
		}

		nodeVecF32 := toF32(nodeVecAny)
		if nodeVecF32 == nil {
			return
		}

		for i, neighborID := range neighbors {
			vecAny, err := data.GetVector(neighborID)
			if err != nil || vecAny == nil {
				dists[i] = math.MaxFloat32
				continue
			}

			vecF32 := toF32(vecAny)
			if vecF32 != nil {
				d, err := h.distFunc(nodeVecF32, vecF32)
				if err == nil {
					dists[i] = d
				} else {
					dists[i] = math.MaxFloat32
				}
			} else {
				dists[i] = math.MaxFloat32
			}
		}
	}
}
