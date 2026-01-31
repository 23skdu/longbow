package store

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *GraphData, source, target uint32, layer, maxConn int, dist float32) {
	// 1. Structural/Promotion (Optimistic check first)
	// COW Promotion
	data = h.promoteNode(data, source)

	cID := chunkID(source)
	cOff := chunkOffset(source)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// Reload data and try again
		data = h.data.Load()
		data = h.promoteNode(data, source)
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return
		}
	}

	// 2. Lock-free Duplicate Check (Seqlock read style)
	// We check for duplicates without the lock first.
	// If found, we skip completely.
	currentNeighbors := data.GetNeighbors(layer, source, nil)
	for _, n := range currentNeighbors {
		if n == target {
			return
		}
	}

	// 3. Acquire Per-Node Lock
	oldVer := data.LockNode(layer, source)
	// Ensure we release lock and increment version
	defer data.UnlockNode(layer, source, oldVer)

	// Re-check duplicates under lock
	currentCount := atomic.LoadInt32(&countsChunk[cOff])
	baseIdx := int(cOff) * types.MaxNeighbors

	for i := 0; i < int(currentCount); i++ {
		if atomic.LoadUint32(&neighborsChunk[baseIdx+i]) == target {
			return // Already connected
		}
	}

	// 4. Update metadata and data
	slot := int(currentCount)
	if slot >= types.MaxNeighbors {
		return
	}

	// Perform physical write
	atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)

	// Update metadata (Counts) - make visible AFTER data write
	countAddr := &countsChunk[cOff]
	newCount := atomic.AddInt32(countAddr, 1)

	// Increment global version
	atomic.AddUint64(&data.GlobalVersion, 1)

	// --- Packed Neighbors Integration ---
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		// Use already fetched currentNeighbors + NEW target
		newNeighbors := make([]uint32, len(currentNeighbors)+1)
		copy(newNeighbors, currentNeighbors)
		newNeighbors[len(currentNeighbors)] = target

		if h.config.Float16Enabled {
			_, existingDists, _ := pn.GetNeighborsF16(source)
			newDists := make([]float16.Num, len(currentNeighbors)+1)
			if len(existingDists) == len(currentNeighbors) {
				copy(newDists, existingDists)
			} else {
				for i := range currentNeighbors {
					newDists[i] = float16.New(0)
				}
			}
			newDists[len(currentNeighbors)] = float16.New(dist)
			_ = pn.SetNeighborsF16(source, newNeighbors, newDists)
		} else {
			_ = pn.SetNeighbors(source, newNeighbors)
		}
	}

	// 5. Prune if needed (Still under node-lock)
	if int(newCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
	}
}

// AddConnectionsBatch adds multiple directed edges to a single target node at the given layer.
// This is an optimized version of AddConnection for batch scenarios to reduce lock contention.
func (h *ArrowHNSW) AddConnectionsBatch(ctx *ArrowSearchContext, data *GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) {
	if len(sources) == 0 {
		return
	}

	cID := chunkID(target)
	cOff := chunkOffset(target)

	// Ensure chunk exists
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return
		}
	}

	// 1. Optimistic Duplicate Check
	currentNeighbors := data.GetNeighbors(layer, target, nil)
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
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer)
	}
}

// PruneConnections removes excess connections from a node's neighbor list.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *GraphData, id uint32, maxConn, layer int) {
	// COW Promotion
	data = h.promoteNode(data, id)

	oldVer := data.LockNode(layer, id)
	defer data.UnlockNode(layer, id, oldVer)

	h.pruneConnectionsLocked(ctx, data, id, maxConn, layer)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

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

	countAddr := &countsChunk[cOff]
	count := int(atomic.LoadInt32(countAddr))

	if count <= maxConn {
		return
	}

	// Collect all current neighbors as candidates
	baseIdx := int(cOff) * types.MaxNeighbors

	var dists []float32
	if ctx != nil {
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists = ctx.scratchDists[:count]
	} else {
		dists = make([]float32, count)
	}

	var candidates []Candidate
	if ctx != nil {
		if cap(ctx.scratchRemaining) < count {
			ctx.scratchRemaining = make([]Candidate, count*2)
		}
		candidates = ctx.scratchRemaining[:count]
	} else {
		candidates = make([]Candidate, count)
	}

	// Unified Distance Calculation (v0.1.4-rc2)
	// We MUST handle mixed types because SQ8 nodes can have Float32 neighbors (from early training)
	nodeVecAny, err := data.GetVector(nodeID)
	if err != nil || nodeVecAny == nil {
		return
	}

	// Helper to get float32 representation for distance calc
	toF32 := func(v any) []float32 {
		switch vf := v.(type) {
		case []float32:
			return vf
		case []int8:
			if h.quantizer != nil && h.sq8Ready.Load() {
				// Safe cast to byte
				byteVec := *(*[]byte)(unsafe.Pointer(&vf))
				return h.quantizer.Decode(byteVec)
			}
			res := make([]float32, len(vf))
			for i, val := range vf {
				res[i] = float32(uint8(val))
			}
			return res
		case []uint8:
			if h.quantizer != nil && h.sq8Ready.Load() {
				return h.quantizer.Decode(vf)
			}
			res := make([]float32, len(vf))
			for i, val := range vf {
				res[i] = float32(val)
			}
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
		neighborID := neighborsChunk[baseIdx+i]
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
	// Populate candidates with IDs and distances
	for i := 0; i < count; i++ {
		candidates[i] = Candidate{ID: neighborsChunk[baseIdx+i], Dist: dists[i]}
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

	// Seqlock write start: odd = dirty
	verChunk := data.GetVersionsChunk(layer, cID)
	var verAddr *uint32
	if verChunk != nil {
		verAddr = &verChunk[cOff]
		atomic.AddUint32(verAddr, 1)
	}

	// Write back
	for i, cand := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], cand.ID)
	}
	// Update count
	atomic.StoreInt32(countAddr, int32(len(selected)))

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
