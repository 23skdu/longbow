package core

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"math"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) *types.GraphData {
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		if data != nil { return data }
		return h.data.Load()
	}

	if data == nil {
		data = h.data.Load()
	}
	cID := types.ChunkID(source)

	if int(source) < data.Capacity && data.GetNeighborsChunk(0, cID) != nil {
		oldVer := data.LockNode(layer, source)
		defer data.UnlockNode(layer, source, oldVer)
		h.addConnectionLocked(ctx, data, source, target, layer, maxConn)
		return data
	}

	data = h.promoteNode(data, source)

	oldVer := data.LockNode(layer, source)
	defer data.UnlockNode(layer, source, oldVer)
	h.addConnectionLocked(ctx, data, source, target, layer, maxConn)
	return data
}

// AddConnectionLocked is like AddConnection but assumes growMu.Lock() is already held.
func (h *ArrowHNSW) AddConnectionLocked(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) *types.GraphData {
	// 0. Use Lock-Free path if applicable
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		return data
	}

	// COW Promotion and Locking (Locked version)
	data = h.promoteNodeLocked(data, source)

	func() {
		oldVer := data.LockNode(layer, source)
		defer data.UnlockNode(layer, source, oldVer)
		h.addConnectionLocked(ctx, data, source, target, layer, maxConn)
	}()

	return data
}

// AddConnectionsBatch adds multiple directed edges to a single target node at the given layer.
func (h *ArrowHNSW) AddConnectionsBatch(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) *types.GraphData {
	if len(sources) == 0 {
		if data != nil { return data }
		return h.data.Load()
	}

	if data == nil {
		data = h.data.Load()
	}
	cID := types.ChunkID(target)

	if int(target) < data.Capacity && data.GetNeighborsChunk(0, cID) != nil {
		oldVer := data.LockNode(layer, target)
		defer data.UnlockNode(layer, target, oldVer)
		h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
		return data
	}

	data = h.promoteNode(data, target)

	oldVer := data.LockNode(layer, target)
	defer data.UnlockNode(layer, target, oldVer)
	h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
	return data
}
func (h *ArrowHNSW) AddConnectionsBatchLocked(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) *types.GraphData {
	if len(sources) == 0 {
		return data
	}

	cID := types.ChunkID(target)
	if int(target) < data.Capacity && data.GetNeighborsChunk(0, cID) != nil {
		oldVer := data.LockNode(layer, target)
		defer data.UnlockNode(layer, target, oldVer)
		h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
		return data
	}

	data = h.promoteNodeLocked(data, target)

	oldVer := data.LockNode(layer, target)
	defer data.UnlockNode(layer, target, oldVer)
	h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
	return data
}

// PruneConnections removes excess connections from a node's neighbor list.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *types.GraphData, id uint32, maxConn, layer int) *types.GraphData {
	// COW Promotion and Locking
	data = h.promoteNode(data, id)

	func() {
		oldVer := data.LockNode(layer, id)
		defer data.UnlockNode(layer, id, oldVer)
		h.pruneConnectionsLocked(ctx, data, id, maxConn, layer, nil)
	}()

	return data
}

// addConnectionLocked performs mutation assuming lock held.
func (h *ArrowHNSW) addConnectionLocked(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int) {
	cID := types.ChunkID(source)
	cOff := types.ChunkOffset(source)
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	
	var currentNeighbors []uint32
	if countsChunk != nil && neighborsChunk != nil {
		count := int(atomic.LoadInt32(&countsChunk[cOff]))
		currentNeighbors = make([]uint32, count)
		baseIdx := int(cOff) * types.MaxNeighbors
		for i := 0; i < count; i++ {
			currentNeighbors[i] = atomic.LoadUint32(&neighborsChunk[baseIdx+i])
		}
	} else {
		// Fallback if chunks are missing
		currentNeighbors = h.GetNeighborsCombinedManual(data, layer, source)
	}

	for _, n := range currentNeighbors {
		if n == target { return }
	}

	if len(currentNeighbors) >= maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer, []uint32{target})
		return // All work done by prune
	}

	if len(currentNeighbors) >= types.MaxNeighbors { return }
	
	if countsChunk != nil && neighborsChunk != nil {
		slot := len(currentNeighbors)
		baseIdx := int(cOff) * types.MaxNeighbors
		atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)
		atomic.StoreInt32(&countsChunk[cOff], int32(slot+1)) // #nosec G115
	}

	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		newNeighbors := append(currentNeighbors, target)
		_ = pn.SetNeighbors(source, newNeighbors)
	}

	atomic.AddUint64(&data.GlobalVersion, 1)
}

// addConnectionsBatchLocked performs batch mutation assuming lock held.
func (h *ArrowHNSW) addConnectionsBatchLocked(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, layer, maxConn int) {
	cID := types.ChunkID(target)
	cOff := types.ChunkOffset(target)
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil { return }

	countAddr := &countsChunk[cOff]
	currentCount := atomic.LoadInt32(countAddr)
	baseIdx := int(cOff) * types.MaxNeighbors

	if int(currentCount)+len(sources) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer, sources)
		return 
	}

	added := 0
	for _, src := range sources {
		if int(currentCount) >= types.MaxNeighbors { break }
		found := false
		for i := 0; i < int(currentCount); i++ {
			if atomic.LoadUint32(&neighborsChunk[baseIdx+i]) == src {
				found = true
				break
			}
		}
		if !found {
			atomic.StoreUint32(&neighborsChunk[baseIdx+int(currentCount)], src)
			currentCount++
			added++
		}
	}

	atomic.StoreInt32(countAddr, currentCount)
	atomic.AddUint64(&data.GlobalVersion, 1)

	// If we exceeded maxConn, prune to maintain graph diversity and search efficiency.
	// This is critical for parallel ingestion where nodes may receive many reverse connections.
	if int(currentCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer, nil)
	} else if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		newNeighbors := h.GetNeighborsCombinedManual(data, layer, target)
		_ = pn.SetNeighbors(target, newNeighbors)
	}
}

// pruneConnectionsLocked reduces connections using robust diversity heuristic.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, maxConn, layer int, newNeighbors []uint32) {
	cID := types.ChunkID(nodeID)
	cOff := types.ChunkOffset(nodeID)
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	var currentNeighbors []uint32
	if countsChunk != nil && neighborsChunk != nil {
		count := int(atomic.LoadInt32(&countsChunk[cOff]))
		currentNeighbors = make([]uint32, count)
		baseIdx := int(cOff) * types.MaxNeighbors
		for i := 0; i < count; i++ {
			currentNeighbors[i] = atomic.LoadUint32(&neighborsChunk[baseIdx+i])
		}
	} else {
		currentNeighbors = h.GetNeighborsCombinedManual(data, layer, nodeID)
	}

	// Include new candidates in the pruning pool
	if len(newNeighbors) > 0 {
		seen := make(map[uint32]struct{}, len(currentNeighbors)+len(newNeighbors))
		for _, n := range currentNeighbors { seen[n] = struct{}{} }
		for _, n := range newNeighbors {
			if _, exists := seen[n]; !exists && n != nodeID {
				currentNeighbors = append(currentNeighbors, n)
				seen[n] = struct{}{}
			}
		}
	}

	count := len(currentNeighbors)
	if count <= maxConn { return }

	dists := make([]float32, count)
	h.computeDistances(data, nodeID, currentNeighbors, dists)
	
	candidates := make([]types.Candidate, count)
	for i := 0; i < count; i++ {
		candidates[i] = types.Candidate{ID: currentNeighbors[i], Dist: dists[i]}
	}

	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	if len(selected) > maxConn { selected = selected[:maxConn] }

	if h.topLayerManager != nil {
		h.topLayerManager.ClearNeighbors(layer, nodeID)
	}

	cID = types.ChunkID(nodeID)
	cOff = types.ChunkOffset(nodeID)
	
	countsChunk = data.GetCountsChunk(layer, cID)
	neighborsChunk = data.GetNeighborsChunk(layer, cID)
	baseIdx := int(cOff) * types.MaxNeighbors
	for i, cand := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], cand.ID)
	}
	atomic.StoreInt32(&countsChunk[cOff], int32(len(selected))) // #nosec G115

	atomic.AddUint64(&data.GlobalVersion, 1)

	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		ids := make([]uint32, len(selected))
		for i, cand := range selected { ids[i] = cand.ID }
		_ = pn.SetNeighbors(nodeID, ids)
	}
}

// computeDistances calculates distance from nodeID to multiple targets using type-aware helper.
func (h *ArrowHNSW) computeDistances(data *types.GraphData, nodeID uint32, neighbors []uint32, dists []float32) {
	v1 := h.getVectorF32(data, nodeID)
	if v1 == nil { return }

	for i, nbID := range neighbors {
		v2 := h.getVectorF32(data, nbID)
		if v2 != nil {
			d, _ := h.distFunc(v1, v2)
			dists[i] = d
		} else {
			dists[i] = math.MaxFloat32
		}
	}
}

// getVectorF32 ensures the vector is returned as []float32 for distance calculations.
func (h *ArrowHNSW) getVectorF32(data *types.GraphData, id uint32) []float32 {
	vecAny, err := data.GetVector(id)
	if err != nil || vecAny == nil { return nil }

	switch v := vecAny.(type) {
	case []float32: return v
	case []int32:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []uint32:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []int8:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []uint8:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []int16:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []uint16:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []float64:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = float32(val) }
		return f
	case []float16.Num:
		f := make([]float32, len(v))
		for i, val := range v { f[i] = val.Float32() }
		return f
	case []complex64:
		// Use real part or magnitude? Standard HNSW for complex usually uses magnitude.
		// But here we need []float32. Let's return real parts for now if that's the pattern,
		// or better, return it in a way that matches extractVector.
		f := make([]float32, len(v)*2)
		for i, val := range v {
			f[2*i] = real(val)
			f[2*i+1] = imag(val)
		}
		return f
	case []complex128:
		f := make([]float32, len(v)*2)
		for i, val := range v {
			f[2*i] = float32(real(val))
			f[2*i+1] = float32(imag(val))
		}
		return f
	default: return nil
	}
}
