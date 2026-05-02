package core

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"math"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) *types.GraphData {
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		if data != nil { return data }
		return h.data.Load()
	}

	// 1. Try Lock-Free path with PackedNeighbors (High Throughput)
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		_ = pn.UpdateNeighbors(source, func(old []uint32) []uint32 {
			for _, n := range old {
				if n == target { return nil } // No change
			}
			
			var next []uint32
			if len(old) < maxConn {
				next = make([]uint32, len(old)+1)
				copy(next, old)
				next[len(old)] = target
			} else {
				// Pruning needed - Diversity heuristic
				next = h.computePrunedNeighbors(ctx, data, source, old, []uint32{target}, maxConn)
			}
			
			atomic.AddUint64(&data.GlobalVersion, 1)
			return next
		})
		return data
	}

	// 2. Fallback to COW + Mutex path for legacy storage
	if data == h.data.Load() {
		data = data.Clone()
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

// PruneConnections removes excess connections from a node's neighbor list using lock-free CAS.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *types.GraphData, id uint32, maxConn, layer int) *types.GraphData {
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		_ = pn.UpdateNeighbors(id, func(old []uint32) []uint32 {
			if len(old) <= maxConn { return nil }

			next := h.computePrunedNeighbors(ctx, data, id, old, nil, maxConn)
			atomic.AddUint64(&data.GlobalVersion, 1)
			return next
		})
		return data
	}

	// Legacy path
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
		currentNeighbors = h.GetNeighborsCombinedManual(data, layer, source)
	}

	for _, n := range currentNeighbors {
		if n == target { return }
	}

	if len(currentNeighbors) >= maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer, []uint32{target})
		return
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

// computePrunedNeighbors is the core diversity-aware pruning logic, reusable by CAS loops.
func (h *ArrowHNSW) computePrunedNeighbors(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, current []uint32, extra []uint32, maxConn int) []uint32 {
	pool := current
	if len(extra) > 0 {
		seen := make(map[uint32]struct{}, len(current)+len(extra))
		for _, n := range current { seen[n] = struct{}{} }
		for _, n := range extra {
			if _, exists := seen[n]; !exists && n != nodeID {
				pool = append(pool, n)
				seen[n] = struct{}{}
			}
		}
	}

	if len(pool) <= maxConn { return pool }

	dists := make([]float32, len(pool))
	h.computeDistances(ctx, data, nodeID, pool, dists)
	
	candidates := make([]types.Candidate, len(pool))
	for i := 0; i < len(pool); i++ {
		candidates[i] = types.Candidate{ID: pool[i], Dist: dists[i]}
	}

	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	if len(selected) > maxConn { selected = selected[:maxConn] }

	result := make([]uint32, len(selected))
	for i, cand := range selected { result[i] = cand.ID }
	return result
}

// pruneConnectionsLocked reduces connections using robust diversity heuristic.
// Legacy method for non-PackedNeighbors storage.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, maxConn, layer int, newNeighbors []uint32) {
	selected := h.computePrunedNeighbors(ctx, data, nodeID, h.GetNeighborsCombinedManual(data, layer, nodeID), newNeighbors, maxConn)

	if h.topLayerManager != nil {
		h.topLayerManager.ClearNeighbors(layer, nodeID)
	}

	cID := types.ChunkID(nodeID)
	cOff := types.ChunkOffset(nodeID)
	
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		return
	}

	baseIdx := int(cOff) * types.MaxNeighbors
	for i, id := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], id)
	}
	atomic.StoreInt32(&countsChunk[cOff], int32(len(selected))) // #nosec G115

	atomic.AddUint64(&data.GlobalVersion, 1)

	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		_ = pn.SetNeighbors(nodeID, selected)
	}
}

// computeDistances calculates distance from nodeID to multiple targets using type-aware helper.
func (h *ArrowHNSW) computeDistances(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, neighbors []uint32, dists []float32) {
	v1 := h.getVectorF32Optimized(ctx, data, nodeID, 0)
	if v1 == nil { return }

	for i, nbID := range neighbors {
		v2 := h.getVectorF32Optimized(ctx, data, nbID, 1)
		if v2 != nil {
			d, _ := h.distFunc(v1, v2)
			dists[i] = d
		} else {
			dists[i] = math.MaxFloat32
		}
	}
}

// getVectorF32Optimized ensures the vector is returned as []float32 for distance calculations using ctx buffers.
func (h *ArrowHNSW) getVectorF32Optimized(ctx *ArrowSearchContext, data *types.GraphData, id uint32, bufIdx int) []float32 {
	vecAny, err := data.GetVector(id)
	if err != nil || vecAny == nil { return nil }

	if v, ok := vecAny.([]float32); ok {
		return v
	}

	// Use pre-allocated buffers from ctx to avoid allocations
	var dst []float32
	if ctx != nil {
		if bufIdx == 0 {
			if len(ctx.bufF32) < data.Dims {
				ctx.bufF32 = make([]float32, data.Dims*2)
			}
			dst = ctx.bufF32[:data.Dims]
		} else {
			if len(ctx.bufF32_2) < data.Dims {
				ctx.bufF32_2 = make([]float32, data.Dims*2)
			}
			dst = ctx.bufF32_2[:data.Dims]
		}
	}

	switch v := vecAny.(type) {
	case []int32:
		if dst != nil { simd.Int32ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Int32ToFloat32(v, f); return f
	case []uint32:
		if dst != nil { simd.Uint32ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Uint32ToFloat32(v, f); return f
	case []int8:
		if dst != nil { simd.Int8ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Int8ToFloat32(v, f); return f
	case []uint8:
		if dst != nil { simd.Uint8ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Uint8ToFloat32(v, f); return f
	case []int16:
		if dst != nil { simd.Int16ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Int16ToFloat32(v, f); return f
	case []uint16:
		if dst != nil { simd.Uint16ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Uint16ToFloat32(v, f); return f
	case []float64:
		if dst != nil { for i, val := range v { dst[i] = float32(val) }; return dst }
		f := make([]float32, len(v)); for i, val := range v { f[i] = float32(val) }; return f
	case []float16.Num:
		if dst != nil { simd.Float16ToFloat32(v, dst); return dst }
		f := make([]float32, len(v)); simd.Float16ToFloat32(v, f); return f
	}
	return nil
}
