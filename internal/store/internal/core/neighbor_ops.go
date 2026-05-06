package core

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
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

	// 2. Fallback to Mutex path for legacy storage
	// Note: We don't need to clone here anymore because Neighbor/Count updates
	// are performed on the shared arena using atomic operations and per-node locks.
	// We only clone if we need to GROW the GraphData structure (handled in EnsureChunk).
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
	vQuery, err := data.GetVector(nodeID)
	if err != nil || vQuery == nil { return }

	computer := h.resolveHNSWComputer(data, ctx, vQuery, false)
	if computer == nil { return }

	// Use specialized computer if available
	if comp, ok := computer.(interface {
		ComputeSingle(id uint32) (float32, error)
	}); ok {
		for i, nbID := range neighbors {
			d, err := comp.ComputeSingle(nbID)
			if err == nil {
				dists[i] = d
			} else {
				dists[i] = math.MaxFloat32
			}
		}
		return
	}

	// Fallback to manual computation
	v1, err := data.GetVector(nodeID)
	if err != nil || v1 == nil { return }

	for i, nbID := range neighbors {
		v2, err := data.GetVector(nbID)
		if err != nil || v2 == nil {
			dists[i] = math.MaxFloat32
			continue
		}
		
		d, err := h.DispatchDistance(data.Type, v1, v2)
		if err == nil {
			dists[i] = d
		} else {
			dists[i] = math.MaxFloat32
		}
	}
}

// DispatchDistance is a helper to compute distance between any two vectors of the same type.
func (h *ArrowHNSW) DispatchDistance(vt types.VectorDataType, a, b any) (float32, error) {
	switch vt {
	case types.VectorTypeFloat32:
		return h.distFunc(a.([]float32), b.([]float32))
	case types.VectorTypeFloat64:
		return h.distFuncF64(a.([]float64), b.([]float64))
	case types.VectorTypeInt8:
		return h.distFuncInt8(a.([]int8), b.([]int8))
	case types.VectorTypeInt16:
		return h.distFuncInt16(a.([]int16), b.([]int16))
	case types.VectorTypeInt32:
		return h.distFuncInt32(a.([]int32), b.([]int32))
	case types.VectorTypeInt64:
		return h.distFuncInt64(a.([]int64), b.([]int64))
	case types.VectorTypeUint8:
		return h.distFuncUint8(a.([]uint8), b.([]uint8))
	case types.VectorTypeUint16:
		return h.distFuncUint16(a.([]uint16), b.([]uint16))
	case types.VectorTypeUint32:
		return h.distFuncUint32(a.([]uint32), b.([]uint32))
	case types.VectorTypeUint64:
		return h.distFuncUint64(a.([]uint64), b.([]uint64))
	case types.VectorTypeComplex64:
		return h.distFuncC64(a.([]complex64), b.([]complex64))
	case types.VectorTypeComplex128:
		return h.distFuncC128(a.([]complex128), b.([]complex128))
	default:
		return 0, fmt.Errorf("unsupported vector type for distance: %v", vt)
	}
}
