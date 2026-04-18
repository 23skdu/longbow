package core

import (
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
)

type TopLayerManager struct {
	// layers[layer] stores map[nodeID]*atomic.Pointer[LockFreeAdjacency]
	layers [types.ArrowMaxLayers]sync.Map
	entryPoints [types.ArrowMaxLayers]*ConcurrentSkipList
	threshold   int
}

// LockFreeAdjacency represents a persistent, lock-free neighbor list.
type LockFreeAdjacency struct {
	Neighbors []uint32
}

func NewTopLayerManager(threshold int) *TopLayerManager {
	tlm := &TopLayerManager{
		threshold: threshold,
	}
	for i := 0; i < types.ArrowMaxLayers; i++ {
		tlm.entryPoints[i] = NewConcurrentSkipList()
	}
	return tlm
}

// GetNeighborsLockFree returns the neighbor list for a node in the upper layers
// without taking any locks.
func (tlm *TopLayerManager) GetNeighborsLockFree(layer int, id uint32) []uint32 {
	if layer < tlm.threshold || layer >= types.ArrowMaxLayers {
		return nil
	}
	
	val, ok := tlm.layers[layer].Load(id)
	if !ok {
		return nil
	}
	
	ptr := val.(*atomic.Pointer[LockFreeAdjacency])
	adj := ptr.Load()
	if adj == nil {
		return nil
	}
	return adj.Neighbors
}

// AddConnectionCAS attempts to add a connection using Compare-And-Swap
// instead of a mutex.
func (tlm *TopLayerManager) AddConnectionCAS(layer int, source, target uint32) bool {
	if layer < tlm.threshold || layer >= types.ArrowMaxLayers {
		return false
	}
	
	actualPtr, _ := tlm.layers[layer].LoadOrStore(source, &atomic.Pointer[LockFreeAdjacency]{})
	ptr := actualPtr.(*atomic.Pointer[LockFreeAdjacency])

	for {
		oldAdj := ptr.Load()
		var newNeighbors []uint32
		if oldAdj == nil {
			newNeighbors = []uint32{target}
		} else {
			// Check if already exists
			for _, n := range oldAdj.Neighbors {
				if n == target {
					return true
				}
			}
			if len(oldAdj.Neighbors) >= types.MaxNeighbors {
				return false // Capacity reached or pruning needed
			}
			newNeighbors = make([]uint32, len(oldAdj.Neighbors)+1)
			copy(newNeighbors, oldAdj.Neighbors)
			newNeighbors[len(oldAdj.Neighbors)] = target
		}

		newAdj := &LockFreeAdjacency{Neighbors: newNeighbors}
		if ptr.CompareAndSwap(oldAdj, newAdj) {
			// Record as entry point for this layer
			tlm.entryPoints[layer].Insert(source)
			return true
		}
		// Lose race, retry
	}
}

// GetNeighborsCombined returns neighbors from both the standard graph data
// and the lock-free upper layers, ensuring a consistent view.
func (h *ArrowHNSW) GetNeighborsCombined(layer int, id uint32) []uint32 {
	// 1. Try Lock-Free first
	lf := h.topLayerManager.GetNeighborsLockFree(layer, id)
	if lf != nil {
		return lf
	}
	
	// 2. Fallback to standard types.GraphData (which is lock-safe for reads)
	return h.data.Load().GetNeighbors(layer, id, nil)
}
