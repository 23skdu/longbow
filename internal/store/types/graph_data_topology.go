package types

import (
	"fmt"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"math"
	"runtime"
	"strconv"
	"sync/atomic"
)

func (g *GraphData) SetNeighbors(id uint32, neighbors []uint32) error {
	return g.SetNeighborsAtLayer(0, id, neighbors)
}

// ensureLayerNeighborsChunk allocates the neighbor arena slab for (layer, cID)
// if missing. EnsureChunk only pre-allocates layer 0; upper layers normally use
// PackedNeighbors, but SetNeighborsAtLayer must also be able to write the
// legacy Neighbors slice for export / disk-graph serialization.

func (g *GraphData) SetNeighborsAtLayer(layer int, id uint32, neighbors []uint32) error {
	mu := &g.ShardedMus[id%1024]
	mu.Lock()
	defer mu.Unlock()

	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Ensure chunk exists
	countsChunk := g.GetCountsChunk(layer, cID)
	neighborsChunk := g.GetNeighborsChunk(layer, cID)
	versionsChunk := g.GetVersionsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		if err := g.EnsureChunk(cID, cOff, g.Dims); err != nil {
			return err
		}
		// EnsureChunk only pre-allocates layer-0 neighbor slabs (upper layers
		// normally live in PackedNeighbors). When writing the legacy slice for
		// a specific upper layer (export / disk graph), allocate on demand.
		if neighborsChunk == nil {
			if err := g.ensureLayerNeighborsChunk(layer, cID); err != nil {
				return err
			}
		}
		countsChunk = g.GetCountsChunk(layer, cID)
		neighborsChunk = g.GetNeighborsChunk(layer, cID)
		versionsChunk = g.GetVersionsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return fmt.Errorf("failed to allocate chunk for SetNeighbors")
		}
	}

	if len(neighbors) > MaxNeighbors {
		neighbors = neighbors[:MaxNeighbors]
	}

	if versionsChunk != nil {
		atomic.AddUint32(&versionsChunk[cOff], 1)
	}

	baseIdx := cOff * MaxNeighbors

	// Write neighbors
	for i, n := range neighbors {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], n)
	}
	atomic.StoreInt32(&countsChunk[cOff], int32(len(neighbors))) // #nosec G115

	if versionsChunk != nil {
		atomic.AddUint32(&versionsChunk[cOff], 1)
	}

	// Increment global version
	atomic.AddUint64(&g.GlobalVersion, 1)

	return nil
}

func (g *GraphData) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	return g.GetNeighborsWithGen(layer, id, buf, math.MaxUint64)
}

// GetNeighborsWithGen returns the neighbor list for a node with generation isolation.
func (g *GraphData) GetNeighborsWithGen(layer int, id uint32, buf []uint32, maxGen uint64) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunkWithGen(layer, cID, maxGen)
	neighbors := g.GetNeighborsChunkWithGen(layer, cID, maxGen)
	versions := g.GetVersionsChunkWithGen(layer, cID, maxGen)

	// When both chunk-based neighbors and counts are nil (upper layers after Fix #1),
	// try PackedNeighbors first, then fall back to BackingGraph.
	if counts == nil && neighbors == nil {
		// 1. Try Lock-Free PackedNeighbors first
		if layer < len(g.PackedNeighbors) && g.PackedNeighbors[layer] != nil {
			if res, ok := g.PackedNeighbors[layer].GetNeighborsWithGen(id, maxGen); ok {
				return res
			}
		}
		// 2. Fall back to DiskGraph / backing graph
		if g.BackingGraph != nil {
			if bg, ok := g.BackingGraph.(graphFallback); ok {
				return bg.GetNeighbors(layer, id, buf)
			}
		}
		return nil
	}

	// Try Lock-Free PackedNeighbors (also applies when counts exist but neighbors are nil)
	if layer < len(g.PackedNeighbors) && g.PackedNeighbors[layer] != nil {
		if res, ok := g.PackedNeighbors[layer].GetNeighborsWithGen(id, maxGen); ok {
			return res
		}
	}

	// If counts exists but neighbors are nil (upper layers after Fix #1), we're done
	if counts == nil || neighbors == nil {
		if g.BackingGraph != nil {
			if bg, ok := g.BackingGraph.(graphFallback); ok {
				return bg.GetNeighbors(layer, id, buf)
			}
		}
		return nil
	}

	countAddr := &counts[cOff]

	base := cOff * MaxNeighbors

	// Seqlock read loop
	for attempts := 0; attempts < 100; attempts++ {
		var v1 uint32
		if versions != nil {
			v1 = atomic.LoadUint32(&versions[cOff])
			if v1&NodeLockMask != 0 {
				// Writer is active/locked, spin
				continue
			}
		}

		count := int(atomic.LoadInt32(countAddr))
		if count == 0 {
			if g.BackingGraph != nil {
				if bg, ok := g.BackingGraph.(graphFallback); ok {
					return bg.GetNeighbors(layer, id, buf)
				}
			}
			return nil
		}
		// SetNeighbors clamps writes to MaxNeighbors, so any other value
		// means we are reading a slot that is not a live count: a torn
		// seqlock read, or a counts chunk that was swapped under us by a
		// concurrent EnsureChunk (the three chunk offsets are loaded
		// independently, so they are not guaranteed to be consistent).
		// Rejecting it here keeps a garbage count from reaching the slice
		// bounds below, where a negative value panics.
		if count < 0 || count > MaxNeighbors {
			return nil
		}
		if base+count > len(neighbors) {
			return nil
		}

		var res []uint32
		if buf != nil && cap(buf) >= count {
			res = buf[:count]
		} else {
			res = make([]uint32, count)
		}

		// Atomic copy to satisfy race detector and coordinate with seqlock
		for i := 0; i < count; i++ {
			res[i] = atomic.LoadUint32(&neighbors[base+i])
		}

		if versions != nil {
			v2 := atomic.LoadUint32(&versions[cOff])
			if v1 == v2 {
				return res
			}
			// Version changed during read, retry
			continue
		}
		return res
	}

	return nil
}

// GetNeighborsLockFree returns neighbors without seqlock checks.
// Should only be used when the node lock is already held.
func (g *GraphData) GetNeighborsLockFree(layer int, id uint32) []uint32 {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	counts := g.GetCountsChunk(layer, cID)
	neighbors := g.GetNeighborsChunk(layer, cID)

	if counts == nil || neighbors == nil {
		return nil
	}

	count := int(atomic.LoadInt32(&counts[cOff]))
	if count == 0 {
		return nil
	}
	// Same invariant as GetNeighborsWithGen: SetNeighbors never writes a
	// count outside [0, MaxNeighbors], so anything else is a torn or
	// chunk-swapped read and must not reach make([]uint32, count).
	if count < 0 || count > MaxNeighbors {
		return nil
	}

	base := cOff * MaxNeighbors
	res := make([]uint32, count)
	for i := 0; i < count; i++ {
		res[i] = atomic.LoadUint32(&neighbors[base+i])
	}
	return res
}

// GetVersion returns the current version/lock state of a node at a given layer.
func (g *GraphData) GetVersion(layer int, id uint32) uint32 {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0
	}
	return atomic.LoadUint32(&versions[int(id)%ChunkSize])
}

// LockNode acquires a per-node spinlock.
func (g *GraphData) LockNode(layer int, id uint32) uint32 {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0
	}
	verAddr := &versions[int(id)%ChunkSize]

	var spinCycles uint64
	for {
		v := atomic.LoadUint32(verAddr)
		if v&NodeLockMask == 0 {
			if atomic.CompareAndSwapUint32(verAddr, v, v|NodeLockMask) {
				if spinCycles > 0 {
					metrics.LockNodeSpinCyclesTotal.WithLabelValues(g.Name, strconv.Itoa(layer)).Add(float64(spinCycles))
				}
				return v // Return old version for Unlock
			}
		}
		// Spin with exponential backoff
		spinCycles++
		if spinCycles < 20 {
			for i := 0; i < 10; i++ {
				simd.Pause()
			}
		} else {
			// Yield the processor to other goroutines
			runtime.Gosched()
		}
	}
}

// UnlockNode releases the per-node spinlock and increments the version.
func (g *GraphData) UnlockNode(layer int, id, oldVersion uint32) {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return
	}
	verAddr := &versions[int(id)%ChunkSize]
	// Increment version and clear lock bit
	newVersion := (oldVersion + 1) & (NodeLockMask - 1)
	atomic.StoreUint32(verAddr, newVersion)
}

// TryLockNode attempts to acquire the lock once.
func (g *GraphData) TryLockNode(layer int, id uint32) (uint32, bool) {
	versions := g.GetVersionsChunk(layer, int(id)/ChunkSize)
	if versions == nil {
		return 0, false
	}
	verAddr := &versions[int(id)%ChunkSize]

	v := atomic.LoadUint32(verAddr)
	if v&NodeLockMask == 0 {
		if atomic.CompareAndSwapUint32(verAddr, v, v|NodeLockMask) {
			return v, true
		}
	}
	return 0, false
}
