package store

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *GraphData, source, target uint32, layer, maxConn int, dist float32) {
	// Acquire lock for the specific node (shard)
	lockID := source % ShardedLockCount
	lockStart := time.Now()
	h.shardedLocks.Lock(uint64(lockID))
	h.metricLockWait.WithLabelValues("sharded").Observe(time.Since(lockStart).Seconds())
	defer h.shardedLocks.Unlock(uint64(lockID))

	cID := chunkID(source)
	cOff := chunkOffset(source)

	// Ensure chunk exists (it should, as we resized before insert)
	// Promote node from Disk if needed (Copy-On-Write)
	data = h.promoteNode(data, source)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// This indicates a stale data snapshot or a logic error.
		// Reload data and try again, or return error. For now, reload.
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			// Still nil, cannot proceed. This should ideally not happen if Grow/ensureChunk works.
			return
		}
	}

	// Check for duplicates first to ensure idempotency
	// This requires reading current neighbors
	currentCount := atomic.LoadInt32(&countsChunk[cOff])
	baseIdx := int(cOff) * types.MaxNeighbors

	for i := 0; i < int(currentCount); i++ {
		if atomic.LoadUint32(&neighborsChunk[baseIdx+i]) == target {
			return // Already connected
		}
	}

	// 3a. Update metadata (Counts)
	// Counts[level][chunkID][chunkOffset] atomic increment
	// 3a. Prepare for write
	// Calculate slot using currentCount (stable under shard lock)
	slot := int(currentCount)
	if slot >= types.MaxNeighbors {
		// Should not happen if MaxNeighbors >> maxConn and pruning works
		return
	}

	// Seqlock write start: increment version to odd
	verChunk := data.GetVersionsChunk(layer, cID)
	if verChunk == nil {
		return
	}
	verAddr := &verChunk[cOff]
	atomic.AddUint32(verAddr, 1)

	// Atomic store to satisfy race detector
	atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)

	// Update metadata (Counts) - make visible AFTER data write
	countAddr := &countsChunk[cOff]
	newCount := atomic.AddInt32(countAddr, 1)

	// Seqlock write: increment version (even = clean)
	atomic.AddUint32(verAddr, 1)

	// Increment global version
	atomic.AddUint64(&data.GlobalVersion, 1)

	// --- Packed Neighbors Integration (v0.1.4-rc1) ---
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		// Get existing neighbors from packed to check for duplicates (though we checked above)
		existing, ok := pn.GetNeighbors(source)
		if !ok || len(existing) == 0 {
			if h.config.Float16Enabled {
				_ = pn.SetNeighborsF16(source, []uint32{target}, []float16.Num{float16.New(dist)})
			} else {
				_ = pn.SetNeighbors(source, []uint32{target})
			}
		} else {
			// Check if already present in packed
			found := false
			for _, nid := range existing {
				if nid == target {
					found = true
					break
				}
			}
			if !found {
				newNeighbors := make([]uint32, len(existing)+1)
				copy(newNeighbors, existing)
				newNeighbors[len(existing)] = target

				// Optional: Store distances if Float16 enabled
				if h.config.Float16Enabled {
					// We need to manage distances too
					_, existingDists, f16ok := pn.GetNeighborsF16(source)
					newDists := make([]float16.Num, len(existing)+1)
					if f16ok && len(existingDists) == len(existing) {
						copy(newDists, existingDists)
					} else {
						// Create dummy distances if missing
						for i := range existing {
							newDists[i] = float16.New(0)
						}
					}
					newDists[len(existing)] = float16.New(dist)
					_ = pn.SetNeighborsF16(source, newNeighbors, newDists)
				} else {
					_ = pn.SetNeighbors(source, newNeighbors)
				}
			}
		}
	}

	// Prune if needed
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

	// Acquire lock for the target node
	lockID := target % ShardedLockCount
	h.shardedLocks.Lock(uint64(lockID))
	defer h.shardedLocks.Unlock(uint64(lockID))

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

	// Read current state
	countAddr := &countsChunk[cOff]
	currentCount := atomic.LoadInt32(countAddr)

	// Collect actual additions (filter duplicates)
	// We read current neighbors to check against incoming sources.
	// Since duplicates are rare in HNSW construction (unless duplicate vectors),
	// we can do a simple check.
	baseIdx := int(cOff) * types.MaxNeighbors

	// Identify distinct new sources that are NOT already connected
	// Optimization: Use a small stack map or simple loop if small.
	var toAddIdxs []int

	for i, src := range sources {
		found := false
		for j := 0; j < int(currentCount); j++ {
			if atomic.LoadUint32(&neighborsChunk[baseIdx+j]) == src {
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

	// Double check capacity
	// In extremely rare cases (huge batch), we might exceed MaxNeighbors even before pruning?
	// MaxNeighbors is usually 64-128. If we add 100, we overflow.
	// HNSW paper allows temporary overflow or hard cap.
	// Our storage is fixed size `MaxNeighbors`. We CANNOT exceed it.
	// So we must bound `toAddIdxs` to fit available space OR implement a "pre-prune" strategy.
	// A simpler safe approach: Cap at MaxNeighbors - currentCount

	available := types.MaxNeighbors - int(currentCount)
	if available < 0 {
		available = 0
	}

	// If we have more new edges than space, we should prioritize?
	// But we haven't computed distances of *existing* edges here to compare.
	// Standard HNSW connects and then prunes. If we can't fit, we have a problem with fixed storage.
	// Longbow `MaxNeighbors` should be `M_max * 2` or similar to allow overflow.
	// If `mMax` ~= 32 and `MaxNeighbors` ~= 64, we have room for 32 additions.
	if len(toAddIdxs) > available {
		// Just take the first N that fit.
		// Or better: Pre-sort incoming by distance and take best?
		// Assuming sources/dists are correlated.
		// Let's implement pre-sorting if we are overflowing.
		// For now simple truncation to avoid buffer overflow panic.
		toAddIdxs = toAddIdxs[:available]
	}

	if len(toAddIdxs) == 0 {
		return
	}

	// Perform Writes
	verChunk := data.GetVersionsChunk(layer, cID)
	var verAddr *uint32
	if verChunk != nil {
		verAddr = &verChunk[cOff]
		atomic.AddUint32(verAddr, 1) // Odd = dirty
	}

	for _, idx := range toAddIdxs {
		slot := int(currentCount)
		src := sources[idx]

		atomic.StoreUint32(&neighborsChunk[baseIdx+slot], src)
		currentCount++
		// We don't update atomic count yet, we do it at once or incrementally?
		// Incrementally is safer if we crash? No, batch write.
	}

	// Update Count Atomically
	atomic.StoreInt32(countAddr, currentCount)

	if verAddr != nil {
		atomic.AddUint32(verAddr, 1) // Even = clean
	}

	// Increment global version
	atomic.AddUint64(&data.GlobalVersion, 1)

	// Packed Neighbors Updates
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		// This is expensive: Read-Modify-Write Packed Neighbors
		// But AddConnection does it per edge. Here we batch.
		// "SetNeighbors" implements replacement or merge?
		// Looking at usage in AddConnection, it seems to be "Set" (overwrite) or manual merge?
		// AddConnection logic: "Get existing... if !found copy... SetNeighbors".
		// We can optimize: Get existing once, append all new, Set.

		existing, ok := pn.GetNeighbors(target)
		// We already filtered duplicates against *Arrow* chunks. Should match.
		// Construct new list.
		// Careful: `existing` might be reused buffer? No, usually copy.

		var newNeighbors []uint32
		if ok {
			newNeighbors = make([]uint32, len(existing)+len(toAddIdxs))
			copy(newNeighbors, existing)
		} else {
			newNeighbors = make([]uint32, len(toAddIdxs))
		}

		offset := 0
		if ok {
			offset = len(existing)
		}
		for i, idx := range toAddIdxs {
			newNeighbors[offset+i] = sources[idx]
		}

		if h.config.Float16Enabled {
			// Handle distances
			var newF16Dists []float16.Num
			if ok {
				_, existingDists, _ := pn.GetNeighborsF16(target)
				// If existingDists missing but existing present (rare sync issue), pad.
				newF16Dists = make([]float16.Num, len(existing)+len(toAddIdxs))
				copy(newF16Dists, existingDists)
			} else {
				newF16Dists = make([]float16.Num, len(toAddIdxs))
			}

			for i, idx := range toAddIdxs {
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
	lockID := id % ShardedLockCount
	h.shardedLocks.Lock(uint64(lockID))
	defer h.shardedLocks.Unlock(uint64(lockID))

	// COW Promotion
	data = h.promoteNode(data, id)

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
