package store

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// RepairTombstones scans the graph for connections to deleted nodes and repairs them.
// It differs from CleanupTombstones by attempting to find replacement neighbors
// ("wiring around") instead of just pruning.
func (h *ArrowHNSW) RepairTombstones(ctx context.Context, batchSize int) int {
	start := time.Now()
	repaired := 0
	defer func() {
		metrics.HNSWRepairDuration.Observe(time.Since(start).Seconds())
		metrics.HNSWRepairTotal.Inc()
		metrics.HNSWRepairedConnections.Add(float64(repaired))
	}()

	data := h.data.Load()
	if data == nil {
		return 0
	}

	maxID := int(h.nodeCount.Load())

	poolCtx := h.searchPool.Get().(*ArrowSearchContext)
	poolCtx.Reset()
	defer h.searchPool.Put(poolCtx)

	// Iterate valid nodes to check THEIR outgoing connections
	for i := 0; i < maxID; i++ {
		// Throttling / Context Check
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return repaired
			default:
			}
		}

		nid := uint32(i)
		if h.deleted.Contains(int(nid)) {
			// If node itself is deleted, skip
			continue
		}

		// Lock node
		lockID := nid % 1024
		h.shardedLocks.Lock(uint64(lockID))

		// Re-check validity
		if h.deleted.Contains(int(nid)) {
			h.shardedLocks.Unlock(uint64(lockID))
			continue
		}

		// Scan layers
		for lvl := 0; lvl < ArrowMaxLayers; lvl++ {
			cID := chunkID(nid)
			cOff := chunkOffset(nid)

			// Get Neighbors
			// NOTE: We need direct access to modify.
			if lvl >= len(data.Neighbors) || int(cID) >= len(data.Neighbors[lvl]) || data.Neighbors[lvl][cID] == 0 {
				continue
			}

			neighborsChunk := data.GetNeighborsChunk(lvl, cID)
			countsChunk := data.GetCountsChunk(lvl, cID)
			if neighborsChunk == nil || countsChunk == nil {
				continue
			}

			countAddr := &countsChunk[cOff]
			count := int(atomic.LoadInt32(countAddr))
			if count == 0 {
				continue
			}

			baseIdx := int(cOff) * MaxNeighbors

			// Check for tombstones in neighbor list
			hasTombstone := false
			var knownTombstones []uint32

			for r := 0; r < count; r++ {
				neighborID := neighborsChunk[baseIdx+r]
				if h.deleted.Contains(int(neighborID)) {
					hasTombstone = true
					knownTombstones = append(knownTombstones, neighborID)
				}
			}

			if !hasTombstone {
				// Don't unlock here, just continue to next layer
				continue
			} else {
				// Repair Logic
				// 1. Identify valid candidates: (Current Neighbors - Tombstones) U (Tombstones' Neighbors)

				// Reuse candidate heap
				poolCtx.candidates.Clear()
				poolCtx.visited.ClearSIMD()

				// Add existing VALID neighbors
				for r := 0; r < count; r++ {
					neighborID := neighborsChunk[baseIdx+r]
					if !h.deleted.Contains(int(neighborID)) {
						dist, err := h.distFunc(getVec(h, data, nid), getVec(h, data, neighborID))
						if err != nil {
							dist = math.MaxFloat32
						}
						poolCtx.candidates.Push(Candidate{ID: neighborID, Dist: dist})
						poolCtx.visited.Set(neighborID)
					}
				}

				// Add Tombstones' neighbors
				for _, tID := range knownTombstones {
					// Read T's neighbors at SAME level
					tCID := chunkID(tID)
					tCOff := chunkOffset(tID)

					if lvl >= len(data.Neighbors) || int(tCID) >= len(data.Neighbors[lvl]) {
						continue
					}
					tNeighbors := data.GetNeighborsChunk(lvl, tCID)
					tCounts := data.GetCountsChunk(lvl, tCID)
					if tNeighbors == nil || tCounts == nil {
						continue
					}

					tCount := int(atomic.LoadInt32(&tCounts[tCOff]))
					tBase := int(tCOff) * MaxNeighbors

					for k := 0; k < tCount; k++ {
						candidateID := tNeighbors[tBase+k]
						if candidateID == nid {
							continue
						} // Don't add self
						if h.deleted.Contains(int(candidateID)) {
							continue
						} // Skip recursive tombstones
						if poolCtx.visited.IsSet(candidateID) {
							continue
						}

						v1 := getVec(h, data, nid)
						v2 := getVec(h, data, candidateID)
						if v1 != nil && v2 != nil {
							dist, err := h.distFunc(v1, v2)
							if err != nil {
								dist = math.MaxFloat32
							}
							poolCtx.candidates.Push(Candidate{ID: candidateID, Dist: dist})
							poolCtx.visited.Set(candidateID)
						}
					}
				}

				// Select Best M
				limitM := h.m
				if lvl == 0 {
					limitM = h.m * 2
				}
				maxConn := h.mMax
				if lvl == 0 {
					maxConn = h.mMax0
				}

				var candList []Candidate
				for poolCtx.candidates.Len() > 0 {
					c, _ := poolCtx.candidates.Pop()
					candList = append(candList, c)
				}

				selected := h.selectNeighbors(poolCtx, candList, limitM, data)

				// Write back
				versChunk := data.GetVersionsChunk(lvl, cID)
				verAddr := &versChunk[cOff]
				atomic.AddUint32(verAddr, 1) // Odd

				writeIdx := 0
				for _, sel := range selected {
					if writeIdx >= maxConn {
						break
					}
					atomic.StoreUint32(&neighborsChunk[baseIdx+writeIdx], sel.ID)
					writeIdx++
				}
				// Clear remainder
				for k := writeIdx; k < maxConn; k++ {
					atomic.StoreUint32(&neighborsChunk[baseIdx+k], 0)
				}

				atomic.StoreInt32(countAddr, int32(writeIdx))
				atomic.AddUint32(verAddr, 1) // Even

				repaired++
			}
		}
		h.shardedLocks.Unlock(uint64(lockID))
	}

	return repaired
}

func getVec(h *ArrowHNSW, data *GraphData, id uint32) []float32 {
	v := h.mustGetVectorFromData(data, id)
	if vf32, ok := v.([]float32); ok {
		return vf32
	}
	return nil
}
