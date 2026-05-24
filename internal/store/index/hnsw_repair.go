package index

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
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

	meta := h.metadataRegistry.Load()
	maxID := int(meta.NodeCount)

	poolCtx := h.searchPool.Get()
	poolCtx.MaxNodeCount = meta.NodeCount
	poolCtx.MaxGeneration = meta.Generation
	poolCtx.Reset()
	defer h.searchPool.PutWithMetrics(poolCtx, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))

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
		if h.deleted.Contains(nid) {
			// If node itself is deleted, skip
			continue
		}

		// Scan layers
		for lvl := 0; lvl < types.ArrowMaxLayers; lvl++ {
			// Reload data regularly to see latest snapshots from other threads/COW
			data = h.data.Load()

			// Scan neighbors using unified accessor (Shadow Topology support)
			neighbors := h.GetNeighborsCombinedManual(data, lvl, nid, poolCtx.neighborBatch, meta.Generation)
			if len(neighbors) == 0 {
				continue
			}

			// COW Promotion: Ensure nid is promoted before locking and modifying
			data = h.promoteNode(data, nid)

			// Acquire Node Lock for this layer
			oldVer := data.LockNode(lvl, nid)
			if oldVer&types.NodeLockMask != 0 {
				continue
			}

			cID := types.ChunkID(nid)
			cOff := types.ChunkOffset(nid)
			neighborsChunk := data.GetNeighborsChunk(lvl, cID)
			countsChunk := data.GetCountsChunk(lvl, cID)

			if neighborsChunk == nil || countsChunk == nil {
				data.UnlockNode(lvl, nid, oldVer)
				continue
			}

			countAddr := &countsChunk[cOff]

			allNeighbors := neighbors

			if len(allNeighbors) == 0 {
				data.UnlockNode(lvl, nid, oldVer)
				continue
			}

			// Check for tombstones in neighbor list
			hasTombstone := false
			var knownTombstones []uint32

			for _, neighborID := range allNeighbors {
				isDel := h.deleted.Contains(neighborID)
				if nid == 0 {
				}
				if isDel {
					hasTombstone = true
					knownTombstones = append(knownTombstones, neighborID)
				}
			}

			if !hasTombstone {
				if nid%10 == 0 {
				}
				data.UnlockNode(lvl, nid, oldVer)
				continue
			} else {
				// 1. Identify valid candidates: (Current Neighbors - Tombstones) U (Tombstones' Neighbors)

				// Reuse candidate heap
				poolCtx.candidates.Clear()
				poolCtx.visited.ClearSIMD()

				// Add existing VALID neighbors
				for _, neighborID := range allNeighbors {
					if !h.deleted.Contains(neighborID) {
						dist, err := h.distFunc(getVec(h, data, nid), getVec(h, data, neighborID))
						if err != nil {
							dist = math.MaxFloat32
						}
						poolCtx.candidates.Push(types.Candidate{ID: neighborID, Dist: dist})
						poolCtx.visited.Set(int(neighborID))
					}
				}

				baseIdx := int(cOff) * types.MaxNeighbors

				// Add Tombstones' neighbors
				for _, tID := range knownTombstones {
					// Read T's neighbors at SAME level
					tCID := types.ChunkID(tID)

					if lvl >= len(data.Neighbors) || int(tCID) >= len(data.Neighbors[lvl]) || data.Neighbors[lvl][tCID] == 0 {
						continue
					}
					tNeighbors := h.GetNeighborsCombinedManual(data, lvl, tID, poolCtx.neighborBatch, meta.Generation)
					tCount := len(tNeighbors)

					for k := 0; k < tCount; k++ {
						candidateID := tNeighbors[k]
						if candidateID == nid {
							continue
						} // Don't add self
						if h.deleted.Contains(candidateID) {
							continue
						} // Skip recursive tombstones
						if poolCtx.visited.IsSet(int(candidateID)) {
							continue
						}

						v1 := getVec(h, data, nid)
						v2 := getVec(h, data, candidateID)
						if v1 != nil && v2 != nil {
							dist, err := h.distFunc(v1, v2)
							if err != nil {
								dist = math.MaxFloat32
							}
							poolCtx.candidates.Push(types.Candidate{ID: candidateID, Dist: dist})
							poolCtx.visited.Set(int(candidateID))
						}
					}
				}

				// Select Best M
				limitM := int(h.m.Load())
				if lvl == 0 {
					limitM = int(h.m.Load()) * 2
				}
				maxConn := int(h.mMax.Load())
				if lvl == 0 {
					maxConn = int(h.mMax0.Load())
				}

				var candList []types.Candidate
				for poolCtx.candidates.Len() > 0 {
					c, _ := poolCtx.candidates.PopCandidate()
					candList = append(candList, c)
				}

				selected := h.selectNeighbors(poolCtx, candList, limitM, data)

				if nid%10 == 0 {
				}

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
				// Clear remainder up to MaxNeighbors to ensure no tombstones left in legacy arena
				for k := writeIdx; k < types.MaxNeighbors; k++ {
					atomic.StoreUint32(&neighborsChunk[baseIdx+k], 0)
				}

				atomic.StoreInt32(countAddr, int32(writeIdx))
				atomic.AddUint32(verAddr, 1) // Even

				// Also update PackedNeighbors for consistency
				if lvl < len(data.PackedNeighbors) && data.PackedNeighbors[lvl] != nil {
					newIDs := make([]uint32, len(selected))
					for k, sel := range selected {
						newIDs[k] = sel.ID
					}
					_ = data.PackedNeighbors[lvl].SetNeighbors(nid, newIDs)
				}

				// CRITICAL: Clear shadow neighbors in TopLayerManager so they don't hide the repair
				if h.topLayerManager != nil {
					h.topLayerManager.ClearNeighbors(lvl, nid)
				}

				// Clear lock-free neighbor cache to prevent stale reads
				if lvl >= 0 && lvl < len(h.neighborCache) && h.neighborCache[lvl] != nil {
					h.neighborCache[lvl].Remove(nid)
				}

				repaired++

				// CAS the updated data pointer back to h.data to ensure visibility
				latest := h.data.Load()
				swapped := h.data.CompareAndSwap(latest, data)
				if !swapped {
				}
			}
			data.UnlockNode(lvl, nid, oldVer)
		}
	}

	return repaired
}

func getVec(h *ArrowHNSW, data *types.GraphData, id uint32) []float32 {
	v := h.mustGetVectorFromData(data, id)
	if vf32, ok := v.([]float32); ok {
		return vf32
	}
	return nil
}
