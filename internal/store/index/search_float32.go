package index

import (
	"container/heap"
	"context"
	"math"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

func (h *ArrowHNSW) searchLayerFloat32(goCtx context.Context, computer *float32ToFloat32Computer, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData) ([]types.Candidate, error) {
	meta := h.GetMetadataSnapshot()

	if entryPoint == math.MaxUint32 {
		return nil, nil
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}

	start := time.Now()
	defer func() {
		if ctx != nil {
			ctx.distComputeTime += time.Since(start)
		}
	}()

	// Cache atomics for hot loop
	cachedMMax := h.mMax.Load()

	// Compute entry point distance and prefetch neighbors.
	// No type assertion needed — computer is *float32ToFloat32Computer.
	var epDist float32
	{
		var err error
		if ctx != nil {
			ctx.distComputeCount++
		}
		maxCommitted := meta.NodeCount
		if ctx != nil && ctx.MaxNodeCount > 0 {
			maxCommitted = ctx.MaxNodeCount
		}

		if !ctx.AllowUncommitted && int64(entryPoint) >= maxCommitted {
			oldVer := data.LockNode(0, entryPoint)
			epDist, err = computer.ComputeSingle(entryPoint)
			data.UnlockNode(0, entryPoint, oldVer)
		} else {
			epDist, err = computer.ComputeSingle(entryPoint)
		}
		if err != nil {
			return nil, err
		}
		computer.Prefetch(entryPoint)
	}

	// 1. Reset Frontier for this layer
	ctx.candidates = ctx.candidates[:0]
	ctx.resultSet = ctx.resultSet[:0]
	ctx.visited.Clear()

	minHeap := (*MinCandidateHeapAdapter)(&ctx.candidates)
	resultSetAdapter := (*MaxCandidateHeapAdapter)(&ctx.resultSet)

	epCand := types.Candidate{ID: entryPoint, Dist: epDist}
	heap.Push(minHeap, epCand)

	passes := true
	if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(entryPoint) {
		passes = false
	}
	if passes && h.IsDeleted(entryPoint) {
		passes = false
	}
	if passes && ctx.predicate != nil && !ctx.predicate.IsMatch(entryPoint) {
		passes = false
	}
	if passes {
		heap.Push(resultSetAdapter, epCand)
	}
	ctx.visited.Set(int(entryPoint))

	for minHeap.Len() > 0 {
		if err := goCtx.Err(); err != nil {
			return nil, err
		}

		if ctx.visitedNodesBudget > 0 && ctx.nodesVisitedCount >= ctx.visitedNodesBudget {
			metrics.HNSWEarlyTerminationTotal.WithLabelValues(h.name, "budget_exceeded").Inc()
			break
		}

		curr := heap.Pop(minHeap).(types.Candidate)
		ctx.nodesVisitedCount++

		if len(ctx.resultSet) > 0 {
			furthest := ctx.resultSet[0]
			threshold := furthest.Dist
			if h.config.SQ8Enabled {
				threshold *= 1.05
			}
			if curr.Dist > threshold && ctx.resultSet.Len() >= ef {
				break
			}
		}

		neighbors := h.GetNeighborsCombinedManual(data, layer, curr.ID, ctx.neighborBatch, ctx.MaxGeneration)

		prefetchLimit := cachedMMax
		if prefetchLimit > 64 {
			prefetchLimit = 64
		}
		if prefetchLimit < 16 {
			prefetchLimit = 16
		}
		maxCommitted := meta.NodeCount
		if ctx != nil && ctx.MaxNodeCount > 0 {
			maxCommitted = ctx.MaxNodeCount
		}

		for i := 0; i < len(neighbors) && i < int(prefetchLimit); i++ {
			nID := neighbors[i]
			if !ctx.AllowUncommitted && int64(nID) >= maxCommitted {
				continue
			}
			cID := int(nID) / types.ChunkSize
			chunk := data.GetVectorsChunkWithGen(cID, maxGen)
			if chunk != nil {
				// Touch data to warm cache — Prefetch is called by computer
				_ = chunk
			}
		}

		// Prefetch neighbor vectors to hide memory latency
		for i := range neighbors {
			if i+2 < len(neighbors) {
				nextN := neighbors[i+2]
				if int64(nextN) < maxCommitted {
					computer.Prefetch(nextN)
				}
			}
		}

		if ctx.predicate != nil {
			batch := ctx.neighborBatch[:0]
			for _, n := range neighbors {
				if !ctx.AllowUncommitted && int64(n) >= maxCommitted {
					continue
				}
				if ctx.visited.IsSet(int(n)) {
					continue
				}
				ctx.visited.Set(int(n))
				batch = append(batch, n)
			}

			if len(batch) > 0 {
				results := ctx.EvaluatePredicateBatch(batch)

				for i, n := range batch {
					if results[i] == 0 {
						metrics.HNSWNodesSkippedTotal.WithLabelValues(h.name).Inc()
						continue
					}

					ctx.distComputeCount++
					d, err := computer.ComputeSingle(n)
					if err != nil {
						continue
					}

					cand := types.Candidate{ID: n, Dist: d}
					heap.Push(minHeap, cand)

					if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(n) {
						continue
					}
					if h.IsDeleted(n) {
						continue
					}

					if len(ctx.resultSet) > 0 {
						furthest := ctx.resultSet[0]
						if ctx.resultSet.Len() < ef || d < furthest.Dist {
							heap.Push(resultSetAdapter, cand)
							if ctx.resultSet.Len() > ef {
								heap.Pop(resultSetAdapter)
							}
						}
					} else {
						heap.Push(resultSetAdapter, cand)
					}
				}
			}
		} else {
			for _, n := range neighbors {
				if !ctx.AllowUncommitted && int64(n) >= maxCommitted {
					continue
				}
				if ctx.visited.IsSet(int(n)) {
					continue
				}
				ctx.visited.Set(int(n))

				ctx.distComputeCount++
				d, err := computer.ComputeSingle(n)
				if err != nil {
					continue
				}

				cand := types.Candidate{ID: n, Dist: d}

				heap.Push(minHeap, cand)

				if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(n) {
					continue
				}
				if h.deleted != nil && h.deleted.Contains(n) {
					continue
				}

				if len(ctx.resultSet) > 0 {
					furthest := ctx.resultSet[0]

					if ctx.resultSet.Len() < ef || d < furthest.Dist {
						heap.Push(resultSetAdapter, cand)
						if ctx.resultSet.Len() > ef {
							heap.Pop(resultSetAdapter)
						}
					}
				} else {
					heap.Push(resultSetAdapter, cand)
				}
			}
		}
	}

	count := len(ctx.resultSet)
	var res []types.Candidate
	if ctx != nil {
		if cap(ctx.layerCandidates) >= count {
			res = ctx.layerCandidates[:count]
		} else {
			res = make([]types.Candidate, count)
			ctx.layerCandidates = res
		}
	} else {
		res = make([]types.Candidate, count)
	}

	for i := count - 1; i >= 0; i-- {
		res[i] = heap.Pop(resultSetAdapter).(types.Candidate)
	}
	return res, nil
}
