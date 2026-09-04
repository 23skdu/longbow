package index

import (
	"context"
	"math"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// searchLayerFloat64 is a monomorphic specialization of searchLayer for Float64 vectors.
// It implements cache-blocked SIMD traversal (64-vector chunks) and active candidate prefetching
// to eliminate L3 cache eviction stalls on CPU at scale.
func (h *ArrowHNSW) searchLayerFloat64(goCtx context.Context, computer *float64Computer, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData) ([]types.Candidate, error) {
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
	minHeap.PushCandidate(epCand)

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
		resultSetAdapter.PushCandidate(epCand)
	}
	ctx.visited.Set(int(entryPoint))

	const traversalBlockSize = 64

	for minHeap.Len() > 0 {
		if err := goCtx.Err(); err != nil {
			return nil, err
		}

		if ctx.visitedNodesBudget > 0 && ctx.nodesVisitedCount >= ctx.visitedNodesBudget {
			metrics.HNSWEarlyTerminationTotal.WithLabelValues("budget_exceeded").Inc()
			break
		}

		maxCommitted := meta.NodeCount
		if ctx != nil && ctx.MaxNodeCount > 0 {
			maxCommitted = ctx.MaxNodeCount
		}

		curr := minHeap.PopCandidate()
		ctx.nodesVisitedCount++

		// Active prefetch of the next best candidate on the heap to hide traversal latency
		if minHeap.Len() > 0 {
			nextBest := (*minHeap)[0]
			if int64(nextBest.ID) < maxCommitted {
				computer.Prefetch(nextBest.ID)
			}
		}

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

		neighbors := h.GetNeighborsCombinedManual(data, layer, curr.ID, ctx.neighborBatch, maxGen)

		_ = cachedMMax

		// Prefetch neighbor vectors aggressively across all cache lines (prefetching 2 and 4 steps ahead)
		for i := range neighbors {
			if i+2 < len(neighbors) {
				nextN := neighbors[i+2]
				if int64(nextN) < maxCommitted {
					computer.Prefetch(nextN)
				}
			}
			if i+4 < len(neighbors) {
				nextN4 := neighbors[i+4]
				if int64(nextN4) < maxCommitted {
					computer.Prefetch(nextN4)
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

				var validBatch []uint32
				for i, n := range batch {
					if results[i] == 1 {
						validBatch = append(validBatch, n)
					} else {
						metrics.HNSWNodesSkippedTotal.WithLabelValues(h.name).Inc()
					}
				}

				if len(validBatch) > 0 {
					// Cache-blocked candidate evaluation in 64-vector chunks
					for chunkStart := 0; chunkStart < len(validBatch); chunkStart += traversalBlockSize {
						chunkEnd := chunkStart + traversalBlockSize
						if chunkEnd > len(validBatch) {
							chunkEnd = len(validBatch)
						}
						block := validBatch[chunkStart:chunkEnd]

						// Prefetch candidate vectors in next tile
						if chunkEnd < len(validBatch) {
							nextEnd := chunkEnd + traversalBlockSize
							if nextEnd > len(validBatch) {
								nextEnd = len(validBatch)
							}
							for _, nextN := range validBatch[chunkEnd:nextEnd] {
								if int64(nextN) < maxCommitted {
									computer.Prefetch(nextN)
								}
							}
						}

						ctx.distComputeCount += len(block)
						if cap(ctx.distsTemp) < len(block) {
							ctx.distsTemp = make([]float32, len(block))
						}
						dists, err := computer.ComputeBatch(block, ctx.distsTemp[:len(block)])
						if err == nil {
							for i, n := range block {
								d := dists[i]
								cand := types.Candidate{ID: n, Dist: d}
								minHeap.PushCandidate(cand)

								if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(n) {
									continue
								}
								if h.IsDeleted(n) {
									continue
								}

								if len(ctx.resultSet) > 0 {
									furthest := ctx.resultSet[0]
									if ctx.resultSet.Len() < ef || d < furthest.Dist {
										resultSetAdapter.PushCandidate(cand)
										if ctx.resultSet.Len() > ef {
											resultSetAdapter.PopCandidate()
										}
									}
								} else {
									resultSetAdapter.PushCandidate(cand)
								}
							}
						}
					}
				}
			}
		} else {
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
				// Cache-blocked candidate evaluation in 64-vector chunks
				for chunkStart := 0; chunkStart < len(batch); chunkStart += traversalBlockSize {
					chunkEnd := chunkStart + traversalBlockSize
					if chunkEnd > len(batch) {
						chunkEnd = len(batch)
					}
					block := batch[chunkStart:chunkEnd]

					// Prefetch candidate vectors in next tile
					if chunkEnd < len(batch) {
						nextEnd := chunkEnd + traversalBlockSize
						if nextEnd > len(batch) {
							nextEnd = len(batch)
						}
						for _, nextN := range batch[chunkEnd:nextEnd] {
							if int64(nextN) < maxCommitted {
								computer.Prefetch(nextN)
							}
						}
					}

					ctx.distComputeCount += len(block)
					if cap(ctx.distsTemp) < len(block) {
						ctx.distsTemp = make([]float32, len(block))
					}
					dists, err := computer.ComputeBatch(block, ctx.distsTemp[:len(block)])
					if err == nil {
						for i, n := range block {
							d := dists[i]
							cand := types.Candidate{ID: n, Dist: d}

							minHeap.PushCandidate(cand)

							if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(n) {
								continue
							}
							if h.deleted != nil && h.deleted.Contains(n) {
								continue
							}

							if len(ctx.resultSet) > 0 {
								furthest := ctx.resultSet[0]

								if ctx.resultSet.Len() < ef || d < furthest.Dist {
									resultSetAdapter.PushCandidate(cand)
									if ctx.resultSet.Len() > ef {
										resultSetAdapter.PopCandidate()
									}
								}
							} else {
								resultSetAdapter.PushCandidate(cand)
							}
						}
					}
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
		res[i] = resultSetAdapter.PopCandidate()
	}
	return res, nil
}
