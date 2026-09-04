package index

import (
	"context"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) searchLayer(goCtx context.Context, computer any, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData, queryVec any) ([]types.Candidate, error) {
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

	// Define polymorphic distance computer
	var distComputer func(uint32) (float32, error)
	var distBatchComputer func([]uint32, []float32) ([]float32, error)
	var epDist float32

	var disk *DiskGraph
	if ctx != nil {
		disk = ctx.diskGraph
	}

	// Optimization: Use unified DistanceComputer interface
	if comp, ok := computer.(DistanceComputer); ok {
		distComputer = comp.ComputeSingle
		distBatchComputer = comp.ComputeBatch
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
			epDist, err = comp.ComputeSingle(entryPoint)
			data.UnlockNode(0, entryPoint, oldVer)
		} else {
			epDist, err = comp.ComputeSingle(entryPoint)
		}
		if err != nil {
			return nil, err
		}

		// Prefetch neighbors of entry point if possible
		comp.Prefetch(entryPoint)
	} else {
		// Fallback for types not yet refactored or using simple func
		// (though all core types should be refactored by now)
		switch q := queryVec.(type) {
		case []float32:
			distComputer = func(id uint32) (float32, error) {
				var disk *DiskGraph
				if ctx != nil {
					disk = ctx.diskGraph
				}
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				switch v := vecAny.(type) {
				case []float32:
					return h.distFunc(q, v)
				case []float64:
					if h.distFuncF64 == nil {
						return math.MaxFloat32, nil
					}
					var q64 []float64
					if ctx != nil {
						if cap(ctx.queryF64) < len(q) {
							ctx.queryF64 = make([]float64, len(q))
						}
						ctx.queryF64 = ctx.queryF64[:len(q)]
						for i, val := range q {
							ctx.queryF64[i] = float64(val)
						}
						q64 = ctx.queryF64
					} else {
						q64 = make([]float64, len(q))
						for i, val := range q {
							q64[i] = float64(val)
						}
					}
					return h.distFuncF64(q64, v)
				case []float16.Num:
					if h.distFuncF16 == nil {
						return math.MaxFloat32, nil
					}
					var q16 []float16.Num
					if ctx != nil {
						if cap(ctx.queryF16) < len(q) {
							ctx.queryF16 = make([]float16.Num, len(q))
						}
						ctx.queryF16 = ctx.queryF16[:len(q)]
						for i, val := range q {
							ctx.queryF16[i] = float16.New(val)
						}
						q16 = ctx.queryF16
					} else {
						q16 = make([]float16.Num, len(q))
						for i, val := range q {
							q16[i] = float16.New(val)
						}
					}
					return h.distFuncF16(q16, v)
				case []int8, []uint8:
					var v8 []uint8
					if vi8, ok := v.([]int8); ok {
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
					} else {
						v8 = v.([]uint8)
					}

					if h.quantizer != nil && h.sq8Ready.Load() {
						minV, maxV := h.quantizer.Params()
						scale := (maxV - minV) / 255.0
						var sum float32
						for i, val := range q {
							deq := minV + float32(v8[i])*scale
							diff := val - deq
							sum += diff * diff
						}
						return float32(math.Sqrt(float64(sum))), nil
					}
					// Fallback
					var sum float32
					for i, val := range q {
						diff := val - float32(v8[i])
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				case []complex64:
					qLen := len(q)
					var qComplex []complex64
					if ctx != nil {
						if cap(ctx.queryC64) < qLen/2 {
							ctx.queryC64 = make([]complex64, qLen/2)
						}
						ctx.queryC64 = ctx.queryC64[:qLen/2]
						for i := 0; i < qLen/2; i++ {
							ctx.queryC64[i] = complex(q[2*i], q[2*i+1])
						}
						qComplex = ctx.queryC64
					} else {
						qComplex = make([]complex64, qLen/2)
						for i := 0; i < qLen/2; i++ {
							qComplex[i] = complex(q[2*i], q[2*i+1])
						}
					}
					var sum float32
					for i, val := range qComplex {
						if i < len(v) {
							diff := val - v[i]
							modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
							sum += modSq
						}
					}
					return float32(math.Sqrt(float64(sum))), nil
				case []complex128:
					qLen := len(q)
					var qComplex []complex128
					if ctx != nil {
						if cap(ctx.queryC128) < qLen/2 {
							ctx.queryC128 = make([]complex128, qLen/2)
						}
						ctx.queryC128 = ctx.queryC128[:qLen/2]
						for i := 0; i < qLen/2; i++ {
							ctx.queryC128[i] = complex(float64(q[2*i]), float64(q[2*i+1]))
						}
						qComplex = ctx.queryC128
					} else {
						qComplex = make([]complex128, qLen/2)
						for i := 0; i < qLen/2; i++ {
							qComplex[i] = complex(float64(q[2*i]), float64(q[2*i+1]))
						}
					}
					var sum float64
					for i, val := range qComplex {
						if i < len(v) {
							diff := val - v[i]
							modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
							sum += modSq
						}
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}

		case []int8:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				switch vAny := vecAny.(type) {
				case []float32:
					// Convert q to float32
					var minV, maxV float32
					var scale float32
					if h.quantizer != nil {
						minV, maxV = h.quantizer.Params()
						scale = (maxV - minV) / 255.0
					}
					var sum float32
					for i, val := range q {
						deq := minV + float32(val)*scale
						diff := deq - vAny[i]
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				case []int8, []uint8:
					var v8 []uint8
					if vi8, ok := vAny.([]int8); ok {
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
					} else {
						v8 = vAny.([]uint8)
					}

					var q8 []uint8
					q8 = *(*[]uint8)(unsafe.Pointer(&q)) // #nosec G103

					if len(q8) != len(v8) {
						return math.MaxFloat32, nil
					}

					var sum float32
					if h.quantizer != nil && h.sq8Ready.Load() {
						minV, maxV := h.quantizer.Params()
						scale := (maxV - minV) / 255.0
						for i, val := range q8 {
							// De-quantize: min + level * scale
							deqQ := minV + float32(val)*scale
							deqV := minV + float32(v8[i])*scale
							diff := deqQ - deqV
							sum += diff * diff
						}
					} else {
						// use optimized SIMD kernel
						qI8 := *(*[]int8)(unsafe.Pointer(&q8)) // #nosec G103
						vI8 := *(*[]int8)(unsafe.Pointer(&v8)) // #nosec G103
						return h.distFuncInt8(qI8, vI8)
					}
				}
				return math.MaxFloat32, nil
			}

		case []complex64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]complex64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float32
					for i, val := range q {
						diff := val - v[i]
						modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
						sum += modSq
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}

		case []complex128:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]complex128); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float64
					for i, val := range q {
						diff := val - v[i]
						modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
						sum += modSq
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}

		case []float64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]float64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					if h.distFuncF64 != nil {
						return h.distFuncF64(q, v)
					}
					// Fallback Euclidean
					var sum float64
					for i, val := range q {
						diff := val - v[i]
						sum += diff * diff
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}

		case []float16.Num:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]float16.Num); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					if h.distFuncF16 != nil {
						return h.distFuncF16(q, v)
					}
					// Fallback Euclidean
					var sum float32
					for i, val := range q {
						diff := val.Float32() - v[i].Float32()
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}

		case []uint32:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint32); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float32
					for i, val := range q {
						diff := float32(val) - float32(v[i])
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}

		case []int32:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int32); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt32(q, v), nil
				}
				return math.MaxFloat32, nil
			}

		case []int16:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int16); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt16(q, v), nil
				}
				return math.MaxFloat32, nil
			}

		case []uint16:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint16); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceUint16(q, v), nil
				}
				return math.MaxFloat32, nil
			}

		case []int64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt64(q, v), nil
				}
				return math.MaxFloat32, nil
			}

		case []uint64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id, maxGen)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceUint64(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			maxCommitted := meta.NodeCount
			if ctx != nil && ctx.MaxNodeCount > 0 {
				maxCommitted = ctx.MaxNodeCount
			}
			// Race Protection: If entry point is not yet committed, lock it to ensure SetVector is finished.
			if int64(entryPoint) >= maxCommitted {
				oldVer := data.LockNode(0, entryPoint)
				epDist, _ = distComputer(entryPoint)
				data.UnlockNode(0, entryPoint, oldVer)
			} else {
				epDist, _ = distComputer(entryPoint)
			}

		default:
			return nil, fmt.Errorf("searchLayer: unsupported query vector type %T", queryVec)
		}

		distBatchComputer = func(ids []uint32, dst []float32) ([]float32, error) {
			dists := make([]float32, len(ids))
			for i, id := range ids {
				d, err := distComputer(id)
				if err != nil {
					return nil, err
				}
				dists[i] = d
			}
			return dists, nil
		}
	}

	// 1. Reset Frontier for this layer
	ctx.candidates = ctx.candidates[:0]
	ctx.resultSet = ctx.resultSet[:0]
	ctx.visited.Clear()

	minHeap := (*MinCandidateHeapAdapter)(&ctx.candidates)
	resultSetAdapter := (*MaxCandidateHeapAdapter)(&ctx.resultSet)

	epCand := types.Candidate{ID: entryPoint, Dist: epDist}
	minHeap.PushCandidate(epCand)

	// Only add to result set if it passes filters and isn't deleted
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
	ctx.visited.Set(int(entryPoint)) // #nosec G115

	for minHeap.Len() > 0 {
		if err := goCtx.Err(); err != nil {
			return nil, err
		}

		// 0. Early termination: Visited nodes budget check
		if ctx.visitedNodesBudget > 0 && ctx.nodesVisitedCount >= ctx.visitedNodesBudget {
			metrics.HNSWEarlyTerminationTotal.WithLabelValues("budget_exceeded").Inc()
			break
		}

		// Pop closest candidate
		curr := minHeap.PopCandidate()
		ctx.nodesVisitedCount++

		maxCommitted := meta.NodeCount
		if ctx != nil && ctx.MaxNodeCount > 0 {
			maxCommitted = ctx.MaxNodeCount
		}

		// Active prefetch of the next best candidate on the heap to hide traversal latency
		if minHeap.Len() > 0 {
			nextBest := (*minHeap)[0]
			if int64(nextBest.ID) < maxCommitted {
				if comp, ok := computer.(DistanceComputer); ok {
					comp.Prefetch(nextBest.ID)
				}
			}
		}

		if len(ctx.resultSet) > 0 {
			furthest := ctx.resultSet[0]
			threshold := furthest.Dist
			if h.config.SQ8Enabled {
				// Be more lenient for SQ8 during searching as distance might be slightly noisy
				threshold *= 1.05
			}
			if curr.Dist > threshold && ctx.resultSet.Len() >= ef {
				// Optimization: if closest candidate is worse than worst result, stop
				break
			}
		}

		// Explore neighbors
		neighbors := h.GetNeighborsCombinedManual(data, layer, curr.ID, ctx.neighborBatch, ctx.MaxGeneration)

		prefetchLimit := cachedMMax
		if prefetchLimit > 64 {
			prefetchLimit = 64
		}
		if prefetchLimit < 16 {
			prefetchLimit = 16
		}

		for i := 0; i < len(neighbors) && i < int(prefetchLimit); i++ {
			nID := neighbors[i]
			if !ctx.AllowUncommitted && int64(nID) >= maxCommitted {
				continue
			}
			cID := int(nID) / types.ChunkSize  // #nosec G115
			cOff := int(nID) % types.ChunkSize // #nosec G115
			chunk := data.GetVectorsChunkWithGen(cID, maxGen)
			if chunk != nil {
				// Prefetch is handled below via simd.Prefetch
			}
			if len(data.VectorsSQ8) > cID {
				if sq8Chunk := data.GetVectorsSQ8ChunkWithGen(cID, maxGen); sq8Chunk != nil {
					paddedDims := (data.Dims + 63) & ^63
					start := cOff * paddedDims
					if start+data.Dims <= len(sq8Chunk) {
						if int64(nID) < maxCommitted {
							_ = sq8Chunk[start]
						}
					}
				}
			}
			if len(data.VectorsTQ) > cID {
		if tqChunk := data.GetVectorsTQChunkWithGen(cID, maxGen); tqChunk != nil {
				stride := PackedSize(int(data.Dims), data.TurboQuantBits)
				start := cOff * stride
					if start+stride <= len(tqChunk) {
						if int64(nID) < maxCommitted {
							_ = tqChunk[start]
						}
					}
				}
			}
		}

		// Optimization: Prefetch neighbor vectors to hide memory latency
		if comp, ok := computer.(DistanceComputer); ok {
			for i := range neighbors {
				if i+2 < len(neighbors) {
					nextN := neighbors[i+2]
					if int64(nextN) < maxCommitted {
						comp.Prefetch(nextN)
					}
				}
			}
		}

		const traversalBlockSize = 64

		if ctx.predicate != nil {
			// Vectorized Predicate Path
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
					for chunkStart := 0; chunkStart < len(validBatch); chunkStart += traversalBlockSize {
						chunkEnd := chunkStart + traversalBlockSize
						if chunkEnd > len(validBatch) {
							chunkEnd = len(validBatch)
						}
						block := validBatch[chunkStart:chunkEnd]

						if comp, ok := computer.(DistanceComputer); ok && chunkEnd < len(validBatch) {
							nextEnd := chunkEnd + traversalBlockSize
							if nextEnd > len(validBatch) {
								nextEnd = len(validBatch)
							}
							for _, nextN := range validBatch[chunkEnd:nextEnd] {
								if int64(nextN) < maxCommitted {
									comp.Prefetch(nextN)
								}
							}
						}

						ctx.distComputeCount += len(block)
						if cap(ctx.distsTemp) < len(block) {
							ctx.distsTemp = make([]float32, len(block))
						}
						if cplx, ok := computer.(*complex128Computer); ok && len(ctx.resultSet) > 0 {
							cplx.SetThreshold(ctx.resultSet[0].Dist)
						}
						dists, err := distBatchComputer(block, ctx.distsTemp[:len(block)])
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
			// Standard Path (No Predicate)
			batch := ctx.neighborBatch[:0]
			for _, n := range neighbors {
				if !ctx.AllowUncommitted && int64(n) >= maxCommitted {
					continue
				}
				if ctx.visited.IsSet(int(n)) { // #nosec G115
					continue
				}
				ctx.visited.Set(int(n)) // #nosec G115
				batch = append(batch, n)
			}

			if len(batch) > 0 {
				for chunkStart := 0; chunkStart < len(batch); chunkStart += traversalBlockSize {
					chunkEnd := chunkStart + traversalBlockSize
					if chunkEnd > len(batch) {
						chunkEnd = len(batch)
					}
					block := batch[chunkStart:chunkEnd]

					if comp, ok := computer.(DistanceComputer); ok && chunkEnd < len(batch) {
						nextEnd := chunkEnd + traversalBlockSize
						if nextEnd > len(batch) {
							nextEnd = len(batch)
						}
						for _, nextN := range batch[chunkEnd:nextEnd] {
							if int64(nextN) < maxCommitted {
								comp.Prefetch(nextN)
							}
						}
					}

					ctx.distComputeCount += len(block)
					if cap(ctx.distsTemp) < len(block) {
						ctx.distsTemp = make([]float32, len(block))
					}
					if cplx, ok := computer.(*complex128Computer); ok && len(ctx.resultSet) > 0 {
						cplx.SetThreshold(ctx.resultSet[0].Dist)
					}
					dists, err := distBatchComputer(block, ctx.distsTemp[:len(block)])
					if err == nil {
						for i, n := range block {
							d := dists[i]
							cand := types.Candidate{ID: n, Dist: d}

							// Add to candidates for traversal regardless of filter
							minHeap.PushCandidate(cand)

							// Only add to resultSet if it passes filters
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
										resultSetAdapter.PopCandidate() // Remove furthest
									}
								}
							} else {
								// Empty resultSet
								resultSetAdapter.PushCandidate(cand)
							}
						}
					}
				}
			}
		}
	}

	// Return results as sorted slice (ascending distance)
	// resultSet is a MaxHeap, so popping from it gives largest first.
	// We populate the result slice from end to beginning.
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
