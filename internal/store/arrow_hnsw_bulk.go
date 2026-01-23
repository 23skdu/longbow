package store

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// BULK_INSERT_THRESHOLD defines the minimum batch size to trigger parallel bulk insert
const BULK_INSERT_THRESHOLD = 1000

// reverseUpdate tracks a reverse connection to be added.
type reverseUpdate struct {
	target uint32
	source uint32
	dist   float32
}

// AddBatchBulk attempts to insert a batch of vectors in parallel using a bulk strategy.
// It assumes IDs, locations, and capacity have already been prepared/reserved.
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs any) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		h.metricBulkInsertDuration.Observe(duration)
		h.metricBulkVectors.Add(float64(n))

		// Enhanced Observability
		typeStr := h.config.DataType.String()
		dims := int(h.dims.Load())
		metrics.HNSWBulkInsertLatencyByType.WithLabelValues(typeStr).Observe(duration)
		metrics.HNSWBulkInsertLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)
	}()

	// 1. Prepare Active Set
	// We need to track the active nodes we are inserting.
	// Since we reserved contiguous IDs, they are just [startID, startID + n).

	type activeNode struct {
		id    uint32
		level int
		vec   any // Can be []float32, []float16.Num, etc.
	}

	activeNodes := make([]activeNode, n)

	// Pre-load vectors and generate levels (Parallel)
	data := h.data.Load()

	// Use errgroup for parallel prep
	gPrep, ctxPrep := errgroup.WithContext(ctx)
	// Optimization: Limit concurrency to GOMAXPROCS * 2 to hide I/O latency (if any)
	// For pure CPU tasks like this, GOMAXPROCS is ideal.
	gPrep.SetLimit(runtime.GOMAXPROCS(0))

	// Slice into chunks for workers to amortize goroutine overhead
	chunkSize := (n + runtime.NumCPU() - 1) / runtime.NumCPU()
	if chunkSize < 256 {
		chunkSize = 256 // Minimum chunk size to justify overhead
	}

	for i := 0; i < n; i += chunkSize {
		end := i + chunkSize
		if end > n {
			end = n
		}
		offset := i

		gPrep.Go(func() error {
			if ctxPrep.Err() != nil {
				return ctxPrep.Err()
			}
			for j := offset; j < end; j++ {
				id := startID + uint32(j)
				// Ensure chunks allocated (Versions etc needed for search)
				cID := chunkID(id)
				cOff := chunkOffset(id)
				dims := int(h.dims.Load())
				h.ensureChunk(data, cID, cOff, dims)

				// Level generation
				level := h.generateLevel()

				// Vector Ingestion (Zero-Copy from passed batch)
				var v any
				// Type switch to extract vector from generic batch
				switch vs := vecs.(type) {
				case [][]float32:
					v = vs[j]
				case [][]float16.Num:
					v = vs[j]
				case [][]int8:
					v = vs[j]
				case [][]float64:
					v = vs[j]
				case [][]complex64:
					v = vs[j]
				case [][]complex128:
					v = vs[j]
				default:
					return fmt.Errorf("unsupported vector type in bulk insert: %T", vecs)
				}

				// Basic validation
				if v == nil {
					return fmt.Errorf("vector missing for bulk insert ID %d (nil slice)", id)
				}

				// Validate dimensions based on type
				// Since we are inside generic handling, we use reflection or just assume the type switch gave us a valid slice.
				// We can check length here.
				var vLen int
				switch vec := v.(type) {
				case []float32:
					vLen = len(vec)
				case []float16.Num:
					vLen = len(vec)
				case []int8:
					vLen = len(vec)
				case []float64:
					vLen = len(vec)
				case []complex64:
					vLen = len(vec)
				case []complex128:
					vLen = len(vec)
				}

				if vLen != dims {
					metrics.BulkInsertDimensionErrorsTotal.Inc()
					return NewVectorDimensionMismatchError(int(id), dims, vLen)
				}

				// Always ingest into hot storage using method that handles all types
				if err := data.SetVector(id, v); err != nil {
					return err
				}

				// 2. SQ8 Ingestion
				// Parallelize quantization - significant speedup for large batches
				if h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
					sq8Chunk := data.GetVectorsSQ8Chunk(cID)
					if sq8Chunk != nil {
						// SQ8 Quantizer currently only supports []float32
						// To make it generic, use NewGenericSQ8Quantizer wrapper around SQ8Encoder
						// Type conversion from float16/int8 to float32 would use helpers in generic_quantizer.go
						// For now, only support float32 for SQ8 optimization path here
						if vf32, ok := v.([]float32); ok {
							sq8Stride := (dims + 63) & ^63
							start := int(cOff) * sq8Stride
							dest := sq8Chunk[start : start+dims]
							h.quantizer.Encode(vf32, dest)
						}
					}
				}

				// 3. BQ Ingestion
				if h.config.BQEnabled && h.bqEncoder != nil {
					bqChunk := data.GetVectorsBQChunk(cID)
					if bqChunk != nil {
						// BQ encoder supports generic input?
						// Currently BQEncoder.Encode takes []float32 (implied) in original code
						// Need to check BQ signature. Assuming it takes float32 for now.
						if vf32, ok := v.([]float32); ok {
							code := h.bqEncoder.Encode(vf32)
							numWords := h.bqEncoder.CodeSize()
							dest := bqChunk[int(cOff)*numWords : (int(cOff)+1)*numWords]
							copy(dest, code)
						}
					}
				}

				// 4. PQ Ingestion
				if h.config.PQEnabled && h.pqEncoder != nil {
					pqChunk := data.GetVectorsPQChunk(cID)
					if pqChunk != nil {
						// PQEncoder.Encode takes []float32
						if vf32, ok := v.([]float32); ok {
							code, err := h.pqEncoder.Encode(vf32)
							if err == nil {
								pqM := h.config.PQM
								dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
								copy(dest, code)
							}
						}
					}
				}

				activeNodes[j] = activeNode{
					id:    id,
					level: level,
					vec:   v,
				}

				// Init levels chunk if needed (done in Insert usually, here we do it)
				// We assume EnsureChunk called by caller or Grow.
				// But we need to set the level.
				levelsChunk := data.GetLevelsChunk(cID)
				if levelsChunk != nil {
					levelsChunk[cOff] = uint8(level)
				}
			}
			return nil
		})
	}

	if err := gPrep.Wait(); err != nil {

		return err
	}

	// 2. Global Entry Point
	// We start search from the current global entry point.
	// We need to update entry point potentially at the end.
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Determine max level in this batch to update global max later
	batchMaxLevel := -1
	batchEpCandidate := uint32(0) // ID of node with max level in batch

	for _, node := range activeNodes {
		if node.level > batchMaxLevel {
			batchMaxLevel = node.level
			batchEpCandidate = node.id
		}
	}

	// 3. Layer-by-Layer Insertion (Top Down)
	// We iterate max(maxL, batchMaxLevel) down to 0.

	topL := maxL
	if batchMaxLevel > topL {
		topL = batchMaxLevel
	}

	// Current entry points for all active nodes. Initially global EP.
	currentEps := make([]uint32, n)
	for i := range currentEps {
		currentEps[i] = ep
	}

	// Deferred Connection Pipeline (Phase 15 Implementation)
	// ----------------------------------------------------
	// We replace the phased approach (Link -> Wait -> Reverse) with a fully parallel pipeline.
	// Producers (gLink) compute neighbors and add Forward connections immediately.
	// They also Push Reverse connections to Sharded Lock-Free Ring Buffers.
	// Consumers (gRev) drain the rings and apply Reverse connections concurrently.

	// Constants
	numShards := ShardedLockCount
	ringSize := uint64(4096) // Capacity per shard ring

	// Initialize Rings
	rings := make([]*LockFreeRingBuffer[reverseUpdate], numShards)
	for i := 0; i < numShards; i++ {
		rings[i] = NewLockFreeRingBuffer[reverseUpdate](ringSize)
	}

	for lc := topL; lc >= 0; lc-- {

		// Identify nodes active at this layer
		activeIndices := make([]int, 0, n)
		for i, node := range activeNodes {
			if node.level >= lc {
				activeIndices = append(activeIndices, i)
			}
		}

		if len(activeIndices) == 0 {
			continue // Should not happen if topL is correct
		}

		// 3a. Search against Graph (Parallel)
		// ... (Same logic as before) ...

		// Outputs
		graphCandidates := make([][]Candidate, n) // Only for activeIndices

		gLayer, _ := errgroup.WithContext(ctx)
		gLayer.SetLimit(runtime.GOMAXPROCS(0))

		// Batch active activeIndices
		chunkSize = (len(activeIndices) + runtime.NumCPU() - 1) / runtime.NumCPU()
		if chunkSize < 50 {
			chunkSize = 50
		}

		for i := 0; i < len(activeIndices); i += chunkSize {
			// Capture range
			end := i + chunkSize
			if end > len(activeIndices) {
				end = len(activeIndices)
			}
			indices := activeIndices[i:end]

			gLayer.Go(func() error {
				// Thread-local context
				ctxSearch := h.searchPool.Get().(*ArrowSearchContext)
				ctxSearch.Reset()
				defer h.searchPool.Put(ctxSearch)

				for _, idx := range indices {
					node := activeNodes[idx]
					currEp := currentEps[idx]

					if lc > node.level {
						// Descent phase: ef=1
						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, 1, lc, data)
						if err != nil {
							return err
						}
						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					} else {
						// Insertion phase
						// Use adaptive EF
						ef := int(h.efConstruction.Load())
						if h.config.AdaptiveEf {
							ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
						}

						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, ef, lc, data)
						if err != nil {
							return err
						}
						// Store candidates (make copy as ctx is reused)
						// searchLayerForInsert returns slice from ctx.scratchResults
						cp := make([]Candidate, len(res), len(res)+16) // Pre-alloc extra cap for intra-batch
						copy(cp, res)
						graphCandidates[idx] = cp

						// Update ep for next layer
						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					}
				}
				return nil
			})
		}
		if err := gLayer.Wait(); err != nil {
			return err
		}

		// 3b. Intra-Batch Matching (Blocked Matrix Multiplication)
		// ... (Same logic, relying on graphCandidates pre-alloc) ...

		// Optimization: Only do this for L <= node.level
		insertingIndices := make([]int, 0, len(activeIndices))
		for _, idx := range activeIndices {
			if activeNodes[idx].level >= lc {
				insertingIndices = append(insertingIndices, idx)
			}
		}

		numNew := len(insertingIndices)
		if numNew > 1 {
			// Pre-allocate results matrix (Upper Triangular is sufficient, but full matrix simplifies parallel writes)
			// Flattened: row i is at resultsMatrix[i*numNew : (i+1)*numNew]
			// Or simple [][]float32 since we parallelize by row anyway?
			// [][]float32 is safer for concurrent writes to different rows.
			resultsMatrix := make([][]float32, numNew)
			for i := 0; i < numNew; i++ {
				resultsMatrix[i] = make([]float32, numNew)
			}

			// Blocked Matrix Multiplication
			// Tile size for cache locality
			const tileSize = 64

			gIntra, _ := errgroup.WithContext(ctx)
			gIntra.SetLimit(runtime.GOMAXPROCS(0))

			// Iterate over tiles
			for i := 0; i < numNew; i += tileSize {
				// Capture outer loop variable
				iStart := i

				gIntra.Go(func() error {

					iEnd := iStart + tileSize
					if iEnd > numNew {
						iEnd = numNew
					}

					// Prepare Quantizer helpers once per tile/worker
					useSQ8 := h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load()
					useBQ := h.config.BQEnabled && h.bqEncoder != nil
					var qSQ8, tSQ8 []byte
					var qBQ, tBQ []uint64
					var scale float32

					// Pre-allocation for reuse within this tile's worker
					if useBQ {
						// ...
					} else if useSQ8 {
						dims := int(h.dims.Load())
						qSQ8 = make([]byte, dims)
						tSQ8 = make([]byte, dims)
						scale = h.quantizer.L2Scale()
					}

					for row := iStart; row < iEnd; row++ {
						qIdx := insertingIndices[row]
						qVec := activeNodes[qIdx].vec // generic

						// Pre-encode Q for optimization if supported (float32 only currently)
						if v, ok := qVec.([]float32); ok {
							switch {
							case useBQ:
								qBQ = h.bqEncoder.Encode(v)
							case useSQ8:
								h.quantizer.Encode(v, qSQ8)
							}
						}

						// Inner loop (Tiles)
						for j := 0; j < numNew; j += tileSize {
							jEnd := j + tileSize
							if jEnd > numNew {
								jEnd = numNew
							}

							for col := j; col < jEnd; col++ {
								if row == col {
									continue
								}

								tIdx := insertingIndices[col]
								tVec := activeNodes[tIdx].vec // generic

								var dist float32
								// Try Optimized Path first (SQ8/BQ - assume float32 input for now)
								// If optimizations enabled, we assume vectors are float32 or compatible because
								// we only set useBQ/useSQ8 if config enabled it, AND we usually enforce types.
								// However, safely checking type is better.

								done := false
								if qF32, ok := qVec.([]float32); ok {
									if tF32, ok := tVec.([]float32); ok {
										switch {
										case useBQ:
											tBQ = h.bqEncoder.Encode(tF32)
											dist = float32(h.bqEncoder.HammingDistance(qBQ, tBQ))
											done = true
										case useSQ8:
											h.quantizer.Encode(tF32, tSQ8)
											d, err := h.quantizer.Distance(qSQ8, tSQ8)
											if err != nil {
												dist = math.MaxFloat32
											} else {
												dist = float32(d) * scale
											}
											done = true
										default:
											d, err := h.distFunc(qF32, tF32)
											if err == nil {
												dist = d
												done = true
											}
										}
									}
								}

								if !done {
									// Fallback dispatch for other types or if optimization skipped
									switch q := qVec.(type) {
									case []float16.Num:
										if t, ok := tVec.([]float16.Num); ok {
											d, err := h.distFuncF16(q, t)
											if err == nil {
												dist = d
												done = true
											}
										}
									case []float64:
										if t, ok := tVec.([]float64); ok {
											d, err := h.distFuncF64(q, t)
											if err == nil {
												dist = float32(d)
												done = true
											}
										}
									case []complex64:
										if t, ok := tVec.([]complex64); ok {
											d, err := h.distFuncC64(q, t)
											if err == nil {
												dist = d
												done = true
											}
										}
									case []complex128:
										if t, ok := tVec.([]complex128); ok {
											d, err := h.distFuncC128(q, t)
											if err == nil {
												dist = d
												done = true
											}
										}
									case []int8:
										if t, ok := tVec.([]int8); ok {
											// Fallback: Convert to float32 on the fly and compute Euclidean
											var sum float32
											for k := 0; k < len(q) && k < len(t); k++ {
												diff := float32(q[k]) - float32(t[k])
												sum += diff * diff
											}
											dist = float32(math.Sqrt(float64(sum)))
											done = true
										}
									default:
										dist = math.MaxFloat32
									}
								}
								resultsMatrix[row][col] = dist
							}
						}
					}
					return nil
				})
			}

			if err := gIntra.Wait(); err != nil {
				return err
			}

			// Merge Intra-results into candidates
			for i, activeIdx := range insertingIndices {
				cands := graphCandidates[activeIdx] // Copy

				for j, otherActiveIdx := range insertingIndices {
					if i == j {
						continue
					}

					dist := resultsMatrix[i][j]
					otherID := activeNodes[otherActiveIdx].id
					cands = append(cands, Candidate{ID: otherID, Dist: dist})
				}
				graphCandidates[activeIdx] = cands
			}
		}

		// 3c. Linkage (Pipeline)
		// Launch Consumers First
		producersDone := atomic.Bool{}
		producersDone.Store(false)

		gRev, _ := errgroup.WithContext(ctx)
		// Consumers: 1 per CPU core, processing shards of rings
		numConsumers := runtime.NumCPU()
		gRev.SetLimit(numConsumers)

		for workerID := 0; workerID < numConsumers; workerID++ {
			workerID := workerID
			gRev.Go(func() error {
				// Each worker handles slices: i, i+numConsumers, i+2*numConsumers...
				// Efficient sharding

				ctxSearch := h.searchPool.Get().(*ArrowSearchContext)
				ctxSearch.Reset()
				defer h.searchPool.Put(ctxSearch)

				maxConn := h.mMax
				if lc == 0 {
					maxConn = h.mMax0
				}

				// Reusable batch buffers
				const batchSize = 64
				var curTarget uint32
				curSources := make([]uint32, 0, batchSize)
				curDists := make([]float32, 0, batchSize)

				flush := func() {
					if len(curSources) > 0 {
						h.AddConnectionsBatch(ctxSearch, data, curTarget, curSources, curDists, lc, maxConn)
						curSources = curSources[:0]
						curDists = curDists[:0]
					}
				}

				// Loop until completed
				defer func() {
					if r := recover(); r != nil {
						h.config.Logger.Error().Interface("panic", r).Msg("Panic in HNSW bulk reverse writer")
					}
				}()

				for {
					anyWork := false

					// Iterate assigned rings
					for rIdx := workerID; rIdx < numShards; rIdx += numConsumers {
						ring := rings[rIdx]

						// Drain ring completely
						for {
							up, ok := ring.Pop()
							if !ok {
								break
							}
							anyWork = true

							// Batch logic
							if len(curSources) > 0 {
								if up.target != curTarget || len(curSources) >= batchSize {
									flush()
								}
							}
							curTarget = up.target
							curSources = append(curSources, up.source)
							curDists = append(curDists, up.dist)
						}
					}

					flush() // Ensure flushed after drain cycle

					if !anyWork {
						if producersDone.Load() {
							// Check one last time to ensure no race where producer pushed just before setting done
							// Actually atomic store gives happens-before?
							// A simple double check is safe.
							stillEmpty := true
							for rIdx := workerID; rIdx < numShards; rIdx += numConsumers {
								if rings[rIdx].Len() > 0 {
									stillEmpty = false
									break
								}
							}
							if stillEmpty {
								return nil
							}
						} else {
							runtime.Gosched()
						}
					}
				}
			})
		}

		// Producers (gLink)
		gLink, _ := errgroup.WithContext(ctx)
		gLink.SetLimit(runtime.GOMAXPROCS(0)) /**/

		for i := 0; i < len(activeIndices); i += chunkSize {
			// Capture range
			end := i + chunkSize
			if end > len(activeIndices) {
				end = len(activeIndices)
			}
			indices := activeIndices[i:end]

			gLink.Go(func() error {

				ctxSearch := h.searchPool.Get().(*ArrowSearchContext)
				ctxSearch.Reset()
				defer h.searchPool.Put(ctxSearch)

				for _, idx := range indices {
					node := activeNodes[idx]
					// Only connect if insertion phase
					if lc > node.level {
						continue
					}

					candidates := graphCandidates[idx]

					// Determine M
					m := h.m
					maxConn := h.mMax
					if lc == 0 {
						m = h.m * 2
						maxConn = h.mMax0
					}
					if m > maxConn {
						m = maxConn
					}

					// Select Neighbors (Robust Prune)
					slices.SortFunc(candidates, func(a, b Candidate) int {
						if a.Dist < b.Dist {
							return -1
						}
						if a.Dist > b.Dist {
							return 1
						}
						return 0
					})

					neighbors := h.selectNeighbors(ctxSearch, candidates, m, data)

					// 1. Forward Connections (Immediate)
					fwdTargets := make([]uint32, len(neighbors))
					fwdDists := make([]float32, len(neighbors))
					for j, nb := range neighbors {
						fwdTargets[j] = nb.ID
						fwdDists[j] = nb.Dist
					}
					h.AddConnectionsBatch(ctxSearch, data, node.id, fwdTargets, fwdDists, lc, maxConn)

					// 2. Reverse Connections (Push to Ring)
					for _, neighbor := range neighbors {
						targetID := neighbor.ID
						lockID := targetID % uint32(numShards) // Shard selection

						update := reverseUpdate{target: targetID, source: node.id, dist: neighbor.Dist}

						// Push Blocking to handle backpressure
						if !rings[lockID].PushBlocking(update, 100*time.Millisecond) {
							// Check context before spinning
							if ctx.Err() != nil {
								return ctx.Err()
							}

							// Force push loop with context check to avoid infinite hang if consumers fail
							for !rings[lockID].Push(update) {
								if ctx.Err() != nil {
									return ctx.Err()
								}
								runtime.Gosched()
							}
						}
					}
				}
				return nil
			})
		}

		// Wait for producers
		if err := gLink.Wait(); err != nil {
			return err
		}

		// Signal consumers
		producersDone.Store(true)

		// Wait for consumers
		if err := gRev.Wait(); err != nil {
			return err
		}

		// End of Layer Loop
	}

	// 4. Update Global Stats
	// Update Max Level / Entry Point atomically
	h.initMu.Lock()
	currentMax := int(h.maxLevel.Load())
	if batchMaxLevel > currentMax {
		h.maxLevel.Store(int32(batchMaxLevel))
		h.entryPoint.Store(batchEpCandidate)
	}
	h.initMu.Unlock()

	h.nodeCount.Add(int64(n))

	return nil
}

// pruneIfNecessary checks if a node's connections exceed maxConn and prunes them
func (h *ArrowHNSW) pruneIfNecessary(ctx *ArrowSearchContext, data *GraphData, id uint32, layer, maxConn int) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	countsChunk := data.GetCountsChunk(layer, cID)
	if countsChunk == nil {
		return
	}

	count := atomic.LoadInt32(&countsChunk[cOff])
	if int(count) > maxConn {
		h.PruneConnections(ctx, data, id, maxConn, layer)
	}
}
