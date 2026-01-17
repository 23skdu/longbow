package store

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/23skdu/longbow/internal/metrics"
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
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs [][]float32) error {
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
		vec   []float32 // Cache for speed, careful with memory if batch is huge
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
				// Optimization: Pre-fetch vector into L1/L2 cache
				v := vecs[j]
				if v == nil {
					return fmt.Errorf("vector missing for bulk insert ID %d", id)
				}

				// Always ingest into hot storage for bulk path to ensure searchability during construction.
				if err := data.SetVectorFromFloat32(id, v); err != nil {
					return err
				}

				// 2. SQ8 Ingestion
				// Parallelize quantization - significant speedup for large batches
				if h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
					sq8Chunk := data.GetVectorsSQ8Chunk(cID)
					if sq8Chunk != nil {
						sq8Stride := (dims + 63) & ^63
						start := int(cOff) * sq8Stride
						dest := sq8Chunk[start : start+dims]
						h.quantizer.Encode(v, dest)
					}
				}

				// 3. BQ Ingestion
				if h.config.BQEnabled && h.bqEncoder != nil {
					bqChunk := data.GetVectorsBQChunk(cID)
					if bqChunk != nil {
						code := h.bqEncoder.Encode(v)
						numWords := h.bqEncoder.CodeSize()
						dest := bqChunk[int(cOff)*numWords : (int(cOff)+1)*numWords]
						copy(dest, code)
					}
				}

				// 4. PQ Ingestion
				if h.config.PQEnabled && h.pqEncoder != nil {
					pqChunk := data.GetVectorsPQChunk(cID)
					if pqChunk != nil {
						code, err := h.pqEncoder.Encode(v)
						if err == nil {
							pqM := h.config.PQM
							dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
							copy(dest, code)
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
						res := h.searchLayerForInsert(ctxSearch, node.vec, currEp, 1, lc, data)
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

						res := h.searchLayerForInsert(ctxSearch, node.vec, currEp, ef, lc, data)
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
						qVec := activeNodes[qIdx].vec

						switch {
						case useBQ:
							qBQ = h.bqEncoder.Encode(qVec)
						case useSQ8:
							h.quantizer.Encode(qVec, qSQ8)
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
								tVec := activeNodes[tIdx].vec

								var dist float32
								switch {
								case useBQ:
									tBQ = h.bqEncoder.Encode(tVec)
									dist = float32(h.bqEncoder.HammingDistance(qBQ, tBQ))
								case useSQ8:
									h.quantizer.Encode(tVec, tSQ8)
									dist = float32(h.quantizer.Distance(qSQ8, tSQ8)) * scale
								default:
									dist = h.distFunc(qVec, tVec)
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
							// If timed out, drop reverse connection?
							// Or just retry indefinitely? Indefinitely is safer for correctness.
							// In practice 4096 buffer should be plenty drainable.
							// Force push? No method.
							// Loop forever.
							for !rings[lockID].Push(update) {
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
	// ... (Same as before) ...
	h.nodeCount.Add(int64(n))

	// Update Max Level / Entry Point atomically
	h.initMu.Lock()
	currentMax := int(h.maxLevel.Load())
	if batchMaxLevel > currentMax {
		h.maxLevel.Store(int32(batchMaxLevel))
		h.entryPoint.Store(batchEpCandidate)
	}
	h.initMu.Unlock()

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
