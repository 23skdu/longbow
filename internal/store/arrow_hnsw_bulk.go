package store

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// BULK_INSERT_THRESHOLD defines the minimum batch size to trigger parallel bulk insert
const BULK_INSERT_THRESHOLD = 1000

// AddBatchBulk attempts to insert a batch of vectors in parallel using a bulk strategy.
// It assumes IDs, locations, and capacity have already been prepared/reserved.
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs [][]float32) error {
	start := time.Now()
	defer func() {
		h.metricBulkInsertDuration.Observe(time.Since(start).Seconds())
		h.metricBulkVectors.Add(float64(n))
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

	// Struct for batched reverse updates
	type ReverseUpdate struct {
		target uint32
		source uint32
		dist   float32
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

		// Batch active indices
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
						ef := h.efConstruction
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

		// 3b. Intra-Batch Matching (Blocked Matrix Multiplication)
		// For nodes inserting at this layer (lc <= node.level), they should see other inserting nodes.
		// We compute pairwise distances between all inserting nodes at this layer.
		// We add them to 'graphCandidates' if they are closer.

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
						// BQ encodes to slice, allocation unavoidable unless pooled or pre-extracted?
						// We can use activeNodes[].vec directly but encoding is per-query.
						// Actually activeNodes[].vec is float32.
						// Optimize: Pre-encode BQ/SQ8 for all inserting nodes *before* matrix mult?
						// Yes, but let's stick to blocked logic first. Optimization 10 covers mapped codebooks.
						// Current bulk prep handles pre-encoding into storage chunks but not memory cache.
						// We'll compute on fly or usage storage chunks? Memory cache (activeNodes) is float32.
					} else if useSQ8 {
						dims := int(h.dims.Load())
						qSQ8 = make([]byte, dims)
						tSQ8 = make([]byte, dims)
						scale = h.quantizer.L2Scale()
					}

					for row := iStart; row < iEnd; row++ {
						qIdx := insertingIndices[row]
						qVec := activeNodes[qIdx].vec

						if useBQ {
							qBQ = h.bqEncoder.Encode(qVec)
						} else if useSQ8 {
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

								// Symmetric check: if row > col, we might have computed it?
								// But we need full matrix for simple aggregation.
								// Double work is fine for parallel simplicity vs synchronization.

								tIdx := insertingIndices[col]
								tVec := activeNodes[tIdx].vec

								var dist float32
								if useBQ {
									tBQ = h.bqEncoder.Encode(tVec)
									dist = float32(h.bqEncoder.HammingDistance(qBQ, tBQ))
								} else if useSQ8 {
									h.quantizer.Encode(tVec, tSQ8)
									dist = float32(h.quantizer.Distance(qSQ8, tSQ8)) * scale
								} else {
									// Dense - call distance func directly to avoid overhead
									// h.distFunc is resolved in NewArrowHNSW
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

		// 3c. Linkage (Connections)
		// Multi-phase approach:
		// 1. Forward Connections (Parallel): Connect source -> neighbors. New nodes are local/owned, so safe.
		// 2. Reverse Connections (Batched): Collect all neighbor -> source edges, then apply in batches.

		// direct updatesByLock array to avoid map overhead
		updatesByLock := make([][]ReverseUpdate, ShardedLockCount)
		updatesMu := make([]sync.Mutex, ShardedLockCount)

		gLink, _ := errgroup.WithContext(ctx)
		gLink.SetLimit(runtime.GOMAXPROCS(0))

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

					// 1. Forward Connections
					fwdTargets := make([]uint32, len(neighbors))
					fwdDists := make([]float32, len(neighbors))
					for j, nb := range neighbors {
						fwdTargets[j] = nb.ID
						fwdDists[j] = nb.Dist
					}
					h.AddConnectionsBatch(ctxSearch, data, node.id, fwdTargets, fwdDists, lc, maxConn)

					// 2. Collect Reverse Connections
					for _, neighbor := range neighbors {
						targetID := neighbor.ID
						lockID := targetID % ShardedLockCount

						updatesMu[lockID].Lock()
						updatesByLock[lockID] = append(updatesByLock[lockID], ReverseUpdate{target: targetID, source: node.id, dist: neighbor.Dist})
						updatesMu[lockID].Unlock()
					}
				}
				return nil
			})
		}
		if err := gLink.Wait(); err != nil {
			return err
		}

		// 3. Process Reverse Connections (Batched)

		// Execute updates parallelized by LockID
		gRev, _ := errgroup.WithContext(ctx)
		gRev.SetLimit(runtime.GOMAXPROCS(0))

		for l := 0; l < ShardedLockCount; l++ {
			l := l
			updates := updatesByLock[l]
			if len(updates) == 0 {
				continue
			}

			gRev.Go(func() error {
				ctxSearch := h.searchPool.Get().(*ArrowSearchContext)
				ctxSearch.Reset()
				defer h.searchPool.Put(ctxSearch)

				maxConn := h.mMax
				if lc == 0 {
					maxConn = h.mMax0
				}

				// Sort by target to group batch updates
				// Since all targets hash to the same lock, we can process them sequentially safely.
				slices.SortFunc(updates, func(a, b ReverseUpdate) int {
					if a.target < b.target {
						return -1
					}
					if a.target > b.target {
						return 1
					}
					return 0
				})

				var curTarget uint32
				// Reuse buffers
				curSources := make([]uint32, 0, 16)
				curDists := make([]float32, 0, 16)

				flush := func() {
					if len(curSources) > 0 {
						h.AddConnectionsBatch(ctxSearch, data, curTarget, curSources, curDists, lc, maxConn)
						curSources = curSources[:0]
						curDists = curDists[:0]
					}
				}

				for _, up := range updates {
					if len(curSources) > 0 && up.target != curTarget {
						flush()
					}
					curTarget = up.target
					curSources = append(curSources, up.source)
					curDists = append(curDists, up.dist)
				}
				flush()

				return nil
			})
		}

		if err := gRev.Wait(); err != nil {
			return err
		}

		// End of Layer Loop
	}

	// 4. Update Global Stats
	h.nodeCount.Add(int64(n)) // Done already? No, we didn't increment earlier.
	// Wait, caller `AddBatch` logic in standard uses `Insert` which increments.
	// We implement `AddBatch`, so we are responsible.
	// Actually `AddBatch` calls `Grow` with size.
	// But `nodeCount` tracks *inserted* nodes.

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
