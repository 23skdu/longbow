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
	gPrep.SetLimit(runtime.GOMAXPROCS(0))

	// Slice into chunks for workers
	chunkSize := (n + runtime.NumCPU() - 1) / runtime.NumCPU()
	if chunkSize < 100 {
		chunkSize = 100
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
				v := vecs[j]
				if v == nil {
					return fmt.Errorf("vector missing for bulk insert ID %d", id)
				}

				// Always ingest into hot storage for bulk path to ensure searchability during construction.
				// Always ingest into hot storage for bulk path to ensure searchability during construction.
				if err := data.SetVectorFromFloat32(id, v); err != nil {
					return err
				}

				// 2. SQ8 Ingestion
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
	// For nodes higher than current global maxL, they conceptually start at their level?
	// No, HNSW structure implies strictly hierarchical.
	// If a new node is higher than global max, it becomes the new EP for layers above old max.
	// But effectively we can just treat them as having "no neighbors" until they reach maxL?
	// Or we just link them to each other?
	// Simplification: Standard insertion searches from MaxL down to node.level + 1 to find ep.
	// Then inserts from node.level down to 0.

	// We track 'currentEp' for each active node.
	currentEps := make([]uint32, n)
	for i := range currentEps {
		currentEps[i] = ep
	}

	// Search Phase 1: Descent to element level (Finding entry point)
	// For each active node, we need to bring its 'currentEp' down to level+1.
	// We can do this in parallel batches.

	// But we can optimize: We process layers.
	// For layer L:
	//   For each active node:
	//     If L > node.level: We just SearchLayer(beam=1) to update currentEp.
	//     If L <= node.level: We SearchLayer(ef) -> SelectNeighbors -> Connect.

	// Issue: Concurrency. If multiple nodes are at same level, they should see each other?
	// Standard HNSW insert is sequential. Parallel insert usually locks or assumes disjointness.
	// "Bulk" strategy:
	// At layer L, all nodes that exist at this level (node.level >= L) participate.
	// They search against the *existing* graph.
	// AND they compute distances to *each other* (Intra-batch).

	for lc := topL; lc >= 0; lc-- {
		// Identify nodes active at this layer
		var activeIndices []int
		for i, node := range activeNodes {
			if node.level >= lc {
				activeIndices = append(activeIndices, i)
			}
		}

		if len(activeIndices) == 0 {
			continue // Should not happen if topL is correct
		}

		// 3a. Search against Graph (Parallel)
		// Update currentEps for all active nodes using the graph
		// If L > node.level, we search with ef=1 to find next EP.
		// If L <= node.level, we search with efConstruction to find candidates.

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
						cp := make([]Candidate, len(res))
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

		// 3b. Intra-Batch Matching (The "Blocked Matrix Multiply" part)
		// For nodes inserting at this layer (lc <= node.level), they should see other inserting nodes.
		// We compute pairwise distances between all inserting nodes at this layer.
		// We add them to 'graphCandidates' if they are closer.

		// Optimization: Only do this for L <= node.level
		var insertingIndices []int
		for _, idx := range activeIndices {
			if activeNodes[idx].level >= lc {
				insertingIndices = append(insertingIndices, idx)
			}
		}

		if len(insertingIndices) > 1 {
			// Compute pairwise distances
			// Naive O(N^2) or Blocked?
			// With N=1000, N^2 = 1M distances. Fast with SIMD.
			// Batched Distance Compute: For each inserting node (query), compute against all other inserting nodes (targets).

			// We can flatten vectors for batch call
			flatVecs := make([][]float32, len(insertingIndices))
			for i, idx := range insertingIndices {
				flatVecs[i] = activeNodes[idx].vec
			}

			// Parallelize the outer loop (Queries)
			gIntra, _ := errgroup.WithContext(ctx)
			gIntra.SetLimit(runtime.GOMAXPROCS(0))

			resultsMatrix := make([][]float32, len(insertingIndices))

			for i := 0; i < len(insertingIndices); i++ {
				i := i
				resultsMatrix[i] = make([]float32, len(insertingIndices))

				gIntra.Go(func() error {
					qVec := flatVecs[i]
					useSQ8 := h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load()
					useBQ := h.config.BQEnabled && h.bqEncoder != nil

					if useBQ {
						qBQ := h.bqEncoder.Encode(qVec)
						for j, tIdx := range insertingIndices {
							if i == j {
								continue
							}
							tBQ := h.bqEncoder.Encode(activeNodes[tIdx].vec)
							resultsMatrix[i][j] = float32(h.bqEncoder.HammingDistance(qBQ, tBQ))
						}
					} else if useSQ8 {
						qSQ8 := make([]byte, len(qVec))
						h.quantizer.Encode(qVec, qSQ8)
						tSQ8 := make([]byte, len(qVec))
						scale := h.quantizer.L2Scale()
						for j, tIdx := range insertingIndices {
							if i == j {
								continue
							}
							h.quantizer.Encode(activeNodes[tIdx].vec, tSQ8)
							resultsMatrix[i][j] = float32(h.quantizer.Distance(qSQ8, tSQ8)) * scale
						}
					} else {
						// Fallback to dense
						for j, tIdx := range insertingIndices {
							if i == j {
								continue
							}
							resultsMatrix[i][j] = h.distance(qVec, activeNodes[tIdx].id, data, nil) // Safe dense distance
						}
					}
					return nil
				})
			}
			if err := gIntra.Wait(); err != nil {
				return err
			}

			// Merge Intra-results into candidates
			// Accessing graphCandidates[activeIdx] requires synchronization?
			// No, insertingIndices are distinct.
			for i, activeIdx := range insertingIndices {
				// Current candidates from graph
				cands := graphCandidates[activeIdx]

				// Add intra-batch candidates
				for j, otherActiveIdx := range insertingIndices {
					if i == j {
						continue
					} // Self
					dist := resultsMatrix[i][j]

					// Simple merge strategy: append and rely on selectNeighbors to prune
					// Or maintain heap?
					// Appending is faster, SelectNeighbors handles sorting/pruning.
					otherID := activeNodes[otherActiveIdx].id
					cands = append(cands, Candidate{ID: otherID, Dist: dist})
				}
				graphCandidates[activeIdx] = cands
			}
		}

		// 3c. Linkage (Connections)
		// For all inserting nodes, select best M and connect.
		// Requires locking?
		// AddConnection uses Sharded Locks. Secure.

		// 3c. Linkage (Connections)
		// Multi-phase approach:
		// 1. Forward Connections (Parallel): Connect source -> neighbors. New nodes are local/owned, so safe.
		// 2. Reverse Connections (Batched): Collect all neighbor -> source edges, then apply in batches.

		// deferredUpdates maps TargetNode -> []Candidate (Source, Dist)
		// We use an array of maps to shard accumulation and reduce mutex contention during collection
		const numShards = 16
		deferredUpdates := make([]map[uint32][]Candidate, numShards)
		for i := 0; i < numShards; i++ {
			deferredUpdates[i] = make(map[uint32][]Candidate)
		}
		var deferredMu [numShards]sync.Mutex

		gLink, _ := errgroup.WithContext(ctx)
		gLink.SetLimit(runtime.GOMAXPROCS(0))

		for i := 0; i < len(activeIndices); i += chunkSize {
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
					// We are the owner of 'node', so we can write to it without lock (or with local lock).
					// But AddConnection acquires lock. Since it's a new node, it maps to a lock bucket.
					// Multiple new nodes might map to the same bucket.
					// Ideally we used AddConnectionsBatch here too for the source?
					// But AddConnection does "add one".
					// Since we have the whole list 'neighbors', we can just write them directly?
					// No, pruning logic is complex.
					// Let's use AddConnectionsBatch for FORWARD connections too!
					// source = node.id, targets = neighbors

					fwdTargets := make([]uint32, len(neighbors))
					fwdDists := make([]float32, len(neighbors))
					for j, nb := range neighbors {
						fwdTargets[j] = nb.ID
						fwdDists[j] = nb.Dist
					}
					h.AddConnectionsBatch(ctxSearch, data, node.id, fwdTargets, fwdDists, lc, maxConn)

					// 2. Collect Reverse Connections
					for _, neighbor := range neighbors {
						// We want to add edge: neighbor -> node
						targetID := neighbor.ID
						shard := targetID % uint32(numShards)

						deferredMu[shard].Lock()
						deferredUpdates[shard][targetID] = append(deferredUpdates[shard][targetID], Candidate{ID: node.id, Dist: neighbor.Dist})
						deferredMu[shard].Unlock()
					}
				}
				return nil
			})
		}
		if err := gLink.Wait(); err != nil {
			return err
		}

		// 3. Process Reverse Connections (Batched)
		// We iterate over the collected maps.
		// We can parallelize this processing too.
		// Note: We should ideally group by ShardedLock to maximize parallelism and avoid contention inside AddConnectionsBatch.
		// AddConnectionsBatch locks `target % ShardedLockCount`.

		// Flatten and Group by LockID
		updatesByLock := make([][]struct {
			target  uint32
			sources []uint32
			dists   []float32
		}, ShardedLockCount)

		for s := 0; s < numShards; s++ {
			for target, sources := range deferredUpdates[s] {
				lockID := target % ShardedLockCount

				srcs := make([]uint32, len(sources))
				dists := make([]float32, len(sources))
				for j, c := range sources {
					srcs[j] = c.ID
					dists[j] = c.Dist
				}

				updatesByLock[lockID] = append(updatesByLock[lockID], struct {
					target  uint32
					sources []uint32
					dists   []float32
				}{target, srcs, dists})
			}
		}

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

				for _, up := range updates {
					// AddConnectionsBatch acquires the lock.
					// Since we grouped by lock, we strictly serialize updates to the same lock in this thread.
					// This avoids invalid contention, but means we acquire/release lock many times.
					// Optimization: We COULD acquire lock once for all updates in this bucket?
					// But AddConnectionsBatch encapsulates locking.
					// Given we are singly threaded per lock bucket, the contention is 0 (except from readers).
					h.AddConnectionsBatch(ctxSearch, data, up.target, up.sources, up.dists, lc, maxConn)
				}
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
