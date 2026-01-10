package store

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// BULK_INSERT_THRESHOLD defines the minimum batch size to trigger parallel bulk insert
const BULK_INSERT_THRESHOLD = 1000

// AddBatchBulk attempts to insert a batch of vectors in parallel using a bulk strategy.
// It assumes IDs, locations, and capacity have already been prepared/reserved.
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, rowIdxs []int) error {
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

				// Vector Retrieval
				// We can access 'data' directly since we ensured it's grown.
				v := h.mustGetVectorFromData(data, id) // Use internal helper
				if v == nil {
					return fmt.Errorf("vector missing for bulk insert ID %d", id)
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
					// Compute distances from i to all others
					// h.batchComputer uses optimized batch func
					if bc, ok := h.batchComputer.(*BatchDistanceComputer); ok {
						if _, err := bc.ComputeL2DistancesInto(flatVecs[i], flatVecs, resultsMatrix[i]); err != nil {
							return err
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

		gLink, _ := errgroup.WithContext(ctx)
		gLink.SetLimit(runtime.GOMAXPROCS(0))

		for i := 0; i < len(activeIndices); i += chunkSize {
			end := i + chunkSize
			if end > len(activeIndices) {
				end = len(activeIndices)
			}
			indices := activeIndices[i:end]

			gLink.Go(func() error {
				ctxSearch := h.searchPool.Get().(*ArrowSearchContext) // Need ctx for SelectNeighbors
				ctxSearch.Reset()
				defer h.searchPool.Put(ctxSearch)

				for _, idx := range indices {
					node := activeNodes[idx]
					// Only connect if insertion phase
					if lc > node.level {
						continue
					} // Descent phase only

					candidates := graphCandidates[idx]

					// Determine M
					m := h.m
					maxConn := h.mMax
					if lc == 0 {
						m = h.m * 2
						maxConn = h.mMax0
					}

					// Select Neighbors (Robust Prune)
					// Need to sort candidates first if we just appended?
					// Yes, "candidates are already sorted" comment in selectNeighbors.
					// So we must sort mixed candidates.
					slices.SortFunc(candidates, func(a, b Candidate) int {
						if a.Dist < b.Dist {
							return -1
						}
						if a.Dist > b.Dist {
							return 1
						}
						return 0
					})

					// Filter for M
					neighbors := h.selectNeighbors(ctxSearch, candidates, m, data)

					// Connect bidirectional
					// Since other nodes in batch are also connecting, does this double connect?
					// AddConnection checks for duplicates.
					id := node.id

					// Parallel additions
					for _, neighbor := range neighbors {
						h.AddConnection(ctxSearch, data, id, neighbor.ID, lc, maxConn)
						// Reverse connection
						// If neighbor is in batch, it will also try to add connection to us.
						// Duplicate checks handle this.
						h.AddConnection(ctxSearch, data, neighbor.ID, id, lc, maxConn)

						h.pruneIfNecessary(ctxSearch, data, neighbor.ID, lc, maxConn)
					}
				}
				return nil
			})
		}
		if err := gLink.Wait(); err != nil {
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
