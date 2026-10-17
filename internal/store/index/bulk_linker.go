package index

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	types "github.com/23skdu/longbow/internal/store/types"
)

// ChainLinksPerNode is the number of degree slots the bulk linker reserves for
// the insertion-order chain links (one to the predecessor, one from the
// successor) it adds to every node at layer 0.
const ChainLinksPerNode = 2

// bulkChainLinksEnabled reports whether the bulk linker adds the
// insertion-order chain edges described above.
//
// The edge is a fix for bulk-inserted nodes ending up with in-degree zero: a
// fresh node's only inbound links are reverse links it hands to its pre-batch
// neighbours, and those are pruned away as soon as such a neighbour is already
// at its connection limit. Linking to the insertion-order predecessor always
// works on collinear or otherwise sorted input, where node i and node i-1 really
// are nearest neighbours, which is the case the regression test
// (TestBulkInsert_CollinearGraphStaysConnected) exercises.
//
// On unsorted input the same edge is a long-range shortcut between arbitrary
// nodes, so every node gets an arbitrary edge in its layer-0 neighbourhood.
// Set LONGBOW_HNSW_BULK_CHAIN_LINKS=0 to turn it off, which trades the
// stranding guarantee for a graph whose layer 0 is selected purely by distance.
var bulkChainLinksEnabled = func() bool {
	if v := os.Getenv("LONGBOW_HNSW_BULK_CHAIN_LINKS"); v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "0", "false", "no", "off":
			return false
		case "1", "true", "yes", "on":
			return true
		}
	}
	return true
}()

// linkOp is one cross-node edge waiting for P2: the list it points at, the
// node that will be added to it, and the distance to record.
type linkOp struct {
	target  uint32
	src     uint32
	dist    float32
	maxConn int32
}

// bootstrapSequential establishes a stable graph hierarchy by sequentially inserting
// the first seedCount nodes.
func (h *ArrowHNSW) bootstrapSequential(
	ctx context.Context,
	startID uint32,
	n, seedCount int,
	activeNodes []activeNode,
	data *types.GraphData,
) (*types.GraphData, uint32, int32, error) {
	// Verify first and last vector
	vStart, _ := data.GetVector(startID)
	if vStart == nil {
		return nil, 0, 0, fmt.Errorf("failed to retrieve first vector %d after fill", startID)
	}
	vEnd, _ := data.GetVector(startID + uint32(n) - 1)
	if vEnd == nil {
		return nil, 0, 0, fmt.Errorf("failed to retrieve last vector %d after fill", startID+uint32(n)-1)
	}

	var bootstrapEp uint32 = math.MaxUint32
	var bootstrapMaxL int32 = -1
	for i := 0; i < seedCount; i++ {
		node := activeNodes[i]
		var err error
		data, err = h.insertInternal(node.id, node.vec, node.level, true, data)
		if err != nil {
			return nil, 0, 0, err
		}
		if int32(node.level) > bootstrapMaxL || bootstrapEp == math.MaxUint32 { // #nosec G115
			bootstrapMaxL = int32(node.level) // #nosec G115
			bootstrapEp = node.id
			h.updateMetadataIfHigher(bootstrapEp, bootstrapMaxL)
		}
	}
	h.compareAndSwapData(h.data.Load(), data.Clone())
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}

	if n <= seedCount {
		// Update metadata registry with new node count and entry point BEFORE returning
		h.updateMetadata(func(meta *HNSWMetadata) {
			if int32(bootstrapMaxL) > meta.MaxLevel || meta.EntryPoint == math.MaxUint32 {
				meta.MaxLevel = int32(bootstrapMaxL)
				meta.EntryPoint = bootstrapEp
			}
			if int64(startID)+int64(n) > meta.NodeCount {
				meta.NodeCount = int64(startID) + int64(n)
			}
		})

		// Even for small batches, we should register nodes in pools
		h.initMu.Lock()
		for i := 0; i < n; i++ {
			node := activeNodes[i]
			if node.level > 0 && node.level < len(h.entryPointPools) {
				h.entryPointPools[node.level].Insert(node.id)
				if h.multiCentroids != nil && node.level >= 2 {
					h.multiCentroids.AddCentroid(node.id)
				}
			}
		}
		h.initMu.Unlock()
	}

	return data, bootstrapEp, bootstrapMaxL, nil
}

// linkRemainingNodes executes the parallel layer-by-layer sub-batch linkage phase for remaining nodes.
func (h *ArrowHNSW) linkRemainingNodes(
	ctx context.Context,
	startID uint32,
	n, seedCount int,
	activeNodes []activeNode,
	data *types.GraphData,
) (*types.GraphData, int, uint32, error) {
	pool := GetSharedPool()

	// Advance nodeCount so search kernels see the new data
	finalID := int64(startID + uint32(n))
	h.commitMu.Lock()
	if h.nodeCount.Load() < finalID {
		h.nodeCount.Store(finalID)
		h.commitCond.Broadcast()
	}
	h.commitMu.Unlock()

	// Refresh data snapshot after bootstrap loop as InsertWithVector updated the global state.
	freshPublished := h.data.Load()
	if freshPublished != nil {
		freshPublished.AcquireReader()
		data = freshPublished.Clone()
		freshPublished.ReleaseReader()
	} else {
		data = nil
	}

	// Shift to remaining nodes for parallel linkage
	remainingNodes := activeNodes[seedCount:]
	numRemaining := len(remainingNodes)

	// Refresh metadata after bootstrap
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Determine max level in remaining batch
	batchMaxLevel := -1
	batchEpCandidate := uint32(0)
	for _, node := range remainingNodes {
		if node.level > batchMaxLevel {
			batchMaxLevel = node.level
			batchEpCandidate = node.id
		}
	}

	topL := maxL
	if batchMaxLevel > topL {
		topL = batchMaxLevel
	}

	// Populate ephemeral TQScratchArena with reconstructed float32 vectors for fast construction.
	tq := h.tq()
	if (h.config.DataType == types.VectorTypeTQ || h.config.TurboQuantEnabled) && tq != nil && tq.compute != nil {
		arena := h.EnableTQScratchArena()
		numNodes := len(activeNodes)
		chunkSize := (numNodes + runtime.NumCPU() - 1) / runtime.NumCPU()
		if chunkSize < 64 {
			chunkSize = 64
		}
		pool.ParallelFor(numNodes, chunkSize, func(start, end int) {
			for idx := start; idx < end; idx++ {
				node := activeNodes[idx]
				tqCode, err := tq.compute.getTQBytes(node.id, nil, math.MaxUint64)
				if err == nil && len(tqCode) > 0 {
					_, _ = arena.GetOrDecode(node.id, func(_ uint32, dst []float32) error {
						return tq.compute.encoder.DecodeInto(tqCode, dst)
					})
				}
			}
		})
	}

	// Current entry points for remaining active nodes. Initially global EP.
	currentEps := make([]uint32, numRemaining)
	for i := range currentEps {
		currentEps[i] = ep
	}

	// Counting-sort cursors and the in-degree snapshot backing store.
	linkageCounts := make([]int, int(h.nodeCount.Load())+numRemaining+1)
	linkageCursors := make([]int, len(linkageCounts))
	var linkageInDegree inDegreeSnapshot

	for lc := topL; lc >= 0; lc-- {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}

		// Identify nodes active at this layer
		activeIndices := make([]int, numRemaining)
		for i := range activeIndices {
			activeIndices[i] = i
		}

		// Sub-batches for organic growth
		subBatchSize := 4
		if lc > 0 {
			subBatchSize = len(activeIndices)
		} else {
			totalAfterBatch := int(h.nodeCount.Load()) + n
			if totalAfterBatch < 10000 {
				subBatchSize = 64
			} else if totalAfterBatch < 100000 {
				subBatchSize = 1024
			} else {
				subBatchSize = 4096
			}
			if subBatchSize > len(activeIndices) {
				subBatchSize = len(activeIndices)
			}
		}

		edgeChunks := make([][]linkOp, (subBatchSize+15)/16)
		graphCandidates := make([]*[]types.Candidate, numRemaining)

		for i := 0; i < len(activeIndices); i += subBatchSize {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
			endBatch := i + subBatchSize
			if endBatch > len(activeIndices) {
				endBatch = len(activeIndices)
			}
			subIndices := activeIndices[i:endBatch]

			// 3a. Search against Graph (Parallel for Sub-Batch)
			var errLayer error
			var layerMu sync.Mutex

			layerChunkSize := (len(subIndices) + runtime.NumCPU() - 1) / runtime.NumCPU()
			if layerChunkSize < 32 {
				layerChunkSize = 32
			}

			releaseCandidateLists := func() {
				for _, idx := range subIndices {
					if prev := graphCandidates[idx]; prev != nil {
						h.putCandidateSlice(*prev)
						graphCandidates[idx] = nil
					}
				}
			}

			pool.ParallelFor(len(subIndices), layerChunkSize, func(start, end int) {
				layerMu.Lock()
				if errLayer != nil {
					layerMu.Unlock()
					return
				}
				layerMu.Unlock()

				indices := subIndices[start:end]
				workerData := data
				meta := h.metadataRegistry.Load()

				ctxSearch := h.searchPool.Get()
				ctxSearch.MaxNodeCount = h.nodeCount.Load()
				ctxSearch.MaxGeneration = meta.Generation
				ctxSearch.Reset()
				ctxSearch.AllowUncommitted = true
				defer h.searchPool.PutWithMetrics(ctxSearch, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))

				for _, idx := range indices {
					if err := ctx.Err(); err != nil {
						layerMu.Lock()
						if errLayer == nil {
							errLayer = err
						}
						layerMu.Unlock()
						return
					}
					node := remainingNodes[idx]
					currEp := currentEps[idx]

					if lc > node.level {
						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, 1, lc, workerData)
						if err != nil {
							layerMu.Lock()
							errLayer = err
							layerMu.Unlock()
							return
						}
						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					} else {
						ef := h.bulkEfConstruction(int(h.nodeCount.Load()))
						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, ef, lc, workerData)
						if err != nil {
							layerMu.Lock()
							errLayer = err
							layerMu.Unlock()
							return
						}
						graphCandidates[idx] = &res
						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					}
				}
			})

			if errLayer != nil {
				return nil, 0, 0, errLayer
			}

			// 3b. Linkage, in three passes (P1, P2, P3)
			K := len(subIndices)
			linkageChunkSize := (K + runtime.NumCPU() - 1) / runtime.NumCPU()
			if linkageChunkSize < 16 {
				linkageChunkSize = 16
			}

			numChunks := (K + linkageChunkSize - 1) / linkageChunkSize
			if numChunks > len(edgeChunks) {
				edgeChunks = make([][]linkOp, numChunks)
			}
			chunkOps := edgeChunks[:numChunks]
			for ci := range chunkOps {
				chunkOps[ci] = chunkOps[ci][:0]
			}

			// P1: decide, and write this node's own list only.
			pool.ParallelFor(K, linkageChunkSize, func(start, end int) {
				chunk := start / linkageChunkSize
				ops := chunkOps[chunk]
				defer func() { chunkOps[chunk] = ops }()
				for pos := start; pos < end; pos++ {
					idx := subIndices[pos]
					if err := ctx.Err(); err != nil {
						layerMu.Lock()
						if errLayer == nil {
							errLayer = err
						}
						layerMu.Unlock()
						return
					}
					node := remainingNodes[idx]
					if lc > node.level {
						continue
					}

					meta := h.metadataRegistry.Load()
					ctxLink := h.searchPool.Get()
					ctxLink.MaxNodeCount = h.nodeCount.Load()
					ctxLink.MaxGeneration = meta.Generation
					ctxLink.Reset()
					ctxLink.AllowUncommitted = true

					candidatesBuf := graphCandidates[idx]
					if candidatesBuf == nil {
						h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					allCandidates := *candidatesBuf
					candidates := append(ctxLink.bulkCandidates[:0], allCandidates[:0:0]...)
					for _, c := range allCandidates {
						if c.ID != node.id {
							candidates = append(candidates, c)
						}
					}
					ctxLink.bulkCandidates = candidates[:0]

					if len(candidates) == 0 {
						h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					slices.SortFunc(candidates, func(a, b types.Candidate) int {
						if a.Dist < b.Dist {
							return -1
						}
						if a.Dist > b.Dist {
							return 1
						}
						return 0
					})

					linkSrc := append(ctxLink.linkSrc[:0], node.id)
					ctxLink.linkSrc = linkSrc[:0]
					linkDist := append(ctxLink.linkDist[:0], float32(0))
					ctxLink.linkDist = linkDist[:0]
					if lc == 0 && node.id > 0 && bulkChainLinksEnabled {
						h.computeDistances(ctxLink, data, node.id-1, linkSrc, linkDist)
						medianDist := candidates[len(candidates)/2].Dist
						if linkDist[0] <= medianDist {
							ops = append(ops, linkOp{
								target:  node.id - 1,
								src:     node.id,
								dist:    linkDist[0],
								maxConn: int32(h.mMax0.Load()), // #nosec G115
							})
							backSrc := append(ctxLink.linkSrc[:0], node.id-1)
							ctxLink.linkSrc = backSrc[:0]
							_ = h.AddConnectionsBatch(ctxLink, data, node.id, backSrc, linkDist, lc, int(h.mMax0.Load()))
						}
					}

					m := h.m.Load()
					maxConn := h.mMax.Load()
					if lc == 0 {
						maxConn = h.mMax0.Load()
						m = h.m.Load() * 2
						if m > maxConn {
							m = maxConn
						}
					} else if m > maxConn {
						m = maxConn
					}

					neighbors := h.selectNeighbors(ctxLink, candidates, int(m), data)
					if len(neighbors) == 0 {
						h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					fSources := ctxLink.bulkSources[:0]
					for _, n := range neighbors {
						fSources = append(fSources, n.ID)
					}
					fDists := ctxLink.bulkDists[:0]
					for _, n := range neighbors {
						fDists = append(fDists, n.Dist)
					}
					ctxLink.bulkSources = fSources[:0]
					ctxLink.bulkDists = fDists[:0]

					_ = h.AddConnectionsBatch(ctxLink, data, node.id, fSources, fDists, lc, int(maxConn))

					for i, nID := range fSources {
						ops = append(ops, linkOp{
							target:  nID,
							src:     node.id,
							dist:    fDists[i],
							maxConn: int32(maxConn), // #nosec G115
						})
					}

					h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
				}
			})

			if errLayer != nil {
				return nil, 0, 0, errLayer
			}

			// P2: apply the cross-node edges.
			totalOps := 0
			maxTarget := uint32(0)
			for _, chunk := range chunkOps {
				totalOps += len(chunk)
				for _, op := range chunk {
					if op.target > maxTarget {
						maxTarget = op.target
					}
				}
			}
			if totalOps > 0 {
				if err := ctx.Err(); err != nil {
					return nil, 0, 0, err
				}
				if uint64(maxTarget) >= uint64(len(linkageCounts)) {
					grow := int(maxTarget) + 1 - len(linkageCounts)
					linkageCounts = append(linkageCounts, make([]int, grow)...)
					linkageCursors = append(linkageCursors, make([]int, grow)...)
				}
				for _, chunk := range chunkOps {
					for _, op := range chunk {
						linkageCounts[op.target]++
					}
				}
				prefix := 0
				for t := range linkageCounts {
					linkageCursors[t] = prefix
					prefix += linkageCounts[t]
				}
				flat := make([]linkOp, prefix)
				for _, chunk := range chunkOps {
					for _, op := range chunk {
						flat[linkageCursors[op.target]] = op
						linkageCursors[op.target]++
						linkageCounts[op.target] = 0
					}
				}

				type targetRun struct{ start, end int }
				runs := make([]targetRun, 0, K+1)
				for i := 0; i < len(flat); {
					j := i + 1
					for j < len(flat) && flat[j].target == flat[i].target {
						j++
					}
					runs = append(runs, targetRun{i, j})
					i = j
				}
				runChunk := (len(runs) + runtime.NumCPU() - 1) / runtime.NumCPU()
				if runChunk < 1 {
					runChunk = 1
				}
				metaApply := h.metadataRegistry.Load()
				var inDegreeView inDegreeReader
				if lc == 0 && InboundEdgeGuardEnabled {
					snap := h.inDegreeL0.snapshotInto(uint32(len(linkageCounts)), linkageInDegree)
					linkageInDegree = snap
					inDegreeView = snap
				}
				pool.ParallelFor(len(runs), runChunk, func(start, end int) {
					ctxApply := h.searchPool.Get()
					ctxApply.MaxNodeCount = h.nodeCount.Load()
					ctxApply.MaxGeneration = metaApply.Generation
					ctxApply.Reset()
					ctxApply.AllowUncommitted = true
					ctxApply.inDegreeView = inDegreeView
					defer h.searchPool.PutWithMetrics(ctxApply, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
					for ri := start; ri < end; ri++ {
						if err := ctx.Err(); err != nil {
							layerMu.Lock()
							if errLayer == nil {
								errLayer = err
							}
							layerMu.Unlock()
							return
						}
						run := runs[ri]
						for _, op := range flat[run.start:run.end] {
							opSrc := append(ctxApply.linkSrc[:0], op.src)
							ctxApply.linkSrc = opSrc[:0]
							opDist := append(ctxApply.linkDist[:0], op.dist)
							ctxApply.linkDist = opDist[:0]
							_ = h.AddConnectionsBatch(ctxApply, data, op.target, opSrc, opDist, lc, int(op.maxConn))
						}
					}
				})
				if errLayer != nil {
					return nil, 0, 0, errLayer
				}
			}

			// P3: R26 - guarantee that node.id has at least one inbound edge in layer 0.
			if lc == 0 && InboundEdgeGuardEnabled {
				if err := ctx.Err(); err != nil {
					return nil, 0, 0, err
				}
				ctxGuard := h.searchPool.Get()
				ctxGuard.MaxNodeCount = h.nodeCount.Load()
				metaGuard := h.metadataRegistry.Load()
				ctxGuard.MaxGeneration = metaGuard.Generation
				ctxGuard.Reset()
				ctxGuard.AllowUncommitted = true
				guardMaxConn := int(h.mMax0.Load())
				guardM := int(h.m.Load()) * 2
				if guardM > guardMaxConn {
					guardM = guardMaxConn
				}
				for pos := 0; pos < K; pos++ {
					idx := subIndices[pos]
					nodeID := remainingNodes[idx].id
					if h.inDegreeL0.Get(nodeID) > 0 {
						continue
					}
					candidatesBuf := graphCandidates[idx]
					if candidatesBuf == nil {
						continue
					}
					allCandidates := *candidatesBuf
					candidates := append(ctxGuard.bulkCandidates[:0], allCandidates[:0:0]...)
					for _, c := range allCandidates {
						if c.ID != nodeID {
							candidates = append(candidates, c)
						}
					}
					ctxGuard.bulkCandidates = candidates[:0]
					if len(candidates) == 0 {
						continue
					}
					slices.SortFunc(candidates, func(a, b types.Candidate) int {
						if a.Dist < b.Dist {
							return -1
						}
						if a.Dist > b.Dist {
							return 1
						}
						return 0
					})
					neighbors := h.selectNeighbors(ctxGuard, candidates, guardM, data)
					if len(neighbors) == 0 {
						continue
					}
					guardSources := ctxGuard.bulkSources[:0]
					for _, n := range neighbors {
						guardSources = append(guardSources, n.ID)
					}
					ctxGuard.bulkSources = guardSources[:0]
					for _, src := range guardSources {
						if h.inDegreeL0.Get(nodeID) > 0 {
							break
						}
						h.ensureInboundEdge(ctxGuard, data, src, nodeID, guardMaxConn)
					}
				}
				h.searchPool.PutWithMetrics(ctxGuard, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
			}

			runtime.KeepAlive(data)
			releaseCandidateLists()

			if errLayer != nil {
				return nil, 0, 0, errLayer
			}

			// Update global snapshot for organic growth so next sub-batch sees these nodes
			if i+subBatchSize < len(activeIndices) {
				data = data.Clone()
				h.compareAndSwapData(h.data.Load(), data)
			}
		}

		// Final layer publish
		data = data.Clone()
		h.compareAndSwapData(h.data.Load(), data)
	}

	runtime.KeepAlive(data)
	return data, batchMaxLevel, batchEpCandidate, nil
}
