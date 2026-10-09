package index

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"time"

	"sync"

	"github.com/23skdu/longbow/internal/pq"

	"math"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// BulkInsertThreshold defines the minimum batch size to trigger parallel bulk insert
var BulkInsertThreshold = func() int {
	if v := os.Getenv("LONGBOW_HNSW_BULK_INSERT_THRESHOLD"); v != "" {
		if t, err := strconv.Atoi(v); err == nil && t > 0 {
			return t
		}
	}
	return 256
}()

// BulkInsertBudget defines the wall-clock time budget for a bulk insert operation.
// Exceeding this budget emits a diagnostic log with node count, elapsed time, and per-vector cost (roadmap R22).
var BulkInsertBudget = func() time.Duration {
	if v := os.Getenv("LONGBOW_HNSW_BULK_INSERT_BUDGET"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
		if s, err := strconv.ParseFloat(v, 64); err == nil && s > 0 {
			return time.Duration(s * float64(time.Second))
		}
	}
	return 30 * time.Second
}()

// ShardedLockCount is the number of shards for node locking.
const ShardedLockCount = 131072

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

// AddBatchBulk attempts to insert a batch of vectors in parallel using a bulk strategy.
// It assumes IDs, locations, and capacity have already been prepared/reserved.
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs any) error {
	h.bulkMu.Lock()
	h.inBulkInsert.Add(1)
	err := h.addBatchBulkInternal(ctx, startID, n, vecs)
	h.inBulkInsert.Add(-1)
	h.bulkMu.Unlock()

	if n > 0 {
		finalID := int64(startID + uint32(n)) // #nosec G115
		h.commitMu.Lock()
		for h.nodeCount.Load() < int64(startID) {
			h.commitCond.Wait()
		}
		if h.nodeCount.Load() < finalID {
			h.nodeCount.Store(finalID)
		}
		h.commitCond.Broadcast()
		h.commitMu.Unlock()

		// Flush to disk after bulk insert if UseDisk is enabled
		h.maybeFlushToDisk()
	}

	return err
}

func (h *ArrowHNSW) addBatchBulkInternal(ctx context.Context, startID uint32, n int, vecs any) error {
	if n <= 0 {
		return nil
	}
	totalN := n
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.HNSWBulkInsertDurationSeconds.Observe(duration)
		metrics.HNSWInsertOpsTotal.WithLabelValues(h.name, h.config.DataType.String()).Add(float64(n))
		metrics.HNSWNodesAddedTotal.WithLabelValues(h.name).Add(float64(n))
		if !h.disableNodeCountMetric.Load() {
			metrics.HNSWNodeCount.WithLabelValues(h.name, "0").Set(float64(h.nodeCount.Load()))
		}

		// Enhanced Observability
		typeStr := h.config.DataType.String()
		dims := int(h.dims.Load())
		metrics.HNSWBulkInsertLatencyByType.WithLabelValues(typeStr).Observe(duration)
		metrics.HNSWBulkInsertLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)

		elapsed := time.Since(start)
		if elapsed > BulkInsertBudget && totalN > 0 {
			perVecMs := float64(elapsed.Milliseconds()) / float64(totalN)
			fmt.Printf("[WARN][HNSW] Bulk insert exceeded time budget: dataset=%s type=%s nodes=%d elapsed=%v per_vector=%.3fms budget=%v\n",
				h.name, typeStr, totalN, elapsed, perVecMs, BulkInsertBudget)
		}
	}()

	// 1. Ensure dimensions are initialized if this is the first insert
	dims := int(h.dims.Load())
	if dims == 0 {
		// Identify dimensions from batch
		switch vs := vecs.(type) {
		case [][]float32:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case []arrow.RecordBatch:
			if len(vs) > 0 {
				// Find first valid record
				for _, r := range vs {
					if r != nil {
						idx := h.getVectorColumnIndex(r)
						if idx != -1 {
							col := r.Column(idx)
							if fsl, ok := col.DataType().(*arrow.FixedSizeListType); ok {
								dims = int(fsl.Len())
								break
							}
						}
					}
				}
			}
		case [][]float16.Num:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]int8:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]uint8:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]float64:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]complex64:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]complex128:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]uint32:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]int32:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]uint16:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]int16:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]int64:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		case [][]uint64:
			if len(vs) > 0 {
				dims = len(vs[0])
			}
		}

		if dims > 0 {
			h.initMu.Lock()
			if h.dims.Load() == 0 {
				h.dims.Store(int32(dims))
				// Ensure distance functions are initialized with correct dims
				h.resolveAllDistanceFuncs()

				// Allocate initial graph data if not already present with these dims
				data := h.data.Load()
				capacity := h.config.InitialCapacity
				if capacity < 1000 {
					capacity = 1000
				}
				if data == nil || data.Dims == 0 {
					_ = h.Grow(capacity, dims)
				}
			} else {
				dims = int(h.dims.Load())
			}
			h.initMu.Unlock()
		}
	}

	if dims == 0 {
		return fmt.Errorf("failed to determine dimensions for bulk insert")
	}

	maxID := startID + uint32(n) - 1 // #nosec G115
	cIDStart := types.ChunkID(startID)
	cIDEnd := types.ChunkID(maxID)

	// Pre-allocate all required chunks in a single COW operation
	data, err := h.EnsureChunks(int(cIDStart), int(cIDEnd), dims)
	if err != nil {
		return err
	}
	// Pin the original data across the Clone() call so a concurrent
	// compareAndSwapData-driven Release() cannot nil out the typed-
	// arenas before we read them via g.Int8Arena.Slab(). The pin
	// must be released BEFORE the first compareAndSwapData() further
	// down in this function, because that call's Release() spins on
	// the readerCount and would otherwise deadlock with ourselves.
	original := data
	original.AcquireReader()
	clone := original.Clone()
	original.ReleaseReader()
	data = clone

	growMuReleased := true // #nosec G101 - No longer needed with EnsureChunks
	_ = growMuReleased

	type activeNode struct {
		id    uint32
		level int
		vec   any // Can be []float32, []float16.Num, etc.
	}

	activeNodes := make([]activeNode, n)

	// Pre-load vectors and generate levels (Parallel)

	// Use SharedWorkerPool for parallel prep
	pool := GetSharedPool()
	var errPrep error
	var errMu sync.Mutex

	// Slice into chunks for workers to amortize goroutine overhead
	chunkSize := (n + runtime.NumCPU() - 1) / runtime.NumCPU()
	if chunkSize < 64 {
		chunkSize = 64 // Minimum chunk size to justify overhead
	}

	highPriority := types.IsHighPriority(ctx)
	parallelFor := pool.ParallelFor
	if highPriority {
		parallelFor = pool.ParallelForHighPriority
	}

	parallelFor(n, chunkSize, func(start, end int) {
		errMu.Lock()
		if errPrep != nil || ctx.Err() != nil {
			if errPrep == nil {
				errPrep = ctx.Err()
			}
			errMu.Unlock()
			return
		}
		errMu.Unlock()

		for j := start; j < end; j++ {
			id := startID + uint32(j) // #nosec G115
			cID := types.ChunkID(id)
			cOff := types.ChunkOffset(id)

			// Level generation
			level := h.generateLevel()

			// Vector Ingestion (Zero-Copy from passed batch)
			var v any
			// Type switch to extract vector from generic batch
			switch vs := vecs.(type) {
			case [][]float32:
				switch h.config.DataType {
				case types.VectorTypeComplex64:
					f32s := vs[j]
					c64s := make([]complex64, len(f32s)/2)
					for k := 0; k < len(f32s)/2; k++ {
						c64s[k] = complex(f32s[2*k], f32s[2*k+1])
					}
					v = c64s
				case types.VectorTypeComplex128:
					f32s := vs[j]
					c128s := make([]complex128, len(f32s)/2)
					for k := 0; k < len(f32s)/2; k++ {
						c128s[k] = complex(float64(f32s[2*k]), float64(f32s[2*k+1]))
					}
					v = c128s
				default:
					v = vs[j]
				}
			case []arrow.RecordBatch:
				// Discover vector within the batch of records
				// Assuming standard sequential mapping, skipping any nil batches
				row := j
				recIdx := 0
				for recIdx < len(vs) {
					if vs[recIdx] == nil {
						recIdx++
						continue
					}
					numRows := int(vs[recIdx].NumRows())
					if row < numRows {
						break
					}
					row -= numRows
					recIdx++
				}
				if recIdx < len(vs) && vs[recIdx] != nil {
					rec := vs[recIdx]
					idx := h.getVectorColumnIndex(rec)
					if idx != -1 {
						v = h.extractVector(rec, idx, row)
					}
				}
			case [][]uint32:
				v = vs[j]
			case [][]int32:
				v = vs[j]
			case [][]uint16:
				v = vs[j]
			case [][]int16:
				v = vs[j]
			case [][]uint8:
				v = vs[j]
			case [][]int8:
				v = vs[j]
			case [][]int64:
				v = vs[j]
			case [][]uint64:
				v = vs[j]
			case [][]float64:
				v = vs[j]
			case [][]complex64:
				v = vs[j]
			case [][]complex128:
				v = vs[j]
			case [][]float16.Num:
				v = vs[j]
			default:
				errMu.Lock()
				errPrep = fmt.Errorf("unsupported vector type in bulk insert: %T", vecs)
				errMu.Unlock()
				return
			}

			// Basic validation
			if v == nil {
				errMu.Lock()
				errPrep = fmt.Errorf("vector missing for bulk insert ID %d (nil slice)", id)
				errMu.Unlock()
				return
			}

			// Validate dimensions based on type
			vLen := VectorLength(v)

			if vLen != dims {
				metrics.BulkInsertDimensionErrorsTotal.Inc()
				errMu.Lock()
				errPrep = types.NewVectorDimensionMismatchError(int(id), dims, vLen)
				errMu.Unlock()
				return
			}

			// Always ingest into hot storage using method that handles all types
			// Use private data snapshot for vector storage
			if err := data.SetVector(id, v); err != nil {
				errMu.Lock()
				errPrep = err
				errMu.Unlock()
				return
			}

			// 2. SQ8 Ingestion
			// Parallelize quantization - significant speedup for large batches
			if h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
				sq8Chunk := data.GetVectorsSQ8Chunk(cID)
				if sq8Chunk != nil {
					if vf32, ok := v.([]float32); ok {
						sq8Stride := (dims + 63) & ^63
						startOff := int(cOff) * sq8Stride
						dest := sq8Chunk[startOff : startOff+dims]
						h.quantizer.Encode(vf32, dest)
					}
				}
			}

			// 3. BQ Ingestion
			if h.config.BQEnabled && h.bqEncoder != nil {
				bqChunk := data.GetVectorsBQChunk(cID)
				if bqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						code := h.bqEncoder.Encode(vf32)
						numWords := h.bqEncoder.CodeSize()
						dest := bqChunk[int(cOff)*numWords : (int(cOff)+1)*numWords]
						copy(dest, code)
					}
				}
			}

			// 4. PQ Ingestion
			if h.config.PQEnabled && h.oopqEncoder != nil {
				pqChunk := data.GetVectorsPQChunk(cID)
				if pqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						switch enc := h.oopqEncoder.(type) {
						case *pq.PQEncoder:
							code, err := enc.Encode(vf32)
							if err == nil {
								pqM := h.config.PQM
								dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
								copy(dest, code)
							}
						case *pq.OPQEncoder:
							code, err := enc.Encode(vf32)
							if err == nil {
								pqM := h.config.PQM
								dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
								copy(dest, code)
							}
						}
					}
				}
			}

			// 5. TQ Ingestion
			if h.tqEncoder != nil {
				tqChunk := data.GetVectorsTQChunk(cID)
				if tqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						code, err := h.tqEncoder.Encode(vf32)
						if err == nil {
							stride := data.PackedSize()
							dest := tqChunk[int(cOff)*stride : (int(cOff)+1)*stride]
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

			// Mandatory location registration for HNSW navigator
			h.SetLocation(types.VectorID(id), types.Location{BatchIdx: 0, RowIdx: int(id)})

			// Init levels chunk if needed
			levelsChunk := data.GetLevelsChunk(cID)
			if levelsChunk != nil {
				atomic.StoreUint32(&levelsChunk[cOff], uint32(level)) // #nosec G115
			}
		}
	})

	if errPrep != nil {
		return errPrep
	}

	// Create a stable, read-only version for other workers to clone from
	stableData := data.Clone()
	h.compareAndSwapData(h.data.Load(), stableData)

	// 3. Sequential Bootstrap Phase
	// Establish a stable hierarchy by inserting a portion sequentially.
	seedCount := BulkInsertThreshold
	if n < seedCount {
		seedCount = n
	}

	// Verify first and last vector
	vStart, _ := data.GetVector(startID)
	if vStart == nil {
		return fmt.Errorf("failed to retrieve first vector %d after fill", startID)
	}
	vEnd, _ := data.GetVector(startID + uint32(n) - 1)
	if vEnd == nil {
		return fmt.Errorf("failed to retrieve last vector %d after fill", startID+uint32(n)-1)
	}

	var bootstrapEp uint32 = math.MaxUint32
	var bootstrapMaxL int32 = -1
	for i := 0; i < seedCount; i++ {
		node := activeNodes[i]
		var err error
		data, err = h.insertInternal(node.id, node.vec, node.level, true, data)
		if err != nil {
			return err
		}
		if int32(node.level) > bootstrapMaxL || bootstrapEp == math.MaxUint32 { // #nosec G115
			bootstrapMaxL = int32(node.level) // #nosec G115
			bootstrapEp = node.id
			h.updateMetadataIfHigher(bootstrapEp, bootstrapMaxL)
		}
	}
	h.compareAndSwapData(h.data.Load(), data.Clone())
	if err := ctx.Err(); err != nil {
		return err
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
			}
		}
		h.initMu.Unlock()
		return nil
	}

	// 4. Parallel Linkage Phase
	// Link remaining nodes to the bootstrap backbone and each other.

	// Advance nodeCount so search kernels see the new data
	// (Safe because we are under growMu/initMu or sequentially ordered)
	finalID := int64(startID + uint32(n))
	h.commitMu.Lock()
	if h.nodeCount.Load() < finalID {
		h.nodeCount.Store(finalID)
		h.commitCond.Broadcast()
	}
	h.commitMu.Unlock()

	// Refresh data snapshot after bootstrap loop as InsertWithVector updated the global state.
	// Must Clone() the published data: another concurrent addBatchBulkInternal could
	// CAS-publish a newer snapshot and Release() this one, nilling the typed-arenas.
	// Clone() takes a fresh Retain on the Slab, so subsequent data.Clone() and
	// data.SetVector calls have a valid live GraphData to work on.
	// Pin the published snapshot across the Clone() call to prevent
	// the typed-arena nil-out race during Slab() reads in Clone().
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

	// Pre-decode TQ vectors to float32 for fast construction across all cores
	if h.config.DataType == types.VectorTypeTQ && h.tqCompute != nil {
		dim := int(h.dims.Load())
		totalIDs := int(startID + uint32(n))
		cache := make([]float32, totalIDs*dim)
		numNodes := len(activeNodes)
		chunkSize := (numNodes + runtime.NumCPU() - 1) / runtime.NumCPU()
		if chunkSize < 64 {
			chunkSize = 64
		}
		pool.ParallelFor(numNodes, chunkSize, func(start, end int) {
			for idx := start; idx < end; idx++ {
				node := activeNodes[idx]
				tqCode, err := h.tqCompute.getTQBytes(node.id, nil, math.MaxUint64)
				if err == nil && len(tqCode) > 0 {
					startOffset := int(node.id) * dim
					target := cache[startOffset : startOffset+dim]
					_ = h.tqCompute.encoder.DecodeInto(tqCode, target)
				}
			}
		})
		h.tqDecodeCache.Store(&tqDecodeCache{data: cache})
		defer func() { h.tqDecodeCache.Store(nil) }()
	}

	// Current entry points for remaining active nodes. Initially global EP.
	currentEps := make([]uint32, numRemaining)
	for i := range currentEps {
		currentEps[i] = ep
	}

	// Deferred Connection Pipeline (Phase 15 Implementation)
	// ----------------------------------------------------

	// 2.5 Pre-Promote all nodes in the batch and SET VECTORS (Parallel)
	// This ensures chunks are allocated and vectors are persistent before linkage.
	pool.ParallelFor(numRemaining, (numRemaining+runtime.NumCPU()-1)/runtime.NumCPU(), func(start, end int) {
		// Vector data is already set in the previous ParallelFor using the latest h.data pointer.
		// No need to promote nodes here as chunks were pre-allocated and published.
	})

	for lc := topL; lc >= 0; lc-- {
		// Honour cancellation between layers: a single layer over a large
		// batch can run for minutes, so the caller must not be stuck waiting
		// on a deadline that has already passed.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Identify nodes active at this layer
		activeIndices := make([]int, 0, numRemaining)
		for i, node := range remainingNodes {
			if node.level >= lc {
				activeIndices = append(activeIndices, i)
			}
		}

		if len(activeIndices) == 0 {
			continue // Should not happen if topL is correct
		}

		// 3. Layer-by-Layer Insertion with Organic Growth
		// Divide nodes into sub-batches to ensure the graph grows organically,
		// preventing the "star graph" problem where all nodes link to the same few bootstrap nodes.
		subBatchSize := 4
		if lc > 0 {
			subBatchSize = len(activeIndices) // Higher layers are small, process in one go
		} else {
			// Adaptive subBatchSize for layer 0 to prevent O(N^2) COW clones
			// Use existingNodes + n because nodeCount defer-update hasn't run yet
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

		for i := 0; i < len(activeIndices); i += subBatchSize {
			// Sub-batches are the unit of work between two graph clones, so
			// this is the natural place to bail out without leaving a
			// half-linked layer behind.
			if err := ctx.Err(); err != nil {
				return err
			}
			endBatch := i + subBatchSize
			if endBatch > len(activeIndices) {
				endBatch = len(activeIndices)
			}
			subIndices := activeIndices[i:endBatch]

			// 3a. Search against Graph (Parallel for Sub-Batch)
			graphCandidates := make([]*[]types.Candidate, numRemaining)
			var errLayer error
			var layerMu sync.Mutex

			layerChunkSize := (len(subIndices) + runtime.NumCPU() - 1) / runtime.NumCPU()
			if layerChunkSize < 32 {
				layerChunkSize = 32
			}

			pool.ParallelFor(len(subIndices), layerChunkSize, func(start, end int) {
				layerMu.Lock()
				if errLayer != nil {
					layerMu.Unlock()
					return
				}
				layerMu.Unlock()

				indices := subIndices[start:end]
				workerData := data // Use the current snapshot (updated after each sub-batch)
				meta := h.metadataRegistry.Load()

				ctxSearch := h.searchPool.Get()
				ctxSearch.MaxNodeCount = h.nodeCount.Load()
				ctxSearch.MaxGeneration = meta.Generation
				ctxSearch.Reset()
				ctxSearch.AllowUncommitted = true
				defer h.searchPool.PutWithMetrics(ctxSearch, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))

				for _, idx := range indices {
					// Check per node so an in-flight layer aborts promptly
					// instead of running to completion past its deadline.
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
						// Descent phase: ef=1
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
						// Insertion phase
						ef := int(h.efConstruction.Load())
						if h.config.AdaptiveEf {
							ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
						}

						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, ef, lc, workerData)
						if err != nil {
							layerMu.Lock()
							errLayer = err
							layerMu.Unlock()
							return
						}
						pBuf := new([]types.Candidate)
						*pBuf = make([]types.Candidate, 0, len(res))
						*pBuf = append(*pBuf, res...)
						graphCandidates[idx] = pBuf

						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					}
				}
			})

			if errLayer != nil {
				return errLayer
			}

			// 3b. Linkage (Parallel for Sub-Batch)
			linkageChunkSize := (len(subIndices) + runtime.NumCPU() - 1) / runtime.NumCPU()
			if linkageChunkSize < 16 {
				linkageChunkSize = 16
			}

			pool.ParallelFor(len(subIndices), linkageChunkSize, func(start, end int) {
				for _, idx := range subIndices[start:end] {
					// Linkage is the expensive half of a layer (neighbour
					// selection per node), so check cancellation here too.
					// Checked before ctxLink is taken so there is no pooled
					// context to return on the abort path.
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

					// Chain this node to the one inserted just before it, in both
					// directions, before anything else touches the graph.
					//
					// Every node in a sub-batch searches the same frozen graph, so
					// no node of the sub-batch can appear in another node's
					// candidate list, and a fresh node's only inbound edges are the
					// reverse links it hands to its pre-batch neighbours. Those
					// are pruned away as soon as such a neighbour already sits at
					// its connection limit, and on degenerate geometry (collinear
					// points) every node in the sub-batch picks the same
					// neighbours, so entire sub-batches end up with in-degree
					// zero and are unreachable from the entry point even when ef
					// covers the whole dataset. The insertion-order predecessor is
					// always linked and adjacent in distance for sorted-ish
					// data, so this edge both exists and survives pruning.
					if lc == 0 && node.id > 0 && bulkChainLinksEnabled {
						var chainDist [1]float32
						h.computeDistances(ctxLink, data, node.id-1, []uint32{node.id}, chainDist[:])
						_ = h.AddConnectionsBatch(ctxLink, data, node.id-1, []uint32{node.id}, chainDist[:], lc, int(h.mMax0.Load()))
						_ = h.AddConnectionsBatch(ctxLink, data, node.id, []uint32{node.id - 1}, chainDist[:], lc, int(h.mMax0.Load()))
					}

					candidatesBuf := graphCandidates[idx]
					if candidatesBuf == nil {
						h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					allCandidates := *candidatesBuf
					var candidates []types.Candidate
					for _, c := range allCandidates {
						if c.ID != node.id {
							candidates = append(candidates, c)
						}
					}

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

					m := h.m.Load()
					maxConn := h.mMax.Load()
					if lc == 0 {
						m = h.m.Load() * 2
						maxConn = h.mMax0.Load()
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

					fSources := make([]uint32, 0, len(neighbors))
					fDists := make([]float32, 0, len(neighbors))
					for _, n := range neighbors {
						fSources = append(fSources, n.ID)
						fDists = append(fDists, n.Dist)
					}

					_ = h.AddConnectionsBatch(ctxLink, data, node.id, fSources, fDists, lc, int(maxConn))

					// Iterate the private copy: AddConnectionsBatch re-enters
					// neighbour selection (to prune an over-capacity target),
					// which overwrites the shared scratch buffer that
					// selectNeighbors returned.
					for i, nID := range fSources {
						_ = h.AddConnectionsBatch(ctxLink, data, nID, []uint32{node.id}, []float32{fDists[i]}, lc, int(maxConn))
					}

					h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
				}
			})

			runtime.KeepAlive(data) // Keep GraphData alive during blocking parallel linkage

			// Surface an abort raised by the linkage workers before the
			// snapshot clone below, which would otherwise publish a
			// partially linked layer as if it had succeeded.
			if errLayer != nil {
				return errLayer
			}

			// Update the global snapshot for organic growth so next sub-batch sees these nodes
			// Only clone if there are more sub-batches to process to avoid final redundant clone
			if i+subBatchSize < len(activeIndices) {
				data = data.Clone()
				h.compareAndSwapData(h.data.Load(), data)
			}
		}

		// Final layer publish
		data = data.Clone()
		h.compareAndSwapData(h.data.Load(), data)
	}

	runtime.KeepAlive(data) // Prevent premature GC of GraphData while parallel workers may still use it

	// 4. Update Global Stats
	// Update Max Level / Entry Point atomically
	h.updateMetadata(func(meta *HNSWMetadata) {
		if int32(batchMaxLevel) > meta.MaxLevel { // #nosec G115
			meta.MaxLevel = int32(batchMaxLevel) // #nosec G115
			meta.EntryPoint = batchEpCandidate
		}
		// Update NodeCount to include the new batch
		if int64(startID)+int64(totalN) > meta.NodeCount {
			meta.NodeCount = int64(startID) + int64(totalN)
		}
	})

	// Register all nodes in this batch into entry point pools if they have upper layer presence
	h.initMu.Lock()
	for _, node := range activeNodes {
		if node.level > 0 && node.level < len(h.entryPointPools) {
			h.entryPointPools[node.level].Insert(node.id)
		}
	}
	h.initMu.Unlock()

	h.compareAndSwapData(h.data.Load(), data.Clone())

	// R8: Empirical graph quality guard.
	// Sample a subset of newly inserted nodes to verify layer 0 reachability from entry point.
	if err := h.checkBulkGraphQuality(ctx, startID, totalN); err != nil {
		return err
	}

	if h.config.SQ8Enabled && h.quantizer != nil && !h.sq8Ready.Load() {
		if vecsF32, ok := vecs.([][]float32); ok {
			h.ensureTrained(int(startID)+totalN-1, vecsF32, data)
			return nil
		}
	}

	if h.config.PQEnabled && h.oopqEncoder == nil && !h.pqTrained.Load() {
		if vecsF32, ok := vecs.([][]float32); ok {
			h.ensurePQTrained(vecsF32)
		}
	}

	runtime.KeepAlive(data)
	return nil
}

// BulkGraphQualityFloor is the layer-0 reachability a bulk-inserted batch must
// show, measured from the entry point, before the batch is accepted. It only
// applies when the gate is enabled; see bulkGraphQualityGuardEnabled.
//
// Set LONGBOW_HNSW_BULK_REACHABILITY_FLOOR to override.
var BulkGraphQualityFloor = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_REACHABILITY_FLOOR"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	return 0.80
}()

// bulkGraphQualityGuardEnabled reports whether the R8 graph-quality gate
// enforces its floors, or only reports them. Enforcing is the default, because
// the bulk path on its own is a regression: a 100k float32 corpus built
// entirely through bulk insertion indexed in 23.5s and served 700 dense QPS,
// against 85-215s and 3286-3474 QPS for the same corpus with the gate rejecting
// batches and rebuilding them sequentially.
//
// Set LONGBOW_HNSW_BULK_QUALITY_GUARD=0 to report the measurements without
// enforcing them.
//
// Which floor does the rejecting is still open, and all three are documented on
// their own:
//
//   - Sampled reachability is the only one that tracked search throughput in
//     practice, but it is only a valid measurement when no other bulk insert is
//     mutating the graph, so it is skipped under concurrent AddBatch.
//   - Mean degree correlates with throughput across whole builds but not within
//     one, and is disabled by default: enforcing it at 0.50 rejected four of the
//     nine batches in a 100k float32 build for a 215s index build that served
//     3286 dense QPS against 3474 ungated.
//   - Descent depth is the closest proxy to what search actually does, and it
//     stayed within budget (2.3-4.0 hops against a 5.0-6.4 budget) on both the
//     graphs that served 3474 QPS and the one that served 650.
var bulkGraphQualityGuardEnabled = func() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LONGBOW_HNSW_BULK_QUALITY_GUARD"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}()

// BulkGraphQualityMinDegreeRatio is the fraction of MMax0 that layer 0 must
// reach, on average, for a bulk-inserted batch to be accepted.
//
// Disabled by default, which is a measured decision. Enforcing it at 0.50 makes
// the gate reject four of the nine batches in a 100k float32 build, each one
// falling back to sequential insertion, and the resulting 215s index build
// misses TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress's 90s budget. It also
// buys nothing: the runs it accepts served 3286 dense QPS against 3474 for the
// ungated build, inside the +-5% run-to-run spread. The sequential rebuilds it
// triggers are not cheaper.
//
// Degree is reported on every batch so the trade is visible, and the knob is
// here for deployments that would rather pay the index time than ship a sparse
// layer. Set LONGBOW_HNSW_BULK_MIN_DEGREE_RATIO to enable; 0 or unset disables.
var BulkGraphQualityMinDegreeRatio = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_MIN_DEGREE_RATIO"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	return 0
}()

// BulkGraphMaxHopDepth is the mean layer-0 descent depth a bulk-inserted batch
// may have before the gate rejects it, expressed as a multiple of the depth a
// navigable graph of this size should need: log(N)/log(MMax0), which is about 5
// hops at 100k with MMax0=16, so the default admits up to 7.5.
//
// It only applies when the gate is enabled. Set LONGBOW_HNSW_BULK_MAX_HOP_DEPTH
// to override; 0 disables the check.
var BulkGraphMaxHopDepth = func() float64 {
	if v := os.Getenv("LONGBOW_HNSW_BULK_MAX_HOP_DEPTH"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return f
		}
	}
	return 1.5
}()

// bulkGraphQuality is what measureBulkGraphQuality measured, so the caller can
// report it whether or not the floor was met.
type bulkGraphQuality struct {
	Nodes        int
	Reachable    int
	Sampled      int
	Reachability float64
	MeanDegree   float64
	MeanHops     float64
	IdealHops    float64
}

// measureHopDepth greedy-descends layer 0 from the entry point towards each of
// the sampled targets and reports the mean number of hops.
//
// This walks the same accessor the gate already used for the reachability BFS
// and computes distances through the index's own typed distance path, so it
// costs a handful of distance evaluations per sample and needs no new
// plumbing per element type. Descent stops as soon as no neighbour of the
// current node is closer to the target than the node itself, which is the
// standard HNSW greedy rule: a node the descent cannot improve on is where the
// path ends, whether that is the target or not.
func (h *ArrowHNSW) measureHopDepth(ctx context.Context, ep uint32, targets []uint32) (mean, ideal float64) {
	ideal = h.idealHopDepth()
	if len(targets) == 0 {
		return 0, ideal
	}

	searchCtx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(searchCtx, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
	searchCtx.MaxNodeCount = h.nodeCount.Load()
	searchCtx.MaxGeneration = h.GetMetadataSnapshot().Generation
	searchCtx.AllowUncommitted = true

	total := 0.0
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return total / float64(len(targets)), ideal
		}
		total += float64(h.greedyHops(searchCtx, ep, target))
	}
	return total / float64(len(targets)), ideal
}

// idealHopDepth is the depth a navigable layer 0 of this size needs to reach an
// arbitrary node: log(N)/log(MMax0), the logarithmic scaling HNSW is built on.
//
// It has to come from the configured degree, not the measured one. A degenerate
// layer 0 measures a small degree, which inflates the ideal and hands the worst
// graphs the loosest budget - exactly backwards.
func (h *ArrowHNSW) idealHopDepth() float64 {
	degree := float64(h.mMax0.Load())
	if degree < 2 {
		return math.Log(float64(h.nodeCount.Load()))
	}
	return math.Log(float64(h.nodeCount.Load())) / math.Log(degree)
}

// maxGreedyHops bounds a single descent so a disconnected target cannot spin.
const maxGreedyHops = 512

func (h *ArrowHNSW) greedyHops(ctx *ArrowSearchContext, ep, target uint32) int {
	cur := ep
	if cur == target {
		return 0
	}
	var dist [1]float32
	h.computeDistances(ctx, h.data.Load(), cur, []uint32{target}, dist[:])
	best := dist[0]

	hops := 0
	for hops < maxGreedyHops {
		hops++
		nb, err := h.GetLayerNeighbors(cur, 0)
		if err != nil || len(nb) == 0 {
			return hops
		}
		dists := make([]float32, len(nb))
		h.computeDistances(ctx, h.data.Load(), target, nb, dists)

		next := uint32(0)
		nextDist := float32(math.MaxFloat32)
		for i, v := range nb {
			if v == target {
				return hops
			}
			if dists[i] < nextDist {
				next, nextDist = v, dists[i]
			}
		}
		if nextDist >= best {
			// Nothing adjacent to cur is closer to the target than cur is, so
			// the descent has converged. That is the standard greedy stopping
			// rule and it is where the hop count ends whether or not cur is
			// the target.
			return hops
		}
		cur, best = next, nextDist
	}
	return hops
}

// checkBulkGraphQuality measures layer-0 reachability and mean degree for the
// batch just linked in, and falls back to sequential insertion when either
// indicates a graph the search path would not navigate well (R8).
func (h *ArrowHNSW) checkBulkGraphQuality(ctx context.Context, startID uint32, n int) error {
	q := h.measureBulkGraphQuality(ctx, startID, n)
	if q.Sampled == 0 {
		return nil
	}
	enforce := bulkGraphQualityGuardEnabled
	minDegree := float64(h.mMax0.Load()) * BulkGraphQualityMinDegreeRatio
	maxHops := q.IdealHops * BulkGraphMaxHopDepth
	degradedHops := enforce && BulkGraphMaxHopDepth > 0 && q.MeanHops > maxHops
	degradedDegree := enforce && minDegree > 0 && q.MeanDegree < minDegree
	degradedReach := enforce && q.Reachability < BulkGraphQualityFloor
	// Always report: this is the only place the numbers exist, and a threshold
	// nobody can see is a threshold nobody can tune.
	fmt.Printf("[HNSW] bulk batch nodes=%d reachable=%d/%d sampled=%.1f%% mean_degree=%.2f hops=%.1f/%.1f addbatch_inflight=%d fallback=%v\n",
		q.Nodes, q.Reachable, q.Nodes, 100*q.Reachability, q.MeanDegree, q.MeanHops, maxHops,
		h.inAddBatch.Load(), degradedHops || degradedDegree || degradedReach)
	switch {
	case degradedHops:
		return fmt.Errorf("bulk graph needs %.1f descent hops to reach its own batch (budget %.1f)", q.MeanHops, maxHops)
	case degradedDegree:
		return fmt.Errorf("bulk layer-0 mean degree %.2f < %.2f", q.MeanDegree, minDegree)
	case degradedReach:
		return fmt.Errorf("bulk graph reachability degraded (%.1f%% < %.0f%%)", q.Reachability*100, BulkGraphQualityFloor*100)
	}
	return nil
}

// measureBulkGraphQuality walks layer 0 from the entry point and reports what it
// found. The walk is bounded by the node count rather than by a visit budget,
// because the cheapest way to sample a graph whose quality is in question is to
// look at all of it; the sample of batch nodes is what gets turned into a
// verdict.
func (h *ArrowHNSW) measureBulkGraphQuality(ctx context.Context, startID uint32, n int) bulkGraphQuality {
	out := bulkGraphQuality{Nodes: int(startID) + n}
	ep := h.entryPoint.Load()
	if ep == math.MaxUint32 {
		return out
	}

	seen := make(map[uint32]struct{}, out.Nodes)
	queue := make([]uint32, 0, 1024)
	seen[ep] = struct{}{}
	queue = append(queue, ep)

	degreeSum := 0
	for len(queue) > 0 && len(seen) < out.Nodes {
		if err := ctx.Err(); err != nil {
			return out
		}
		cur := queue[0]
		queue = queue[1:]
		nb, err := h.GetLayerNeighbors(cur, 0)
		if err != nil {
			continue
		}
		degreeSum += len(nb)
		for _, v := range nb {
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			queue = append(queue, v)
		}
	}
	out.Reachable = len(seen)
	if out.Reachable > 0 {
		out.MeanDegree = float64(degreeSum) / float64(out.Reachable)
	}

	sampleCount := 20
	if n < sampleCount {
		sampleCount = n
	}
	step := n / sampleCount
	if step == 0 {
		step = 1
	}
	for i := 0; i < sampleCount; i++ {
		targetID := startID + uint32(i*step)
		if targetID >= startID+uint32(n) {
			break
		}
		if _, ok := seen[targetID]; ok {
			out.Sampled++
		}
	}
	if sampleCount > 0 {
		out.Reachability = float64(out.Sampled) / float64(sampleCount)
	}

	targets := make([]uint32, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		targetID := startID + uint32(i*step)
		if targetID >= startID+uint32(n) {
			break
		}
		targets = append(targets, targetID)
	}
	out.MeanHops, out.IdealHops = h.measureHopDepth(ctx, ep, targets)
	return out
}
