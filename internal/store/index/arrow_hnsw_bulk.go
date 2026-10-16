package index

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
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

// resolveArrowBatchLayout flattens an []arrow.RecordBatch into a per-row index
// so that the parallel preparation loop can locate the owning batch of a row
// with a binary search and reuse a pre-resolved vector column index.
//
// recRowStart[k] is the first global row index of batch k (recRowStart[0] == 0);
// batches are laid out back to back in slice order. recVecCol[k] caches the
// vector column index, or -1 when it could not be resolved and the caller
// should fall back to the metadata lookup.
//
// For any other vector representation both results are nil, which makes the
// caller skip the lookup entirely.
func resolveArrowBatchLayout(vecs any) (recRowStart []int, recVecCol []int) {
	vs, ok := vecs.([]arrow.RecordBatch)
	if !ok || len(vs) == 0 {
		return nil, nil
	}

	recRowStart = make([]int, len(vs)+1)
	recVecCol = make([]int, len(vs))
	for i, rec := range vs {
		if rec != nil {
			recRowStart[i+1] = recRowStart[i] + int(rec.NumRows())
		} else {
			recRowStart[i+1] = recRowStart[i]
		}
	}
	return recRowStart, recVecCol
}

// bulkEfConstruction returns the ef to use for a bulk sub-batch insertion.
//
// Every candidate in the beam costs a full TurboQuant polar reconstruction plus
// L2 when the data type is TQ - roughly an order of magnitude more work than the
// float32 kernel - so a fixed ef that is affordable for float32 turns the build
// super-linear as the graph grows. The beam is narrowed as the graph passes the
// scale at which full-quality search stopped being affordable.
//
// The clamp floor is the layer-0 degree cap: a beam narrower than the number of
// links a node may hold cannot fill them, and an under-filled layer 0 is what
// makes a graph unreachable rather than merely inaccurate.
func (h *ArrowHNSW) bulkEfConstruction(nodeCount int) int {
	ef := int(h.efConstruction.Load())
	if h.config.AdaptiveEf {
		ef = h.getAdaptiveEf(nodeCount)
	}

	if h.config.DataType != types.VectorTypeTQ && !h.config.TurboQuantEnabled {
		return ef
	}

	switch {
	case nodeCount >= turboQuantEfFullGraph:
		ef = ef / 4
	case nodeCount >= turboQuantEfRampMid:
		ef = ef / 2
	}

	if floor := int(h.mMax0.Load()); floor > 0 && ef < floor {
		ef = floor
	}
	if ef < 1 {
		ef = 1
	}
	return ef
}

// Graph sizes at which the TurboQuant construction beam is stepped down.
const (
	turboQuantEfRampMid   = 250_000
	turboQuantEfFullGraph = 1_000_000
)

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
	// EnsureChunks is what gives the graph data its dimension, which is what
	// the TurboQuant encoder binds to. Until this runs, an index constructed
	// without a configured dimension measures every TurboQuant distance
	// against a one-element rotated query and evaluates every candidate to 0.
	h.ensureTQComponents(dims)
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

	// Resolve the Arrow batch layout once, before dispatching workers.
	//
	// The old per-row path walked the record slice linearly to find which batch
	// owns row j, then re-asked the schema for the vector column on every
	// iteration. With B batches that is O(n*B) record probes and n schema
	// lookups for a batch whose shape is fixed at entry.
	recRowStart, recVecCol := resolveArrowBatchLayout(vecs)
	if recVecCol != nil {
		for i, rec := range vecs.([]arrow.RecordBatch) {
			if rec != nil {
				recVecCol[i] = h.getVectorColumnIndex(rec)
			}
		}
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

			// Level generation, derived from the node ID so the bulk linker
			// produces the same levels whatever order the parallel prep loop
			// reaches the nodes in (roadmap Item 31).
			level := h.levelFor(id)

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
				// recRowStart is the prefix sum of the batch row counts,
				// so locating the owning batch is a binary search instead
				// of a scan, and the vector column index was resolved
				// once for the whole batch instead of per row.
				recIdx := sort.Search(len(recRowStart), func(k int) bool {
					return recRowStart[k] > j
				}) - 1
				if recIdx >= 0 && recIdx < len(vs) && vs[recIdx] != nil {
					rec := vs[recIdx]
					idx := recVecCol[recIdx]
					if idx == -1 {
						idx = h.getVectorColumnIndex(rec)
					}
					if idx != -1 {
						v = h.extractVector(rec, idx, j-recRowStart[recIdx])
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
			tqEnc := tqEncoderOf(h)
			if tqEnc != nil {
				tqChunk := data.GetVectorsTQChunk(cID)
				if tqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						code, err := tqEnc.Encode(vf32)
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
			if _, ok := h.GetLocation(id); !ok {
				h.SetLocation(types.VectorID(id), types.Location{BatchIdx: 0, RowIdx: int(id)})
			}

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

	// Populate ephemeral TQScratchArena with reconstructed float32 vectors for fast construction across all cores.
	// This eliminates redundant dequantization during HNSW graph linkage across all batches.
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

	// Deferred Connection Pipeline (Phase 15 Implementation)
	// ----------------------------------------------------

	// 2.5 Pre-Promote all nodes in the batch and SET VECTORS (Parallel)
	// This ensures chunks are allocated and vectors are persistent before linkage.
	pool.ParallelFor(numRemaining, (numRemaining+runtime.NumCPU()-1)/runtime.NumCPU(), func(start, end int) {
		// Vector data is already set in the previous ParallelFor using the latest h.data pointer.
		// No need to promote nodes here as chunks were pre-allocated and published.
	})

	// linkOp is one cross-node edge waiting for P2: the list it points at, the
	// node that will be added to it, and the distance to record.
	type linkOp struct {
		target  uint32
		src     uint32
		dist    float32
		maxConn int32
	}

	// Counting-sort cursors and the in-degree snapshot backing store for the
	// linkage passes below. All three are sized for this batch and reused by
	// every sub-batch of every layer: P2 leaves the cursors zeroed, so there is
	// nothing to clear between sub-batches (roadmap Item 33).
	//
	// nodeCount has not been updated for this batch yet, so nodeCount+n is the
	// widest node id it can produce.
	linkageCounts := make([]int, int(h.nodeCount.Load())+numRemaining+1)
	linkageCursors := make([]int, len(linkageCounts))
	var linkageInDegree inDegreeSnapshot

	for lc := topL; lc >= 0; lc-- {
		// Honour cancellation between layers: a single layer over a large
		// batch can run for minutes, so the caller must not be stuck waiting
		// on a deadline that has already passed.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Identify nodes active at this layer: all nodes participate in either
		// descent (ef=1 when lc > node.level) to update currentEps, or insertion (when lc <= node.level).
		activeIndices := make([]int, numRemaining)
		for i := range activeIndices {
			activeIndices[i] = i
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

		// edgeChunks holds one growth buffer per worker chunk for the linkage
		// passes, reused across sub-batches by resetting each to length zero.
		// The cross-node edges of a sub-batch are recorded there before P2
		// sorts them by target; at 100k, reallocating those buffers per
		// sub-batch was 178 MB of the ingest's heap traffic (roadmap Item 33).
		// Sized from this layer's widest sub-batch - the chunk count is
		// ceil(K/chunk) and chunk is never below 16 - so no sub-batch ever has
		// to grow it.
		edgeChunks := make([][]linkOp, (subBatchSize+15)/16)

		// Candidate lists produced by the graph search of the current sub-batch,
		// indexed by position within remainingNodes.
		//
		// Allocated once for the whole batch rather than once per sub-batch per
		// layer. At 1M vectors the per-sub-batch shape allocated and zeroed an
		// 8 MB pointer array ~244 times per layer, which is several GB of pure
		// memset for a structure that only ever holds subBatchSize live
		// entries. Entries outside the current sub-batch are stale and are
		// cleared as the sub-batch is dispatched.
		graphCandidates := make([]*[]types.Candidate, numRemaining)

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
			var errLayer error
			var layerMu sync.Mutex

			layerChunkSize := (len(subIndices) + runtime.NumCPU() - 1) / runtime.NumCPU()
			if layerChunkSize < 32 {
				layerChunkSize = 32
			}

			// releaseCandidateLists returns this sub-batch's search results to the
			// candidate pool and clears the slots so the next sub-batch repopulates
			// them. searchLayerForInsert hands out pooled slices, so without this
			// every node of every layer leaked one to the GC.
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
						ef := h.bulkEfConstruction(int(h.nodeCount.Load()))

						res, err := h.searchLayerForInsert(ctx, ctxSearch, node.vec, currEp, ef, lc, workerData)
						if err != nil {
							layerMu.Lock()
							errLayer = err
							layerMu.Unlock()
							return
						}
						// searchLayerForInsert already returns a pooled slice; the old
						// code copied it into a second, unpooled slice per node per
						// layer, which meant one candidate slice leaked to the GC
						// for every node in every layer of every bulk build.
						graphCandidates[idx] = &res

						if len(res) > 0 {
							currentEps[idx] = res[0].ID
						}
					}
				}
			})

			if errLayer != nil {
				return errLayer
			}

			// 3b. Linkage, in three passes (roadmap Item 31).
			//
			// This used to be one parallel loop that decided each node's
			// neighbours and wrote them at the same time. Every node writes
			// reverse edges into its *neighbours'* lists, so two goroutines
			// landed on the same list, and a list over its connection limit is
			// pruned by a heuristic that depends on the order the edges arrived
			// in. Which edges survived - and so the whole graph, and every
			// answer read from it - depended on the scheduler: two builds of one
			// corpus with one seed produced two different graphs.
			//
			// The passes give every adjacency list exactly one writer at a time,
			// in an order that is a function of the sub-batch alone:
			//
			//	P1  decide against the frozen graph and write only this node's
			//	    own list (the back half of a chain link and the forward set).
			//	    The targets are disjoint, so the pass stays parallel.
			//	P2  apply the cross-node edges - the forward half of each chain
			//	    link and every reverse link - counting-sorted by target, so
			//	    one goroutine owns one list, and in sub-batch order within
			//	    it. That also removes the CAS contention the interleaved
			//	    version spent its time on: the measured cost of dropping the
			//	    parallelism outright was +12% on a 100k bulk build.
			//	P3  run the inbound guard, which decides from the in-degree P2
			//	    just wrote, so it runs after P2 and in node order.
			//
			// Selection reads only the candidate list and the vectors, which
			// neither P1 nor P2 touches, so P3 recovers a node's forward set by
			// selecting again rather than P1 carrying it out of the pooled
			// buffers.
			K := len(subIndices)
			linkageChunkSize := (K + runtime.NumCPU() - 1) / runtime.NumCPU()
			if linkageChunkSize < 16 {
				linkageChunkSize = 16
			}

			// One edge buffer per worker chunk: P1 appends to its own without a
			// lock, and concatenating the buffers in chunk order gives a total
			// order that depends on the sub-batch, not on the schedule.
			numChunks := (K + linkageChunkSize - 1) / linkageChunkSize
			if numChunks > len(edgeChunks) {
				// Sized from subBatchSize, which is the widest sub-batch this
				// layer runs, so this cannot happen; reallocating would still
				// be correct, only wasteful.
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
					candidatesBuf := graphCandidates[idx]
					if candidatesBuf == nil {
						h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					allCandidates := *candidatesBuf
					// Filter out the node itself into a pooled buffer. The old
					// `var candidates []types.Candidate` + append grew from nil,
					// costing log2(len) reallocations per node per layer.
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

					// R5: Gate chain link on proximity (chainDist <= median(candidates)),
					// avoiding artificial long-range edges on shuffled data while preserving
					// connectivity on collinear/ordered data.
					//
					// The single-element source/distance slices are pooled: this
					// used to allocate two of them per node on top of the two
					// more it allocated for every reverse link.
					linkSrc := append(ctxLink.linkSrc[:0], node.id)
					ctxLink.linkSrc = linkSrc[:0]
					linkDist := append(ctxLink.linkDist[:0], float32(0))
					ctxLink.linkDist = linkDist[:0]
					if lc == 0 && node.id > 0 && bulkChainLinksEnabled {
						h.computeDistances(ctxLink, data, node.id-1, linkSrc, linkDist)
						medianDist := candidates[len(candidates)/2].Dist
						if linkDist[0] <= medianDist {
							// The forward half points at the predecessor's list, which
							// this node does not own: hand it to P2. The values are
							// copied out here because backSrc below writes over the
							// buffer linkSrc shares.
							ops = append(ops, linkOp{
								target:  node.id - 1,
								src:     node.id,
								dist:    linkDist[0],
								maxConn: int32(h.mMax0.Load()), // #nosec G115 -- layer connection limit, not a row count
							})
							// The back half points at this node's own list, so P1
							// applies it here, ahead of the forward set, as before.
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

					// Record rather than apply: the reverse edge points at a
					// neighbour's list, and both which neighbour and in what
					// order must not depend on who ran first, so P2 applies them.
					// fSources/fDists are the private copies the comment above
					// describes, so they survive the AddConnectionsBatch above.
					for i, nID := range fSources {
						ops = append(ops, linkOp{
							target:  nID,
							src:     node.id,
							dist:    fDists[i],
							maxConn: int32(maxConn), // #nosec G115 -- layer connection limit, not a row count
						})
					}

					h.searchPool.PutWithMetrics(ctxLink, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
				}
			})

			if errLayer != nil {
				return errLayer
			}

			// P2: apply the cross-node edges. The scatter below walks the chunks
			// in order, so a list that receives edges from several nodes
			// receives them in sub-batch order however P1 was scheduled, and the
			// runs then hand each list to exactly one goroutine.
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
					return err
				}
				if uint64(maxTarget) >= uint64(len(linkageCounts)) {
					// Only reachable if a target id escapes the sizing above;
					// grow rather than index past the end.
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
						// Leave the counts zeroed for the next sub-batch.
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
				// The pruning guard decides from inbound-edge counts, and P2 is
				// the one pass that prunes a list P1 has already written to.
				// Reading the live tracker from these goroutines would make the
				// survivors depend on how far the other goroutines had got, so
				// the guard reads the counts as they stood when P1 finished.
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
					return errLayer
				}
			}

			// P3: R26 - guarantee that node.id has at least one inbound edge in
			// layer 0. If all reverse links were pruned away by saturated hosts,
			// link from the nearest geometric neighbours. It reads the in-degree
			// P2 just wrote and decides from it, so it runs afterwards and in node
			// order; running it earlier could add an inbound edge that a later
			// prune then removed.
			if lc == 0 && InboundEdgeGuardEnabled {
				if err := ctx.Err(); err != nil {
					return err
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

			runtime.KeepAlive(data) // Keep GraphData alive during blocking parallel linkage

			// The sub-batch is linked; its search results are dead. Return them
			// before the next sub-batch repopulates the same slots.
			releaseCandidateLists()

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

// bulkHopFloor is the smallest hop budget the gate will apply, in mean layer-0
// descent hops.
//
// The budget is log(N)/log(MMax0) times BulkGraphMaxHopDepth, and that estimate
// degenerates on small graphs: at 2,000 nodes with MMax0=64 it is 1.8 hops, so
// the gate rejects anything past 2.7 - including a perfectly navigable graph
// measured at 3.0, which is what it did to TestArrowHNSW_ConcurrentAddBatch_-
// Int8_2Batches. The gate's remedy is a sequential rebuild of the whole batch,
// and sequential insertion with MMax0=64 runs the O(m^2) diversity heuristic per
// node, so a 0.3-hop overshoot on a 2,000-node batch turned a sub-second build
// into one that did not finish inside a five-minute deadline under -race. That
// is the same trade BulkGraphQualityMinDegreeRatio documents and the reason it
// is off by default.
//
// A floor does not weaken the gate where it matters: at 100k with MMax0=16 the
// estimate is 4.1 hops and the budget stays above the floor, and the graphs this
// gate was built to catch are caught by the reachability floor, not by hop
// depth. Measured on the pre-a955a0c1 TurboQuant graph - the defect the gate
// exists for - the hop check passed (2.6 against a 6.0 budget) and only
// reachability (8.2% against an 80% floor) fired.
const bulkHopFloor = 4.0

// bulkHopBudget is the mean descent depth a batch of this size is allowed.
func bulkHopBudget(idealHops float64) float64 {
	budget := idealHops * BulkGraphMaxHopDepth
	if budget < bulkHopFloor {
		return bulkHopFloor
	}
	return budget
}

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
	enforce := bulkGraphQualityGuardEnabled && h.inAddBatch.Load() <= 1
	minDegree := float64(h.mMax0.Load()) * BulkGraphQualityMinDegreeRatio
	maxHops := bulkHopBudget(q.IdealHops)
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
	// The walk is bounded by how many nodes the graph has, not by where this
	// batch ends. Concurrent AddBatch calls reserve their ids before the bulk
	// link releases bulkMu, so by the time a batch is measured another one may
	// already have published thousands of nodes past startID+n; bounding by the
	// batch end then stops the BFS part-way through the graph and reports the
	// nodes it happened to reach first as if they were all of them. That is how
	// a healthy int8 build reported "reachable=5024/5000 sampled=30.0%" and
	// tripped the gate into a full sequential rebuild.
	graphNodes := int(h.nodeCount.Load())
	batchEnd := int(startID) + n
	if graphNodes < batchEnd {
		graphNodes = batchEnd
	}
	out := bulkGraphQuality{Nodes: graphNodes}
	ep := h.entryPoint.Load()
	if ep == math.MaxUint32 {
		return out
	}

	seen := make(map[uint32]struct{}, graphNodes)
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
	if out.Reachable > out.Nodes {
		// The walk stops on a bound check, so the last node processed can still
		// enqueue neighbours past it. Never report more reachable nodes than
		// the graph is said to have: a ratio over 100% is the kind of number
		// that stops anyone reading the line that follows it.
		out.Nodes = out.Reachable
	}
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
		targetID := startID + uint32(i*step) // #nosec G115
		if targetID >= startID+uint32(n) {   // #nosec G115
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
		targetID := startID + uint32(i*step) // #nosec G115
		if targetID >= startID+uint32(n) {   // #nosec G115
			break
		}
		targets = append(targets, targetID)
	}
	out.MeanHops, out.IdealHops = h.measureHopDepth(ctx, ep, targets)
	return out
}
