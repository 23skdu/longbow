package core

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strconv"

	"time"

	"github.com/23skdu/longbow/internal/pq"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// BULK_INSERT_THRESHOLD defines the minimum batch size to trigger parallel bulk insert
const BULK_INSERT_THRESHOLD = 1000


const ShardedLockCount = 1024


// AddBatchBulk attempts to insert a batch of vectors in parallel using a bulk strategy.
// It assumes IDs, locations, and capacity have already been prepared/reserved.
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs any) error {
	if n <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	// Ensure nodeCount is advanced even on error/cancellation to unblock subsequent writers.
	defer func(batchSize int) {
		// Always advance to the end of the requested batch range to unblock sequential writers,
		// even if this specific batch failed or only partially committed.
		finalID := int64(startID + uint32(batchSize)) // #nosec G115
		h.commitMu.Lock()
		for h.nodeCount.Load() < int64(startID) {
			h.commitCond.Wait()
		}
		if h.nodeCount.Load() < finalID {
			h.nodeCount.Store(finalID)
			h.commitCond.Broadcast()
		}
		h.commitMu.Unlock()
	}(n)
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
				h.distFunc = h.resolveDistanceFunc()
				h.distFuncF64 = h.resolveDistanceFuncF64()
				h.distFuncF16 = h.resolveDistanceFuncF16()
				h.distFuncC64 = h.resolveDistanceFuncC64()
				h.distFuncC128 = h.resolveDistanceFuncC128()
				h.distFuncInt8 = h.resolveDistanceFuncInt8()
				h.distFuncUint8 = h.resolveDistanceFuncUint8()
				h.distFuncInt16 = h.resolveDistanceFuncInt16()
				h.distFuncUint16 = h.resolveDistanceFuncUint16()
				h.distFuncInt32 = h.resolveDistanceFuncInt32()
				h.distFuncUint32 = h.resolveDistanceFuncUint32()
				h.distFuncInt64 = h.resolveDistanceFuncInt64()
				h.distFuncUint64 = h.resolveDistanceFuncUint64()

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
	cID_start := types.ChunkID(startID)
	cID_end := types.ChunkID(maxID)

	// Pre-allocate all required chunks in a single COW operation
	data, err := h.EnsureChunks(int(cID_start), int(cID_end), dims)
	if err != nil {
		return err
	}
	// Use a private clone for the entire batch operation
	data = data.Clone()

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

	pool.ParallelFor(n, chunkSize, func(start, end int) {
		errMu.Lock()
		if errPrep != nil || ctx.Err() != nil {
			if errPrep == nil { errPrep = ctx.Err() }
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
			var vLen int
			switch vec := v.(type) {
			case []float32:
				vLen = len(vec)
			case []float16.Num:
				vLen = len(vec)
			case []int8:
				vLen = len(vec)
			case []uint8:
				vLen = len(vec)
			case []uint32:
				vLen = len(vec)
			case []int32:
				vLen = len(vec)
			case []uint16:
				vLen = len(vec)
			case []int16:
				vLen = len(vec)
			case []int64:
				vLen = len(vec)
			case []uint64:
				vLen = len(vec)
			case []float64:
				vLen = len(vec)
			case []complex64:
				vLen = len(vec)
			case []complex128:
				vLen = len(vec)
			default:
				vLen = 0 // Trigger mismatch
			}

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
							stride := h.tqEncoder.PackedSize()
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
				levelsChunk[cOff] = uint8(level) // #nosec G115
			}
		}
	})

	if errPrep != nil {
		return errPrep
	}
 
	// Create a stable, read-only version for other workers to clone from
	stableData := data.Clone()
	h.compareAndSwapData(stableData)

	// 2. Sequential Bootstrap Phase
	// Establish a stable hierarchy by inserting a portion sequentially.
	seedCount := 1024
	if n < seedCount {
		seedCount = n
	}

	for i := 0; i < seedCount; i++ {
		node := activeNodes[i]
		var err error
		data, err = h.insertInternal(node.id, node.vec, node.level, false, data)
		if err != nil {
			return err
		}
	}
	h.compareAndSwapData(data.Clone())
	if err := ctx.Err(); err != nil {
		return err
	}

	if n <= seedCount {
		return nil
	}


	// Shift to remaining nodes for parallel linkage
	activeNodes = activeNodes[seedCount:]
	n = len(activeNodes)

	// Refresh metadata after bootstrap
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Determine max level in remaining batch
	batchMaxLevel := -1
	batchEpCandidate := uint32(0)
	for _, node := range activeNodes {
		if node.level > batchMaxLevel {
			batchMaxLevel = node.level
			batchEpCandidate = node.id
		}
	}

	topL := maxL
	if batchMaxLevel > topL {
		topL = batchMaxLevel
	}

	// Current entry points for remaining active nodes. Initially global EP.
	currentEps := make([]uint32, n)
	for i := range currentEps {
		currentEps[i] = ep
	}

	// Deferred Connection Pipeline (Phase 15 Implementation)
	// ----------------------------------------------------


	// 2.5 Pre-Promote all nodes in the batch and SET VECTORS (Parallel)
	// This ensures chunks are allocated and vectors are persistent before linkage.
	pool.ParallelFor(n, (n+runtime.NumCPU()-1)/runtime.NumCPU(), func(start, end int) {
		// Vector data is already set in the previous ParallelFor using the latest h.data pointer.
		// No need to promote nodes here as chunks were pre-allocated and published.
	})

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


		// 3. Layer-by-Layer Insertion with Organic Growth
		// Divide nodes into sub-batches to ensure the graph grows organically,
		// preventing the "star graph" problem where all nodes link to the same few bootstrap nodes.
		subBatchSize := 2048
		if lc > 0 {
			subBatchSize = len(activeIndices) // Higher layers are small, process in one go
		}

		for i := 0; i < len(activeIndices); i += subBatchSize {
			endBatch := i + subBatchSize
			if endBatch > len(activeIndices) {
				endBatch = len(activeIndices)
			}
			subIndices := activeIndices[i:endBatch]

			// 3a. Search against Graph (Parallel for Sub-Batch)
			graphCandidates := make([]*[]types.Candidate, n)
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

				ctxSearch := h.searchPool.Get()
				ctxSearch.Reset()
				defer h.searchPool.PutWithMetrics(ctxSearch, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))

				for _, idx := range indices {
					node := activeNodes[idx]
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
					node := activeNodes[idx]
					if lc > node.level {
						continue
					}

					candidatesBuf := graphCandidates[idx]
					if candidatesBuf == nil {
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
						continue
					}

					m := h.m.Load()
					maxConn := h.mMax.Load()
					if lc == 0 {
						m = h.m.Load() * 2
						maxConn = h.mMax0.Load()
					}
					if m > maxConn {
						m = maxConn
					}

					slices.SortFunc(candidates, func(a, b types.Candidate) int {
						if a.Dist < b.Dist { return -1 }
						if a.Dist > b.Dist { return 1 }
						return 0
					})

					ctxPrune := h.searchPool.Get()
					ctxPrune.Reset()

					neighbors := h.selectNeighbors(ctxPrune, candidates, int(m), data)
					if len(neighbors) == 0 {
						h.searchPool.PutWithMetrics(ctxPrune, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
						continue
					}

					fSources := make([]uint32, 0, len(neighbors))
					fDists := make([]float32, 0, len(neighbors))
					for _, n := range neighbors {
						fSources = append(fSources, n.ID)
						fDists = append(fDists, n.Dist)
					}
					
					_ = h.AddConnectionsBatch(ctxPrune, data, node.id, fSources, fDists, lc, int(maxConn))

					for _, neighbor := range neighbors {
						_ = h.AddConnectionsBatch(ctxPrune, data, neighbor.ID, []uint32{node.id}, []float32{neighbor.Dist}, lc, int(maxConn))
					}

					h.searchPool.PutWithMetrics(ctxPrune, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
				}
			})

			// Update the global snapshot for organic growth so next sub-batch sees these nodes
			// Only clone if there are more sub-batches to process to avoid final redundant clone
			if i+subBatchSize < len(activeIndices) {
				h.compareAndSwapData(data.Clone())
			}
		}

		// Final layer publish
		stableLayerData := data.Clone()
		h.compareAndSwapData(stableLayerData)
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

	h.compareAndSwapData(data.Clone())

	if h.config.SQ8Enabled && h.quantizer != nil && !h.sq8Ready.Load() {
		if vecsF32, ok := vecs.([][]float32); ok {
			h.ensureTrained(int(startID)+n-1, vecsF32)
			return nil
		}
	}

	return nil
}
