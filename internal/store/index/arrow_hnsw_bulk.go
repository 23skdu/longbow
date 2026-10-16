package index

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

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

// Graph sizes at which the TurboQuant construction beam is stepped down.
const (
	turboQuantEfRampMid   = 250_000
	turboQuantEfFullGraph = 1_000_000
)

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
	h.ensureTQComponents(dims)

	original := data
	original.AcquireReader()
	clone := original.Clone()
	original.ReleaseReader()
	data = clone

	// Ingest vectors and generate active nodes in parallel
	activeNodes, err := h.extractAndIngestBatchVectors(ctx, startID, n, dims, vecs, data)
	if err != nil {
		return err
	}

	// Create a stable, read-only version for other workers to clone from
	stableData := data.Clone()
	h.compareAndSwapData(h.data.Load(), stableData)

	// 2. Sequential Bootstrap Phase
	seedCount := BulkInsertThreshold
	if n < seedCount {
		seedCount = n
	}

	data, _, _, err = h.bootstrapSequential(ctx, startID, n, seedCount, activeNodes, data)
	if err != nil {
		return err
	}
	if n <= seedCount {
		return nil
	}

	// 3. Parallel Linkage Phase
	data, batchMaxLevel, batchEpCandidate, err := h.linkRemainingNodes(ctx, startID, n, seedCount, activeNodes, data)
	if err != nil {
		return err
	}

	// 4. Update Global Stats atomically
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

	h.syncBatchToGPU(startID, totalN, vecs)

	runtime.KeepAlive(data)
	return nil
}
