package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// Ensure ArrowHNSW implements VectorIndex
var _ VectorIndex = (*ArrowHNSW)(nil)

// AddByLocation implements VectorIndex.
// It assigns a new VectorID, records the location, and inserts into the graph.
func (h *ArrowHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	// Check context
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// 1. Generate new VectorID AND Store location
	// ChunkedLocationStore.Append handles atomic ID generation and storage
	vecID := h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	id := uint32(vecID)

	// 2. (Location stored in step 1)

	// 3. Generate Random Level
	level := h.generateLevel()

	// 4. Insert into Graph
	if err := h.Insert(id, level); err != nil {
		return 0, err
	}

	return id, nil
}

// generateLevel chooses a random level for a new node.
func (h *ArrowHNSW) generateLevel() int {
	// -ln(uniform(0,1)) * ml
	return int(math.Floor(-math.Log(rand.Float64()) * h.ml))
}

// AddByRecord implements VectorIndex.
func (h *ArrowHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return h.AddByLocation(ctx, batchIdx, rowIdx)
}

// NewArrowHNSW creates a new HNSW index backed by Arrow records.
// If locationStore is nil, a new one is created.
func NewArrowHNSW(dataset *Dataset, config ArrowHNSWConfig, locStore *ChunkedLocationStore) *ArrowHNSW { //nolint:gocritic
	// Merge with defaults if partially initialized
	defaults := DefaultArrowHNSWConfig()
	if config.M == 0 {
		config.M = defaults.M
	}
	if config.MMax == 0 {
		config.MMax = defaults.MMax
	}
	if config.MMax0 == 0 {
		config.MMax0 = defaults.MMax0
	}
	if config.EfConstruction == 0 {
		config.EfConstruction = defaults.EfConstruction
	}
	if config.Metric == "" {
		config.Metric = defaults.Metric
	}
	if config.DataType == VectorTypeUnknown {
		config.DataType = defaults.DataType
	}

	dsName := "default"
	if dataset != nil {
		dsName = dataset.Name
	}

	// NEW: Auto-infer DataType and Dims if they are unknown or 0 and dataset is available
	if dataset != nil && dataset.Schema != nil {
		if config.DataType == VectorTypeUnknown || config.DataType == VectorTypeFloat32 {
			colName := "vector"
			config.DataType = InferVectorDataType(dataset.Schema, colName)
		}
		if config.Dims == 0 {
			// Extract Dims from schema
			for _, field := range dataset.Schema.Fields() {
				colName := "vector"
				if field.Name == colName {
					if listType, ok := field.Type.(*arrow.FixedSizeListType); ok {
						config.Dims = int(listType.Len())
						if config.DataType == VectorTypeComplex64 || config.DataType == VectorTypeComplex128 {
							// For complex types, the logical dimension is half the physical dimension
							config.Dims /= 2
						}
						break
					}
				}
			}
		}
	}

	h := &ArrowHNSW{
		dataset: dataset,
		config:  config,
		m:       config.M,
		mMax:    config.MMax,
		mMax0:   config.MMax0,
		// efConstruction initialized below
		ml:           1.0 / math.Log(float64(config.M)),
		deleted:      query.NewAtomicBitset(), // Initial capacity, grows
		metric:       config.Metric,
		vectorColIdx: -1,

		// Initialize Cached Metrics
		metricInsertDuration:     metrics.HNSWInsertDurationSeconds,
		metricNodeCount:          metrics.HNSWNodesTotal.WithLabelValues(dsName),
		metricGraphHeight:        metrics.HnswGraphHeight.WithLabelValues(dsName),
		metricActiveReaders:      metrics.HnswActiveReaders.WithLabelValues(dsName),
		metricLockWait:           metrics.IndexLockWaitDuration.MustCurryWith(prometheus.Labels{"dataset": dsName}),
		metricBulkInsertDuration: metrics.HNSWBulkInsertDurationSeconds,
		metricBulkVectors:        metrics.HNSWBulkVectorsProcessedTotal,
		metricBQVectors:          metrics.BQVectorsTotal.WithLabelValues(dsName),
		metricBitmapEntries:      metrics.HNSWBitmapIndexEntriesTotal.WithLabelValues(dsName),
		metricBitmapFilterDelta:  metrics.HNSWBitmapFilterDurationSeconds.WithLabelValues(dsName),
		metricEarlyTermination:   metrics.HNSWSearchEarlyTerminationsTotal,

		bitmapIndex: NewBitmapIndex(),
	}
	h.efConstruction.Store(int32(config.EfConstruction))

	h.distFunc = h.resolveDistanceFunc()
	h.distFuncF16 = h.resolveDistanceFuncF16()
	h.distFuncF64 = h.resolveDistanceFuncF64()
	h.distFuncC64 = h.resolveDistanceFuncC64()
	h.distFuncC128 = h.resolveDistanceFuncC128()
	h.batchDistFunc = h.resolveBatchDistanceFunc()

	// Initialize cache-aligned sharded mutex for reduced contention
	h.shardedLocks = NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards:      ShardedLockCount,
		EnableAdaptive: false,
	})

	if locStore != nil {
		h.locationStore = locStore
	} else {
		h.locationStore = NewChunkedLocationStore()
	}

	// Initialize with empty graph data
	// Start with reasonable capacity
	initialCap := config.InitialCapacity
	if initialCap <= 0 {
		initialCap = 1024
	}

	gd := NewGraphData(initialCap, config.Dims, config.SQ8Enabled, config.PQEnabled, config.PQM, config.BQEnabled, config.Float16Enabled, config.PackedAdjacencyEnabled, config.DataType)
	if dataset != nil && dataset.DiskStore != nil {
		gd.DiskStore = dataset.DiskStore
	}
	h.data.Store(gd)
	h.backend.Store(gd)
	if config.Dims > 0 {
		h.dims.Store(int32(config.Dims))
	}

	// Pre-allocate pools
	h.searchPool = NewArrowSearchContextPool()
	// h.dims is 0 here usually, unless passed differently? No, NewArrowHNSW signature doesn't take dims.
	// But it might be updated later? BatchDistanceComputer needs fixed dims?
	// It copies dims. If dims is 0, it might need re-init later?
	// For now, just load it.
	h.batchComputer = NewBatchDistanceComputer(memory.DefaultAllocator, int(h.dims.Load()))

	// Detect vector column index
	if dataset != nil && dataset.Schema != nil {
		for i, field := range dataset.Schema.Fields() {
			if field.Name == "vector" {
				h.vectorColIdx = i
				break
			}
		}
	}

	// Initialize Repair Agent (disabled by default)
	h.repairAgent = NewRepairAgent(h, DefaultRepairAgentConfig())

	return h
}

// AddBatch implements VectorIndex.
func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := len(rowIdxs)
	if n == 0 {
		return nil, nil
	}

	// 1. Prepare Locations and Reserve IDs
	// Use BatchAppend to lock once and reserve a contiguous block of IDs
	locs := make([]Location, n)
	for i := 0; i < n; i++ {
		locs[i] = Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}
	}

	startID := h.locationStore.BatchAppend(locs)

	// 2. Pre-grow graph to ensure capacity for all new items
	// This avoids "Stop-the-World" pauses during parallel insertion
	dims := int(h.dims.Load())
	if dims == 0 && n > 0 {
		// Resolve dimensions from first vector in the batch
		var rec arrow.RecordBatch
		if len(recs) == n {
			rec = recs[0]
		} else {
			if len(batchIdxs) > 0 && batchIdxs[0] < len(recs) {
				rec = recs[batchIdxs[0]]
			} else if len(recs) > 0 {
				rec = recs[0]
			}
		}

		if rec != nil {
			v, err := ExtractVectorFromArrow(rec, rowIdxs[0], h.vectorColIdx)
			if err == nil && v != nil {
				dims = len(v)
				h.dims.Store(int32(dims))
			}
		}
	}

	finalSize := int(startID) + n
	h.Grow(finalSize, dims)

	ids := make([]uint32, n)

	// Check for Bulk Optimized Path (All types supported)
	if n >= BULK_INSERT_THRESHOLD {
		useDirectIndex := len(recs) == n

		// Prepare typed slice
		var vecs any

		switch h.config.DataType {
		case VectorTypeFloat32:
			vecsF32 := make([][]float32, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				// Use Generic extraction which is safe and efficient
				// We expect float32
				v, e := ExtractVectorGeneric[float32](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsF32[i] = v
			}
			vecs = vecsF32

		case VectorTypeFloat16:
			vecsF16 := make([][]float16.Num, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				v, e := ExtractVectorGeneric[float16.Num](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsF16[i] = v
			}
			vecs = vecsF16

		case VectorTypeInt8:
			vecsInt8 := make([][]int8, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				v, e := ExtractVectorGeneric[int8](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsInt8[i] = v
			}
			vecs = vecsInt8

		case VectorTypeFloat64:
			vecsF64 := make([][]float64, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				v, e := ExtractVectorGeneric[float64](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsF64[i] = v
			}
			vecs = vecsF64

		case VectorTypeComplex64:
			vecsC64 := make([][]complex64, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				v, e := ExtractVectorGeneric[complex64](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsC64[i] = v
			}
			vecs = vecsC64

		case VectorTypeComplex128:
			vecsC128 := make([][]complex128, n)
			for i := 0; i < n; i++ {
				rec := getRecordBatch(i, recs, batchIdxs, useDirectIndex)
				v, e := ExtractVectorGeneric[complex128](rec, rowIdxs[i], h.vectorColIdx)
				if e != nil {
					return nil, e
				}
				vecsC128[i] = v
			}
			vecs = vecsC128

		default:
			// Fallback for unknown types or unoptimized types: use float32 conversion if possible?
			// Or just skip bulk insert?
			// Current AddBatchBulk handles generic errors.
			// Let's force skip if type not handled above to assume standard serial insert.
			// But since we want to optimize, maybe log warning?
			// Actually, if we skip here, it falls through to standard insert (Step 4).
			vecs = nil
		}

		if vecs != nil {
			// Chunking for Bulk Insert to avoid O(N^2) memory explosion
			const bulkChunkSize = 2500 // Limit to ~25MB matrix (2500^2 * 4) + overhead

			for chunkStart := 0; chunkStart < n; chunkStart += bulkChunkSize {
				chunkEnd := chunkStart + bulkChunkSize
				if chunkEnd > n {
					chunkEnd = n
				}
				chunkN := chunkEnd - chunkStart

				// Slice the generic vecs
				var chunkVecs any
				switch v := vecs.(type) {
				case [][]float32:
					chunkVecs = v[chunkStart:chunkEnd]
				case [][]float16.Num:
					chunkVecs = v[chunkStart:chunkEnd]
				case [][]int8:
					chunkVecs = v[chunkStart:chunkEnd]
				case [][]float64:
					chunkVecs = v[chunkStart:chunkEnd]
				case [][]complex64:
					chunkVecs = v[chunkStart:chunkEnd]
				case [][]complex128:
					chunkVecs = v[chunkStart:chunkEnd]
				}

				// Calculate chunk startID
				chunkStartID := uint32(startID) + uint32(chunkStart)

				if err := h.AddBatchBulk(ctx, chunkStartID, chunkN, chunkVecs); err != nil {
					return nil, err
				}
			}

			// Populate returned IDs
			for i := 0; i < n; i++ {
				ids[i] = uint32(startID) + uint32(i)
			}
			return ids, nil
		}
	}

	// 3. Preemptively Train SQ8 if needed
	// This avoids race conditions and lock contention during parallel insert.
	if h.config.SQ8Enabled && !h.sq8Ready.Load() {
		threshold := h.config.SQ8TrainingThreshold
		if threshold <= 0 {
			threshold = 1000
		}

		var samples [][]float32
		count := 0

		// Extract samples from the batch
		for _, rec := range recs {
			idx := h.vectorColIdx
			if idx < 0 {
				indices := rec.Schema().FieldIndices("vector")
				if len(indices) > 0 {
					idx = indices[0]
					// Cache it? No, unsafe to write concurrently without lock if h is shared.
					// But we are in AddBatch.
				}
			}

			if int64(idx) < 0 || int64(idx) >= rec.NumCols() {
				continue
			}
			col := rec.Column(idx)
			// Assuming FixedSizeList of Float32
			if listArr, ok := col.(*array.FixedSizeList); ok {
				if floatArr, ok := listArr.ListValues().(*array.Float32); ok {
					dims := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
					rawValues := floatArr.Float32Values()

					for i := 0; i < listArr.Len(); i++ {
						if listArr.IsValid(i) {
							off := i * dims
							if off+dims <= len(rawValues) {
								vec := make([]float32, dims)
								copy(vec, rawValues[off:off+dims])
								samples = append(samples, vec)
								count++
								if count >= threshold {
									break
								}
							}
						}
					}
				}
			}
			if count >= threshold {
				break
			}
		}

		// Train and backfill up to startID-1
		// Existing vectors (0 to startID-1) will be backfilled.
		// New vectors (startID to end) will be inserted by workers using the trained quantizer.
		if len(samples) > 0 {
			h.ensureTrained(int(startID)-1, samples)
		}
	}

	// 4. Parallel Insert
	// Writes to vector chunks are disjoint based on ID, so parallel execution is safe.
	// Chunk allocation (ensureChunk) handles concurrency via atomic CAS.
	// SQ8/BQ race conditions avoided by preemptive training (above) or thread-local buffers.

	// Parallel insertion for non-SQ8 indices
	// We bypass AddByLocation as we already have ID and Capacity
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for i := 0; i < n; i++ {
		i := i
		// Calculate pre-assigned ID
		id := uint32(startID) + uint32(i)
		ids[i] = id

		// 5. Index Metadata (if enabled)
		if len(h.config.IndexedColumns) > 0 {
			// Find record and row offset
			// recs might be multiple batches. rowIdxs are absolute into those batches?
			// Actually, AddBatch documentation says rowIdxs and batchIdxs are provided.
			// The loop uses startID, which is contiguous.
			// Let's assume h.indexMetadata handles everything.
			h.indexMetadata(id, recs, i, rowIdxs, batchIdxs)
		}

		g.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Generate random level for the node
			// Note: Global rand is thread-safe but contended.
			// For extremely high throughput, thread-local rand is better.
			level := h.generateLevel()

			// Insert into the graph
			// Insert is thread-safe and now mostly lock-free (except for sharded locks)
			if err := h.Insert(id, level); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return ids, nil
}

// SearchVectors implements VectorIndex.
func (h *ArrowHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	// Post-filtering with Adaptive Expansion
	initialFactor := 10
	retryFactor := 50
	maxRetries := 1
	q := queryVec // Alias to avoid shadowing package query

	// Track throughput by dimension
	dim := 0
	switch v := q.(type) {
	case []float32:
		dim = len(v)
	case []float16.Num:
		dim = len(v)
	case []complex64, []complex128:
		// approximations
		dim = 0 // handled inside resolveHNSWComputer
	}
	dimBucket := "other"
	if dim == 128 || dim == 256 || dim == 384 || dim == 768 || dim == 1024 || dim == 1536 || dim == 3072 {
		dimBucket = fmt.Sprintf("%d", dim)
	} else if dim > 3072 {
		dimBucket = ">3072"
	}
	metrics.HnswSearchThroughputDims.WithLabelValues(dimBucket).Inc()

	limit := k
	qBitset := (*query.Bitset)(nil)

	if len(filters) > 0 && h.bitmapIndex != nil {
		// Try to build a pre-filter bitset from the bitmap index
		criteria := make(map[string]string)
		allSimple := true
		for _, f := range filters {
			isIndexed := false
			for _, col := range h.config.IndexedColumns {
				if col == f.Field {
					isIndexed = true
					break
				}
			}

			if (f.Operator == "=" || f.Operator == "==") && isIndexed {
				criteria[f.Field] = f.Value
			} else {
				allSimple = false
				break
			}
		}

		if allSimple {
			startFilter := time.Now()
			bm, err := h.bitmapIndex.Filter(criteria)
			if err == nil && bm != nil {
				h.metricBitmapFilterDelta.Observe(time.Since(startFilter).Seconds())
				qBitset = query.NewBitsetFromRoaring(bm)
				defer qBitset.Release()
				// If we have a bitset, we can skip post-filtering if we trust the bitset completely.
				// For HNSW, we pass it to Search which applies it during traversal.
			}
		}
	}

	if len(filters) > 0 && qBitset == nil {
		limit = k * initialFactor
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		ef := limit + 100

		// Pass qBitset if we matched simple criteria
		candidates, err := h.Search(ctx, q, limit, ef, qBitset)
		if err != nil {
			return nil, err
		}

		// Filter candidates (only if we didn't use qBitset or if there's complex logic)
		var res []SearchResult
		if len(filters) > 0 && qBitset == nil {
			// (Existing post-filtering logic)
			// TODO: Handle multiple batches. Currently binding to Batch 0.
			if h.dataset == nil || len(h.dataset.Records) == 0 {
				return nil, fmt.Errorf("dataset empty during filter")
			}

			evaluator, err := query.NewFilterEvaluator(h.dataset.Records[0], filters)
			if err != nil {

				return nil, err
			}

			res = make([]SearchResult, 0, len(candidates))

			// Support filtering across multiple batches
			evaluators := make(map[int]*query.FilterEvaluator)
			evaluators[0] = evaluator

			for _, candle := range candidates {
				loc, ok := h.locationStore.Get(VectorID(candle.ID))
				if !ok {

					continue
				}

				// Get or create evaluator for this batch
				ev, ok := evaluators[loc.BatchIdx]
				if !ok {
					if loc.BatchIdx >= len(h.dataset.Records) {

						continue
					}
					ev, err = query.NewFilterEvaluator(h.dataset.Records[loc.BatchIdx], filters)
					if err != nil {

						continue
					}
					evaluators[loc.BatchIdx] = ev
				}

				matches := ev.Matches(loc.RowIdx)

				if matches {

					res = append(res, candle)
				}
			}

		} else {
			res = candidates
		}

		if len(res) >= k || attempt == maxRetries || len(filters) == 0 {
			// Truncate to k
			if len(res) > k {
				res = res[:k]
			}

			if options.IncludeVectors {
				h.extractVectors(res, options.VectorFormat)
			}
			return res, nil
		}

		// Retry with larger limit
		limit = k * retryFactor
	}

	return nil, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *query.Bitset, options SearchOptions) []SearchResult {
	ef := k + 100
	// Calls h.Search which returns ([]SearchResult, error)
	// The interface signature returns only []SearchResult
	res, _ := h.Search(ctx, q, k, ef, filter)
	return res
}

// GetLocation implements VectorIndex.
func (h *ArrowHNSW) GetLocation(id VectorID) (Location, bool) {
	return h.locationStore.Get(id)
}

// GetVectorID implements VectorIndex.
// It returns the ID for a given location using the reverse index.
func (h *ArrowHNSW) GetVectorID(loc Location) (VectorID, bool) {
	return h.locationStore.GetID(loc)
}

// SetLocation allows manually setting the location for a vector ID.
// This is used by ShardedHNSW to populate shard-local location stores for filtering.
func (h *ArrowHNSW) SetLocation(id VectorID, loc Location) {
	h.locationStore.EnsureCapacity(id)
	h.locationStore.Set(id, loc)
	h.locationStore.UpdateSize(id)
}

// GetNeighbors returns the nearest neighbors for a given vector ID from the graph
// at the base layer (Layer 0).
func (h *ArrowHNSW) GetNeighbors(id VectorID) ([]VectorID, error) {
	data := h.data.Load()
	if int(id) >= int(h.nodeCount.Load()) {
		return nil, fmt.Errorf("vector ID %d out of range", id)
	}

	// Layer 0 neighbors
	layer := 0

	cID := chunkID(uint32(id))
	cOff := chunkOffset(uint32(id))

	countsChunk := data.GetCountsChunk(layer, cID)
	if countsChunk == nil {
		return nil, fmt.Errorf("chunk %d not allocated", cID)
	}
	count := int(atomic.LoadInt32(&countsChunk[cOff]))
	if count == 0 {
		return []VectorID{}, nil
	}

	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if neighborsChunk == nil {
		return nil, fmt.Errorf("neighbors chunk %d missing", cID)
	}
	baseIdx := int(cOff) * MaxNeighbors

	results := make([]VectorID, count)
	chunk := neighborsChunk
	for i := 0; i < count; i++ {
		results[i] = VectorID(chunk[baseIdx+i])
	}

	return results, nil
}

// Len implements VectorIndex.
func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// GetDimension implements VectorIndex.
func (h *ArrowHNSW) GetDimension() uint32 {
	return uint32(h.dims.Load())
}

// SetDimension updates the dimension and re-initializes the batch computer.
func (h *ArrowHNSW) SetDimension(dim int) {
	if dim <= 0 {
		return
	}
	if h.dims.CompareAndSwap(0, int32(dim)) {
		h.batchComputer = NewBatchDistanceComputer(memory.DefaultAllocator, dim)
		// Ensure backend data also has the correct dimension set
		data := h.data.Load()
		if data.Dims == 0 {
			h.Grow(data.Capacity, dim)
		}
	}
}

// PreWarm ensures the index has capacity and allocated chunks for targetSize vectors.
// This reduces "cold start" latency during initial ingestion by avoiding lazy allocation.
func (h *ArrowHNSW) PreWarm(targetSize int) {
	if targetSize <= 0 {
		return
	}

	dims := int(h.dims.Load())
	h.Grow(targetSize, dims)

	data := h.data.Load()
	if data == nil {
		return
	}

	// Calculate number of chunks needed
	// ChunkSize is constant (1024)
	numChunks := (targetSize + ChunkSize - 1) / ChunkSize

	// Pre-allocate all chunks
	// We iterate up to numChunks. ensureChunk handles double-checked locking.
	for i := 0; i < numChunks; i++ {
		// cID is uint32
		h.ensureChunk(data, uint32(i), 0, dims)
	}
}

// SetEfConstruction updates the efConstruction parameter dynamically.
func (h *ArrowHNSW) SetEfConstruction(ef int) {
	h.efConstruction.Store(int32(ef))
}

// Warmup implements VectorIndex.
func (h *ArrowHNSW) Warmup() int {
	// Defaults to current size, just ensuring all are allocated
	size := int(h.nodeCount.Load())
	if size > 0 {
		h.PreWarm(size)
	}
	return size
}

// SetIndexedColumns implements VectorIndex.
func (h *ArrowHNSW) SetIndexedColumns(cols []string) {
	// No-op for now
}

// RemapFromBatchInfo efficiently updates locations based on batch movements.
// It iterates all locations in the store and updates them if they belong to moved batches.
func (h *ArrowHNSW) RemapFromBatchInfo(remapping map[int]BatchRemapInfo) error {
	// Iterate mutable allows us to update atomic entries directly
	h.locationStore.IterateMutable(func(id VectorID, val *atomic.Uint64) {
		currentVal := val.Load()
		loc := unpackLocation(currentVal)

		if info, ok := remapping[loc.BatchIdx]; ok {
			// This location was in a moved/modified batch
			if loc.RowIdx < len(info.NewRowIdxs) {
				newRowIdx := info.NewRowIdxs[loc.RowIdx]
				if newRowIdx != -1 {
					// Update location
					newLoc := Location{
						BatchIdx: info.NewBatchIdx,
						RowIdx:   newRowIdx,
					}
					val.Store(packLocation(newLoc))
				} else {
					// Row deleted. We mark it as tombstoned in location store
					tombstone := Location{BatchIdx: -1, RowIdx: -1}
					val.Store(packLocation(tombstone))
				}
			}
		}
	})
	return nil
}

// EstimateMemory implements VectorIndex.
func (h *ArrowHNSW) EstimateMemory() int64 {
	data := h.data.Load()
	if data == nil {
		return 0
	}

	capacity := int64(data.Capacity)
	dims := int64(h.dims.Load())

	// memory = capacity * (Level[1] + Vector[dims*4] + Neighbors[ArrowMaxLayers*MaxNeighbors*4] + Counts[ArrowMaxLayers*4] + Versions[ArrowMaxLayers*4])
	// Neighbors per layer: ChunkSize * MaxNeighbors * 4 bytes
	// Neighborhood overhead for all layers
	neighborhoodMem := capacity * ArrowMaxLayers * MaxNeighbors * 4

	// Metadata: Levels (1 byte), Counts (4 bytes), Versions (4 bytes) per layer
	metadataMem := capacity * (1 + ArrowMaxLayers*4 + ArrowMaxLayers*4)

	// Vectors: dims * 4 bytes
	vectorMem := capacity * dims * 4

	// Sharded locks overhead (1024 * size of Mutex)
	locksMem := int64(1024 * 64) // Approximation for sync.Mutex

	// Bitset memory (deleted nodes)
	bitsetMem := capacity / 8

	return neighborhoodMem + metadataMem + vectorMem + locksMem + bitsetMem
}

// Close implements VectorIndex.
func (h *ArrowHNSW) Close() error {
	// Release GraphData and SlabArenas
	var firstErr error
	data := h.data.Swap(nil)
	backend := h.backend.Swap(nil)

	if data != nil {
		if data.SlabArena != nil {
			data.SlabArena.Free()
		}
		if err := data.Close(); err != nil {
			firstErr = err
		}
	}

	if backend != nil && backend != data {
		// Only free backend arena if it's different from data's arena
		if backend.SlabArena != nil && (data == nil || backend.SlabArena != data.SlabArena) {
			backend.SlabArena.Free()
		}
		if err := backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Release Bitsets (Roaring Bitmaps are pooled)
	if h.deleted != nil {
		h.deleted.Release()
		h.deleted = nil
	}

	// Clear pools and stores
	h.searchPool = nil
	h.batchComputer = nil

	// Break references to dataset and locationStore
	h.dataset = nil
	h.locationStore = nil

	return firstErr
}

func (h *ArrowHNSW) indexMetadata(id uint32, recs []arrow.RecordBatch, i int, rowIdxs, batchIdxs []int) {
	if h.bitmapIndex == nil {
		return
	}

	// Determine which record and row to use
	// For small batches, all recs might be the same or distributed.
	// For small batches, all recs might be the same or distributed.
	// We use the batchIdxs[i] to pick the record.
	var rec arrow.RecordBatch
	if len(recs) == len(batchIdxs) {
		rec = recs[i]
	} else {
		bIdx := batchIdxs[i]
		if bIdx < 0 || bIdx >= len(recs) {
			return
		}
		rec = recs[bIdx]
	}
	rowIdx := rowIdxs[i]

	schema := rec.Schema()
	for _, colName := range h.config.IndexedColumns {
		indices := schema.FieldIndices(colName)
		if len(indices) == 0 {
			continue
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)

		if col.IsNull(rowIdx) {
			continue
		}

		// Extract value as string (simplest for generic bitmap index)
		var valStr string
		switch arr := col.(type) {
		case *array.Int64:
			valStr = fmt.Sprintf("%d", arr.Value(rowIdx))
		case *array.String:
			valStr = arr.Value(rowIdx)
		case *array.Float32:
			valStr = fmt.Sprintf("%f", arr.Value(rowIdx))
		default:
			// Fallback to slow string conversion for other types
			valStr = col.ValueStr(rowIdx)
		}

		if valStr != "" {
			_ = h.bitmapIndex.Add(id, colName, valStr)
		}
	}
	h.metricBitmapEntries.Set(float64(h.bitmapIndex.Count()))
}

// resolveDistanceFunc returns the distance function based on configuration.
func (h *ArrowHNSW) extractVectors(results []SearchResult, format string) {
	data := h.data.Load()
	if data == nil {
		return
	}

	for i := range results {
		id := uint32(results[i].ID)
		cID := chunkID(id)
		cOff := chunkOffset(id)

		if format == "quantized" {
			// Check SQ8
			if data.VectorsSQ8 != nil && int(cID) < len(data.VectorsSQ8) {
				chunk := data.GetVectorsSQ8Chunk(cID)
				if chunk != nil {
					dims := data.Dims
					off := int(cOff) * dims
					if off+dims <= len(chunk) {
						// Need to make a copy since SearchResult.Vector is []byte (and we might reuse chunks?)
						// Actually chunk is in Arena. It's safe to point if we retain?
						// But SearchResult might outlive the query.
						res := make([]byte, dims)
						copy(res, chunk[off:off+dims])
						results[i].Vector = res
						continue
					}
				}
			}
			// Check PQ
			if data.VectorsPQ != nil && int(cID) < len(data.VectorsPQ) {
				chunk := data.GetVectorsPQChunk(cID)
				if chunk != nil {
					pqDims := data.PQDims
					off := int(cOff) * pqDims
					if off+pqDims <= len(chunk) {
						res := make([]byte, pqDims)
						copy(res, chunk[off:off+pqDims])
						results[i].Vector = res
						continue
					}
				}
			}
		}

		// Fallback: Extract from Arrow record if SQ8/PQ not available or different format requested
		if h.dataset == nil {
			continue
		}
		loc, ok := h.locationStore.Get(VectorID(id))
		if !ok || loc.BatchIdx == -1 {
			continue
		}

		h.dataset.dataMu.RLock()
		if loc.BatchIdx >= len(h.dataset.Records) {
			h.dataset.dataMu.RUnlock()
			continue
		}
		rec := h.dataset.Records[loc.BatchIdx]
		h.dataset.dataMu.RUnlock()

		colIdx := h.vectorColIdx
		if colIdx == -1 {
			// Resolve column index
			for j, field := range rec.Schema().Fields() {
				if field.Name == "vector" {
					colIdx = j
					h.vectorColIdx = colIdx
					break
				}
			}
		}

		vec, err := ExtractVectorFromArrow(rec, loc.RowIdx, colIdx)
		if err == nil {
			switch format {
			case "f16":
				results[i].Vector = float32SliceToF16Bytes(vec)
			case "f32", "quantized", "":
				// Fallback to f32 for quantized if encoding failed/missing
				results[i].Vector = make([]byte, len(vec)*4)
				copy(results[i].Vector, float32SliceToBytes(vec))
			}
		}
	}
}
