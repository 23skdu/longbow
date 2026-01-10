package store

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/coder/hnsw"
	"github.com/prometheus/client_golang/prometheus"
)

// VectorID and Location are now aliases from internal/core
// See internal/store/type_aliases.go

// HNSWIndex implements VectorIndex using hierarchical navigable small world graphs.
// It supports concurrent reads and writes, zero-copy vector access, and SIMD optimizations.
type HNSWIndex struct {
	mu sync.RWMutex // Protects Graph and locationStore resizing

	dataset       *Dataset
	Graph         *hnsw.Graph[VectorID]
	locationStore *ChunkedLocationStore // Scalable location mapping
	nextVecID     atomic.Uint32         // Next available vector ID (monotonic)
	dims          int
	dimsOnce      sync.Once

	// Zero-copy support
	// vectorColIdx caches the index of the vector column in batches
	vectorColIdx atomic.Int32

	// Epoch-based memory safety for zero-copy
	currentEpoch  atomic.Uint64
	activeReaders atomic.Int64

	// Object pooling to reduce GC pressure
	resultPool *resultPool // Search result slice pool (internal type)
	searchPool *SearchPool // Search context pool (exported type)

	// Product Quantization (PQ) State
	pqEnabled           bool
	pqTrainingEnabled   bool
	pqTrainingThreshold int // Count at which to trigger training
	pqEncoder           *pq.PQEncoder
	pqCodes             [][]uint8    // Compact storage for encoded vectors (indexed by VectorID)
	pqCodesMu           sync.RWMutex // Protects PQ codes resizing/access

	// Parallel Search Configuration
	// Renamed to parallelConfig to match usage in parallel_search.go
	parallelConfig ParallelSearchConfig

	// GPU Support
	gpuEnabled  bool
	gpuFallback bool
	gpuIndex    gpu.Index

	// Metric for vector distance
	Metric        DistanceMetric
	distFunc      func(a, b []float32) float32
	batchDistFunc func(query []float32, vectors [][]float32, results []float32)

	// Cached Metrics (Curried)
	metricInsertDuration       prometheus.Observer
	metricIndexBuildDuration   prometheus.Observer
	metricNodeCount            prometheus.Gauge
	metricGraphHeight          prometheus.Gauge
	metricVectorAllocations    prometheus.Counter
	metricVectorAllocatedBytes prometheus.Counter
	metricActiveReaders        prometheus.Gauge
	metricLockWait             prometheus.ObserverVec // Needs label "type"
}

// NewHNSWIndex creates a new HNSW index for the given dataset.
// Takes optional config arguments. If provided, the first config is used.
func NewHNSWIndex(dataset *Dataset, opts ...*HNSWConfig) *HNSWIndex {
	config := DefaultConfig()
	if len(opts) > 0 && opts[0] != nil {
		config = opts[0]
	}

	dsName := "default"
	if dataset != nil {
		dsName = dataset.Name
	}

	h := &HNSWIndex{
		dataset:             dataset,
		Graph:               hnsw.NewGraph[VectorID](),
		locationStore:       NewChunkedLocationStore(),
		resultPool:          newResultPool(),
		searchPool:          NewSearchPool(),
		pqCodes:             make([][]uint8, 0, 1024),
		parallelConfig:      config.ParallelSearch,
		pqTrainingEnabled:   config.PQTrainingEnabled,
		pqTrainingThreshold: config.PQTrainingThreshold,
		Metric:              config.Metric, // Line 88

		// Initialize Cached Metrics
		metricInsertDuration:       metrics.HNSWInsertDurationSeconds, // Use global as base, observe will label
		metricIndexBuildDuration:   metrics.IndexBuildDurationSeconds.WithLabelValues(dsName),
		metricNodeCount:            metrics.HNSWNodesTotal.WithLabelValues(dsName),
		metricGraphHeight:          metrics.HnswGraphHeight.WithLabelValues(dsName),
		metricVectorAllocations:    metrics.HNSWVectorAllocations,
		metricVectorAllocatedBytes: metrics.HNSWVectorAllocatedBytes,
		metricActiveReaders:        metrics.HnswActiveReaders.WithLabelValues(dsName),
		metricLockWait:             metrics.IndexLockWaitDuration.MustCurryWith(prometheus.Labels{"dataset": dsName}),
	}

	// Initialize vector column cache to -1 (unknown)
	h.vectorColIdx.Store(-1)

	// Initialize distance functions
	h.distFunc = h.resolveDistanceFunc()
	h.batchDistFunc = h.resolveBatchDistanceFunc()

	// Set distance metric for the graph
	h.Graph.Distance = h.distFunc

	return h
}

// DefaultConfig returns safe default configuration
func DefaultConfig() *HNSWConfig {
	return &HNSWConfig{
		M:                   32,  // Edges per node
		EfConstruction:      200, // Build accuracy
		ParallelSearch:      ParallelSearchConfig{Enabled: true, Threshold: 1000},
		PQTrainingEnabled:   false,
		PQTrainingThreshold: 100000,
		Metric:              MetricEuclidean,
	}
}

type HNSWConfig struct {
	M                   int
	EfConstruction      int
	EfSearch            int
	ParallelSearch      ParallelSearchConfig
	PQTrainingEnabled   bool
	PQTrainingThreshold int
	Metric              DistanceMetric
}

// RemapLocations updates the location mapping for a set of VectorIDs.
// This is used during compaction when records are moved to new batches.
func (h *HNSWIndex) RemapLocations(mapping map[VectorID]Location) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, loc := range mapping {
		h.locationStore.Set(id, loc)
	}
	return nil
}

// RemapFromBatchInfo efficiently updates locations based on batch movements.
// It iterates all locations in the store and updates them if they belong to moved batches.
func (h *HNSWIndex) RemapFromBatchInfo(remapping map[int]BatchRemapInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()

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
					// Row deleted. We mark it as tombstoned in location store?
					tombstone := Location{BatchIdx: -1, RowIdx: -1}
					val.Store(packLocation(tombstone))
				}
			}
		}
	})
	return nil
}

// GetLocation implements VectorIndex.
func (h *HNSWIndex) GetLocation(id VectorID) (Location, bool) {
	return h.locationStore.Get(id)
}

// GetVectorID implements VectorIndex.
func (h *HNSWIndex) GetVectorID(loc Location) (VectorID, bool) {
	return h.locationStore.GetID(loc)
}

// Warmup implements VectorIndex (stub for now).
func (h *HNSWIndex) Warmup() int {
	// Return the graph size as approximation of warmed up nodes
	return h.Graph.Len()
}

// SetIndexedColumns implements VectorIndex (stub for now).
func (h *HNSWIndex) SetIndexedColumns(cols []string) {
	// No-op for HNSW
}

// resolveDistanceFunc returns the configured distance function, accounting for PQ state.
func (h *HNSWIndex) resolveDistanceFunc() func(a, b []float32) float32 {
	h.pqCodesMu.RLock()
	defer h.pqCodesMu.RUnlock()

	if h.pqEnabled && h.pqEncoder != nil {
		encoder := h.pqEncoder
		return func(a, b []float32) float32 {
			codesA := pq.UnpackFloat32sToBytes(a, encoder.M)
			codesB := pq.UnpackFloat32sToBytes(b, encoder.M)

			var dist float32
			for m := 0; m < encoder.M; m++ {
				idxA := int(codesA[m])
				idxB := int(codesB[m])
				if idxA == idxB {
					continue
				}
				// Compute distance between centroids[m][idxA] and centroids[m][idxB]
				subA := encoder.Codebooks[m][idxA*encoder.SubDim : (idxA+1)*encoder.SubDim]
				subB := encoder.Codebooks[m][idxB*encoder.SubDim : (idxB+1)*encoder.SubDim]

				// L2 Distance (consistent with codebase)
				var subDist float32
				for i := range subA {
					d := subA[i] - subB[i]
					subDist += d * d
				}
				dist += subDist
			}
			return float32(math.Sqrt(float64(dist)))
		}
	}

	switch h.Metric {
	case MetricCosine:
		return hnsw.CosineDistance
	case MetricDotProduct:
		// HNSW minimizes distance. For Dot Product (simd returns dot), we return -dot.
		// NOTE: hnsw library might not support negative distances well in some heuristics?
		// But strictly speaking, it just does comparisons.
		// Assuming simd.DotProduct returns dot product (sum a*b).
		// We negate it so higher dot product = lower "distance".
		return func(a, b []float32) float32 {
			return -simd.DotProduct(a, b)
		}
	default:
		return hnsw.EuclideanDistance
	}
}

// getVector retrieves the vector for a given ID.
// It uses the cached vector column index for speed.
// Returns nil if not found or deleted.
func (h *HNSWIndex) getVector(id VectorID) []float32 {
	loc, ok := h.locationStore.Get(id)
	if !ok {
		return nil
	}
	if loc.BatchIdx == -1 {
		return nil // Tombstoned
	}

	h.dataset.dataMu.RLock()
	defer h.dataset.dataMu.RUnlock()

	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]

	colIdx := h.vectorColIdx.Load()
	if colIdx == -1 {
		// Resolve column index
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" || field.Name == "embedding" {
				colIdx = int32(i)
				h.vectorColIdx.Store(colIdx)
				break
			}
		}
	}

	if colIdx == -1 {
		return nil
	}

	vecCol := rec.Column(int(colIdx))
	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil
	}

	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return nil
	}

	// Make a copy for safety as we don't hold lock
	vec := make([]float32, width)
	copy(vec, floatArr.Float32Values()[start:end])
	return vec
}

// getVectorUnsafe retrieves a zero-copy view of the vector.
// Returns the vector slice and a release function.
// The release function MUST be called to exit the epoch.
func (h *HNSWIndex) getVectorUnsafe(id VectorID) (vec []float32, release func()) {
	loc, ok := h.locationStore.Get(id)
	if !ok || loc.BatchIdx == -1 {
		return nil, nil // Not found or deleted
	}

	h.enterEpoch()

	// Optimized path: Use Unsafe pointer logic to skip Arrow overhead
	// We still need dataset read lock to access Records slice pointer safely
	h.dataset.dataMu.RLock() // Can this be RLock? Yes, Records slice won't change, only appends.

	if loc.BatchIdx >= len(h.dataset.Records) {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	// rec variable removed as it was unused in logic,
	// we use loc directly in helper.

	// Use internal helper
	vec = h.getVectorLockedUnsafe(loc)
	h.dataset.dataMu.RUnlock() // Release lock immediately, rely on epoch

	if vec == nil {
		h.exitEpoch()
		return nil, nil
	}

	return vec, func() { h.exitEpoch() }
}

// getVectorLockedUnsafe returns a direct reference to the vector data.
// Caller MUST hold h.dataset.dataMu.RLock() AND h.enterEpoch().
// Provides true zero-copy access by bypassing Arrow array wrappers and RLock overhead.
func (h *HNSWIndex) getVectorLockedUnsafe(loc Location) []float32 {
	if h.dataset.Records == nil || loc.BatchIdx < 0 || loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]
	if rec == nil {
		return nil
	}

	colIdx := h.vectorColIdx.Load()
	if colIdx == -1 {
		// Resolve column index
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" {
				colIdx = int32(i)
				h.vectorColIdx.Store(colIdx)
				break
			}
		}
	}

	if colIdx == -1 {
		return nil
	}

	vecCol := rec.Column(int(colIdx))
	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil
	}

	// Fast Path: Direct buffer access via unsafe
	values := listArr.ListValues()
	data := values.Data()
	if len(data.Buffers()) < 2 || data.Buffers()[1] == nil {
		return nil
	}

	buf := data.Buffers()[1].Bytes()
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	offset := loc.RowIdx * width * 4

	if offset+width*4 > len(buf) {
		return nil
	}

	ptr := unsafe.Pointer(&buf[offset])
	return unsafe.Slice((*float32)(ptr), width)
}

func (h *HNSWIndex) enterEpoch() {
	h.activeReaders.Add(1)
}

func (h *HNSWIndex) exitEpoch() {
	h.activeReaders.Add(-1)
}

// RegisterReader increments the active reader count for zero-copy safety
func (h *HNSWIndex) RegisterReader() {
	h.activeReaders.Add(1)
	metrics.HnswActiveReaders.WithLabelValues(h.dataset.Name).Inc()
}

// UnregisterReader decrements the active reader count
func (h *HNSWIndex) UnregisterReader() {
	h.activeReaders.Add(-1)
	metrics.HnswActiveReaders.WithLabelValues(h.dataset.Name).Dec()
}

// advanceEpoch increments the epoch and waits for all active readers to finish.
// This is used for safe resource reclamation (e.g. during resize or compaction).
func (h *HNSWIndex) advanceEpoch() {
	h.currentEpoch.Add(1)

	// Spin wait for active readers to drain
	// In production, might want sleep or condition variable, but specific test expects blocking.
	// Simple spin with yield:
	for h.activeReaders.Load() > 0 {
		runtime.Gosched()
	}
}

// Close releases resources associated with the index.
func (h *HNSWIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Graph = nil
	h.locationStore.Reset() // Clear locations
	h.resultPool = nil
	h.dataset = nil
	return nil
}

// Len returns the number of vectors in the index
func (h *HNSWIndex) Len() int {
	return h.locationStore.Len()
}

// GetDimension returns the vector dimension for this index
func (h *HNSWIndex) GetDimension() uint32 {
	d := uint32(h.dims)
	if d == 0 {
		if h.Len() > 0 {
			if h.dims > 0 {
				d = uint32(h.dims)
			}
		}
	}
	return d
}

// resolveBatchDistanceFunc returns the batch distance function.
func (h *HNSWIndex) resolveBatchDistanceFunc() func(query []float32, vectors [][]float32, results []float32) {
	// For PQ, we might need a different batch function?
	// This function seems to be used for standard float32 vectors.
	// If PQ enabled, usage should call specialized PQ batch functions in search path.
	// This resolver is for *full vector* batch distance (e.g. re-ranking).

	switch h.Metric {
	case MetricCosine:
		return simd.CosineDistanceBatch
	case MetricDotProduct:
		// Wrap to negate results?
		// simd.DotProductBatch returns dot products. HNSW expects distances.
		// If we use this for re-ranking with sorting, we need to know the score type.
		// HNSW Search logic usually sorts by Score Ascending (Distance).
		// If we return raw Dot Product, we should treat it as Score Descending.
		// However, standard interface usually returns "Distance".
		return func(query []float32, vectors [][]float32, results []float32) {
			simd.DotProductBatch(query, vectors, results)
			for i := range results {
				results[i] = -results[i]
			}
		}
	default:
		return simd.EuclideanDistanceBatch
	}
}

// GetDistanceFunc is kept for legacy/interface compatibility, returning the pre-resolved func.
func (h *HNSWIndex) GetDistanceFunc() func(a, b []float32) float32 {
	return h.distFunc
}

// EstimateMemory implements VectorIndex.
func (h *HNSWIndex) EstimateMemory() int64 {
	// Base
	size := int64(256)

	// LocationStore
	count := int64(h.nextVecID.Load())
	size += count * 16

	// Graph
	h.mu.RLock()
	var graphSize int64
	if h.Graph != nil {
		nodeSize := int64(h.dims*4) + 128
		graphSize = int64(h.Graph.Len()) * nodeSize
	}
	h.mu.RUnlock()
	size += graphSize

	// PQ Codes
	h.pqCodesMu.RLock()
	if h.pqCodes != nil {
		size += int64(len(h.pqCodes)) * 24 // slice headers
		// Assuming codes are compact
		if len(h.pqCodes) > 0 && len(h.pqCodes[0]) > 0 {
			size += int64(len(h.pqCodes) * len(h.pqCodes[0]))
		}
	}
	h.pqCodesMu.RUnlock()

	return size
}
