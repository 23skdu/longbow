package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/prometheus/client_golang/prometheus"
)

// ArrowHNSWConfig holds configuration for ArrowHNSW index
type ArrowHNSWConfig struct {
	M              int
	MMax           int
	MMax0          int
	EfConstruction int32
	EfSearch       int32

	AdaptiveEf          bool
	AdaptiveEfMin       int
	AdaptiveEfThreshold int
	InitialCapacity     int

	Workers      int
	Quantization bool
	SQ8Enabled   bool

	UseDisk  bool
	DiskPath string

	Metric   core.DistanceMetric
	Logger   any
	DataType types.VectorDataType

	BQEnabled bool
	PQEnabled bool
	PQM       int
	PQK       int

	Dims                    int
	SelectionHeuristicLimit int

	ParallelSearch types.ParallelSearchConfig

	AdaptiveMEnabled   bool
	AdaptiveMThreshold int

	Float16Enabled         bool
	SQ8TrainingThreshold   int
	PackedAdjacencyEnabled bool

	Registerer prometheus.Registerer
}

// DefaultArrowHNSWConfig returns a configuration with sensible defaults
func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	return ArrowHNSWConfig{
		M:                       32,
		MMax:                    64,
		MMax0:                   64,
		EfConstruction:          400,
		EfSearch:                50,
		AdaptiveEf:              false,
		AdaptiveEfMin:           50,
		AdaptiveEfThreshold:     0,
		InitialCapacity:         10000,
		Workers:                 4,
		Quantization:            false,
		SQ8Enabled:              false,
		UseDisk:                 false,
		DiskPath:                "./data",
		DataType:                types.VectorTypeFloat32,
		SQ8TrainingThreshold:    5000,
		SelectionHeuristicLimit: 400,
		Metric:                  core.MetricEuclidean,
	}
}

// ArrowHNSW implements a Hierarchical Navigable Small World (HNSW) index
// optimized for Apache Arrow data structures with zero-copy operations.
type ArrowHNSW struct {
	config ArrowHNSWConfig

	dataset       *Dataset
	data          atomic.Pointer[types.GraphData]
	locationStore *ChunkedLocationStore

	nodeCount      atomic.Int64
	dims           atomic.Int32
	entryPoint     atomic.Uint32
	maxLevel       atomic.Int32
	efConstruction atomic.Int32

	m     int
	mMax  int
	mMax0 int

	shardedLocks *AlignedShardedMutex

	// DiskGraph backing
	diskGraph atomic.Pointer[DiskGraph]

	quantizer  *ScalarQuantizer
	sq8Ready   atomic.Bool
	bqEncoder  *BQEncoder
	pqEncoder  *pq.PQEncoder
	searchPool *ArrowSearchContextPool

	name         string
	distFunc     func([]float32, []float32) (float32, error)
	distFuncF64  func([]float64, []float64) (float32, error)
	distFuncF16  func([]float16.Num, []float16.Num) (float32, error)
	distFuncC64  func([]complex64, []complex64) (float32, error)
	distFuncC128 func([]complex128, []complex128) (float64, error)

	adaptiveMTriggered atomic.Bool

	initMu sync.Mutex
	growMu sync.RWMutex

	deleted *roaring.Bitmap

	repairAgent *RepairAgent

	// Parallel Search Config
	parallelConfig types.ParallelSearchConfig

	// GPU Support
	gpuMu          sync.RWMutex
	gpuEnabled     bool
	gpuFallback    bool
	gpuIndex       gpu.Index
	gpuConfig      GPUConfig
	gpuResultCache *gpuResultCache

	// GPU Batch Sync Support
	gpuBatchIDs     []int64
	gpuBatchVectors []float32
	gpuBatchMu      sync.Mutex
	gpuLastSyncTime time.Time
	gpuSyncTicker   *time.Ticker
	gpuStopSync     chan struct{}

	// GPU Circuit Breaker
	gpuCircuitBreaker *gpu.CircuitBreaker

	sq8TrainingBuffer [][]float32
	levelMultiplier   float64

	// Graph Navigation
	navigator *GraphNavigator
}

func (h *ArrowHNSW) GetVector(id uint32) (any, error) {
	// Fallback to DiskGraph FIRST in hybrid mode if SQ8/PQ is enabled.
	// This ensures we read the persistent copy even if memory chunks are allocated (due to promotion).
	dg := h.diskGraph.Load()
	if dg != nil {
		if h.config.SQ8Enabled {
			if v := dg.GetVectorSQ8(id); v != nil {
				return v, nil
			}
		}
		if h.config.PQEnabled {
			if v := dg.GetVectorPQ(id); v != nil {
				return v, nil
			}
		}
	}

	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data not initialized")
	}

	// Prefer quantized vectors if enabled
	if h.config.PQEnabled {
		if v := data.GetVectorPQ(id); v != nil {
			return v, nil
		}
	}
	if h.config.BQEnabled {
		if v, err := data.GetVectorBQ(id); err == nil && v != nil {
			return v, nil
		}
	}

	return data.GetVector(id)
}

func (h *ArrowHNSW) getVectorAny(id uint32) (any, error) {
	return h.GetVector(id)
}

func (h *ArrowHNSW) getVectorWithData(data *types.GraphData, id uint32) (any, error) {
	v, err := data.GetVector(id)
	if v != nil || err != nil {
		return v, err
	}

	// Fallback to DiskGraph
	dg := h.diskGraph.Load()
	if dg != nil {
		if h.config.SQ8Enabled {
			return dg.GetVectorSQ8(id), nil
		}
		if h.config.PQEnabled {
			return dg.GetVectorPQ(id), nil
		}
	}

	return nil, nil
}

// NewArrowHNSW creates a new ArrowHNSW index with the given configuration
func NewArrowHNSW(dataset *Dataset, config *ArrowHNSWConfig) *ArrowHNSW {
	h := &ArrowHNSW{
		config:          *config,
		dataset:         dataset,
		m:               config.M,
		mMax:            config.MMax,
		mMax0:           config.MMax0,
		shardedLocks:    NewAlignedShardedMutex(AlignedShardedMutexConfig{NumShards: 64}),
		searchPool:      NewArrowSearchContextPool(),
		locationStore:   NewChunkedLocationStore(),
		deleted:         roaring.New(),
		levelMultiplier: 1.0 / math.Log(float64(config.M)),
	}
	if dataset != nil {
		h.name = dataset.Name
	} else {
		h.name = "unknown"
	}
	if dataset != nil {
		dataset.Index = h
		// Restore PQ Encoder if present in dataset (e.g. from snapshot)
		if dataset.PQEncoder != nil {
			h.pqEncoder = dataset.PQEncoder
			h.config.PQEnabled = true
		}
	}

	// Initialize distance functions
	h.distFunc = simd.EuclideanDistance
	h.distFuncF64 = simd.EuclideanDistanceFloat64
	h.distFuncF16 = simd.EuclideanDistanceF16
	h.distFuncC64 = simd.EuclideanDistanceComplex64
	h.distFuncC128 = func(a, b []complex128) (float64, error) {
		d, err := simd.EuclideanDistanceComplex128(a, b)
		return float64(d), err
	}

	switch config.Metric {
	case core.MetricCosine:
		h.distFunc = simd.CosineDistance
		h.distFuncF16 = simd.CosineDistanceF16
		// Cosine for F64/Complex could be added if needed
	case core.MetricDotProduct:
		h.distFunc = simd.DotProduct
		h.distFuncF16 = simd.DotProductF16
	}

	// Initialize atomic values
	h.efConstruction.Store(config.EfConstruction)
	h.maxLevel.Store(-1)
	h.dims.Store(int32(config.Dims))

	// Initialize quantization if enabled
	if config.SQ8Enabled {
		// Initialize with config dims if available, otherwise lazy init will handle it
		if config.Dims > 0 {
			h.quantizer = NewScalarQuantizer(config.Dims)
		}
		// Do not set sq8Ready to true until trained
	}

	// Init distance funcs based on config
	fmt.Printf("Initializing ArrowHNSW with metric: %v\n", config.Metric)
	switch config.Metric {
	case MetricCosine:
		h.distFunc = simd.CosineDistance
		// TODO: Add specialized F64/F16 cosine if available
		h.distFuncF64 = simd.EuclideanDistanceFloat64 // Fallback or implement
		h.distFuncF16 = simd.EuclideanDistanceF16     // Fallback or implement
	case MetricDotProduct:
		// HNSW requires distance (lower is closer). Dot product is similarity (higher is closer).
		// We use negative dot product as distance.
		h.distFunc = func(a, b []float32) (float32, error) {
			d, err := simd.DotProduct(a, b)
			return -d, err
		}
		h.distFuncF64 = func(a, b []float64) (float32, error) {
			d, err := simd.DotProductF64(a, b)
			return -d, err
		}
		h.distFuncF16 = simd.EuclideanDistanceF16 // Fallback
	default: // MetricEuclidean
		h.distFunc = simd.EuclideanDistance
		h.distFuncF64 = simd.EuclideanDistanceFloat64
		h.distFuncF16 = simd.EuclideanDistanceF16
	}

	// Ensure initial capacity
	capacity := config.InitialCapacity
	if capacity < 1000 {
		capacity = 1000
	}

	// Determine DataType
	dt := config.DataType
	if config.Float16Enabled {
		dt = types.VectorTypeFloat16
	}

	// Initialize GraphData
	gd := types.NewGraphData(
		capacity,
		config.Dims,
		false, // mmap
		config.UseDisk,
		0, // fd
		config.Quantization,
		config.SQ8Enabled,
		config.UseDisk, // persistent
		dt,
		config.BQEnabled,
		config.PQEnabled,
	)
	if h.pqEncoder != nil {
		gd.PQM = h.pqEncoder.CodeSize()
	}
	h.data.Store(gd)

	// Initialize Graph Navigator
	navConfig := NavigatorConfig{
		MaxHops:           10, // Default
		Concurrency:       config.Workers,
		EarlyTerminate:    true,
		DistanceThreshold: 0, // No threshold by default
	}
	dsName := "unknown"
	if dataset != nil {
		dsName = dataset.Name
	}
	h.navigator = NewGraphNavigator(dsName, h.GetData, navConfig, config.Registerer)
	_ = h.navigator.Initialize()

	return h
}

// SetData sets the graph data for the index
func (h *ArrowHNSW) SetData(data *types.GraphData) {
	h.data.Store(data)
}

// GetData returns the current graph data
func (h *ArrowHNSW) GetData() *types.GraphData {
	return h.data.Load()
}

func (h *ArrowHNSW) IsSharded() bool {
	return false
}

// GetConfig returns the current configuration
func (h *ArrowHNSW) GetConfig() ArrowHNSWConfig {
	return h.config
}

// GetM returns the M parameter (connections per layer)
func (h *ArrowHNSW) GetM() int {
	return h.m
}

// GetMMax returns the MMax parameter (max connections)
func (h *ArrowHNSW) GetMMax() int {
	return h.mMax
}

// GetMMax0 returns the MMax0 parameter (max connections in layer 0)
func (h *ArrowHNSW) GetMMax0() int {
	return h.mMax0
}

// GetEfConstruction returns the efConstruction parameter
func (h *ArrowHNSW) GetEfConstruction() int32 {
	return h.efConstruction.Load()
}

// GetNodeCount returns the current number of nodes
func (h *ArrowHNSW) GetNodeCount() int64 {
	return h.nodeCount.Load()
}

// GetDims returns the vector dimensions
func (h *ArrowHNSW) GetDims() int32 {
	return h.dims.Load()
}

// GetDistanceMetric returns the configured distance metric.
func (h *ArrowHNSW) GetDistanceMetric() core.DistanceMetric {
	return h.config.Metric
}

// GetEntryPoint returns the current entry point node
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// GetMaxLevel returns the maximum level in the graph
func (h *ArrowHNSW) GetMaxLevel() int32 {
	return h.maxLevel.Load()
}

// GetShardedLocks returns the sharded mutex for concurrent access
func (h *ArrowHNSW) GetShardedLocks() *AlignedShardedMutex {
	return h.shardedLocks
}

// GetDiskGraph returns the disk graph if enabled
func (h *ArrowHNSW) GetDiskGraph() *DiskGraph {
	return h.diskGraph.Load()
}

// SetDiskGraph sets the disk graph
func (h *ArrowHNSW) SetDiskGraph(disk *DiskGraph) {
	h.diskGraph.Store(disk)
}

// GetQuantizer returns the scalar quantizer
func (h *ArrowHNSW) GetQuantizer() *ScalarQuantizer {
	return h.quantizer
}

// IsSQ8Ready returns whether scalar quantization is ready
func (h *ArrowHNSW) IsSQ8Ready() bool {
	return h.sq8Ready.Load()
}

// GetBQEncoder returns the BQ encoder
func (h *ArrowHNSW) GetBQEncoder() *BQEncoder {
	return h.bqEncoder
}

// SetBQEncoder sets the BQ encoder
func (h *ArrowHNSW) SetBQEncoder(encoder *BQEncoder) {
	h.bqEncoder = encoder
}

// GetPQEncoder returns the PQ encoder
func (h *ArrowHNSW) GetPQEncoder() *pq.PQEncoder {
	return h.pqEncoder
}

// SetPQEncoder sets the PQ encoder
func (h *ArrowHNSW) SetPQEncoder(encoder *pq.PQEncoder) {
	h.pqEncoder = encoder
}

// setDims sets the vector dimensions
func (h *ArrowHNSW) setDims(dims int32) {
	h.dims.Store(dims)
}

// SetDimension sets the absolute dimension of the index.
func (h *ArrowHNSW) SetDimension(dim int) {
	h.setDims(int32(dim))
	// Also ensure GraphData is updated to reflect this dimension
	// This is critical if the index was initialized with 0 dims but large capacity (default config).
	h.initMu.Lock()
	defer h.initMu.Unlock()
	data := h.data.Load()
	if data != nil {
		_ = h.Grow(data.Capacity, dim)
	}
}

// Delete invokes Delete for a single id.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Add(id)
	h.locationStore.Delete(VectorID(id))
	return nil
}

func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	if h.deleted == nil {
		return false
	}
	return h.deleted.Contains(id)
}

// mustGetVectorFromData retrieves a vector from the given data snapshot or panics.
func (h *ArrowHNSW) mustGetVectorFromData(data *types.GraphData, id uint32) any {
	vec, err := h.getVectorWithData(data, id)
	if err != nil {
		panic(err)
	}
	return vec
}

// ensureChunk ensures that the data structures for the given chunk are allocated.
func (h *ArrowHNSW) ensureChunk(data *types.GraphData, cID, cOff, dims int) (*types.GraphData, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	// Strictly serialized growth to avoid structural races in GraphData
	h.growMu.Lock()
	defer h.growMu.Unlock()

	// Reload data to ensure we have the latest view before modifying
	data = h.data.Load()

	// Ensure dims is synced between atomic and data struct
	if data.Dims == 0 && dims > 0 {
		data.Dims = dims
	}

	err := data.EnsureChunk(cID, cOff, dims)
	return data, err
}

// DeleteBatch invokes Delete for each id.
func (h *ArrowHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	for _, id := range ids {
		if err := h.Delete(id); err != nil {
			return err
		}
	}
	return nil
}

// Interface implementation: AddByLocation adds a vector by its location
func (h *ArrowHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	id := uint32(h.nodeCount.Add(1) - 1)

	var vec any
	if h.dataset != nil && batchIdx < len(h.dataset.Records) {
		record := h.dataset.Records[batchIdx]
		// Find vector column
		vecColIdx := -1
		for i := 0; i < int(record.NumCols()); i++ {
			if record.ColumnName(i) == "vector" {
				vecColIdx = i
				break
			}
		}
		if vecColIdx != -1 {
			vec = h.extractVector(record, vecColIdx, rowIdx)
		}
	}

	h.SetLocation(VectorID(id), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.generateLevel())
	if err != nil {
		return 0, err
	}

	return id, nil
}

// AddByRecord implements VectorIndex.
func (h *ArrowHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	id := uint32(h.nodeCount.Add(1) - 1)

	var vec any
	// Find vector column
	vecColIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == "vector" {
			vecColIdx = i
			break
		}
	}
	if vecColIdx != -1 {
		vec = h.extractVector(rec, vecColIdx, rowIdx)
	}

	h.SetLocation(VectorID(id), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.generateLevel())
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (h *ArrowHNSW) extractVector(rec arrow.RecordBatch, colIdx, rowIdx int) any {
	col := rec.Column(colIdx)

	// Helper to extract values from underlying array
	extractValues := func(values arrow.Array, start, end int64) any {
		switch arr := values.(type) {
		case *arrowarray.Float32:
			// Handle Complex64
			if h.config.DataType == types.VectorTypeComplex64 {
				floats := arr.Float32Values()[start:end]
				// size should be 2 * dims
				size := int(end - start)
				complexes := make([]complex64, size/2)
				for i := 0; i < size/2; i++ {
					complexes[i] = complex(floats[2*i], floats[2*i+1])
				}
				return complexes
			}
			// Important: Return copy or ensuring safety?
			// Float32Values returns slice. Arrow semantics: slice is view.
			// But we copy into GraphData immediately in InsertWithVector (SetVector does copy).
			return arr.Float32Values()[start:end]

		case *arrowarray.Float64:
			// Handle Complex128
			if h.config.DataType == types.VectorTypeComplex128 {
				floats := arr.Float64Values()[start:end]
				size := int(end - start)
				complexes := make([]complex128, size/2)
				for i := 0; i < size/2; i++ {
					complexes[i] = complex(floats[2*i], floats[2*i+1])
				}
				return complexes
			}
			return arr.Float64Values()[start:end]

		case *arrowarray.Int8:
			return arr.Int8Values()[start:end]

		case *arrowarray.Float16:
			return arr.Values()[start:end]

		// Add other types as needed
		default:
			return nil
		}
	}

	if list, ok := col.(*arrowarray.FixedSizeList); ok {
		values := list.ListValues()
		size := int64(list.DataType().(*arrow.FixedSizeListType).Len())
		// Account for list array offset
		listOffset := int64(list.Data().Offset())
		offset := (listOffset + int64(rowIdx)) * size
		return extractValues(values, offset, offset+size)
	}

	if list, ok := col.(*arrowarray.List); ok {
		offsets := list.Offsets()
		start := int64(offsets[rowIdx])
		end := int64(offsets[rowIdx+1])
		values := list.ListValues()
		return extractValues(values, start, end)
	}

	return nil
}

// Interface implementation: Search performs k-nearest neighbor search
func (h *ArrowHNSW) Search(ctx context.Context, queryVal any, k int, filter any) ([]types.Candidate, error) {
	if h.nodeCount.Load() == 0 {
		return []types.Candidate{}, nil
	}

	// searchCtx is used inside SearchVectorsWithBitmap now, so we don't need to hold it here
	// to avoid double allocation or misuse.
	// But SearchVectorsWithBitmap signature doesn't take context *pool object*, only Go context.
	// So SearchVectorsWithBitmap will get its own.
	// We can remove the Get/Put here.

	results, err := h.SearchVectorsWithBitmap(ctx, queryVal, k, nil, nil)
	if err != nil {
		return nil, err
	}

	// Convert []SearchResult to []types.Candidate
	typeResults := make([]types.Candidate, len(results))
	for i, r := range results {
		typeResults[i] = types.Candidate{
			ID:   uint32(r.ID),
			Dist: r.Distance,
		}
	}

	return typeResults, nil
}

// Interface implementation: Size returns the number of nodes in the index
func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// Interface implementation: Close cleans up resources
func (h *ArrowHNSW) Close() error {
	if h.navigator != nil {
		if err := h.navigator.Close(); err != nil {
			return err
		}
	}
	h.data.Store(nil)
	h.dataset = nil
	h.searchPool = nil
	h.locationStore = nil
	h.deleted = nil
	return nil
}

// Navigate performs a graph navigation query
func (h *ArrowHNSW) Navigate(ctx context.Context, navQuery NavigatorQuery) (*NavigatorPath, error) {
	if h.navigator == nil {
		return nil, fmt.Errorf("graph navigator not initialized")
	}
	return h.navigator.FindPath(ctx, navQuery)
}

// Extended methods for AdaptiveIndex compatibility

func (h *ArrowHNSW) GetDimension() uint32 {
	return uint32(h.GetDims())
}

func (b *ArrowBitset) ClearSIMD() {
	b.Clear()
}

// MinCandidateHeap for exploration (closest first)
// Uses store.Candidate (ID, Dist) to match ArrowSearchContext
type MinCandidateHeap []Candidate

func (h MinCandidateHeap) Len() int           { return len(h) }
func (h MinCandidateHeap) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinCandidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinCandidateHeap) Push(x any)        { *h = append(*h, x.(Candidate)) }
func (h *MinCandidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MaxCandidateHeapAdapter wraps CandidateHeap to satisfy heap.Interface with Pop() any
type MaxCandidateHeapAdapter struct {
	*CandidateHeap
}

func (h MaxCandidateHeapAdapter) Push(x any) {
	*h.CandidateHeap = append(*h.CandidateHeap, x.(Candidate))
}

func (h MaxCandidateHeapAdapter) Pop() any {
	old := *h.CandidateHeap
	n := len(old)
	x := old[n-1]
	*h.CandidateHeap = old[0 : n-1]
	return x
}

func (h *ArrowHNSW) ensureReady() {
	if h.searchPool == nil {
		h.initMu.Lock()
		if h.searchPool == nil {
			h.searchPool = NewArrowSearchContextPool()
		}
		if h.deleted == nil {
			h.deleted = roaring.New()
		}
		if h.locationStore == nil {
			h.locationStore = NewChunkedLocationStore()
		}
		if h.shardedLocks == nil {
			h.shardedLocks = NewAlignedShardedMutex(AlignedShardedMutexConfig{NumShards: 64})
		}
		h.initMu.Unlock()
	}
}

func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	h.ensureReady()
	if h.nodeCount.Load() == 0 {
		return nil, nil
	}

	if metrics.HNSWSearchPoolGetTotal != nil {
		metrics.HNSWSearchPoolGetTotal.Inc()
	}
	start := time.Now()
	searchCtx := h.searchPool.Get()
	defer func() {
		duration := time.Since(start).Seconds()
		metrics.HNSWSearchDurationSeconds.Observe(duration)

		typeLabel := h.config.DataType.String()
		dimsStr := strconv.Itoa(int(h.dims.Load()))
		metrics.HNSWSearchOpsTotal.WithLabelValues(h.name, typeLabel, dimsStr).Inc()

		if metrics.HNSWSearchPoolPutTotal != nil {
			metrics.HNSWSearchPoolPutTotal.Inc()
		}
		h.searchPool.Put(searchCtx)
	}()

	ep := h.entryPoint.Load()
	maxLevel := h.maxLevel.Load()
	data := h.data.Load()

	// data might be stale relative to ep if a concurrent growth occurred.
	// Reload data if necessary to ensure it covers the entry point.
	if data == nil || int(ep) >= data.Capacity {
		data = h.data.Load()
		if data == nil {
			return nil, fmt.Errorf("graph data is nil")
		}
		// If still out of bounds, it's a critical error or race that shouldn't happen with correct ordering,
		// but we can't proceed.
		if int(ep) >= data.Capacity {
			// It is possible ep was just updated and Grow logic finished, but we loaded data just before Grow swapped?
			// But we just reloaded.
			// Only explanation: ep > data.Capacity.
			// This might happen if 'ep' update happened BUT 'Grow' used a new 'data' pointer, and we see 'ep' but 'data' is still old?
			// Wait, if we reloaded data, we should see the new pointer.
			return nil, fmt.Errorf("entry point %d out of bounds (capacity %d)", ep, data.Capacity)
		}
	}

	// Calculate distance to entry point
	var dist float32
	vec, err := h.getVectorWithData(data, ep)
	if err != nil {
		return nil, err
	}

	if vec == nil {
		return nil, fmt.Errorf("entry point vector not found for id %d", ep)
	}

	// Use specialized computer if possible
	computer := h.resolveHNSWComputer(data, searchCtx, queryVec, false)
	if comp, ok := computer.(interface {
		ComputeSingle(id uint32) (float32, error)
	}); ok {
		dist, err = comp.ComputeSingle(ep)
		if err != nil {
			return nil, err
		}
	} else {
		// Fallback
		switch q := queryVec.(type) {
		case []float32:
			switch v := vec.(type) {
			case []float32:
				dist, err = h.distFunc(q, v)
			case []float64:
				q64 := make([]float64, len(q))
				for i, val := range q {
					q64[i] = float64(val)
				}
				dist, err = h.distFuncF64(q64, v)
			default:
				return nil, fmt.Errorf("unsupported vector type %T for float32 query", vec)
			}
		case []float64:
			if v, ok := vec.([]float64); ok {
				dist, err = h.distFuncF64(q, v)
			}
		case []float16.Num:
			if v, ok := vec.([]float16.Num); ok {
				dist, err = h.distFuncF16(q, v)
			}
		case []complex64:
			if v, ok := vec.([]complex64); ok {
				dist, err = simd.EuclideanDistanceComplex64(q, v)
			}
		case []complex128:
			if v, ok := vec.([]complex128); ok {
				dist, err = simd.EuclideanDistanceComplex128(q, v)
			}
		case []int8, []uint8:
			var v8 []uint8
			switch vt := vec.(type) {
			case []int8:
				v8 = *(*[]uint8)(unsafe.Pointer(&vt))
			case []uint8:
				v8 = vt
			}

			var q8 []uint8
			switch qt := q.(type) {
			case []int8:
				q8 = *(*[]uint8)(unsafe.Pointer(&qt))
			case []uint8:
				q8 = qt
			}

			if h.quantizer != nil && h.sq8Ready.Load() && len(q8) > 0 && len(v8) > 0 {
				minV, maxV := h.quantizer.Params()
				scale := (maxV - minV) / 255.0
				var sum float32
				for i := range q8 {
					deqQ := minV + float32(q8[i])*scale
					deqV := minV + float32(v8[i])*scale
					diff := deqQ - deqV
					sum += diff * diff
				}
				dist = float32(math.Sqrt(float64(sum)))
			} else if len(q8) > 0 && len(v8) > 0 {
				var sum float32
				for i := range q8 {
					diff := float32(q8[i]) - float32(v8[i])
					sum += diff * diff
				}
				dist = float32(math.Sqrt(float64(sum)))
			}
		default:
			return nil, fmt.Errorf("unsupported query vector type %T", queryVec)
		}
		if err != nil {
			return nil, err
		}
	}

	// 1. Search from top layer to 1
	distToEp := dist
	currObj := Candidate{ID: ep, Dist: distToEp}

	for level := int(maxLevel); level > 0; level-- {
		// Greedy search: keep 1 best candidate
		res, err := h.searchLayer(ctx, nil, currObj.ID, 1, level, searchCtx, data, queryVec)
		if err != nil {
			return nil, err
		}

		candidates := res
		if len(candidates) > 0 {
			currObj = candidates[0]
		}
	}

	// 2. Search at layer 0 with adaptive retry
	efSearch := int(h.config.EfSearch)
	if h.config.SQ8Enabled && efSearch < 100 {
		// Provide more search buffer by default for SQ8 to compensate for quantization noise
		efSearch = 100
	}

	if k > efSearch {
		efSearch = k
	}

	var results []SearchResult
	var qv []float32
	var ok bool
	if qv, ok = queryVec.([]float32); !ok {
		// Fallback to non-retry path if not float32 (unlikely for this path)
		res, err := h.searchLayer(ctx, nil, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		if err != nil {
			return nil, err
		}
		sort.Slice(res, func(i, j int) bool { return res[i].Dist < res[j].Dist })
		result := make([]SearchResult, 0, k)
		for _, c := range res {
			if (h.deleted != nil && h.deleted.Contains(c.ID)) || (filter != nil && !filter.Contains(c.ID)) {
				continue
			}
			result = append(result, SearchResult{ID: VectorID(c.ID), Distance: c.Dist, Score: 1.0 / (1.0 + c.Dist)})
			if len(result) >= k {
				break
			}
		}
		return result, nil
	}

	maxNodeCount := int(h.nodeCount.Load())
	for attempt := 0; attempt < 3; attempt++ {
		res, err := h.searchLayer(ctx, nil, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		if err != nil {
			return nil, err
		}

		typeCandidates := make([]types.Candidate, len(res))
		for i, c := range res {
			typeCandidates[i] = types.Candidate{ID: c.ID, Dist: c.Dist}
		}

		results = processResultsParallelInternal(ctx, h, qv, typeCandidates, k, nil, filter)
		if len(results) >= k || attempt == 2 || efSearch >= maxNodeCount {
			break
		}

		// Expand search search space
		efSearch *= 5
		if efSearch > maxNodeCount {
			efSearch = maxNodeCount
		}

	}

	return results, nil
}

func (h *ArrowHNSW) Warmup() int {
	return int(h.nodeCount.Load())
}

func (h *ArrowHNSW) GetNeighbors(id uint32) ([]uint32, error) {
	return nil, nil // Stub
}

func (h *ArrowHNSW) PreWarm(targetSize int) {
	// Stub
}

func (h *ArrowHNSW) growNoLock(_, _ int) error {
	// Simple stub or implementation of resizing without acquiring growMu (caller holds it)
	return nil
}

func (h *ArrowHNSW) Grow(capacity, dims int) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	start := time.Now()
	defer func() {
		metrics.HNSWIndexGrowthDuration.Observe(time.Since(start).Seconds())
	}()

	data := h.data.Load()
	if data == nil {
		// Should have been initialized in NewArrowHNSW
		return fmt.Errorf("index data is nil")
	}

	// Calculate current vs target
	currentCap := data.Capacity
	currentDims := data.Dims

	if capacity <= currentCap && dims == currentDims {
		return nil
	}

	// COW: Clone the current data structure
	// This ensures readers holding old 'data' pointer are safe.
	// New data structure will have updated capacity and chunks.
	newData := data.Clone()
	newData.Capacity = capacity
	newData.Dims = dims

	// Iteratively ensure chunks
	numChunks := (capacity + types.ChunkSize - 1) / types.ChunkSize
	for i := 0; i < numChunks; i++ {
		if err := newData.EnsureChunk(i, 0, dims); err != nil {
			fmt.Printf("Grow EnsureChunk failed: %v\n", err)
			return err
		}
	}

	// Atomic swap
	h.data.Store(newData)
	return nil
}

func (h *ArrowHNSW) SetEfConstruction(ef int32) {
	h.config.EfConstruction = ef
	h.efConstruction.Store(ef)
}

func (h *ArrowHNSW) CleanupTombstones(threshold int) (int, error) {
	return 0, nil // Stub
}

func (h *ArrowHNSW) SetIndexedColumns(columns []string) {
	// No-op for now
}

func (h *ArrowHNSW) generateLevel() int {
	l := int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier))
	if l > types.ArrowMaxLayers-1 {
		l = types.ArrowMaxLayers - 1
	}
	return l
}

// AddBatch implements VectorIndex.
func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, 0, len(rowIdxs))
	for i := range rowIdxs {
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				return ids, err
			}
		}

		// Robust record resolution
		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		switch {
		case bIdx < len(recs) && recs[bIdx] != nil:
			rec = recs[bIdx]
		case len(recs) == 1:
			rec = recs[0]
		case i < len(recs):
			rec = recs[i]
		default:
			return ids, fmt.Errorf("could not resolve record batch for row %d (batchIdx %d, recs len %d)", i, bIdx, len(recs))
		}

		id, err := h.AddByRecord(ctx, rec, rowIdxs[i], batchIdxs[i])
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (h *ArrowHNSW) EstimateMemory() int64 {
	return 1024 * 1024 * 100 // 100MB placeholder
}

func (h *ArrowHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	// Optimization: Convert filters to bitset
	var bitset *query.Bitset
	if len(filters) > 0 && h.dataset != nil {
		var err error
		bitset, err = h.dataset.GenerateFilterBitset(filters)
		if err != nil {
			return nil, err
		}
		if bitset != nil {
			defer bitset.Release()
		}
	}

	var roaringFilter *roaring.Bitmap
	if bitset != nil {
		roaringFilter = bitset.AsRoaring()
	}

	// Returns []SearchResult, error
	return h.SearchVectorsWithBitmap(ctx, queryVec, k, roaringFilter, options)
}

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, _ *ArrowSearchContext, queryVal any, _ bool) any {
	switch q := queryVal.(type) {
	case []float32:
		return &float32Computer{data: data, q: q, dims: len(q), h: h}
	case []int8, []uint8:
		var q8 []uint8
		if qi8, ok := queryVal.([]int8); ok {
			q8 = *(*[]uint8)(unsafe.Pointer(&qi8))
		} else {
			q8 = queryVal.([]uint8)
		}
		return &int8Computer{data: data, q: q8, dims: len(q8), h: h}
	case []float64:
		return &float64Computer{data: data, q: q, dims: len(q)}
	case []complex64:
		return &complex64Computer{data: data, q: q, dims: len(q)}
	case []complex128:
		return &complex128Computer{data: data, q: q, dims: len(q)}
	}
	return nil
}

// searchLayer is used by insertion logic
// searchLayer implements HNSW layer search
func (h *ArrowHNSW) searchLayer(_ context.Context, computer any, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData, queryVec any) ([]Candidate, error) {
	// Initialize Heaps
	// ctx.candidates (MinHeap) - Nodes to visit
	// ctx.resultSet (MaxHeap) - Best nodes found so far

	ctx.visited.Clear()

	// We need MinCandidateHeap wrapper for ctx.candidates (which is []Candidate)
	minHeap := (*MinCandidateHeap)(&ctx.candidates)
	*minHeap = (*minHeap)[:0] // Clear

	// Clear Result Set
	ctx.resultSet.Clear()

	// Adapt MaxHeap
	resultSetAdapter := MaxCandidateHeapAdapter{CandidateHeap: &ctx.resultSet}

	// Define polymorphic distance computer
	var distComputer func(uint32) (float32, error)
	var epDist float32

	// Optimization: Use specialized computer if available
	if comp, ok := computer.(interface {
		ComputeSingle(id uint32) (float32, error)
	}); ok {
		distComputer = comp.ComputeSingle
		var err error
		epDist, err = comp.ComputeSingle(entryPoint)
		if err != nil {
			return nil, err
		}
	} else {
		switch q := queryVec.(type) {
		case []float32:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithData(data, id)
				if err != nil {
					return 0, err
				}
				switch v := vecAny.(type) {
				case []float32:
					return h.distFunc(q, v)
				case []float64:
					if h.distFuncF64 == nil {
						return math.MaxFloat32, nil
					}
					q64 := make([]float64, len(q))
					for i, val := range q {
						q64[i] = float64(val)
					}
					return h.distFuncF64(q64, v)
				case []float16.Num:
					if h.distFuncF16 == nil {
						return math.MaxFloat32, nil
					}
					q16 := make([]float16.Num, len(q))
					for i, val := range q {
						q16[i] = float16.New(val)
					}
					return h.distFuncF16(q16, v)
				case []int8, []uint8:
					var v8 []uint8
					if vi8, ok := v.([]int8); ok {
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8))
					} else {
						v8 = v.([]uint8)
					}

					if h.quantizer != nil && h.sq8Ready.Load() {
						minV, maxV := h.quantizer.Params()
						scale := (maxV - minV) / 255.0
						var sum float32
						for i, val := range q {
							deq := minV + float32(v8[i])*scale
							diff := val - deq
							sum += diff * diff
						}
						return float32(math.Sqrt(float64(sum))), nil
					}
					// Fallback
					var sum float32
					for i, val := range q {
						diff := val - float32(v8[i])
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []int8:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithData(data, id)
				if err != nil {
					return 0, err
				}
				switch vAny := vecAny.(type) {
				case []float32:
					// Convert q to float32
					minV, maxV := h.quantizer.Params()
					scale := (maxV - minV) / 255.0
					var sum float32
					for i, val := range q {
						deq := minV + float32(val)*scale
						diff := deq - vAny[i]
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				case []int8, []uint8:
					var v8 []uint8
					if vi8, ok := vAny.([]int8); ok {
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8))
					} else {
						v8 = vAny.([]uint8)
					}

					var q8 []uint8
					q8 = *(*[]uint8)(unsafe.Pointer(&q))

					if len(q8) != len(v8) {
						return math.MaxFloat32, nil
					}

					var sum float32
					if h.quantizer != nil && h.sq8Ready.Load() {
						minV, maxV := h.quantizer.Params()
						scale := (maxV - minV) / 255.0
						for i, val := range q8 {
							// De-quantize: min + level * scale
							deqQ := minV + float32(val)*scale
							deqV := minV + float32(v8[i])*scale
							diff := deqQ - deqV
							sum += diff * diff
						}
					} else {
						// Fallback
						for i, val := range q8 {
							diff := float32(val) - float32(v8[i])
							sum += diff * diff
						}
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []complex64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithData(data, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]complex64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float32
					for i, val := range q {
						diff := val - v[i]
						modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
						sum += modSq
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []complex128:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithData(data, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]complex128); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float64
					for i, val := range q {
						diff := val - v[i]
						modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
						sum += modSq
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []float64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithData(data, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]float64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					if h.distFuncF64 != nil {
						return h.distFuncF64(q, v)
					}
					// Fallback Euclidean
					var sum float64
					for i, val := range q {
						diff := val - v[i]
						sum += diff * diff
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		default:
			return nil, fmt.Errorf("searchLayer: unsupported query vector type %T", queryVec)
		}
	}

	epCand := Candidate{ID: entryPoint, Dist: epDist}
	heap.Push(minHeap, epCand)
	heap.Push(resultSetAdapter, epCand) // resultSet is MaxHeap
	ctx.visited.Set(int(entryPoint))

	for minHeap.Len() > 0 {
		// Pop closest candidate
		curr := heap.Pop(minHeap).(Candidate)

		// Furthest in resultSet (MaxHeap Top)
		// We can Peek using index 0 if internal structure is known (slice)
		// MaxCandidateHeapAdapter wraps CandidateHeap which is slice.
		// heap.Interface doesn't strictly expose Peek, but we can access underlying slice logic.
		// Or just trust logic.

		if len(ctx.resultSet) > 0 {
			furthest := ctx.resultSet[0]
			threshold := furthest.Dist
			if h.config.SQ8Enabled {
				// Be more lenient for SQ8 during searching as distance might be slightly noisy
				threshold *= 1.05
			}
			if curr.Dist > threshold && ctx.resultSet.Len() >= ef {
				// Optimization: if closest candidate is worse than worst result, stop
				break
			}
		}

		// Explore neighbors
		// Use computer.GetNeighbors logic?
		// Or standard HNSW neighbor retrieval.
		// GetNeighbors returns the Neighbors slice.

		// Lock/RLock needed?
		// Neighbors are atomic unless resize?
		neighbors := data.GetNeighbors(layer, curr.ID, nil) // Copy? Helper above does copy.

		for _, n := range neighbors {
			if ctx.visited.IsSet(int(n)) {
				continue
			}
			ctx.visited.Set(int(n))

			// Distance calculation using polymorphic computer
			d, err := distComputer(n)
			if err != nil {
				// Warn or continue? Continue.
				continue
			}

			cand := Candidate{ID: n, Dist: d}

			if len(ctx.resultSet) > 0 {
				furthest := ctx.resultSet[0]

				if ctx.resultSet.Len() < ef || d < furthest.Dist {
					heap.Push(minHeap, cand)
					heap.Push(resultSetAdapter, cand)

					if ctx.resultSet.Len() > ef {
						heap.Pop(resultSetAdapter) // Remove furthest
					}
				}
			} else {
				// Empty resultSet
				heap.Push(minHeap, cand)
				heap.Push(resultSetAdapter, cand)
			}
		}
	}

	// Return results as sorted slice (ascending distance)
	// resultSet is a MaxHeap, so popping from it gives largest first.
	// We populate the result slice from end to beginning.
	count := len(ctx.resultSet)
	res := make([]Candidate, count)
	for i := count - 1; i >= 0; i-- {
		res[i] = heap.Pop(&resultSetAdapter).(Candidate)
	}
	return res, nil
}

func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// ExportState implements VectorIndex.
func (h *ArrowHNSW) ExportState() ([]byte, error) {
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	locs := make([]Location, 0, h.locationStore.Len())
	h.locationStore.IterateMutable(func(_ VectorID, val *atomic.Uint64) {
		loc := core.UnpackLocation(val.Load())
		locs = append(locs, loc)
	})

	var buf bytes.Buffer
	if err := h.ExportGraph(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportState implements VectorIndex.
func (h *ArrowHNSW) ImportState(data []byte) error {
	return h.ImportGraph(bytes.NewReader(data))
}

// ExportGraph implements VectorIndex.
// SnapshotGraph captures the current graph state for serialization.
// It returns a copy-on-write snapshot that is safe to serialize concurrently with updates.
func (h *ArrowHNSW) SnapshotGraph() (*types.GraphData, *types.SyncState, error) {
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	data := h.data.Load()
	if data == nil {
		return nil, nil, fmt.Errorf("no graph data to snapshot")
	}

	snap := data.CloneForSnapshot()

	locs := make([]Location, 0, h.locationStore.Len())
	h.locationStore.IterateMutable(func(_ VectorID, val *atomic.Uint64) {
		loc := core.UnpackLocation(val.Load())
		locs = append(locs, loc)
	})

	state := &types.SyncState{
		Version:   1,
		Dims:      int(h.dims.Load()),
		Locations: locs,
	}

	return snap, state, nil
}

// ExportGraph implements VectorIndex.
func (h *ArrowHNSW) ExportGraph(w io.Writer) error {
	// 1. Capture Snapshot + Metadata
	var snapshot *types.GraphData
	locs := make([]Location, 0, h.locationStore.Len())
	var dims int

	// Inline lock scope
	func() {
		h.growMu.RLock()
		defer h.growMu.RUnlock()

		if data := h.data.Load(); data != nil {
			snapshot = data.CloneForSnapshot()
		}

		h.locationStore.IterateMutable(func(_ VectorID, val *atomic.Uint64) {
			loc := core.UnpackLocation(val.Load())
			locs = append(locs, loc)
		})
		dims = int(h.dims.Load())
	}()

	if snapshot == nil {
		return fmt.Errorf("no graph data to export")
	}

	state := types.SyncState{
		Version:   1,
		Dims:      dims,
		Locations: locs,
	}

	// Use temporary buffer for metadata part
	var metaBuf bytes.Buffer
	if err := gob.NewEncoder(&metaBuf).Encode(state); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	metaBytes := metaBuf.Bytes()

	// Write Metadata Length + Bytes
	if err := binary.Write(w, binary.LittleEndian, uint32(len(metaBytes))); err != nil {
		return err
	}
	if _, err := w.Write(metaBytes); err != nil {
		return err
	}

	// 3. Export Snapshot GraphData
	return snapshot.Serialize(w)
}

// ImportGraph implements VectorIndex.
func (h *ArrowHNSW) ImportGraph(r io.Reader) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	// 1. Read Metadata
	var metaLen uint32
	if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
		return fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var state types.SyncState
	if err := gob.NewDecoder(bytes.NewReader(metaBytes)).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	// Apply Metadata
	h.dims.Store(int32(state.Dims))
	h.locationStore.Reset()
	for _, loc := range state.Locations {
		h.locationStore.Append(loc)
	}

	// 2. Read GraphData
	data, err := types.DeserializeGraphData(r)
	if err != nil {
		return fmt.Errorf("failed to deserialize graph data: %w", err)
	}

	// Swap data
	h.data.Store(data)

	// Restore configuration flags
	h.config.BQEnabled = data.BQEnabled
	h.config.PQEnabled = data.PQEnabled
	h.config.PQM = data.PQM
	h.config.SQ8Enabled = data.SQ8Enabled
	// Restore node count from metadata (number of valid locations)
	h.nodeCount.Store(int64(len(state.Locations)))
	// Capacity isn't count. Size is count.
	// But serialize loops up to Capacity. If nodes were added sequentially, Len ~ Capacity.
	// We might need to track actual `Len` or `NodeCount` in `GraphData`.
	// For now, assume Capacity matches loaded size (sparse gaps handled as zeros).
	// Ideally `SyncState` should have `NodeCount`.
	// Let's assume capacity for now or update SyncState next time.

	// Reset runtime structures
	if h.searchPool == nil {
		h.searchPool = NewArrowSearchContextPool()
	}

	return nil
}

// ExportDelta implements VectorIndex.
func (h *ArrowHNSW) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	// Delta sync implementation (Phase 4.1 or later)
	// For now return full state if version mismatch too large, or error.
	return nil, fmt.Errorf("ExportDelta not implemented")
}

// ApplyDelta implements VectorIndex.
func (h *ArrowHNSW) ApplyDelta(delta *types.DeltaSync) error {
	return fmt.Errorf("ApplyDelta not implemented")
}

// GetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	h.parallelConfig = cfg
}

// ParallelSearchHost implementation for ArrowHNSW
func (h *ArrowHNSW) GetDataset() *Dataset { return h.dataset }

func (h *ArrowHNSW) GetLocationForParallel(id uint32) (core.Location, bool) {
	return h.locationStore.Get(VectorID(id))
}

func (h *ArrowHNSW) ExtractVectorForParallel(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	// Find vector column by name
	vecColIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == "vector" {
			vecColIdx = i
			break
		}
	}

	if vecColIdx == -1 {
		// Fallback to column 0 if only 1 column
		if rec.NumCols() == 1 {
			vecColIdx = 0
		} else {
			return nil, fmt.Errorf("vector column not found in record")
		}
	}

	return ExtractVectorFromArrow(rec, rowIdx, vecColIdx)
}

func (h *ArrowHNSW) GetDistanceFuncForParallel() func([]float32, []float32) float32 {
	return func(a, b []float32) float32 {
		d, _ := h.distFunc(a, b)
		return d
	}
}

func (h *ArrowHNSW) GetPQEnabledForParallel() bool          { return h.config.PQEnabled }
func (h *ArrowHNSW) GetPQEncoderForParallel() *pq.PQEncoder { return h.pqEncoder }

func (h *ArrowHNSW) ExtractVectorByIDForParallel(id uint32) ([]float32, error) {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return nil, err
	}
	if v, ok := vecAny.([]float32); ok {
		return v, nil
	}

	// Handle Complex128/64 and other types by flattening to float32
	switch v := vecAny.(type) {
	case []complex128:
		res := make([]float32, len(v)*2)
		for i, val := range v {
			res[i*2] = float32(real(val))
			res[i*2+1] = float32(imag(val))
		}
		return res, nil
	case []complex64:
		// Complex64 is 2x float32 in memory, treat it as a flattened float32 slice
		if len(v) == 0 {
			return nil, nil
		}
		res := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2)
		// We should return a copy since the parallel worker might use it concurrently
		resCopy := make([]float32, len(res))
		copy(resCopy, res)
		return resCopy, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			return h.quantizer.Decode(v), nil
		}
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	}

	// Handle SQ8 de-quantization for DiskGraph/Compressed vectors
	if h.quantizer != nil && h.sq8Ready.Load() {
		if v8, ok := vecAny.([]byte); ok {
			return h.quantizer.Decode(v8), nil
		}
	}

	// Handle PQ codes for parallel search (they are returned as []byte from GetVector)
	if h.config.PQEnabled && h.pqEncoder != nil {
		if v8, ok := vecAny.([]byte); ok {
			// The parallel search worker expects PQ codes to be packed into the first M bytes of a []float32
			codeSize := h.pqEncoder.CodeSize()
			if len(v8) == codeSize {
				res := make([]float32, (codeSize+3)/4)
				ptr := unsafe.Pointer(&res[0])
				dest := unsafe.Slice((*byte)(ptr), codeSize)
				copy(dest, v8)
				return res, nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search", vecAny)
}

func (h *ArrowHNSW) SearchForParallel(queryVec []float32, k int) []types.Candidate {
	// Use the existing Search implementation which handles bitmask and conversion
	res, err := h.Search(context.Background(), queryVec, k, nil)
	if err != nil {
		return nil
	}
	return res
}

// SearchWithArena performs k-NN search using an arena allocator for results.
func (h *ArrowHNSW) SearchWithArena(queryVec []float32, k int, arena any) []VectorID {
	// Fallback to standard search if no arena
	if arena == nil {
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]VectorID, len(results))
		for i, r := range results {
			ids[i] = VectorID(r.ID)
		}
		return ids
	}

	searchArena, ok := arena.(*SearchArena)
	if !ok {
		// Try casting if it's passed as interface
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]VectorID, len(results))
		for i, r := range results {
			ids[i] = VectorID(r.ID)
		}
		return ids
	}

	results, err := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
	if err != nil || len(results) == 0 {
		return nil
	}

	ids := searchArena.AllocVectorIDSlice(len(results))
	if ids == nil {
		// Fallback to heap if arena exhausted
		ids = make([]VectorID, len(results))
	}

	for i, r := range results {
		ids[i] = VectorID(r.ID)
	}
	return ids
}

type float32Computer struct {
	data *types.GraphData
	q    []float32
	dims int
	h    *ArrowHNSW
}

func (c *float32Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithData(c.data, id)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []float32:
		return c.h.distFunc(c.q, v)
	case []int8, []uint8:
		var v8 []uint8
		if vi8, ok := v.([]int8); ok {
			v8 = *(*[]uint8)(unsafe.Pointer(&vi8))
		} else {
			v8 = v.([]uint8)
		}

		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deq := minV + float32(v8[i])*scale
				diff := val - deq
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v8[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []complex64:
		// Treats complex64 as flattened []float32 [re, im, re, im...]
		if len(c.q) != len(v)*2 {
			return math.MaxFloat32, nil
		}
		// Unsafe cast to []float32
		vf := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2)
		return simd.EuclideanDistance(c.q, vf)
	case []complex128:
		// Treats complex128 as flattened []float64, but query is float32
		if len(c.q) != len(v)*2 {
			return math.MaxFloat32, nil
		}
		// Calculate Euclidean distance between []float32 and []complex128
		var sum float64
		for i, val := range v {
			re := float64(real(val))
			im := float64(imag(val))
			diffRe := float64(c.q[i*2]) - re
			diffIm := float64(c.q[i*2+1]) - im
			sum += diffRe*diffRe + diffIm*diffIm
		}
		return float32(math.Sqrt(sum)), nil
	case []float64:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float64
		for i, val := range v {
			diff := float64(c.q[i]) - val
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	}
	return math.MaxFloat32, nil
}

type int8Computer struct {
	data *types.GraphData
	q    []uint8
	dims int
	h    *ArrowHNSW
}

func (c *int8Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithData(c.data, id)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []float32:
		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deq := minV + float32(val)*scale
				diff := deq - v[i]
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		var sum float32
		for i, val := range c.q {
			diff := float32(val) - v[i]
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []int8, []uint8:
		var v8 []uint8
		if vi8, ok := v.([]int8); ok {
			v8 = *(*[]uint8)(unsafe.Pointer(&vi8))
		} else {
			v8 = v.([]uint8)
		}

		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deqQ := minV + float32(val)*scale
				deqV := minV + float32(v8[i])*scale
				diff := deqQ - deqV
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		var sum float32
		for i, val := range c.q {
			diff := float32(val) - float32(v8[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	}
	return math.MaxFloat32, nil
}

func (h *ArrowHNSW) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	if h.locationStore == nil {
		return fmt.Errorf("location store not initialized")
	}

	for id, locAny := range mapping {
		if loc, ok := locAny.(core.Location); ok {
			h.locationStore.Set(core.VectorID(id), loc)
		} else if loc, ok := locAny.(Location); ok {
			h.locationStore.Set(core.VectorID(id), loc)
		}
	}
	return nil
}
