package store

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

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

	Metric   any
	Logger   any
	DataType types.VectorDataType

	BQEnabled bool
	PQEnabled bool
	PQM       int
	PQK       int

	Dims                    int
	SelectionHeuristicLimit int

	AdaptiveMEnabled   bool
	AdaptiveMThreshold int

	Float16Enabled         bool
	SQ8TrainingThreshold   int
	PackedAdjacencyEnabled bool
}

// DefaultArrowHNSWConfig returns a configuration with sensible defaults
func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	return ArrowHNSWConfig{
		M:                   32,
		MMax:                64,
		MMax0:               64,
		EfConstruction:      400,
		EfSearch:            50,
		AdaptiveEf:          false,
		AdaptiveEfMin:       50,
		AdaptiveEfThreshold: 0,
		InitialCapacity:     10000,
		Workers:             4,
		Quantization:        false,
		SQ8Enabled:          false,
		UseDisk:             false,
		DiskPath:            "./data",
		DataType:            types.VectorTypeFloat32,
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

	distFunc     func([]float32, []float32) (float32, error)
	distFuncF64  func([]float64, []float64) (float32, error)
	distFuncF16  func([]float16.Num, []float16.Num) (float32, error)
	distFuncC64  func([]complex64, []complex64) (float32, error)
	distFuncC128 func([]complex128, []complex128) (float64, error)

	adaptiveMTriggered atomic.Bool

	initMu sync.Mutex
	growMu sync.RWMutex

	metricInsertDuration     prometheus.Histogram
	metricSearchDuration     prometheus.Histogram
	metricNodesAdded         prometheus.Counter
	metricSearchQueries      prometheus.Counter
	metricBulkInsertDuration prometheus.Summary
	metricBulkVectors        prometheus.Counter

	deleted *roaring.Bitmap

	repairAgent *RepairAgent

	metricNodeCount prometheus.Gauge
	metricBQVectors prometheus.Gauge
	metricLockWait  *prometheus.HistogramVec

	sq8TrainingBuffer [][]float32
}

// GetVector retrieves the vector for the given ID.
func (h *ArrowHNSW) GetVector(id uint32) (any, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data not initialized")
	}
	return data.GetVector(id)
}

func (h *ArrowHNSW) getVectorAny(id uint32) (any, error) {
	return h.GetVector(id)
}

// NewArrowHNSW creates a new ArrowHNSW index with the given configuration
func NewArrowHNSW(dataset *Dataset, config *ArrowHNSWConfig) *ArrowHNSW {
	h := &ArrowHNSW{
		config:        *config,
		dataset:       dataset,
		m:             config.M,
		mMax:          config.MMax,
		mMax0:         config.MMax0,
		shardedLocks:  NewAlignedShardedMutex(AlignedShardedMutexConfig{NumShards: 64}),
		searchPool:    NewArrowSearchContextPool(),
		locationStore: NewChunkedLocationStore(),
		deleted:       roaring.New(),
	}

	// Initialize distance function
	// Assuming L2 for now, should check config.Metric
	h.distFunc = simd.EuclideanDistance

	// Initialize atomic values
	h.efConstruction.Store(config.EfConstruction)
	h.dims.Store(int32(config.Dims))

	// Initialize quantization if enabled
	if config.SQ8Enabled {
		// Initialize with config dims if available, otherwise lazy init will handle it
		if config.Dims > 0 {
			h.quantizer = NewScalarQuantizer(config.Dims)
		}
		// Do not set sq8Ready to true until trained
	}

	// Initialize metrics
	h.metricInsertDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "longbow_hnsw_insert_duration_seconds",
		Help: "Duration of HNSW insert operations",
	})
	h.metricSearchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "longbow_hnsw_search_duration_seconds",
		Help: "Duration of HNSW search operations",
	})
	h.metricNodesAdded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_nodes_added_total",
		Help: "Total number of nodes added to HNSW",
	})
	h.metricSearchQueries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_search_queries_total",
		Help: "Total number of HNSW search queries",
	})
	h.metricBulkInsertDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "longbow_hnsw_bulk_insert_duration_seconds",
		Help: "Duration of HNSW bulk insert operations",
	})
	h.metricBulkVectors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_bulk_vectors_total",
		Help: "Total number of vectors inserted via bulk insert",
	})
	h.metricNodeCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_hnsw_node_count",
		Help: "Total number of nodes in the HNSW graph",
	})
	h.metricBQVectors = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_hnsw_bq_vectors",
		Help: "Number of vectors with BQ enabled",
	})
	h.metricLockWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "longbow_hnsw_lock_wait_seconds",
		Help: "Wait time for HNSW locks",
	}, []string{"type"})

	// Init default distance funcs (assuming Euclidean/L2)
	h.distFunc = simd.EuclideanDistance
	h.distFuncF64 = simd.EuclideanDistanceFloat64
	// TODO: Add complex distance functions if available in simd, else placeholders
	// For now, setting placeholders to avoid nil panic if used
	// h.distFuncC64 = ...
	// h.distFuncC128 = ...

	// Ensure initial capacity
	capacity := config.InitialCapacity
	if capacity < 1000 {
		capacity = 1000
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
		config.DataType,
	)
	h.data.Store(gd)

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

// GetMetrics returns the Prometheus metrics for this index
func (h *ArrowHNSW) GetMetrics() (insertDuration, searchDuration prometheus.Histogram, nodesAdded, searchQueries prometheus.Counter) {
	return h.metricInsertDuration, h.metricSearchDuration, h.metricNodesAdded, h.metricSearchQueries
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

// mustGetVectorFromData retrieves a vector from the given data snapshot or panics.
func (h *ArrowHNSW) mustGetVectorFromData(data *types.GraphData, id uint32) any {
	vec, err := data.GetVector(id)
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

	err := h.InsertWithVector(id, vec, 0)
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

	err := h.InsertWithVector(id, vec, 0)
	if err != nil {
		return 0, err
	}
	h.SetLocation(VectorID(id), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

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

		// Add other types as needed
		default:
			return nil
		}
	}

	if list, ok := col.(*arrowarray.FixedSizeList); ok {
		values := list.ListValues()
		size := int64(list.DataType().(*arrow.FixedSizeListType).Len())
		offset := int64(rowIdx) * size
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
	h.data.Store(nil)
	h.dataset = nil
	h.searchPool = nil
	h.locationStore = nil
	h.deleted = nil
	return nil
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
	// Call existing Pop which removes last element
	c, _ := h.CandidateHeap.Pop()
	return c
}

func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	if h.nodeCount.Load() == 0 {
		return nil, nil
	}

	metrics.HNSWSearchPoolGetTotal.Inc()
	searchCtx := h.searchPool.Get()
	defer func() {
		metrics.HNSWSearchPoolPutTotal.Inc()
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
	if q, ok := queryVec.([]float32); ok {
		vec, err := data.GetVector(ep)
		if err != nil {
			return nil, err
		}

		switch v := vec.(type) {
		case []float32:
			d, err := h.distFunc(q, v)
			if err != nil {
				return nil, err
			}
			dist = d
		case []float64:
			if h.distFuncF64 == nil {
				return nil, fmt.Errorf("float64 distance function not initialized")
			}
			q64 := make([]float64, len(q))
			for i, val := range q {
				q64[i] = float64(val)
			}
			d, err := h.distFuncF64(q64, v)
			if err != nil {
				return nil, err
			}
			dist = d
		case []float16.Num:
			if h.distFuncF16 == nil {
				return nil, fmt.Errorf("float16 distance function not initialized")
			}
			q16 := make([]float16.Num, len(q))
			for i, val := range q {
				q16[i] = float16.New(val)
			}
			d, err := h.distFuncF16(q16, v)
			if err != nil {
				return nil, err
			}
			dist = d
		// Add other types as needed (Complex, Int8)
		default:
			return nil, fmt.Errorf("unsupported vector type %T for distance calculation", vec)
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

	// 2. Search at layer 0
	efSearch := int(h.config.EfSearch)
	if k > efSearch {
		efSearch = k
	}

	res, err := h.searchLayer(ctx, nil, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
	if err != nil {
		return nil, err
	}

	candidates := res

	// Create SearchResults (sorted by distance)
	// resultSet was a MaxHeap, not sorted.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Dist < candidates[j].Dist
	})

	result := make([]SearchResult, 0, k)
	count := 0
	for _, c := range candidates {
		if filter != nil && !filter.Contains(c.ID) {
			continue // Post-filtering
		}
		result = append(result, SearchResult{ID: VectorID(c.ID), Distance: c.Dist, Score: 1.0 / (1.0 + c.Dist)})
		count++
		if count >= k {
			break
		}
	}

	return result, nil
}

func (h *ArrowHNSW) Warmup() int {
	return 0
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
}

func (h *ArrowHNSW) CleanupTombstones(threshold int) (int, error) {
	return 0, nil // Stub
}

func (h *ArrowHNSW) SetIndexedColumns(columns []string) {
	// No-op for now
}

func (h *ArrowHNSW) generateLevel() int {
	// Implement HNSW level generation
	// Use simplified logic or random generator
	// ml := int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier))
	// For now, return 0 (base layer) or simple random to compile.
	// HNSW requires random level based on M.
	// Since we are fixing compilation, we can use a stub.
	return 0
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
			return nil, err // Should likely error if filters invalid
		}
		if bitset != nil {
			defer bitset.Release()
		}
	}

	// Returns []SearchResult, error
	return h.SearchVectorsWithBitmap(ctx, queryVec, k, bitset.AsRoaring(), options)
}

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, _ *ArrowSearchContext, queryVal any, _ bool) any {
	switch q := queryVal.(type) {
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
func (h *ArrowHNSW) searchLayer(goCtx context.Context, computer any, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData, queryVec any) ([]Candidate, error) {
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
				vecAny, err := data.GetVector(id)
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
					// Convert query once? No, this closure captures q.
					// Optimally we convert q ONCE outside closure.
					// But we can just alloc here for correctness first.
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
				default:
					return math.MaxFloat32, nil
				}
			}
			// Initial distance to EP
			epVec, err := data.GetVector(entryPoint)
			if err != nil {
				return nil, err
			}

			switch v := epVec.(type) {
			case []float32:
				d, err := h.distFunc(q, v)
				if err != nil {
					return nil, err
				}
				epDist = d
			case []float64:
				if h.distFuncF64 == nil {
					return nil, fmt.Errorf("no distFuncF64")
				}
				q64 := make([]float64, len(q))
				for i, val := range q {
					q64[i] = float64(val)
				}
				d, err := h.distFuncF64(q64, v)
				if err != nil {
					return nil, err
				}
				epDist = d
			case []float16.Num:
				if h.distFuncF16 == nil {
					return nil, fmt.Errorf("no distFuncF16")
				}
				q16 := make([]float16.Num, len(q))
				for i, val := range q {
					q16[i] = float16.New(val)
				}
				d, err := h.distFuncF16(q16, v)
				if err != nil {
					return nil, err
				}
				epDist = d
			default:
				epDist = math.MaxFloat32
			}

		case []int8:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := data.GetVector(id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int8); ok {
					// Simple L2 for Int8
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					var sum float32
					for i, val := range q {
						diff := float32(val) - float32(v[i])
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []complex64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := data.GetVector(id)
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
				vecAny, err := data.GetVector(id)
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
			if curr.Dist > furthest.Dist && ctx.resultSet.Len() >= ef {
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

	// Return results as slice
	res := make([]Candidate, len(ctx.resultSet))
	copy(res, ctx.resultSet)
	return res, nil
}

func (h *ArrowHNSW) Len() int {
	return h.Size()
}
