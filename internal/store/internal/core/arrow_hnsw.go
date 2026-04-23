package core

// nosec G404 - math/rand is used for HNSW operations, not security-sensitive
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
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)


// ArrowHNSW implements a Hierarchical Navigable Small World (HNSW) index
// optimized for Apache Arrow data structures with zero-copy operations.
type ArrowHNSW struct {
	config types.ArrowHNSWConfig

	dataset       types.IndexDataProvider
	data          atomic.Pointer[types.GraphData]
	locationStore *ChunkedLocationStore

	nodeCount      atomic.Int64 // Number of nodes ready for search
	nextID         atomic.Int64 // Next ID to allocate for insertion
	dims           atomic.Int32
	entryPoint     atomic.Uint32
	maxLevel       atomic.Int32
	efConstruction atomic.Int32

	m     int
	mMax  int
	mMax0 int


	// DiskGraph backing
	diskGraph atomic.Pointer[DiskGraph]

	quantizer  *ScalarQuantizer
	sq8Ready   atomic.Bool
	bqEncoder  *types.BQEncoder
	pqEncoder  *pq.PQEncoder
	tqEncoder  *TurboQuantEncoder
	searchPool    *ArrowSearchContextPool
	candidatePool sync.Pool

	name                   string
	disableNodeCountMetric atomic.Bool
	metricsSampleCounter   atomic.Uint64
	topLayerManager        *TopLayerManager

	distFunc     func([]float32, []float32) (float32, error)
	distFuncF64  func([]float64, []float64) (float32, error)
	distFuncF16  func([]float16.Num, []float16.Num) (float32, error)
	distFuncC64  func([]complex64, []complex64) (float32, error)
	distFuncC128 func([]complex128, []complex128) (float32, error)
	distFuncInt8 func([]int8, []int8) (float32, error)
	distFuncUint8 func([]uint8, []uint8) (float32, error)
	distFuncInt16 func([]int16, []int16) (float32, error)
	distFuncUint16 func([]uint16, []uint16) (float32, error)
	distFuncInt32 func([]int32, []int32) (float32, error)
	distFuncUint32 func([]uint32, []uint32) (float32, error)
	distFuncInt64 func([]int64, []int64) (float32, error)
	distFuncUint64 func([]uint64, []uint64) (float32, error)

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
	tqCompute *TurboQuantCompute
	gpuTrained atomic.Bool
}

func (h *ArrowHNSW) GetVector(id uint32) (any, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data not initialized")
	}

	// 1. Try raw vector from memory first (most accurate)
	if v, err := data.GetVector(id); v != nil || err != nil {
		return v, err
	}

	// 2. Fallback to DiskGraph in hybrid mode
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
		// Try raw from disk if available
		if v, _ := dg.GetVector(id); v != nil {
			return v, nil
		}
	}

	// 3. Last resort: internal compressed copies in types.GraphData (only if raw wasn't found)
	if h.config.SQ8Enabled {
		if v := data.GetVectorSQ8(id); v != nil {
			return v, nil
		}
	}
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
	if h.tqEncoder != nil {
		chunk := data.GetVectorsTQChunk(types.ChunkID(id))
		if chunk != nil {
			stride := 4 + (data.Dims-1)*data.TurboQuantBits/8 + (data.Dims+7)/8
			start := int(types.ChunkOffset(id)) * stride // #nosec G115
			return h.tqEncoder.Decode(chunk[start : start+stride])
		}
	}

	return nil, nil
}

// GetVectorAny returns the vector with the given ID as an interface{}.
func (h *ArrowHNSW) GetVectorAny(id uint32) (any, error) {
	return h.GetVector(id)
}

func (h *ArrowHNSW) getVectorWithData(data *types.GraphData, id uint32) (any, error) {
	return h.getVectorWithCachedDisk(data, nil, id)
}

func (h *ArrowHNSW) getVectorWithCachedDisk(data *types.GraphData, dg *DiskGraph, id uint32) (any, error) {
	v, err := data.GetVector(id)
	if v != nil || err != nil {
		return v, err
	}

	// Fallback to DiskGraph
	if dg == nil {
		dg = h.diskGraph.Load()
	}
	if dg != nil {
		if h.config.SQ8Enabled {
			return dg.GetVectorSQ8(id), nil
		}
		if h.config.PQEnabled {
			return dg.GetVectorPQ(id), nil
		}
	}

	// 4. If all else fails, return a sentinel vector of the correct dimensionality
	// This prevents panics in distance calculations during edge case lookups
	metrics.VectorSentinelHitTotal.Inc()
	if data != nil && data.Dims > 0 {
		return make([]float32, data.Dims), nil
	}
	if dims := h.GetDims(); dims > 0 {
		return make([]float32, dims), nil
	}
	return nil, fmt.Errorf("could not resolve dimensions for Sentinel vector")
}

// GetRecords is a helper to get records from the dataset.
func (h *ArrowHNSW) GetRecords() []arrow.RecordBatch {
	if h.dataset == nil {
		return nil
	}
	return h.dataset.GetRecords()
}

// GetDataset returns the underlying dataset provider.
func (h *ArrowHNSW) GetDataset() types.IndexDataProvider {
	return h.dataset
}
// NewArrowHNSW creates a new ArrowHNSW index.
func NewArrowHNSW(dataset types.IndexDataProvider, config *types.ArrowHNSWConfig) *ArrowHNSW {
	return NewArrowHNSWWithConfig(dataset, *config)
}

// NewArrowHNSWWithConfig creates a new ArrowHNSW index with the given configuration.
func NewArrowHNSWWithConfig(dataset types.IndexDataProvider, config types.ArrowHNSWConfig) *ArrowHNSW {
	h := &ArrowHNSW{
		config:          config,
		dataset:         dataset,
		m:               config.M,
		mMax:            config.MMax,
		mMax0:           config.MMax0,
		searchPool:      NewArrowSearchContextPool(),
		candidatePool: sync.Pool{
			New: func() any {
				s := make([]types.Candidate, 0, config.EfConstruction)
				return &s
			},
		},
		locationStore:   NewChunkedLocationStore(),
		deleted:         roaring.New(),
		levelMultiplier: 1.0 / math.Log(float64(config.M)),
		topLayerManager: NewTopLayerManager(config.LockFreeThreshold),
	}

	if h.topLayerManager.threshold == 0 {
		h.topLayerManager.threshold = 2 // Default to layer 2+
	}

	if dataset != nil {
		h.name = dataset.GetName()
	} else {
		h.name = "unknown"
	}

	if dataset != nil {
		// Restore PQ Encoder if present in dataset (e.g. from snapshot)
		if encoder := dataset.GetPQEncoder(); encoder != nil {
			h.pqEncoder = encoder
			h.config.PQEnabled = true
		}
	}

	// Initialize distance functions using resolvers
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

	// Initialize atomic values
	h.efConstruction.Store(int32(config.EfConstruction))
	h.maxLevel.Store(-1)
	if config.Dims > math.MaxInt32 {
		fmt.Printf("Error: dimensions %d exceed MaxInt32, returning nil index\n", config.Dims)
		return nil
	}
	h.dims.Store(int32(config.Dims)) // #nosec G115

	// Initialize quantization if enabled
	if config.SQ8Enabled {
		// Initialize with config dims if available, otherwise lazy init will handle it
		if config.Dims > 0 {
			h.quantizer = NewScalarQuantizer(config.Dims)
		}
		// Do not set sq8Ready to true until trained
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
	h.config.DataType = dt

	// Optimize HNSW Dimension Index Parameters for All Scalar Types at High Scale
	if config.InitialCapacity >= 10000 && config.Dims >= 384 &&
		(dt == types.VectorTypeFloat32 ||
			dt == types.VectorTypeFloat64 ||
			dt == types.VectorTypeInt8 ||
			dt == types.VectorTypeInt16 ||
			dt == types.VectorTypeInt32 ||
			dt == types.VectorTypeUint32 ||
			dt == types.VectorTypeComplex64 ||
			dt == types.VectorTypeComplex128 ||
			dt == types.VectorTypeTQ) {
		if h.m < 24 {
			h.m = 24
		}
		if h.mMax < h.m*2 {
			h.mMax = h.m * 2
		}
		if h.mMax0 < h.m*2 {
			h.mMax0 = h.m * 2
		}
		h.levelMultiplier = 1.0 / math.Log(float64(h.m))
	}

	// Initialize types.GraphData
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
		config.TurboQuantEnabled,
		config.TurboQuantBits,
	)
	if h.pqEncoder != nil {
		gd.PQM = h.pqEncoder.CodeSize()
	}

	// Initialize Layer 0 Lock-Free Adjacency ([#11] Lock-Free Adjacency)
	gd.PackedNeighbors = make([]types.PackedNeighbors, types.ArrowMaxLayers)
	// We use a dedicated SlabArena for Layer 0 adjacency to maximize throughput
	adjArena := memory.NewSlabArena(1024 * 1024 * 32) // 32MB initial slab
	gd.PackedNeighbors[0] = NewPackedAdjacency(adjArena, capacity)

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
		dsName = dataset.GetName()
	}
	h.navigator = NewGraphNavigator(dsName, h.GetData, navConfig, config.Registerer)
	_ = h.navigator.Initialize()

	if config.DataType == types.VectorTypeTQ {
		bits := 3
		h.tqEncoder = NewTurboQuantEncoder(config.Dims, bits, 42)
		h.data.Load().TurboQuantEnabled = true
		h.data.Load().TurboQuantBits = bits
		h.tqCompute = NewTurboQuantCompute(h)
	}

	return h
}

// getCandidateSlice retrieves a pooled candidate slice.
func (h *ArrowHNSW) getCandidateSlice(capacity int) []types.Candidate {
	ptr := h.candidatePool.Get().(*[]types.Candidate)
	s := *ptr
	if cap(s) < capacity {
		s = make([]types.Candidate, 0, capacity)
	} else {
		s = s[:0]
	}
	return s
}

// putCandidateSlice returns a candidate slice to the pool.
func (h *ArrowHNSW) putCandidateSlice(s []types.Candidate) {
	if s == nil {
		return
	}
	h.candidatePool.Put(&s)
}

// SetDisableNodeCountMetric prevents this ArrowHNSW from reporting HNSWNodeCount.
func (h *ArrowHNSW) SetDisableNodeCountMetric(disable bool) {
	h.disableNodeCountMetric.Store(disable)
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
func (h *ArrowHNSW) GetConfig() types.ArrowHNSWConfig {
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
func (h *ArrowHNSW) GetDistanceMetric() basecore.DistanceMetric {
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
func (h *ArrowHNSW) GetBQEncoder() *types.BQEncoder {
	return h.bqEncoder
}

// SetBQEncoder sets the BQ encoder
func (h *ArrowHNSW) SetBQEncoder(encoder *types.BQEncoder) {
	h.bqEncoder = encoder
}

// GetPQEncoder returns the PQ encoder
func (h *ArrowHNSW) GetPQEncoder() *pq.PQEncoder {
	return h.pqEncoder
}

// SetPQEncoder sets the PQ encoder
func (h *ArrowHNSW) SetPQEncoder(encoder *pq.PQEncoder) {
	h.pqEncoder = encoder
	if encoder != nil {
		h.config.PQM = encoder.M
		h.config.PQK = encoder.K
		h.config.PQEnabled = true // Explicitly enable if encoder is set

		data := h.data.Load()
		if data != nil {
			data.PQEnabled = true
			data.PQM = encoder.M
			// Trigger allocation of current chunks
			numChunks := (data.Capacity + types.ChunkSize - 1) / types.ChunkSize
			for i := 0; i < numChunks; i++ {
				if err := data.EnsureChunk(i, 0, data.Dims); err != nil {
					return
				}
			}
		}
	}
}

// setDims sets the vector dimensions
func (h *ArrowHNSW) setDims(dims int32) {
	h.dims.Store(dims)
}

// SetDimension sets the absolute dimension of the index.
func (h *ArrowHNSW) SetDimension(dim int) error {
	if dim > math.MaxInt32 {
		return fmt.Errorf("dimension %d exceeds MaxInt32", dim)
	}
	h.setDims(int32(dim)) // #nosec G115
	// Also ensure types.GraphData is updated to reflect this dimension
	// This is critical if the index was initialized with 0 dims but large capacity (default config).
	h.initMu.Lock()
	defer h.initMu.Unlock()
	data := h.data.Load()
	if data != nil {
		if err := h.Grow(data.Capacity, dim); err != nil {
			return err
		}
	}
	return nil
}

// Delete invokes Delete for a single id.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Add(id)
	h.locationStore.Delete(types.VectorID(id))
	return nil
}

func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	if h.deleted == nil {
		return false
	}
	return h.deleted.Contains(id)
}

// mustGetVectorFromData retrieves a vector from the given data snapshot.
func (h *ArrowHNSW) mustGetVectorFromData(data *types.GraphData, id uint32) any {
	vec, err := h.getVectorWithData(data, id)
	if err != nil {
		return nil
	}
	return vec
}



// ensureChunk ensures that the data structures for the given chunk are allocated.
func (h *ArrowHNSW) ensureChunk(data *types.GraphData, cID, cOff, dims int) (*types.GraphData, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	// Strictly serialized growth to avoid structural races in types.GraphData
	h.growMu.Lock()
	defer h.growMu.Unlock()

	return h.ensureChunkInternal(cID, cOff, dims)
}

func (h *ArrowHNSW) ensureChunkInternal(cID, cOff, dims int) (*types.GraphData, error) {
	// Reload data to ensure we have the latest view before modifying
	data := h.data.Load()

	// Check if we actually need a new chunk
	if !data.NeedsChunk(cID) {
		// Even if chunk exists, we might need to sync dims if it was 0
		if data.Dims == 0 && dims > 0 {
			// This part still potentially modifies in-place, but Dims is just an int.
			// To be purely safe, we could COW here too, but dims usually set on first insert.
			newData := data.Clone()
			newData.Dims = dims
			h.data.Store(newData)
			return newData, nil
		}
		return data, nil
	}

	// Structural growth requires COW to avoid racing with readers
	newData := data.Clone()
	if newData.Dims == 0 && dims > 0 {
		newData.Dims = dims
	}

	err := newData.EnsureChunk(cID, cOff, dims)
	if err != nil {
		return nil, err
	}

	h.data.Store(newData)
	return newData, nil
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

func (h *ArrowHNSW) commitID(id uint32) {
	for h.nodeCount.Load() < int64(id) {
		runtime.Gosched()
	}
	h.nodeCount.CompareAndSwap(int64(id), int64(id+1))
}

// Interface implementation: AddByLocation adds a vector by its location
func (h *ArrowHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	id := uint32(h.nextID.Add(1) - 1) // Use nextID for allocation // #nosec G115
	defer h.commitID(id)

	var vec any
	if h.dataset != nil {
		records := h.dataset.GetRecords()
		if batchIdx < len(records) {
			record := records[batchIdx]
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
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.generateLevel())
	if err != nil {
		return 0, err
	}

	return id, nil
}

// AddByRecord implements VectorIndex.
func (h *ArrowHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	id := uint32(h.nextID.Add(1) - 1) // Use nextID for allocation, nodeCount will be updated by InsertWithVector // #nosec G115
	defer h.commitID(id)

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

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

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
			// But we copy into types.GraphData immediately in InsertWithVector (SetVector does copy).
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

		case *arrowarray.Uint32:
			return arr.Uint32Values()[start:end]
		case *arrowarray.Int32:
			return arr.Int32Values()[start:end]
		case *arrowarray.Uint16:
			return arr.Uint16Values()[start:end]
		case *arrowarray.Int16:
			return arr.Int16Values()[start:end]
		case *arrowarray.Uint8:
			return arr.Uint8Values()[start:end]
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
	start := time.Now()

	if h.nodeCount.Load() == 0 {
		return []types.Candidate{}, nil
	}

	results, err := h.SearchVectorsWithBitmap(ctx, queryVal, k, nil, nil)

	// Record search metrics
	duration := time.Since(start).Seconds()
	typeStr := h.config.DataType.String()
	dimStr := strconv.Itoa(int(h.dims.Load()))
	metrics.HNSWSearchLatencyByType.WithLabelValues(typeStr).Observe(duration)
	metrics.HNSWSearchLatencyByDim.WithLabelValues(dimStr).Observe(duration)

	if err != nil {
		return nil, err
	}

	// Convert []types.SearchResult to []types.Candidate
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
	// Atomically swap in nil to stop new operations
	data := h.data.Swap(nil)
	if data != nil {
		data.Release()
	}
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
	dims := h.GetDims()
	if dims > 0 {
		return uint32(dims)
	}
	if h.dataset != nil && h.dataset.GetSchema() != nil {
		for _, f := range h.dataset.GetSchema().Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				if fslType, ok := f.Type.(*arrow.FixedSizeListType); ok {
					return uint32(fslType.Len()) // #nosec G115
				}
			}
		}
	}
	return 0
}


// MinCandidateHeap for exploration (closest first)
// Uses store.Candidate (ID, Dist) to match ArrowSearchContext
type MinCandidateHeap []types.Candidate

func (h MinCandidateHeap) Len() int           { return len(h) }
func (h MinCandidateHeap) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinCandidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinCandidateHeap) Push(x any)        { *h = append(*h, x.(types.Candidate)) }
func (h *MinCandidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
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

		h.initMu.Unlock()
	}
}

func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h.ensureReady()

	logicalDims := int(h.dims.Load())
	if logicalDims > 0 {
		var physicalDims int
		switch h.config.DataType {
		case types.VectorTypeComplex128, types.VectorTypeComplex64:
			physicalDims = logicalDims * 2
		default:
			physicalDims = logicalDims
		}

		queryLen := 0
		var isComplexIndexWithFloatQuery bool
		switch q := queryVec.(type) {
		case []float32:
			queryLen = len(q)
			if (h.config.DataType == types.VectorTypeComplex64 || h.config.DataType == types.VectorTypeComplex128) &&
				queryLen == physicalDims {
				isComplexIndexWithFloatQuery = true
			}
		case []float64:
			queryLen = len(q)
		case []complex64:
			queryLen = len(q) * 2
		case []complex128:
			queryLen = len(q) * 2
		}

		if queryLen > 0 && !isComplexIndexWithFloatQuery && queryLen != physicalDims {
			return nil, fmt.Errorf("index expects %d elements (logical dims=%d), got query len %d", physicalDims, logicalDims, queryLen)
		}
	}

	if h.nodeCount.Load() == 0 {
		return nil, nil
	}

	if metrics.HNSWSearchPoolGetTotal != nil {
		metrics.HNSWSearchPoolGetTotal.Inc()
	}
	start := time.Now()
	searchCtx := h.searchPool.Get()

	// Extract search options
	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	searchCtx.diskGraph = h.diskGraph.Load()

	// Handle BQ (Binary Quantization) search path
	// If index has BQ enabled and user requests BQ search, use Hamming distance
	useBQSearch := searchOptions.VectorFormat == types.VectorTypeBQ
	if useBQSearch {
		if h.bqEncoder == nil {
			return nil, fmt.Errorf("BQ search requested but index does not have BQ enabled")
		}
		if qf32, ok := queryVec.([]float32); ok {
			searchCtx.queryBQ = h.bqEncoder.Encode(qf32)
			searchCtx.useBQSearch = true
		} else {
			return nil, fmt.Errorf("BQ search requires float32 query vector")
		}
	}

	searchCtx.filterBitmap = filter
	if filter != nil {
		metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(h.name).Inc()
		if filter.IsEmpty() {
			metrics.HNSWFilterEarlyExitTotal.WithLabelValues(h.name).Inc()
			if metrics.HNSWSearchPoolPutTotal != nil {
				metrics.HNSWSearchPoolPutTotal.Inc()
			}
			h.searchPool.Put(searchCtx)
			return nil, nil
		}
	}

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.HNSWSearchDurationSeconds.Observe(duration)

		typeLabel := h.config.DataType.String()
		dimsStr := strconv.Itoa(int(h.dims.Load()))
		metrics.HNSWSearchOpsTotal.WithLabelValues(h.name, typeLabel, dimsStr).Inc()

		// Polymorphic metrics needed for test
		metrics.HNSWPolymorphicSearchCount.WithLabelValues(typeLabel).Inc()
		metrics.HNSWPolymorphicLatency.WithLabelValues(typeLabel).Observe(duration)

		byteThroughput := float64(int(h.dims.Load()) * h.config.DataType.ElementSize())
		metrics.HNSWPolymorphicThroughput.WithLabelValues(typeLabel).Add(byteThroughput)

		searchCtx.filterBitmap = nil
		h.flushSearchMetrics(searchCtx)

		if metrics.HNSWSearchPoolPutTotal != nil {
			metrics.HNSWSearchPoolPutTotal.Inc()
		}
		h.searchPool.PutWithMetrics(searchCtx, typeLabel, dimsStr)
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
	vec, err := h.getVectorWithCachedDisk(data, searchCtx.diskGraph, ep)
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
			// Already handled above
		case []int16:
			if v, ok := vec.([]int16); ok {
				dist = euclideanDistanceInt16(q, v)
			}
		case []uint16:
			if v, ok := vec.([]uint16); ok {
				dist = euclideanDistanceUint16(q, v)
			}
		case []int32:
			if v, ok := vec.([]int32); ok {
				dist = euclideanDistanceInt32(q, v)
			}
		case []uint32:
			if v, ok := vec.([]uint32); ok {
				dist = euclideanDistanceUint32(q, v)
			}
		case []int64:
			if v, ok := vec.([]int64); ok {
				dist = euclideanDistanceInt64(q, v)
			}
		case []uint64:
			if v, ok := vec.([]uint64); ok {
				dist = euclideanDistanceUint64(q, v)
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
	currObj := types.Candidate{ID: ep, Dist: distToEp}

	// 2. Greedy search down through levels
	for level := int(maxLevel); level > 0; level-- { // #nosec G115
		// Greedy search: keep 1 best candidate
		res, err := h.searchLayer(ctx, computer, currObj.ID, 1, level, searchCtx, data, queryVec)
		if err != nil {
			h.flushSearchMetrics(searchCtx)
			return nil, err
		}

		candidates := res
		if len(candidates) > 0 {
			currObj = candidates[0]
		}
	}

	// 2. Search at layer 0 with adaptive retry
	efSearch := int(h.config.EfSearch)
	if searchOptions.Ef > 0 {
		efSearch = searchOptions.Ef
	}
	if h.config.SQ8Enabled && efSearch < 100 {
		// Provide more search buffer by default for SQ8 to compensate for quantization noise
		efSearch = 100
	}

	if k > efSearch {
		efSearch = k
	}

	var results []types.SearchResult
	var qv []float32
	var ok bool
	if qv, ok = queryVec.([]float32); !ok {
		// Fallback to non-retry path if not float32 (unlikely for this path)
		res, err := h.searchLayer(ctx, computer, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		if err != nil {
			return nil, err
		}
		sort.Slice(res, func(i, j int) bool { return res[i].Dist < res[j].Dist })
		result := make([]types.SearchResult, 0, k)
		for _, c := range res {
			if (h.deleted != nil && h.deleted.Contains(c.ID)) || (filter != nil && !filter.Contains(c.ID)) {
				continue
			}
			result = append(result, types.SearchResult{ID: types.VectorID(c.ID), Distance: c.Dist, Score: 1.0 / (1.0 + c.Dist)})
			if len(result) >= k {
				break
			}
		}
		return result, nil
	}

	maxNodeCount := int(h.nodeCount.Load())
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			h.flushSearchMetrics(searchCtx)
			return nil, err
		}
		res, err := h.searchLayer(ctx, computer, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		if err != nil {
			h.flushSearchMetrics(searchCtx)
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

	h.flushSearchMetrics(searchCtx)
	return results, nil
}

func (h *ArrowHNSW) Warmup() int {
	return int(h.nodeCount.Load())
}

// GetLayerNeighbors returns internal neighbor IDs for a specific layer
func (h *ArrowHNSW) GetLayerNeighbors(id uint32, layer int) ([]uint32, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data is nil")
	}

	maxLevel := h.GetMaxLevel()
	if maxLevel < 0 || int64(id) >= h.nodeCount.Load() {
		return nil, fmt.Errorf("vector id %d not found in index", id)
	}

	if layer < 0 || int32(layer) > maxLevel { // #nosec G115
		return nil, fmt.Errorf("invalid layer: %d", layer)
	}

	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	neighborhood := data.GetNeighborsChunk(layer, cID)
	counts := data.GetCountsChunk(layer, cID)
	if neighborhood == nil || counts == nil {
		return nil, nil
	}

	count := atomic.LoadInt32(&counts[cOff])
	if count == 0 {
		return nil, nil
	}

	neighbors := make([]uint32, count)
	startIdx := int(cOff) * types.MaxNeighbors // #nosec G115
	copy(neighbors, neighborhood[startIdx:startIdx+int(count)]) // #nosec G115

	return neighbors, nil
}

// GetRawNeighbors implements the VectorIndexer interface
func (h *ArrowHNSW) GetRawNeighbors(id uint32) ([]uint32, error) {
	return h.GetLayerNeighbors(id, 0)
}

func (h *ArrowHNSW) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	neighbors, err := h.GetLayerNeighbors(id, 0)
	if err != nil || len(neighbors) == 0 {
		return nil, err
	}

	// 1. Get query vector
	qVecAny, err := h.GetVector(id)
	if err != nil {
		return nil, err
	}
	qVec, ok := qVecAny.([]float32)
	if !ok {
		// If not float32, we can't easily compute distances here for now
		// but we still return the neighbors without distances or with 0
		results := make([]types.SearchResult, 0, min(k, len(neighbors)))
		for i := 0; i < len(neighbors) && i < k; i++ {
			results = append(results, types.SearchResult{
				ID: types.VectorID(neighbors[i]),
			})
		}
		return results, nil
	}

	results := make([]types.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		nID := neighbors[i]
		nVecAny, err := h.GetVector(nID)
		if err != nil || nVecAny == nil {
			continue
		}

		dist := float32(0.0)
		if nVec, ok := nVecAny.([]float32); ok {
			dist, _ = h.distFunc(qVec, nVec)
		}

		results = append(results, types.SearchResult{
			ID:       types.VectorID(nID),
			Distance: dist,
			Score:    dist,
		})
	}

	return results, nil
}

func (h *ArrowHNSW) PreWarm(targetSize int) {
	start := time.Now()
	defer func() {
		metrics.HNSWPreWarmDuration.Observe(time.Since(start).Seconds())
		metrics.HNSWPreWarmTotal.Inc()
	}()

	data := h.data.Load()
	if data == nil {
		return
	}

	targetChunks := (targetSize + types.ChunkSize - 1) / types.ChunkSize
	pageSize := 4096

	h.growMu.RLock()
	defer h.growMu.RUnlock()

	var dummy uint32

	// Prewarm Neighbors (Layer 0) - this is usually the largest memory block
	if len(data.Neighbors) > 0 && len(data.Neighbors[0]) > 0 {
		for i := 0; i < targetChunks && i < len(data.Neighbors[0]); i++ {
			chunk := data.Neighbors[0][i]
			if chunk == nil {
				continue
			}
			stride := pageSize / 4
			for j := 0; j < len(chunk); j += stride {
				dummy += chunk[j] // Force read
			}
			if len(chunk) > 0 {
				dummy += chunk[len(chunk)-1]
			}
		}
	}

	// Prewarm Vectors (Float32 is the most common use-case)
	if data.Type == types.VectorTypeFloat32 || data.Type == types.VectorTypeUnknown {
		for i := 0; i < targetChunks && i < len(data.VectorsF32); i++ {
			chunk := data.GetVectorsChunk(i)
			if chunk == nil {
				continue
			}
			stride := pageSize / 4
			for j := 0; j < len(chunk); j += stride {
				dummy += uint32(chunk[j]) // Force read
			}
			if len(chunk) > 0 {
				dummy += uint32(chunk[len(chunk)-1])
			}
		}
	}

	_ = dummy
}

func (h *ArrowHNSW) growNoLock(capacity, dims int) error {
	return h.growInternal(capacity, dims)
}

func (h *ArrowHNSW) Grow(capacity, dims int) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	return h.growInternal(capacity, dims)
}

func (h *ArrowHNSW) growInternal(capacity, dims int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWIndexGrowthDuration.Observe(time.Since(start).Seconds())
	}()

	data := h.data.Load()
	if data == nil {
		gd := types.NewGraphData(
			capacity,
			dims,
			false,
			h.config.UseDisk,
			0,
			h.config.Quantization,
			h.config.SQ8Enabled,
			h.config.UseDisk,
			h.config.DataType,
			h.config.BQEnabled,
			h.config.PQEnabled,
			h.config.TurboQuantEnabled,
			h.config.TurboQuantBits,
		)
		h.data.Store(gd)
		data = gd
	}

	if dims > math.MaxInt32 {
		return fmt.Errorf("dimensions %d exceed MaxInt32", dims)
	}
	h.dims.Store(int32(dims)) // #nosec G115
	currentDims := data.Dims
	if currentDims == 0 && dims > 0 {
		h.config.Dims = dims
	}

	// Calculate current vs target
	currentCapacity := data.Capacity

	// Never shrink capacity
	if capacity < currentCapacity {
		capacity = currentCapacity
	}

	// If no structural change needed, return early
	if capacity <= currentCapacity && dims == currentDims &&
		data.PQEnabled == h.config.PQEnabled &&
		data.SQ8Enabled == h.config.SQ8Enabled &&
		data.BQEnabled == h.config.BQEnabled &&
		data.TurboQuantEnabled == h.config.TurboQuantEnabled {

		// Ensure all enabled quantization types already have allocated vectors
		alreadyInitialized := true
		if h.config.PQEnabled && len(data.VectorsPQ) == 0 {
			alreadyInitialized = false
		}
		if h.config.SQ8Enabled && len(data.VectorsSQ8) == 0 {
			alreadyInitialized = false
		}
		if h.config.BQEnabled && len(data.VectorsBQ) == 0 {
			alreadyInitialized = false
		}

		if alreadyInitialized {
			return nil
		}
	}

	// COW: Clone the current data structure
	newData := data.Clone()
	newData.Capacity = capacity
	newData.Dims = dims

	// Update flags from config
	newData.PQEnabled = h.config.PQEnabled
	if newData.PQEnabled {
		if h.pqEncoder != nil {
			newData.PQM = h.pqEncoder.CodeSize()
		} else {
			newData.PQM = h.config.PQM
		}
	}
	newData.SQ8Enabled = h.config.SQ8Enabled
	if newData.SQ8Enabled && len(newData.VectorsSQ8) == 0 {
		newData.VectorsSQ8 = []uint64{}
	}
	newData.BQEnabled = h.config.BQEnabled
	if newData.BQEnabled && len(newData.VectorsBQ) == 0 {
		newData.VectorsBQ = []uint64{}
	}
	newData.TurboQuantEnabled = h.config.TurboQuantEnabled
	newData.TurboQuantBits = h.config.TurboQuantBits
	if newData.TurboQuantEnabled && len(newData.VectorsTQ) == 0 {
		newData.VectorsTQ = []uint64{}
	}

	// Ensure PackedNeighbors are resized for new capacity
	for _, pn := range newData.PackedNeighbors {
		if pn != nil {
			pn.EnsureCapacity(uint32(capacity)) // #nosec G115
		}
	}

	// If dims changed OR structural flags changed OR required feature slices are missing
	structuralChange := (dims != currentDims) ||
		(data.PQEnabled != h.config.PQEnabled) ||
		(data.SQ8Enabled != h.config.SQ8Enabled) ||
		(data.BQEnabled != h.config.BQEnabled) ||
		(data.TurboQuantEnabled != h.config.TurboQuantEnabled) ||
		(h.config.PQEnabled && len(newData.VectorsPQ) == 0) ||
		(h.config.SQ8Enabled && len(newData.VectorsSQ8) == 0) ||
		(h.config.BQEnabled && len(newData.VectorsBQ) == 0)

	if structuralChange {
		// If dims changed, we must reset all dimension-dependent state
		if dims != currentDims {
			newData.Float32Arena = nil
			newData.Float64Arena = nil
			newData.Uint8Arena = nil
			newData.Uint16Arena = nil
			newData.Uint32Arena = nil
			newData.Uint64Arena = nil
			newData.Int8Arena = nil
			newData.Int16Arena = nil
			newData.Int32Arena = nil
			newData.Int64Arena = nil
			newData.Float16Arena = nil
			newData.Complex64Arena = nil
			newData.Complex128Arena = nil

			newData.VectorsF32 = nil
			newData.VectorsSQ8 = nil
			newData.VectorsPQ = nil
			newData.VectorsBQ = nil
			newData.VectorsTQ = nil
			newData.VectorsF16 = nil
			newData.VectorsInt8 = nil
			newData.VectorsInt16 = nil
			newData.VectorsUint16 = nil
			newData.VectorsInt32 = nil
			newData.VectorsUint32 = nil
			newData.VectorsInt64 = nil
			newData.VectorsUint64 = nil
			newData.VectorsFloat64Offsets = nil
			newData.VectorsComplex64Offsets = nil
			newData.VectorsComplex128Offsets = nil

			// Legacy legacy
			newData.Vectors = nil
			newData.VectorsFloat64 = nil
			newData.VectorsComplex64 = nil
			newData.VectorsComplex128 = nil
		}

		// Re-run pre-allocation for the current/new structure
		// PreAllocate is now idempotent, so it won't duplicate if nothing changed
		if err := newData.PreAllocate(capacity); err != nil {
			return err
		}
	} else if capacity > currentCapacity {
		// Just capacity growth - PreAllocate is already capacity-aware
		if err := newData.PreAllocate(capacity); err != nil {
			return err
		}
	}

	newData.Dims = dims
	newData.Capacity = capacity

	// Optimized Grow: Ensure metadata slices are sized for the new capacity,
	// but only allocate inner data chunks for the existing node count.
	// This reduces memory spikes by deferring allocation of future chunks.
	numChunks := (capacity + types.ChunkSize - 1) / types.ChunkSize
	numExistingChunks := (int(h.nodeCount.Load()) + types.ChunkSize - 1) / types.ChunkSize

	if numChunks > 0 {
		// grow outer slices without allocating all inner chunks
		// (EnsureChunk(cID) appends up to cID, but doesn't allocate all intermediates if we are careful)
		// Wait, EnsureChunk(i) DOES allocate chunk i.
		// So we loop up to numExistingChunks to ensure they are ALL allocated/updated.
		for i := 0; i < numExistingChunks; i++ {
			if err := newData.EnsureChunk(i, 0, dims); err != nil {
				return err
			}
		}
		// For the rest, we just ensure the outer slices have enough capacity
		newData.GrowMetadataSlices(numChunks)
	}

	oldData := h.data.Swap(newData)
	// We no longer release oldData here because it shares arenas with newData.
	// Manual lifecycle management of shared arenas is complex; for 0.1.8 we let Go GC 
	// handle the metadata slices and reserve manual Release() for ArrowHNSW.Close().
	_ = oldData
	return nil
}

func (h *ArrowHNSW) SetEfConstruction(ef int32) {
	h.efConstruction.Store(ef)
}

func (h *ArrowHNSW) CleanupTombstones(threshold int) int {
	h.dataset.RLockData()
	
	shouldReset := false
	totalPruned := 0
	for _, ts := range h.dataset.GetTombstones() {
		if ts == nil {
			continue
		}
		count := int(ts.Count()) // #nosec G115
		if count > threshold {
			shouldReset = true
			totalPruned = count
			break
		}
	}
	h.dataset.RUnlockData()

	if shouldReset {
		h.dataset.ResetTombstones()
	}
	return totalPruned
}

func (h *ArrowHNSW) SetIndexedColumns(columns []string) {
	// No-op: Column indexing is handled at the VectorStore level, not ArrowHNSW.
	// VectorStore.SetIndexedColumns() stores columns in s.indexedColumns,
	// and VectorStore.applyBatchToIndex() calls columnIndex.IndexRecord().
	// ArrowHNSW doesn't maintain its own column index - it's managed by VectorStore.
}

func (h *ArrowHNSW) generateLevel() int {
	l := int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier)) // #nosec G404
	if l >= types.ArrowMaxLayers {
		l = types.ArrowMaxLayers - 1
	}
	return l
}

// AddBatch implements VectorIndex.
func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	// Bulk optimization path temporarily disabled for 0.1.9-rc1 stability
	if false && len(rowIdxs) >= 1000 && !h.IsSharded() {
		n := len(rowIdxs)
		startID := uint32(h.nextID.Add(int64(n)) - int64(n)) // #nosec G115

		// Discover vector column
		vecColIdx := -1
		if len(recs) > 0 && recs[0] != nil {
			for i := 0; i < int(recs[0].NumCols()); i++ {
				name := recs[0].ColumnName(i)
				if name == "vector" || name == "embedding" || name == "vec" {
					vecColIdx = i
					break
				}
			}
		}

		if vecColIdx != -1 {
			// Extract all vectors into a typed slice for bulk processing
			var vecs any
			supported := true
			switch h.config.DataType {
			case types.VectorTypeFloat32:
				f32s := make([][]float32, n)
				// Cache raw slices per record batch to avoid expensive column calls
				valuesCache := make(map[arrow.RecordBatch][]float32)
				physicalDims := int(h.dims.Load())
				
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					values, ok := valuesCache[rec]
					if !ok {
						col := rec.Column(vecColIdx)
						if f32Arr, okCol := col.(*arrowarray.Float32); okCol {
							values = f32Arr.Float32Values()
							valuesCache[rec] = values
						}
					}
					
					if values != nil {
						start := rowIdxs[i] * physicalDims
						if start+physicalDims <= len(values) {
							f32s[i] = values[start : start+physicalDims]
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
						} else {
							supported = false
							break
						}
					} else {
						// Fallback to slow path if type mismatch
						if v, okC := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float32); okC {
							f32s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
						} else {
							supported = false
							break
						}
					}
				}
				vecs = f32s

				// Zero-Copy Direct Mapping Optimization
				// If we are ingesting a full contiguous block that aligns with HNSW chunks,
				// we map the Arrow memory directly instead of copying into arenas.
				if len(recs) == 1 && startID%uint32(types.ChunkSize) == 0 && n >= types.ChunkSize {
					isContiguous := rowIdxs[0]%types.ChunkSize == 0
					if isContiguous {
						for j := 1; j < n; j++ {
							if rowIdxs[j] != rowIdxs[j-1]+1 {
								isContiguous = false
								break
							}
						}
					}

					if isContiguous {
						rec := recs[0]
						values := valuesCache[rec]
						if values != nil {
							data := h.data.Load()
							numFullChunks := n / types.ChunkSize
							for c := 0; c < numFullChunks; c++ {
								cID := int(startID)/types.ChunkSize + c
								rowOffset := rowIdxs[0] + (c * types.ChunkSize)
								
								offset := rowOffset * physicalDims
								dataSize := types.ChunkSize * physicalDims
								if offset+dataSize <= len(values) {
									chunkData := values[offset : offset+dataSize]
									col := rec.Column(vecColIdx)
									_ = data.SetZeroCopyMapping(cID, chunkData, col)
								}
							}
						}
					}
				}
			case types.VectorTypeFloat16:
				f16s := make([][]float16.Num, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float16.Num); ok {
						f16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f16s
			case types.VectorTypeInt8:
				i8s := make([][]int8, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int8); ok {
						i8s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i8s
			case types.VectorTypeFloat64:
				f64s := make([][]float64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float64); ok {
						f64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f64s
			case types.VectorTypeComplex64:
				c64s := make([][]complex64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex64); ok {
						c64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c64s
			case types.VectorTypeComplex128:
				c128s := make([][]complex128, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex128); ok {
						c128s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c128s
			case types.VectorTypeUint32:
				u32s := make([][]uint32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint32); ok {
						u32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u32s
			case types.VectorTypeInt32:
				i32s := make([][]int32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int32); ok {
						i32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i32s
			default:
				supported = false
			}

			if supported && vecs != nil {
				if err := h.AddBatchBulk(ctx, startID, n, vecs); err == nil {
					ids := make([]uint32, n)
					for i := 0; i < n; i++ {
						ids[i] = startID + uint32(i)
					}
					return ids, nil
				} else {
					fmt.Printf("AddBatchBulk failed for %s: %v\n", h.config.DataType.String(), err)
				}
			}
		}
	}

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
	nodeCount := int(h.nodeCount.Load())
	dims := int(h.dims.Load())

	if nodeCount == 0 || dims == 0 {
		return 0
	}

	vecBytesPer := int64(dims * 4)

	vectorMemory := int64(nodeCount) * vecBytesPer

	m := h.m
	if m == 0 {
		m = 32
	}
	maxLevel := int(h.maxLevel.Load())
	if maxLevel == 0 {
		maxLevel = int(math.Log2(float64(nodeCount)))
		if maxLevel < 1 {
			maxLevel = 1
		}
	}
	graphMemory := int64(nodeCount) * int64(maxLevel) * int64(m) * 4

	levelsMemory := int64(nodeCount) * 1

	locCount := h.locationStore.Len()
		locMemory := int64(locCount) * 8

	return vectorMemory + graphMemory + levelsMemory + locMemory
}


// ProcessResultsParallel is a method wrapper for the parallel search logic.
func (h *ArrowHNSW) ProcessResultsParallel(ctx context.Context, qv any, candidates []types.Candidate, k int, filter any) []types.SearchResult {
	// The implementation is in parallel_search.go
	if vec, ok := qv.([]float32); ok {
		var roaringFilter *roaring.Bitmap
		// Handle filter conversion if needed, or pass nil for now as per test usage
		return processResultsParallelInternal(ctx, h, vec, candidates, k, nil, roaringFilter)
	}
	return nil
}

func (h *ArrowHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]types.SearchResult, error) {
	// Optimization: Convert filters to bitset
	var filterExpr types.FilterExpr
	if opts, ok := options.(types.SearchOptions); ok {
		filterExpr = opts.FilterExpr
	}

	var bitset *query.Bitset
	if (len(filters) > 0 || filterExpr != nil) && h.dataset != nil {
		var err error
		bitset, err = h.dataset.GenerateFilterBitset(filters, filterExpr)
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

	// Returns []types.SearchResult, error
	return h.SearchVectorsWithBitmap(ctx, queryVec, k, roaringFilter, options)
}

func (h *ArrowHNSW) SearchVectorsInRange(ctx context.Context, queryVec any, threshold float32, filters []query.Filter, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h.ensureReady()

	if h.nodeCount.Load() == 0 {
		return nil, nil
	}

	var filterExpr types.FilterExpr
	if opts, ok := options.(types.SearchOptions); ok {
		filterExpr = opts.FilterExpr
	}

	var bitset *query.Bitset
	if (len(filters) > 0 || filterExpr != nil) && h.dataset != nil {
		var err error
		bitset, err = h.dataset.GenerateFilterBitset(filters, filterExpr)
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

	start := time.Now()

	data := h.data.Load()
	if data == nil {
		return nil, nil
	}

	computer := h.resolveHNSWComputer(data, nil, queryVec, false)
	if computer == nil {
		return nil, fmt.Errorf("failed to resolve search computer")
	}

	maxNodeCount := int(h.nodeCount.Load())
	ep := h.entryPoint.Load()
	maxLevel := h.maxLevel.Load()

	searchCtx := h.searchPool.Get()
	defer func() {
		searchCtx.filterBitmap = nil
		if metrics.HNSWSearchPoolPutTotal != nil {
			metrics.HNSWSearchPoolPutTotal.Inc()
		}
		h.searchPool.Put(searchCtx)
	}()

	searchCtx.filterBitmap = roaringFilter
	if roaringFilter != nil {
		metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(h.name).Inc()
	}

	computer = h.resolveHNSWComputer(data, searchCtx, queryVec, false)

	currObj := types.Candidate{ID: ep, Dist: math.MaxFloat32}
	for level := int(maxLevel); level > 0; level-- {
		res, err := h.searchLayer(ctx, computer, currObj.ID, 1, level, searchCtx, data, queryVec)
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			currObj = res[0]
		}
	}

	res, err := h.searchLayer(ctx, computer, currObj.ID, maxNodeCount, 0, searchCtx, data, queryVec)
	if err != nil {
		return nil, err
	}

	var results []types.SearchResult
	for _, c := range res {
		if c.Dist > threshold {
			continue
		}
		if h.deleted != nil && h.deleted.Contains(c.ID) {
			continue
		}
		if roaringFilter != nil && !roaringFilter.Contains(c.ID) {
			continue
		}
		results = append(results, types.SearchResult{
			ID:       types.VectorID(c.ID),
			Distance: c.Dist,
			Score:    1.0 / (1.0 + c.Dist),
		})
	}

	_ = time.Since(start)
	_ = maxNodeCount

	return results, nil
}

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, searchCtx *ArrowSearchContext, queryVal any, _ bool) any {
	switch q := queryVal.(type) {
	case []float32:
		if h.tqCompute != nil && data.TurboQuantEnabled && searchCtx != nil {
			if len(searchCtx.rotatedQueryTQ) < h.tqCompute.encoder.pow2 {
				searchCtx.rotatedQueryTQ = make([]float32, h.tqCompute.encoder.pow2)
			}
			_ = h.tqCompute.PrecomputeRotatedQuery(q, searchCtx.rotatedQueryTQ)
			return &tqComputer{data: data, h: h, rotatedQuery: searchCtx.rotatedQueryTQ, diskGraph: searchCtx.diskGraph}
		}
		if h.config.PQEnabled && h.pqEncoder != nil {
			table, err := h.pqEncoder.BuildADCTable(q)
			if err == nil {
				return &pqComputer{data: data, q: q, table: table, h: h, diskGraph: searchCtx.GetDiskGraph()}
			}
		}
		// Temporarily disabled specialized computer to isolate test regressions
		// if data.Type == types.VectorTypeFloat32 {
		// 	return &float32ToFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: searchCtx.GetDiskGraph()}
		// }
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.GetDiskGraph()
		}
		comp := &float32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg}
		if searchCtx != nil {
			// Populate conversion buffers once
			if data.Type == types.VectorTypeFloat64 {
				searchCtx.queryF64 = searchCtx.queryF64[:0]
				for _, val := range q { searchCtx.queryF64 = append(searchCtx.queryF64, float64(val)) }
				comp.qF64 = searchCtx.queryF64
			}
		}
		return comp
	case []int8, []uint8:
		var q8 []uint8
		var qInt8 []int8
		if qi8, ok := queryVal.([]int8); ok {
			q8 = *(*[]uint8)(unsafe.Pointer(&qi8)) // #nosec G103
			qInt8 = qi8
		} else {
			q8 = queryVal.([]uint8)
			qInt8 = *(*[]int8)(unsafe.Pointer(&q8)) // #nosec G103
		}
		return &int8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: searchCtx.diskGraph}
	case []float64:
		return &float64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: searchCtx.diskGraph}
	case []complex64:
		// Pre-convert if searchCtx available
		if searchCtx != nil {
			if cap(searchCtx.queryC64) < len(q) {
				searchCtx.queryC64 = make([]complex64, len(q))
			}
			searchCtx.queryC64 = searchCtx.queryC64[:len(q)]
			copy(searchCtx.queryC64, q)
			return &complex64Computer{data: data, q: searchCtx.queryC64, dims: len(q), h: h, diskGraph: searchCtx.diskGraph}
		}
		return &complex64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: h.diskGraph.Load()}
	case []complex128:
		if searchCtx != nil {
			if cap(searchCtx.queryC128) < len(q) {
				searchCtx.queryC128 = make([]complex128, len(q))
			}
			searchCtx.queryC128 = searchCtx.queryC128[:len(q)]
			copy(searchCtx.queryC128, q)
			return &complex128Computer{data: data, q: searchCtx.queryC128, dims: len(q), h: h, diskGraph: searchCtx.diskGraph}
		}
		return &complex128Computer{data: data, q: q, dims: len(q), h: h, diskGraph: h.diskGraph.Load()}
	case []int16:
		return &int16Computer{data: data, q: q, dims: len(q), h: h}
	case []uint16:
		return &uint16Computer{data: data, q: q, dims: len(q), h: h}
	case []int32:
		return &int32Computer{data: data, q: q, dims: len(q), h: h}
	case []uint32:
		return &uint32Computer{data: data, q: q, dims: len(q), h: h}
	case []int64:
		return &int64Computer{data: data, q: q, dims: len(q), h: h}
	case []uint64:
		return &uint64Computer{data: data, q: q, dims: len(q), h: h}
	}
	return nil
}

// MinCandidateHeapAdapter makes a []types.Candidate a Min-Heap (closest on top)
type MinCandidateHeapAdapter []types.Candidate

func (h MinCandidateHeapAdapter) Len() int           { return len(h) }
func (h MinCandidateHeapAdapter) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinCandidateHeapAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinCandidateHeapAdapter) Push(x any)        { *h = append(*h, x.(types.Candidate)) }
func (h *MinCandidateHeapAdapter) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// MaxCandidateHeapAdapter makes a []types.Candidate a Max-Heap (furthest on top)
type MaxCandidateHeapAdapter []types.Candidate

func (h MaxCandidateHeapAdapter) Len() int           { return len(h) }
func (h MaxCandidateHeapAdapter) Less(i, j int) bool { return h[i].Dist > h[j].Dist }
func (h MaxCandidateHeapAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MaxCandidateHeapAdapter) Push(x any)        { *h = append(*h, x.(types.Candidate)) }
func (h *MaxCandidateHeapAdapter) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// searchLayer is used by insertion logic
// searchLayer implements HNSW layer search
func (h *ArrowHNSW) searchLayer(goCtx context.Context, computer any, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *types.GraphData, queryVec any) ([]types.Candidate, error) {
	start := time.Now()
	defer func() {
		if ctx != nil {
			ctx.distComputeTime += time.Since(start)
		}
	}()

	// Define polymorphic distance computer
	var distComputer func(uint32) (float32, error)
	var epDist float32

	var disk *DiskGraph
	if ctx != nil {
		disk = ctx.diskGraph
	}

	// Optimization: Use specialized computer if available
	if comp, ok := computer.(interface {
		ComputeSingle(id uint32) (float32, error)
	}); ok {
		distComputer = comp.ComputeSingle
		var err error
		if ctx != nil {
			ctx.distComputeCount++
		}
		epDist, err = comp.ComputeSingle(entryPoint)
		if err != nil {
			return nil, err
		}
	} else {
		switch q := queryVec.(type) {
		case []float32:
			distComputer = func(id uint32) (float32, error) {
				var disk *DiskGraph
				if ctx != nil {
					disk = ctx.diskGraph
				}
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
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
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
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
				case []complex64:
					qLen := len(q)
					qComplex := make([]complex64, qLen/2)
					for i := 0; i < qLen/2; i++ {
						qComplex[i] = complex(q[2*i], q[2*i+1])
					}
					var sum float32
					for i, val := range qComplex {
						if i < len(v) {
							diff := val - v[i]
							modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
							sum += modSq
						}
					}
					return float32(math.Sqrt(float64(sum))), nil
				case []complex128:
					qLen := len(q)
					qComplex := make([]complex128, qLen/2)
					for i := 0; i < qLen/2; i++ {
						qComplex[i] = complex(float64(q[2*i]), float64(q[2*i+1]))
					}
					var sum float64
					for i, val := range qComplex {
						if i < len(v) {
							diff := val - v[i]
							modSq := real(diff)*real(diff) + imag(diff)*imag(diff)
							sum += modSq
						}
					}
					return float32(math.Sqrt(sum)), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []int8:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
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
						v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
					} else {
						v8 = vAny.([]uint8)
					}

					var q8 []uint8
					q8 = *(*[]uint8)(unsafe.Pointer(&q)) // #nosec G103

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
						// use optimized SIMD kernel
						qI8 := *(*[]int8)(unsafe.Pointer(&q8)) // #nosec G103
						vI8 := *(*[]int8)(unsafe.Pointer(&v8)) // #nosec G103
						return h.distFuncInt8(qI8, vI8)
					}
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []complex64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
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
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
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
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
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

		case []float16.Num:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]float16.Num); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					if h.distFuncF16 != nil {
						return h.distFuncF16(q, v)
					}
					// Fallback Euclidean
					var sum float32
					for i, val := range q {
						diff := val.Float32() - v[i].Float32()
						sum += diff * diff
					}
					return float32(math.Sqrt(float64(sum))), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []uint32:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint32); ok {
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

		case []int32:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int32); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt32(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []int16:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int16); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt16(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []uint16:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint16); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceUint16(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []int64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]int64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceInt64(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		case []uint64:
			distComputer = func(id uint32) (float32, error) {
				vecAny, err := h.getVectorWithCachedDisk(data, disk, id)
				if err != nil {
					return 0, err
				}
				if v, ok := vecAny.([]uint64); ok {
					if len(q) != len(v) {
						return math.MaxFloat32, nil
					}
					return euclideanDistanceUint64(q, v), nil
				}
				return math.MaxFloat32, nil
			}
			epDist, _ = distComputer(entryPoint)

		default:
			return nil, fmt.Errorf("searchLayer: unsupported query vector type %T", queryVec)
		}
	}

	// 1. Reset Frontier for this layer
	ctx.candidates = ctx.candidates[:0]
	ctx.resultSet = ctx.resultSet[:0]
	ctx.visited.Clear()

	minHeap := (*MinCandidateHeapAdapter)(&ctx.candidates)
	resultSetAdapter := (*MaxCandidateHeapAdapter)(&ctx.resultSet)

	epCand := types.Candidate{ID: entryPoint, Dist: epDist}
	heap.Push(minHeap, epCand)
	heap.Push(resultSetAdapter, epCand) // resultSet is MaxHeap
	ctx.visited.Set(int(entryPoint)) // #nosec G115

	for minHeap.Len() > 0 {
		if err := goCtx.Err(); err != nil {
			return nil, err
		}
		// Pop closest candidate
		curr := heap.Pop(minHeap).(types.Candidate)
		ctx.nodesVisitedCount++

		// Furthest in resultSet (MaxHeap Top)
		// We can Peek using index 0 if internal structure is known (slice)
		// MaxCandidateHeapAdapter wraps basecore.CandidateHeap which is slice.
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
		neighbors := h.GetNeighborsCombinedCached(layer, curr.ID, disk)

		prefetchLimit := h.mMax
		if prefetchLimit > 64 {
			prefetchLimit = 64
		}
		if prefetchLimit < 16 {
			prefetchLimit = 16
		}
		for i := 0; i < len(neighbors) && i < prefetchLimit; i++ {
			nID := neighbors[i]
			cID := int(nID) / types.ChunkSize // #nosec G115
			cOff := int(nID) % types.ChunkSize // #nosec G115
			chunk := data.GetVectorsChunk(cID)
			if chunk != nil {
				start := cOff * data.Dims
				if start+data.Dims <= len(chunk) {
					_ = chunk[start]
					_ = chunk[start+data.Dims-1]
				}
			}
			if len(data.VectorsSQ8) > cID {
				if sq8Chunk := data.GetVectorsSQ8Chunk(cID); sq8Chunk != nil {
					paddedDims := (data.Dims + 63) & ^63
					start := cOff * paddedDims
					if start+data.Dims <= len(sq8Chunk) {
						_ = sq8Chunk[start]
					}
				}
			}
			if len(data.VectorsTQ) > cID {
				if tqChunk := data.GetVectorsTQChunk(cID); tqChunk != nil {
					stride := 4 + (data.Dims-1)*data.TurboQuantBits/8 + (data.Dims+7)/8
					start := cOff * stride
					if start+stride <= len(tqChunk) {
						_ = tqChunk[start]
					}
				}
			}
		}

		for _, n := range neighbors {
			if ctx.visited.IsSet(int(n)) { // #nosec G115
				continue
			}
			ctx.visited.Set(int(n)) // #nosec G115

			ctx.distComputeCount++
			d, err := distComputer(n)
			if err != nil {
				continue
			}

			cand := types.Candidate{ID: n, Dist: d}

			// Add to candidates for traversal regardless of filter
			heap.Push(minHeap, cand)

			// Only add to resultSet if it passes filters
			if ctx.filterBitmap != nil && !ctx.filterBitmap.Contains(n) {
				continue
			}
			if h.deleted != nil && h.deleted.Contains(n) {
				continue
			}

			if len(ctx.resultSet) > 0 {
				furthest := ctx.resultSet[0]

				if ctx.resultSet.Len() < ef || d < furthest.Dist {
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
	res := make([]types.Candidate, count)
	for i := count - 1; i >= 0; i-- {
		res[i] = heap.Pop(resultSetAdapter).(types.Candidate)
	}
	return res, nil
}

// flushSearchMetrics handles the efficient emission of search-layer metrics,
// including sampling logic for Histogram metrics to avoid overhead.
func (h *ArrowHNSW) flushSearchMetrics(ctx *ArrowSearchContext) {
	if ctx == nil {
		return
	}

	// Always increment global distance counter (low overhead atomic)
	if ctx.distComputeCount > 0 {
		metrics.HnswDistanceCalculations.Add(float64(ctx.distComputeCount))
	}

	// Sampling for Histogram metrics (e.g. nodes visited)
	if h.config.SearchLayerSampleRate > 0 {
		count := h.metricsSampleCounter.Add(1)
		// Deterministic sampling: if rate is 0.1, record every 10th search.
		// For very small rates, it might never trigger if rate < 1/MaxUint64 (unlikely).
		interval := uint64(1.0 / h.config.SearchLayerSampleRate)
		if interval == 0 {
			interval = 1
		}

		if count%interval == 0 {
			metrics.HnswNodesVisited.WithLabelValues(h.name).Observe(float64(ctx.nodesVisitedCount))
		}
	}
}

func (h *ArrowHNSW) Len() int {
	return h.Size()
}

// ExportState implements VectorIndex.
func (h *ArrowHNSW) ExportState() ([]byte, error) {
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

	locs := make([]types.Location, 0, h.locationStore.Len())
	h.locationStore.IterateMutable(func(_ types.VectorID, val *atomic.Uint64) {
		loc := basecore.UnpackLocation(val.Load())
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
	h.growMu.Lock() // Use Write Lock to ensure consistent snapshot against concurrent insertions
	defer h.growMu.Unlock()

	// 1. Capture Snapshot + Metadata
	var snapshot *types.GraphData
	locs := make([]types.Location, 0, h.locationStore.Len())
	dims := int(h.dims.Load())

	if data := h.data.Load(); data != nil {
		snapshot = data.CloneForSnapshot()
	}

	h.locationStore.IterateMutable(func(_ types.VectorID, val *atomic.Uint64) {
		loc := basecore.UnpackLocation(val.Load())
		locs = append(locs, loc)
	})

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
	if err := binary.Write(w, binary.LittleEndian, uint32(len(metaBytes))); err != nil { // #nosec G115
		return err
	}
	if _, err := w.Write(metaBytes); err != nil {
		return err
	}

	// 3. Export Snapshot types.GraphData
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
	if state.Dims > math.MaxInt32 {
		return fmt.Errorf("state dimensions %d exceed MaxInt32", state.Dims)
	}
	h.dims.Store(int32(state.Dims)) // #nosec G115
	h.locationStore.Reset()
	for _, loc := range state.Locations {
		h.locationStore.Append(loc)
	}

	// 2. Read types.GraphData
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
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	currentLen := h.locationStore.Len()
	// Export locations starting from fromVersion up to currentLen
	startIdx := int(fromVersion) // #nosec G115
	if startIdx >= currentLen {
		return &types.DeltaSync{
			FromVersion:  fromVersion,
			ToVersion:    uint64(currentLen), // #nosec G115
			NewLocations: nil,
			StartIndex:   startIdx,
		}, nil
	}

	newLocs := make([]types.Location, 0, currentLen-startIdx)
	idx := 0
	h.locationStore.IterateMutable(func(_ types.VectorID, val *atomic.Uint64) {
		if idx >= startIdx {
			loc := basecore.UnpackLocation(val.Load())
			newLocs = append(newLocs, loc)
		}
		idx++
	})

	return &types.DeltaSync{
		FromVersion:  fromVersion,
		ToVersion:    uint64(currentLen), // #nosec G115
		NewLocations: newLocs,
		StartIndex:   startIdx,
	}, nil
}

// ApplyDelta implements VectorIndex.
func (h *ArrowHNSW) ApplyDelta(delta *types.DeltaSync) error {
	if delta == nil || len(delta.NewLocations) == 0 {
		return nil
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	for i, loc := range delta.NewLocations {
		globalID := types.VectorID(delta.StartIndex + i) // #nosec G115
		h.locationStore.EnsureCapacity(globalID)
		h.locationStore.Set(globalID, loc)
	}

	return nil
}

// GetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	h.parallelConfig = cfg
}



func (h *ArrowHNSW) GetLocationForParallel(id uint32) (types.Location, bool) {
	return h.locationStore.Get(types.VectorID(id))
}

func (h *ArrowHNSW) ExtractVectorForParallel(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	vecColIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == "vector" {
			vecColIdx = i
			break
		}
	}
	if vecColIdx == -1 {
		if rec.NumCols() == 1 {
			vecColIdx = 0
		} else {
			return nil, fmt.Errorf("vector column not found in record")
		}
	}

	vec, err := extractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return nil, err
	}

	// Complex128 stored in Arrow as FixedSizeList<float64>. Convert to []float32 for the SIMD path.
	if h.config.DataType == types.VectorTypeComplex128 {
		vf64, ok := vec.([]float64)
		if !ok {
			return nil, fmt.Errorf("expected []float64 for Complex128, got %T", vec)
		}
		f32 := make([]float32, len(vf64))
		for i, v := range vf64 {
			f32[i] = float32(v)
		}
		return f32, nil
	}

	// Convert other types to float32 for parallel refinement
	switch v := vec.(type) {
	case []float32:
		return v, nil
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
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search refinement", vec)
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
		res := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
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
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
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
func (h *ArrowHNSW) SearchWithArena(queryVec []float32, k int, arena any) []types.VectorID {
	// Fallback to standard search if no arena
	if arena == nil {
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]types.VectorID, len(results))
		for i, r := range results {
			ids[i] = types.VectorID(r.ID)
		}
		return ids
	}

	searchArena, ok := arena.(*SearchArena)
	if !ok {
		// Try casting if it's passed as interface
		results, _ := h.SearchVectorsWithBitmap(context.Background(), queryVec, k, nil, nil)
		ids := make([]types.VectorID, len(results))
		for i, r := range results {
			ids[i] = types.VectorID(r.ID)
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
		ids = make([]types.VectorID, len(results))
	}

	for i, r := range results {
		ids[i] = types.VectorID(r.ID)
	}
	return ids
}

type pqComputer struct {
	data      *types.GraphData
	q         []float32
	table     []float32
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

type tqComputer struct {
	data         *types.GraphData
	h            *ArrowHNSW
	rotatedQuery []float32
	diskGraph    *DiskGraph
}

func (c *tqComputer) ComputeSingle(id uint32) (float32, error) {
	// If it's a TQ search, the TQ distance logic usually needs the TQ code.
	// We can check if it's in data or DiskGraph.
	return c.h.tqCompute.DistanceWithRotatedQueryAndDisk(id, c.rotatedQuery, c.diskGraph)
}

func (c *pqComputer) ComputeSingle(id uint32) (float32, error) {
	code := c.data.GetVectorPQ(id)
	if code == nil {
		// Try DiskGraph
		if c.diskGraph != nil {
			code = c.diskGraph.GetVectorPQ(id)
		} else {
			// fallback/atomic
			dg := c.h.diskGraph.Load()
			if dg != nil {
				code = dg.GetVectorPQ(id)
			}
		}
	}

	if code == nil {
		return math.MaxFloat32, nil
	}
	distSq, err := c.h.pqEncoder.ADCDistance(c.table, code)
	if err != nil {
		return 0, err
	}
	// HNSW Euclidean metric expects distance (sqrt), but ADCDistance returns squared distance.
	if c.h.config.Metric == basecore.MetricEuclidean {
		return float32(math.Sqrt(float64(distSq))), nil
	}
	return distSq, nil
}

type float32Computer struct {
	data *types.GraphData
	q    []float32
	dims int
	h    *ArrowHNSW
	// Pre-converted query buffers for cross-type comparison
	qF64  []float64
	qF16  []float16.Num
	qC64  []complex64
	qC128 []complex128
	diskGraph *DiskGraph
}

func (c *float32Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []float32:
		return c.h.distFunc(c.q, v)
	case []int8, []uint8:
		var v8 []uint8
		if vi8, ok := v.([]int8); ok {
			v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
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
		vf := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
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
		var q64 []float64
		if len(c.qF64) == len(c.q) {
			q64 = c.qF64
		} else {
			q64 = make([]float64, len(c.q))
			for i, val := range c.q { q64[i] = float64(val) }
		}
		return c.h.distFuncF64(q64, v)
	}
	return math.MaxFloat32, nil
}

type float32ToFloat32Computer struct {
	data      *types.GraphData
	q         []float32
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

func (c *float32ToFloat32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsChunk(cID)
	if chunk == nil {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
		if err != nil {
			return 0, err
		}
		if v, ok := vecAny.([]float32); ok {
			return c.h.distFunc(c.q, v)
		}
		return math.MaxFloat32, nil
	}
	
	cOff := int(id) % types.ChunkSize
	start := cOff * c.data.Dims
	if start+c.dims <= len(chunk) {
		v := chunk[start : start+c.dims]
		return c.h.distFunc(c.q, v)
	}
	// Fallback to cached disk for out-of-bounds in memory
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
	if err == nil {
		if v, ok := vecAny.([]float32); ok {
			return c.h.distFunc(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func euclideanDistanceInt16(a, b []int16) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceUint16(a, b []uint16) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceInt32(a, b []int32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceUint32(a, b []uint32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceInt64(a, b []int64) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceUint64(a, b []uint64) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

type int8Computer struct {
	data      *types.GraphData
	q         []uint8
	qInt8     []int8
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

func (c *int8Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	// Try specialized chunk fetch first
	if chunk := c.data.GetVectorsSQ8Chunk(cID); chunk != nil {
		cOff := int(id) % types.ChunkSize // #nosec G115
		start := cOff * c.data.Dims // #nosec G115
		if start+c.dims <= len(chunk) {
			v8 := chunk[start : start+c.dims]
			// Already optimized for SIMD if via distFuncInt8
			return c.h.distFuncInt8(c.qInt8, *(*[]int8)(unsafe.Pointer(&v8))) // #nosec G103
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
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
	case []int8:
		return c.h.distFuncInt8(c.qInt8, v)
	case []uint8:
		v8 := v
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
		} else {
			// use optimized SIMD kernel
			vI8 := *(*[]int8)(unsafe.Pointer(&v8)) // #nosec G103
			return c.h.distFuncInt8(c.qInt8, vI8)
		}
	}
	return math.MaxFloat32, nil
}

func (h *ArrowHNSW) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	if h.locationStore == nil {
		return fmt.Errorf("location store not initialized")
	}

	for id, locAny := range mapping {
		if loc, ok := locAny.(types.Location); ok {
			h.locationStore.Set(types.VectorID(id), loc)
		} else if loc, ok := locAny.(types.Location); ok {
			h.locationStore.Set(types.VectorID(id), loc)
		}
	}
	return nil
}
