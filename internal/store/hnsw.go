package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/coder/hnsw"
	"github.com/prometheus/client_golang/prometheus"
)

// HNSWIndex implements VectorIndex using hierarchical navigable small world graphs.
type HNSWIndex struct {
	mu sync.RWMutex

	dataset       *Dataset
	Graph         *hnsw.Graph[VectorID]
	locationStore *ChunkedLocationStore
	nextVecID     atomic.Uint32
	dims          int
	dimsOnce      sync.Once

	vectorColIdx atomic.Int32

	resultPool *resultPool
	searchPool *SearchPool

	pqEnabled           bool
	pqTrainingEnabled   bool
	pqTrainingThreshold int
	pqEncoder           *pq.PQEncoder
	pqCodes             [][]uint8

	parallelConfig ParallelSearchConfig

	gpuEnabled  bool
	gpuFallback bool
	gpuIndex    gpu.Index

	Metric        DistanceMetric
	distFunc      func(a, b []float32) (float32, error)
	batchDistFunc func(query []float32, vectors [][]float32, results []float32) error

	metricInsertDuration       prometheus.Observer
	metricIndexBuildDuration   prometheus.Observer
	metricNodeCount            prometheus.Gauge
	metricGraphHeight          prometheus.Gauge
	metricVectorAllocations    prometheus.Counter
	metricVectorAllocatedBytes prometheus.Counter
	metricActiveReaders        prometheus.Gauge
	metricLockWait             prometheus.ObserverVec
}

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
		Metric:              config.Metric,

		metricInsertDuration:       metrics.HNSWInsertDurationSeconds,
		metricIndexBuildDuration:   metrics.IndexBuildDurationSeconds.WithLabelValues(dsName),
		metricNodeCount:            metrics.HNSWNodesTotal.WithLabelValues(dsName),
		metricGraphHeight:          metrics.HnswGraphHeight.WithLabelValues(dsName),
		metricVectorAllocations:    metrics.HNSWVectorAllocations,
		metricVectorAllocatedBytes: metrics.HNSWVectorAllocatedBytes,
		metricActiveReaders:        metrics.HnswActiveReaders.WithLabelValues(dsName),
		metricLockWait:             metrics.IndexLockWaitDuration.MustCurryWith(prometheus.Labels{"dataset": dsName}),
	}

	h.vectorColIdx.Store(-1)
	h.distFunc = h.resolveDistanceFunc()
	h.batchDistFunc = h.resolveBatchDistanceFunc()

	h.Graph.Distance = func(a, b []float32) float32 {
		d, err := h.distFunc(a, b)
		if err != nil {
			return math.MaxFloat32
		}
		return d
	}

	return h
}

func DefaultConfig() *HNSWConfig {
	return &HNSWConfig{
		M:                   32,
		EfConstruction:      200,
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

func (h *HNSWIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextVecID.Add(1) - 1
	vid := VectorID(id)
	loc := Location{BatchIdx: batchIdx, RowIdx: rowIdx}
	h.locationStore.Set(vid, loc)

	return id, nil
}

func (h *HNSWIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return h.AddByLocation(ctx, batchIdx, rowIdx)
}

func (h *HNSWIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		id, err := h.AddByLocation(ctx, batchIdxs[i], rowIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (h *HNSWIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil // Stub
}

func (h *HNSWIndex) Search(ctx context.Context, query any, k int, filter any) ([]types.Candidate, error) {
	return nil, nil // Stub
}

func (h *HNSWIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	vec, ok := q.([]float32)
	if !ok {
		return nil, fmt.Errorf("invalid query vector type")
	}
	return h.SearchByID(ctx, vec, k), nil
}

func (h *HNSWIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	return nil, nil
}

func (h *HNSWIndex) SearchByID(ctx context.Context, q []float32, k int) []SearchResult {
	return nil // Stub
}

func (h *HNSWIndex) Size() int {
	return h.Len()
}

func (h *HNSWIndex) Len() int {
	return h.locationStore.Len()
}

func (h *HNSWIndex) GetEntryPoint() uint32 {
	return 0 // Stub
}

func (h *HNSWIndex) GetLocation(id uint32) (any, bool) {
	return h.locationStore.Get(VectorID(id))
}

func (h *HNSWIndex) GetVectorID(loc any) (uint32, bool) {
	if l, ok := loc.(Location); ok {
		id, ok := h.locationStore.GetID(l)
		return uint32(id), ok
	}
	return 0, false
}

func (h *HNSWIndex) GetDimension() uint32 {
	return uint32(h.dims)
}

func (h *HNSWIndex) SetIndexedColumns(cols []string) {}

func (h *HNSWIndex) Warmup() int {
	return h.Len()
}

func (h *HNSWIndex) EstimateMemory() int64 {
	return 0 // Stub
}

func (h *HNSWIndex) GetNeighbors(id uint32) ([]uint32, error) {
	return nil, nil // Stub
}

func (h *HNSWIndex) PreWarm(targetSize int) {
	// Stub
}

func (h *HNSWIndex) TrainPQ(vectors [][]float32) error {
	return nil // Stub
}

func (h *HNSWIndex) GetPQEncoder() *pq.PQEncoder {
	return h.pqEncoder
}

func (h *HNSWIndex) SearchWithArena(query []float32, k int, arena any) []VectorID {
	results, _ := h.Search(context.Background(), query, k, nil)
	ids := make([]VectorID, len(results))
	for i, r := range results {
		ids[i] = VectorID(r.ID)
	}
	return ids
}

func (h *HNSWIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Graph = nil
	h.locationStore.Reset()
	return nil
}

func (h *HNSWIndex) getVector(id VectorID) []float32 {
	loc, ok := h.locationStore.Get(id)
	if !ok || loc.BatchIdx == -1 {
		return nil
	}
	h.dataset.dataMu.RLock()
	defer h.dataset.dataMu.RUnlock()
	if loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]
	_ = rec // Temporary: suppress unused error
	// ... logic to extract vector from record ...
	return nil // Simplified stub for now
}

func (h *HNSWIndex) resolveDistanceFunc() func(a, b []float32) (float32, error) {
	return simd.EuclideanDistance
}

func (h *HNSWIndex) resolveBatchDistanceFunc() func(query []float32, vectors [][]float32, results []float32) error {
	return simd.EuclideanDistanceBatch
}

func (h *HNSWIndex) GetDistanceFunc() func([]float32, []float32) float32 {
	return func(a, b []float32) float32 {
		d, _ := h.distFunc(a, b)
		return d
	}
}

func (h *HNSWIndex) extractVector(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	return ExtractVectorFromArrow(rec, rowIdx, int(h.vectorColIdx.Load()))
}
