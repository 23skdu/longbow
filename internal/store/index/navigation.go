package index

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Search finds the k-closest neighbors to the query vector.
func (h *ArrowHNSW) Search(ctx context.Context, queryVal any, k int, filter any) ([]types.Candidate, error) {
	start := time.Now()

	meta := h.metadataRegistry.Load()
	if meta.NodeCount == 0 {
		return []types.Candidate{}, nil
	}

	// Perform search to find closest neighbors
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

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	return h.GetNodeCount()
}

// Navigate performs a graph navigation query
func (h *ArrowHNSW) Navigate(ctx context.Context, navQuery NavigatorQuery) (*NavigatorPath, error) {
	if h.navigator == nil {
		return nil, fmt.Errorf("graph navigator not initialized")
	}
	return h.navigator.FindPath(ctx, navQuery)
}

// GetDimension returns the vector dimensionality of the index.
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

// GetM returns the M parameter (connections per layer)
func (h *ArrowHNSW) GetM() int {
	return int(h.m.Load())
}

// GetMMax returns the MMax parameter (max connections)
func (h *ArrowHNSW) GetMMax() int {
	return int(h.mMax.Load())
}

// GetMMax0 returns the MMax0 parameter (max connections in layer 0)
func (h *ArrowHNSW) GetMMax0() int {
	return int(h.mMax0.Load())
}

// GetEfConstruction returns the efConstruction parameter
func (h *ArrowHNSW) GetEfConstruction() int32 {
	return h.efConstruction.Load()
}

// GetNodeCount returns the current number of nodes
func (h *ArrowHNSW) GetNodeCount() int {
	meta := h.GetMetadataSnapshot()
	return int(meta.NodeCount)
}

// GetMaxLevel returns the maximum level in the graph
func (h *ArrowHNSW) GetMaxLevel() int32 {
	meta := h.GetMetadataSnapshot()
	return meta.MaxLevel
}

// GetEntryPoint returns the entry point node ID
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	meta := h.GetMetadataSnapshot()
	return meta.EntryPoint
}

// GetDims returns the vector dimensionality
func (h *ArrowHNSW) GetDims() int32 {
	return h.dims.Load()
}

// IsDeleted returns whether the given vector ID is marked as deleted.
func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	if h.deleted == nil {
		return false
	}
	return h.deleted.Contains(id)
}

// Warmup triggers the loading of index data into memory.
func (h *ArrowHNSW) Warmup() int {
	return h.GetNodeCount()
}

// GetIndexType returns "hnsw".
func (h *ArrowHNSW) GetIndexType() string {
	return "hnsw"
}

// Len returns the current size of the index.
func (h *ArrowHNSW) Len() int {
	return h.Size()
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

// SearchVectorsWithBitmap performs k-NN search with a roaring bitmap filter.
func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta := h.GetMetadataSnapshot()
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("graph data is nil")
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

	if meta.NodeCount == 0 {
		return nil, nil
	}

	// Automatic GPU dispatch for supported types
	if h.gpuEnabled && h.gpuIndex != nil && meta.NodeCount >= 1024 {
		gpuResults, err := h.searchGPU(ctx, queryVec, k)
		if err == nil && len(gpuResults) > 0 {
			return gpuResults, nil
		}
		// Fall through to CPU on GPU error
	}

	if metrics.HNSWSearchPoolGetTotal != nil {
		metrics.HNSWSearchPoolGetTotal.Inc()
	}
	start := time.Now()
	searchCtx := h.searchPool.Get()
	searchCtx.MaxNodeCount = meta.NodeCount
	searchCtx.MaxGeneration = meta.Generation

	// Extract search options
	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	searchCtx.diskGraph = h.diskGraph.Load()
	searchCtx.predicate = searchOptions.Predicate

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

	if filter != nil {
		searchCtx.filterBitmap = filter.Clone()
	} else {
		searchCtx.filterBitmap = nil
	}
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
		searchCtx.filterBitmap = nil
		h.flushSearchMetrics(searchCtx)

		if should, mult := metrics.GlobalHotpathSampler.ShouldSample(); should {
			duration := time.Since(start).Seconds()
			metrics.HNSWSearchDurationSeconds.Observe(duration)

			typeLabel := h.config.DataType.String()
			dimsStr := strconv.Itoa(int(h.dims.Load()))
			metrics.HNSWSearchOpsTotal.WithLabelValues(h.name, typeLabel, dimsStr).Add(mult)

			// Polymorphic metrics needed for test
			metrics.HNSWPolymorphicSearchCount.WithLabelValues(typeLabel).Add(mult)
			metrics.HNSWPolymorphicLatency.WithLabelValues(typeLabel).Observe(duration)

			byteThroughput := float64(int(h.dims.Load()) * h.config.DataType.ElementSize())
			metrics.HNSWPolymorphicThroughput.WithLabelValues(typeLabel).Add(byteThroughput * mult)

			if metrics.HNSWSearchPoolPutTotal != nil {
				metrics.HNSWSearchPoolPutTotal.Add(mult)
			}
			h.searchPool.PutWithMetrics(searchCtx, typeLabel, dimsStr)
		} else {
			h.searchPool.Put(searchCtx)
		}
	}()

	ep := meta.EntryPoint
	maxLevel := meta.MaxLevel

	if ep%10 == 0 {
	}

	if ep >= uint32(data.Capacity) { // #nosec G115 -- intentional comparison
		// During initial bulk ingestion, ep might be 0 while data is empty.
		// If data is empty, just return nil.
		if meta.NodeCount == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("entry point %d out of bounds (capacity %d)", ep, data.Capacity)
	}

	// Calculate distance to entry point
	var dist float32
	vec, err := h.getVectorWithCachedDisk(data, searchCtx.diskGraph, ep, searchCtx.MaxGeneration)
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
				dist, err = h.distFuncC64(q, v)
			}
		case []complex128:
			if v, ok := vec.([]complex128); ok {
				dist, err = h.distFuncC128(q, v)
			}
		case []int8:
			if v, ok := vec.([]int8); ok {
				dist, _ = h.distFuncInt8(q, v)
			}
		case []uint8:
			if v, ok := vec.([]uint8); ok {
				dist, _ = h.distFuncUint8(q, v)
			}
		case []int16:
			if v, ok := vec.([]int16); ok {
				dist, _ = h.distFuncInt16(q, v)
			}
		case []uint16:
			if v, ok := vec.([]uint16); ok {
				dist, _ = h.distFuncUint16(q, v)
			}
		case []int32:
			if v, ok := vec.([]int32); ok {
				dist, _ = h.distFuncInt32(q, v)
			}
		case []uint32:
			if v, ok := vec.([]uint32); ok {
				dist, _ = h.distFuncUint32(q, v)
			}
		case []int64:
			if v, ok := vec.([]int64); ok {
				dist, _ = h.distFuncInt64(q, v)
			}
		case []uint64:
			if v, ok := vec.([]uint64); ok {
				dist, _ = h.distFuncUint64(q, v)
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

	searchCtx.queryRadius = 0
	if qv, ok := queryVec.([]float32); ok {
		var sum float32
		for _, x := range qv {
			sum += x * x
		}
		searchCtx.queryRadius = float32(math.Sqrt(float64(sum)))
	}

	// 1. Initial Greedy Search to find entry point at level 0
	if h.gpuEnabled && h.gpuIndex != nil && maxLevel > 0 {
		var qf32 []float32
		var ok bool
		if qf32, ok = queryVec.([]float32); ok {
			newEP, newDist, err := h.gpuIndex.SearchGreedy(qf32, currObj.ID, currObj.Dist)
			if err == nil {
				currObj = types.Candidate{ID: newEP, Dist: newDist}
				goto search_layer0
			}
		}
	}

	for level := int(maxLevel); level > 0; level-- { // #nosec G115
		// Greedy search: keep 1 best candidate
		var res []types.Candidate
		var err error
		if compF32, ok := computer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, 1, level, searchCtx, data)
		} else {
			res, err = h.searchLayer(ctx, computer, currObj.ID, 1, level, searchCtx, data, queryVec)
		}
		if err != nil {
			h.flushSearchMetrics(searchCtx)
			return nil, err
		}

		candidates := res
		if len(candidates) > 0 {
			currObj = candidates[0]
		}
	}

search_layer0:
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
		var res []types.Candidate
		var err error
		if compF32, ok := computer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, efSearch, 0, searchCtx, data)
		} else {
			res, err = h.searchLayer(ctx, computer, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		}
		if err != nil {
			return nil, err
		}
		sort.Slice(res, func(i, j int) bool { return res[i].Dist < res[j].Dist })
		result := make([]types.SearchResult, 0, k)
		for _, c := range res {
			if h.IsDeleted(c.ID) || (filter != nil && !filter.Contains(c.ID)) {
				continue
			}
			result = append(result, types.SearchResult{ID: types.VectorID(c.ID), Distance: c.Dist, Score: 1.0 / (1.0 + c.Dist)})
			if len(result) >= k {
				break
			}
		}
		return result, nil
	}

	maxNodeCount := int(h.GetMetadataSnapshot().NodeCount)
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			h.flushSearchMetrics(searchCtx)
			return nil, err
		}
		var res []types.Candidate
		var err error
		if compF32, ok := computer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, efSearch, 0, searchCtx, data)
		} else {
			res, err = h.searchLayer(ctx, computer, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		}
		if err != nil {
			h.flushSearchMetrics(searchCtx)
			return nil, err
		}

		results = h.ProcessResultsParallel(ctx, qv, queryVec, res, k, filter)
		if len(results) >= k || attempt == 2 || efSearch >= maxNodeCount {
			break
		}

		// Item 3: Adaptive Search Expansion Policy
		// Instead of a blind 5x multiplier, use a heuristic based on the distance distribution
		// and the number of results found vs requested.
		// Use PID-based autonomous efSearch tuning
		// Proxy recall = len(results) / k
		recallProxy := float64(len(results)) / float64(k)
		if recallProxy > 1.0 {
			recallProxy = 1.0
		}

		efSearch = h.efTuner.Update(recallProxy)
	}

	h.flushSearchMetrics(searchCtx)
	return results, nil
}

// searchLayerFloat32 is a monomorphic specialization of searchLayer for the
// float32-query/float32-data case. It avoids DistanceComputer interface dispatch
// by calling *float32ToFloat32Computer methods directly, and skips the fallback
// type-switch closure path entirely.

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, searchCtx *ArrowSearchContext, queryVal any, squared bool) DistanceComputer {
	switch q := queryVal.(type) {
	case []float32:
		if h.tqCompute != nil && data.TurboQuantEnabled && searchCtx != nil {
			if len(searchCtx.rotatedQueryTQ) < h.tqCompute.encoder.pow2 {
				searchCtx.rotatedQueryTQ = make([]float32, h.tqCompute.encoder.pow2)
			}
			_ = h.tqCompute.PrecomputeRotatedQuery(q, searchCtx.rotatedQueryTQ)
			return &tqComputer{data: data, h: h, rotatedQuery: searchCtx.rotatedQueryTQ, diskGraph: searchCtx.diskGraph, maxGen: searchCtx.MaxGeneration}
		}
		if h.config.PQEnabled && h.oopqEncoder != nil {
			var table any
			var err error
			switch enc := h.oopqEncoder.(type) {
			case *pq.PQEncoder:
				table, err = enc.BuildADCTable(q)
			case *pq.OPQEncoder:
				table, err = enc.BuildADCTable(q)
			}
			if err == nil && table != nil {
				return &pqComputer{data: data, q: q, table: table, h: h, diskGraph: searchCtx.GetDiskGraph(), maxGen: searchCtx.MaxGeneration}
			}
		}
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.GetDiskGraph()
		}
		if data.Type == types.VectorTypeFloat32 {
			maxGen := uint64(math.MaxUint64)
			if searchCtx != nil {
				maxGen = searchCtx.MaxGeneration
			}

			// Detect Shared Vector Space and use specialized computer
			if h.sharedVectorSpace.Load() && h.dataset != nil {
				recs := h.dataset.GetRecords()
				slices := make([][]float32, len(recs))
				vecColIdx := -1
				for i, rec := range recs {
					if rec == nil {
						continue
					}
					if vecColIdx == -1 {
						vecColIdx = h.getVectorColumnIndex(rec)
					}
					if vecColIdx != -1 {
						col := rec.Column(vecColIdx)
						if list, ok := col.(*arrowarray.FixedSizeList); ok {
							if values, ok := list.ListValues().(*arrowarray.Float32); ok {
								slices[i] = values.Float32Values()
							}
						}
					}
				}
				return &sharedFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen, slices: slices}
			}

			return &float32ToFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen}
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		comp := &float32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen}
		if searchCtx != nil {
			// Populate conversion buffers once
			if data.Type == types.VectorTypeFloat64 {
				searchCtx.queryF64 = searchCtx.queryF64[:0]
				for _, val := range q {
					searchCtx.queryF64 = append(searchCtx.queryF64, float64(val))
				}
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
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}

		// Detect Shared Vector Space for Int8
		if h.sharedVectorSpace.Load() && h.dataset != nil && (data.Type == types.VectorTypeInt8 || data.Type == types.VectorTypeUint8) {
			recs := h.dataset.GetRecords()
			slices := make([][]int8, len(recs))
			vecColIdx := -1
			for i, rec := range recs {
				if rec == nil {
					continue
				}
				if vecColIdx == -1 {
					vecColIdx = h.getVectorColumnIndex(rec)
				}
				if vecColIdx != -1 {
					col := rec.Column(vecColIdx)
					if list, ok := col.(*arrowarray.FixedSizeList); ok {
						if values, ok := list.ListValues().(*arrowarray.Int8); ok {
							slices[i] = values.Int8Values()
						} else if values, ok := list.ListValues().(*arrowarray.Uint8); ok {
							u8s := values.Uint8Values()
							slices[i] = *(*[]int8)(unsafe.Pointer(&u8s)) // #nosec G103
						}
					}
				}
			}
			return &sharedInt8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: dg, maxGen: maxGen, slices: slices}
		}

		return &int8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: dg, maxGen: maxGen}
	case []float64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &float64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []float16.Num:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &float16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []complex64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
			if cap(searchCtx.queryC64) < len(q) {
				searchCtx.queryC64 = make([]complex64, len(q))
			}
			searchCtx.queryC64 = searchCtx.queryC64[:len(q)]
			copy(searchCtx.queryC64, q)
			return &complex64Computer{data: data, q: searchCtx.queryC64, dims: len(q), h: h, diskGraph: dg, maxGen: searchCtx.MaxGeneration}
		}
		dg = h.diskGraph.Load()
		return &complex64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: math.MaxUint64}
	case []complex128:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
			if cap(searchCtx.queryC128) < len(q) {
				searchCtx.queryC128 = make([]complex128, len(q))
			}
			searchCtx.queryC128 = searchCtx.queryC128[:len(q)]
			copy(searchCtx.queryC128, q)
			return &complex128Computer{data: data, q: searchCtx.queryC128, dims: len(q), h: h, diskGraph: dg, maxGen: searchCtx.MaxGeneration}
		}
		dg = h.diskGraph.Load()
		return &complex128Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: math.MaxUint64}
	case []int16:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &int16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint16:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &uint16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []int32:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &int32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint32:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &uint32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []int64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &int64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		return &uint64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	}
	return nil
}

// SearchVectors performs a search with multiple filters and options.
func (h *ArrowHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]types.SearchResult, error) {
	// Optimization: Convert filters to bitset
	var filterExpr types.FilterExpr
	var predicate types.HNSWPredicate
	if opts, ok := options.(types.SearchOptions); ok {
		filterExpr = opts.FilterExpr
		predicate = opts.Predicate
	}

	var bitset *types.Bitset
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

	searchOptions := types.SearchOptions{
		Predicate: predicate,
	}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	return h.SearchVectorsWithBitmap(ctx, queryVec, k, roaringFilter, searchOptions)
}

// SearchVectorsInRange finds all vectors within a certain distance threshold.
func (h *ArrowHNSW) SearchVectorsInRange(ctx context.Context, queryVec any, threshold float32, filters []query.Filter, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h.ensureReady()

	meta := h.GetMetadataSnapshot()
	if meta.NodeCount == 0 {
		return nil, nil
	}

	var filterExpr types.FilterExpr
	if opts, ok := options.(types.SearchOptions); ok {
		filterExpr = opts.FilterExpr
	}

	var bitset *types.Bitset
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

	if meta.NodeCount == 0 {
		return nil, nil
	}

	computer := h.resolveHNSWComputer(data, nil, queryVec, false)
	if computer == nil {
		return nil, fmt.Errorf("failed to resolve search computer")
	}

	maxNodeCount := int(meta.NodeCount)
	ep := meta.EntryPoint
	maxLevel := meta.MaxLevel

	searchCtx := h.searchPool.Get()
	searchCtx.MaxNodeCount = meta.NodeCount
	searchCtx.MaxGeneration = meta.Generation
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
		var res []types.Candidate
		var err error
		if compF32, ok := computer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, 1, level, searchCtx, data)
		} else {
			res, err = h.searchLayer(ctx, computer, currObj.ID, 1, level, searchCtx, data, queryVec)
		}
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			currObj = res[0]
		}
	}

	var res []types.Candidate
	var err error
	if compF32, ok := computer.(*float32ToFloat32Computer); ok {
		res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, maxNodeCount, 0, searchCtx, data)
	} else {
		res, err = h.searchLayer(ctx, computer, currObj.ID, maxNodeCount, 0, searchCtx, data, queryVec)
	}
	if err != nil {
		return nil, err
	}

	var results []types.SearchResult
	for _, c := range res {
		if c.Dist > threshold {
			continue
		}
		if h.IsDeleted(c.ID) {
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

// ProcessResultsParallel processes search candidates in parallel to compute final search results.
// It applies filters and thresholds while maintaining top-K ordering.
func (h *ArrowHNSW) ProcessResultsParallel(ctx context.Context, qv any, originalQuery any, candidates []types.Candidate, k int, filter any) []types.SearchResult {
	var roaringFilter *roaring.Bitmap
	if f, ok := filter.(*roaring.Bitmap); ok {
		roaringFilter = f
	}

	switch vec := qv.(type) {
	case []float32:
		return processResultsParallelInternal(ctx, parallelSearchHostF32{h}, vec, candidates, k, nil, roaringFilter)
	case []float64:
		return processResultsParallelInternal(ctx, parallelSearchHostF64{h}, vec, candidates, k, nil, roaringFilter)
	default:
		// Check originalQuery for complex types
		switch oq := originalQuery.(type) {
		case []complex128:
			// Treat as float64 with 2N dims
			if len(oq) == 0 {
				return []types.SearchResult{}
			}
			raw := unsafe.Slice((*float64)(unsafe.Pointer(&oq[0])), len(oq)*2) // #nosec G103
			return processResultsParallelInternal(ctx, parallelSearchHostF64{h}, raw, candidates, k, nil, roaringFilter)
		case []complex64:
			// Treat as float32 with 2N dims
			if len(oq) == 0 {
				return []types.SearchResult{}
			}
			raw := unsafe.Slice((*float32)(unsafe.Pointer(&oq[0])), len(oq)*2) // #nosec G103
			return processResultsParallelInternal(ctx, parallelSearchHostF32{h}, raw, candidates, k, nil, roaringFilter)
		}
	}
	return nil
}

type parallelSearchHostF32 struct{ h *ArrowHNSW }

func (p parallelSearchHostF32) GetDataset() types.IndexDataProvider { return p.h.dataset }
func (p parallelSearchHostF32) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}
func (p parallelSearchHostF32) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}
func (p parallelSearchHostF32) GetDistanceFuncForParallel() func(a, b []float32) float32 {
	return func(a, b []float32) float32 {
		d, _ := p.h.distFunc(a, b)
		return d
	}
}
func (p parallelSearchHostF32) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }
func (p parallelSearchHostF32) IsDeleted(id uint32) bool                   { return p.h.IsDeleted(id) }
func (p parallelSearchHostF32) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

type parallelSearchHostF64 struct{ h *ArrowHNSW }

func (p parallelSearchHostF64) GetDataset() types.IndexDataProvider { return p.h.dataset }
func (p parallelSearchHostF64) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}
func (p parallelSearchHostF64) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float64) error {
	return p.h.ExtractVectorF64ToBufferForParallel(rec, rowIdx, dst)
}
func (p parallelSearchHostF64) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}
func (p parallelSearchHostF64) GetDistanceFuncForParallel() func(a, b []float64) float32 {
	return func(a, b []float64) float32 {
		d, _ := p.h.distFuncF64(a, b)
		return d
	}
}
func (p parallelSearchHostF64) ExtractVectorByIDToBufferForParallel(id uint32, dst []float64) error {
	return p.h.ExtractVectorF64ByIDToBufferForParallel(id, dst)
}
func (p parallelSearchHostF64) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }
func (p parallelSearchHostF64) IsDeleted(id uint32) bool                   { return p.h.IsDeleted(id) }
func (p parallelSearchHostF64) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

// ExtractVectorToBufferForParallel extracts a vector directly into a destination buffer.
func (h *ArrowHNSW) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float32) error {
	vecColIdx := h.getVectorColumnIndex(rec)

	if vecColIdx == -1 {
		return fmt.Errorf("vector column not found in record")
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return err
	}

	// Optimized buffer-based conversion
	switch v := vec.(type) {
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		for i, val := range v {
			dst[i*2] = float32(real(val))
			dst[i*2+1] = float32(imag(val))
		}
		return nil
	case []complex64:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		// Complex64 is 2x float32 in memory
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float16.Num:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = val.Float32()
		}
		return nil
	case []int8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			decoded := h.quantizer.Decode(v)
			if len(dst) != len(decoded) {
				return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
			}
			copy(dst, decoded)
			return nil
		}
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	}

	return fmt.Errorf("unsupported vector type %T for buffer-based extraction", vec)
}

// ExtractVectorF64ToBufferForParallel extracts a float64 vector into a destination buffer.

// ExtractVectorByIDToBufferForParallel extracts a vector by ID into a float32 buffer.
func (h *ArrowHNSW) ExtractVectorByIDToBufferForParallel(id uint32, dst []float32) error {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return err
	}

	switch v := vecAny.(type) {
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		for i, val := range v {
			dst[i*2] = float32(real(val))
			dst[i*2+1] = float32(imag(val))
		}
		return nil
	case []complex64:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []float16.Num:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = val.Float32()
		}
		return nil
	case []int8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			decoded := h.quantizer.Decode(v)
			if len(dst) != len(decoded) {
				return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
			}
			copy(dst, decoded)
			return nil
		}
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	}

	return fmt.Errorf("unsupported vector type %T for buffer-based extraction", vecAny)
}

// ExtractVectorF64ByIDToBufferForParallel extracts a vector by ID into a float64 buffer.

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
		interval := uint64(1.0 / h.config.SearchLayerSampleRate)
		if interval == 0 {
			interval = 1
		}

		if count%interval == 0 {
			metrics.HnswNodesVisited.WithLabelValues(h.name).Observe(float64(ctx.nodesVisitedCount))
		}
	}
}

// MinCandidateHeap for exploration (closest first)
// Uses store.Candidate (ID, Dist) to match ArrowSearchContext
// GetLayerNeighbors returns internal neighbor IDs for a specific layer
func (h *ArrowHNSW) GetLayerNeighbors(id uint32, layer int) ([]uint32, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data is nil")
	}

	maxLevel := h.GetMaxLevel()
	meta := h.GetMetadataSnapshot()
	if int64(id) >= meta.NodeCount {
		return nil, fmt.Errorf("%w: id=%d", ErrVectorNotFound, id)
	}
	if meta.MaxLevel < 0 {
		return nil, nil
	}

	if layer < 0 || int32(layer) > maxLevel { // #nosec G115
		return nil, fmt.Errorf("invalid layer: %d", layer)
	}

	// 1. Try PackedNeighbors (Lock-Free)
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		if neighbors, ok := data.PackedNeighbors[layer].GetNeighbors(id); ok {
			return neighbors, nil
		}
	}

	// 2. Fallback to Legacy Chunks
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
	startIdx := int(cOff) * types.MaxNeighbors                  // #nosec G115
	copy(neighbors, neighborhood[startIdx:startIdx+int(count)]) // #nosec G115

	return neighbors, nil
}

// GetRawNeighbors implements the VectorIndexer interface
func (h *ArrowHNSW) GetRawNeighbors(id uint32) ([]uint32, error) {
	return h.GetLayerNeighbors(id, 0)
}

// GetNeighbors retrieves the k-nearest neighbors for a given vector ID.
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

// SearchForParallel performs a search for parallel processing.
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

// GetVector retrieves the vector for the given ID, checking memory and disk caches.

// GetVectorAny returns the vector with the given ID as an interface{}.

// mustGetVectorFromData retrieves a vector from the given data snapshot.
