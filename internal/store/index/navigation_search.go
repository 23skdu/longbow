package index

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.growMu.RLock()
	defer h.growMu.RUnlock()
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
	computer := h.resolveHNSWComputer(data, searchCtx, queryVec, false, options)
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

	upperOptions := searchOptions
	upperOptions.ForceQuantized = true
	upperComputer := h.resolveHNSWComputer(data, searchCtx, queryVec, false, upperOptions)

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
		if compF32, ok := upperComputer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, 1, level, searchCtx, data)
		} else if compSQ8, ok := upperComputer.(*float32ToSQ8Computer); ok {
			res, err = h.searchLayer(ctx, compSQ8, currObj.ID, 1, level, searchCtx, data, queryVec)
		} else {
			res, err = h.searchLayer(ctx, upperComputer, currObj.ID, 1, level, searchCtx, data, queryVec)
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

func (h *ArrowHNSW) SearchVectorsInRange(ctx context.Context, queryVec any, threshold float32, filters []query.Filter, options any) ([]types.SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h.growMu.RLock()
	defer h.growMu.RUnlock()

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

	computer := h.resolveHNSWComputer(data, nil, queryVec, false, options)
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

	computer = h.resolveHNSWComputer(data, searchCtx, queryVec, false, options)

	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	currObj := types.Candidate{ID: ep, Dist: math.MaxFloat32}
	upperOptions := searchOptions
	upperOptions.ForceQuantized = true
	upperComputer := h.resolveHNSWComputer(data, searchCtx, queryVec, false, upperOptions)

	for level := int(maxLevel); level > 0; level-- { // #nosec G115
		var res []types.Candidate
		var err error
		if compF32, ok := upperComputer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, 1, level, searchCtx, data)
		} else if compSQ8, ok := upperComputer.(*float32ToSQ8Computer); ok {
			res, err = h.searchLayer(ctx, compSQ8, currObj.ID, 1, level, searchCtx, data, queryVec)
		} else {
			res, err = h.searchLayer(ctx, upperComputer, currObj.ID, 1, level, searchCtx, data, queryVec)
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

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, searchCtx *ArrowSearchContext, queryVal any, squared bool, options any) DistanceComputer {
	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	switch q := queryVal.(type) {
	case []float32:
		bypassQuantization := searchOptions.VectorType == types.VectorTypeFloat32

		// Fallback dynamically to full-precision Float32 on Layer 0 if primary type is Float32
		// ForceQuantized is true for upper layers (layer > 0)
		if !bypassQuantization && !searchOptions.ForceQuantized && data.Type == types.VectorTypeFloat32 {
			// Unless the user explicitly requested VectorTypeTQ, fall back to Float32
			if searchOptions.VectorType != types.VectorTypeTQ {
				bypassQuantization = true
			}
		}

		if bypassQuantization {
			// Bypass TurboQuant and Product Quantization to force exact matching fallback
		} else {
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

			// Force SQ8 Quantized Navigation on upper layers
			if searchOptions.ForceQuantized && h.config.SQ8Enabled && data.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
				minV, maxV := h.quantizer.Params()
				scale := (maxV - minV) / 255.0
				return &float32ToSQ8Computer{data: data, q: q, dims: len(q), h: h, squared: squared, maxGen: maxGen, minV: minV, scale: scale}
			}

			return &float32ToFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen}
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		if searchCtx != nil {
			if data.Type == types.VectorTypeInt8 || data.Type == types.VectorTypeUint8 {
				searchCtx.queryInt8 = searchCtx.queryInt8[:0]
				for _, val := range q {
					searchCtx.queryInt8 = append(searchCtx.queryInt8, int8(val))
				}
				qUint8 := *(*[]uint8)(unsafe.Pointer(&searchCtx.queryInt8)) // #nosec G103
				return &int8Computer{data: data, q: qUint8, qInt8: searchCtx.queryInt8, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeInt16 {
				searchCtx.queryInt16 = searchCtx.queryInt16[:0]
				for _, val := range q {
					searchCtx.queryInt16 = append(searchCtx.queryInt16, int16(val))
				}
				return &int16Computer{data: data, q: searchCtx.queryInt16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeUint16 {
				searchCtx.queryUint16 = searchCtx.queryUint16[:0]
				for _, val := range q {
					searchCtx.queryUint16 = append(searchCtx.queryUint16, uint16(val))
				}
				return &uint16Computer{data: data, q: searchCtx.queryUint16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeInt32 {
				searchCtx.queryInt32 = searchCtx.queryInt32[:0]
				for _, val := range q {
					searchCtx.queryInt32 = append(searchCtx.queryInt32, int32(val))
				}
				return &int32Computer{data: data, q: searchCtx.queryInt32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeUint32 {
				searchCtx.queryUint32 = searchCtx.queryUint32[:0]
				for _, val := range q {
					searchCtx.queryUint32 = append(searchCtx.queryUint32, uint32(val))
				}
				return &uint32Computer{data: data, q: searchCtx.queryUint32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeInt64 {
				searchCtx.queryInt64 = searchCtx.queryInt64[:0]
				for _, val := range q {
					searchCtx.queryInt64 = append(searchCtx.queryInt64, int64(val))
				}
				return &int64Computer{data: data, q: searchCtx.queryInt64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeUint64 {
				searchCtx.queryUint64 = searchCtx.queryUint64[:0]
				for _, val := range q {
					searchCtx.queryUint64 = append(searchCtx.queryUint64, uint64(val))
				}
				return &uint64Computer{data: data, q: searchCtx.queryUint64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeComplex64 {
				physDims := len(q)
				logDims := physDims / 2
				if cap(searchCtx.queryC64) < logDims {
					searchCtx.queryC64 = make([]complex64, logDims)
				}
				searchCtx.queryC64 = searchCtx.queryC64[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC64[i] = complex(q[2*i], q[2*i+1])
				}
				return &complex64Computer{data: data, q: searchCtx.queryC64, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen}
			}
			if data.Type == types.VectorTypeComplex128 {
				physDims := len(q)
				logDims := physDims / 2
				if cap(searchCtx.queryC128) < logDims {
					searchCtx.queryC128 = make([]complex128, logDims)
				}
				searchCtx.queryC128 = searchCtx.queryC128[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC128[i] = complex(float64(q[2*i]), float64(q[2*i+1]))
				}
				var sum float64
				for _, v := range searchCtx.queryC128 {
					sum += real(v)*real(v) + imag(v)*imag(v)
				}
				return &complex128Computer{data: data, q: searchCtx.queryC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum)}
			}
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
			if data.Type == types.VectorTypeFloat16 {
				searchCtx.queryF16 = searchCtx.queryF16[:0]
				for _, val := range q {
					searchCtx.queryF16 = append(searchCtx.queryF16, float16.New(val))
				}
				comp.qF16 = searchCtx.queryF16
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
		if data.Type == types.VectorTypeComplex128 {
			physDims := len(q)
			logDims := physDims / 2
			if searchCtx != nil {
				if cap(searchCtx.queryC128) < logDims {
					searchCtx.queryC128 = make([]complex128, logDims)
				}
				searchCtx.queryC128 = searchCtx.queryC128[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC128[i] = complex(q[2*i], q[2*i+1])
				}
				var sum float64
				for _, v := range searchCtx.queryC128 {
					sum += real(v)*real(v) + imag(v)*imag(v)
				}
				return &complex128Computer{data: data, q: searchCtx.queryC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum)}
			}
			qC128 := make([]complex128, logDims)
			for i := 0; i < logDims; i++ {
				qC128[i] = complex(q[2*i], q[2*i+1])
			}
			var sum float64
			for _, v := range qC128 {
				sum += real(v)*real(v) + imag(v)*imag(v)
			}
			return &complex128Computer{data: data, q: qC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum)}
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
			var sum float64
			for _, v := range searchCtx.queryC128 {
				sum += real(v)*real(v) + imag(v)*imag(v)
			}
			return &complex128Computer{data: data, q: searchCtx.queryC128, dims: len(q), h: h, diskGraph: dg, maxGen: searchCtx.MaxGeneration, queryMag: math.Sqrt(sum)}
		}
		dg = h.diskGraph.Load()
		var sum float64
		for _, v := range q {
			sum += real(v)*real(v) + imag(v)*imag(v)
		}
		return &complex128Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: math.MaxUint64, queryMag: math.Sqrt(sum)}
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
