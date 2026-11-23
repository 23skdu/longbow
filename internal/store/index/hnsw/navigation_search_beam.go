package hnsw

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// SearchVectorsWithBitmap runs the search vectors with bitmap operation.
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

	// Per-dtype auto routing (roadmap.md §2): pin backend
	// for the duration of this search based on ResolveBackend rules.
	restoreDispatch := applyIndexDispatch(h.config.DataType.String(), int(meta.NodeCount))
	defer restoreDispatch()

	// Automatic GPU dispatch for supported types
	if h.gpuEnabled && h.gpuIndex != nil && meta.NodeCount >= 1024 {
		gpuResults, err := h.searchGPU(ctx, queryVec, k)
		if err == nil && len(gpuResults) > 0 {
			return gpuResults, nil
		}
		// Fall through to CPU on GPU error
	}

	metrics.HNSWSearchPoolGetSharded.Inc()
	start := time.Now()
	searchCtx := h.searchPool.Get()
	searchCtx.MaxNodeCount = meta.NodeCount
	searchCtx.MaxGeneration = meta.Generation

	// Extract search options
	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	// Set visited nodes budget to prevent P99 tail latency.
	ef := searchOptions.Ef
	if ef <= 0 {
		ef = 50
	}
	budget := ef * 40
	if budget < 5000 {
		budget = 5000
	} else if budget > 100000 {
		budget = 100000
	}
	searchCtx.visitedNodesBudget = budget
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
		// Pre-convert the filter to a dense bitmask once so that traversal
		// probes candidates with a single word load instead of a binary search
		// through the roaring containers. Densification falls back to
		// filterBitmap when the filter's id universe is too wide, or its
		// conversion too expensive, to be worth a dense bitmask.
		searchCtx.filterMask, searchCtx.filterBits = buildFilterMask(searchCtx.filterBitmap, searchCtx.filterBits)
	} else {
		searchCtx.filterBitmap = nil
		searchCtx.filterMask = nil
	}
	if filter != nil {
		h.hotpath.PreFilteredCounter(h.name).Inc()
		if filter.IsEmpty() {
			h.hotpath.FilterEarlyExitCounter(h.name).Inc()
			metrics.HNSWSearchPoolPutSharded.Inc()
			searchCtx.filterMask = nil
			h.searchPool.Put(searchCtx)
			return nil, nil
		}
	}

	selectivity := 1.0
	// Keep the count in int64/uint64 throughout: int is 32-bit on some
	// platforms, and meta.NodeCount is int64. Node IDs are uint32, so the
	// addressable ID space bounds selectivity estimation and keeps the
	// uint32 narrowing below provably in range.
	totalNodes := meta.NodeCount
	if totalNodes > math.MaxUint32 {
		totalNodes = math.MaxUint32
	}
	if filter != nil && totalNodes > 0 {
		card := filter.GetCardinality()
		if card > 0 {
			selectivity = float64(card) / float64(totalNodes)
			if selectivity > 1.0 {
				selectivity = 1.0
			}
		} else {
			selectivity = 0.0
		}
	} else if searchCtx.predicate != nil && totalNodes > 0 {
		sampleN := int64(64)
		if totalNodes < sampleN {
			sampleN = totalNodes
		}
		stride := uint64(totalNodes) / uint64(sampleN)
		if stride == 0 {
			stride = 1
		}
		matched := 0
		for i := int64(0); i < sampleN; i++ {
			sampleID := uint64(i) * stride
			if sampleID > math.MaxUint32 {
				sampleID = math.MaxUint32
			}
			if searchCtx.predicate.IsMatch(uint32(sampleID)) {
				matched++
			}
		}
		if matched > 0 {
			selectivity = float64(matched) / float64(sampleN)
		} else {
			selectivity = 1.0 / float64(sampleN*2)
		}
	}

	defer func() {
		searchCtx.filterBitmap = nil
		searchCtx.filterMask = nil
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

			metrics.HNSWSearchPoolPutSharded.Add(mult)
			h.searchPool.PutWithMetrics(searchCtx, typeLabel, dimsStr)
		} else {
			h.searchPool.Put(searchCtx)
		}
	}()

	ep := meta.EntryPoint
	maxLevel := meta.MaxLevel

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
	upperOptions.Predicate = nil
	upperOptions.FilterExpr = nil
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

	{
		upperPredicate := searchCtx.predicate
		upperFilterMask := searchCtx.filterMask
		upperFilterBitmap := searchCtx.filterBitmap
		searchCtx.predicate = nil
		searchCtx.filterMask = nil
		searchCtx.filterBitmap = nil

		for level := int(maxLevel); level > 0; level-- { // #nosec G115
			// Greedy search: keep 1 best candidate
			var res []types.Candidate
			var err error
			if compF32, ok := upperComputer.(*float32ToFloat32Computer); ok {
				res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, 1, level, searchCtx, data)
			} else if compF64, ok := upperComputer.(*float64Computer); ok {
				res, err = h.searchLayerFloat64(ctx, compF64, currObj.ID, 1, level, searchCtx, data)
			} else if compSQ8, ok := upperComputer.(*float32ToSQ8Computer); ok {
				res, err = h.searchLayer(ctx, compSQ8, currObj.ID, 1, level, searchCtx, data, queryVec)
			} else {
				res, err = h.searchLayer(ctx, upperComputer, currObj.ID, 1, level, searchCtx, data, queryVec)
			}
			if err != nil {
				searchCtx.predicate = upperPredicate
				searchCtx.filterMask = upperFilterMask
				searchCtx.filterBitmap = upperFilterBitmap
				h.flushSearchMetrics(searchCtx)
				return nil, err
			}

			candidates := res
			if len(candidates) > 0 {
				currObj = candidates[0]
			}
		}

		searchCtx.predicate = upperPredicate
		searchCtx.filterMask = upperFilterMask
		searchCtx.filterBitmap = upperFilterBitmap
	}

search_layer0:
	// 2. Search at layer 0 with adaptive retry
	baseEf := int(h.config.EfSearch)
	if searchOptions.Ef > 0 {
		baseEf = searchOptions.Ef
	}
	if h.config.SQ8Enabled && baseEf < 100 {
		// Provide more search buffer by default for SQ8 to compensate for quantization noise
		baseEf = 100
	}
	if k > baseEf {
		baseEf = k
	}

	maxEf := 2000
	if h.efTuner != nil {
		if tunerMax := h.efTuner.GetMaxEf(); tunerMax > 0 {
			maxEf = tunerMax
		}
	}
	maxNodeCount := int(meta.NodeCount)
	if maxNodeCount > 0 && maxEf > maxNodeCount {
		maxEf = maxNodeCount
	}

	efSearch := baseEf
	// Step 1: Adaptive ef Scaling for Filtered Graph Traversal (roadmap.md §7 item 1)
	// When a selective predicate or filter is active, scaling efSearch upfront ensures
	// layer-0 graph traversal collects sufficient matching candidates on attempt 0,
	// avoiding 2-3x latency penalties from iterative retries and short result sets.
	if selectivity > 0.0 && selectivity < 0.8 {
		scaledEf := int(math.Ceil(float64(k) / selectivity * 1.25))
		if scaledEf > efSearch {
			efSearch = scaledEf
		}
	}

	if efSearch > maxEf {
		efSearch = maxEf
	}
	if efSearch < baseEf {
		efSearch = baseEf
	}
	if efSearch < k {
		efSearch = k
	}

	// Ensure searchCtx.visitedNodesBudget accommodates the scaled efSearch
	minBudget := efSearch * 40
	if searchCtx.visitedNodesBudget < minBudget {
		searchCtx.visitedNodesBudget = minBudget
		if searchCtx.visitedNodesBudget > 200000 {
			searchCtx.visitedNodesBudget = 200000
		}
	}

	var results []types.SearchResult
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Reset node visit counter between retries so budget is fresh per attempt
		searchCtx.nodesVisitedCount = 0

		var res []types.Candidate
		var err error
		if compF32, ok := computer.(*float32ToFloat32Computer); ok {
			res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, efSearch, 0, searchCtx, data)
		} else if compF64, ok := computer.(*float64Computer); ok {
			res, err = h.searchLayerFloat64(ctx, compF64, currObj.ID, efSearch, 0, searchCtx, data)
		} else {
			res, err = h.searchLayer(ctx, computer, currObj.ID, efSearch, 0, searchCtx, data, queryVec)
		}
		if err != nil {
			return nil, err
		}

		results = h.ProcessResultsParallel(ctx, queryVec, queryVec, res, k, filter)
		if len(results) == 0 && len(res) > 0 {
			results = h.extractSearchResults(res, k, searchCtx)
		}

		if len(results) >= k || attempt == 2 || efSearch >= maxNodeCount {
			break
		}

		// Adaptive Search Expansion Policy:
		// Instead of a blind multiplier, use PID-based autonomous efSearch tuning
		recallProxy := float64(len(results)) / float64(k)
		if recallProxy > 1.0 {
			recallProxy = 1.0
		}

		nextEf := h.efTuner.Update(recallProxy)
		if nextEf <= efSearch {
			efSearch = int(float64(efSearch) * 1.5)
		} else {
			efSearch = nextEf
		}
		if efSearch > maxNodeCount && maxNodeCount > 0 {
			efSearch = maxNodeCount
		}
	}

	return results, nil
}
