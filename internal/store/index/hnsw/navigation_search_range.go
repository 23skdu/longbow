package hnsw

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
)

// SearchVectorsInRange runs the search vectors in range operation.
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
	searchCtx.diskGraph = h.diskGraph.Load()
	searchCtx.nodesVisitedCount = 0
	searchCtx.visitedNodesBudget = max(maxNodeCount*4, 5000)
	defer func() {
		searchCtx.filterBitmap = nil
		searchCtx.filterMask = nil
		searchCtx.diskGraph = nil
		metrics.HNSWSearchPoolPutSharded.Inc()
		h.searchPool.Put(searchCtx)
	}()

	searchCtx.filterBitmap = roaringFilter
	// Pre-convert the filter to a dense bitmask once; the traversal probes it
	// per candidate and the result loop below reuses the same mask.
	searchCtx.filterMask, searchCtx.filterBits = buildFilterMask(roaringFilter, searchCtx.filterBits)
	if roaringFilter != nil {
		h.hotpath.PreFilteredCounter(h.name).Inc()
	}

	computer = h.resolveHNSWComputer(data, searchCtx, queryVec, false, options)

	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	currObj := types.Candidate{ID: ep, Dist: math.MaxFloat32}
	upperOptions := searchOptions
	upperOptions.ForceQuantized = true
	upperOptions.Predicate = nil
	upperOptions.FilterExpr = nil
	upperComputer := h.resolveHNSWComputer(data, searchCtx, queryVec, false, upperOptions)

	{
		upperPredicate := searchCtx.predicate
		upperFilterMask := searchCtx.filterMask
		upperFilterBitmap := searchCtx.filterBitmap
		searchCtx.predicate = nil
		searchCtx.filterMask = nil
		searchCtx.filterBitmap = nil

		for level := int(maxLevel); level > 0; level-- { // #nosec G115
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
				return nil, err
			}
			if len(res) > 0 {
				currObj = res[0]
			}
		}

		searchCtx.predicate = upperPredicate
		searchCtx.filterMask = upperFilterMask
		searchCtx.filterBitmap = upperFilterBitmap
	}

	ef := maxNodeCount
	if searchOptions.MaxResults > 0 {
		ef = max(searchOptions.MaxResults*4, int(h.config.EfSearch), 64)
		if ef > maxNodeCount {
			ef = maxNodeCount
		}
	}

	var res []types.Candidate
	var err error
	if compF32, ok := computer.(*float32ToFloat32Computer); ok {
		res, err = h.searchLayerFloat32(ctx, compF32, currObj.ID, ef, 0, searchCtx, data)
	} else if compF64, ok := computer.(*float64Computer); ok {
		res, err = h.searchLayerFloat64(ctx, compF64, currObj.ID, ef, 0, searchCtx, data)
	} else {
		res, err = h.searchLayer(ctx, computer, currObj.ID, ef, 0, searchCtx, data, queryVec)
	}
	if err != nil {
		return nil, err
	}

	var results []types.SearchResult
	if len(res) > 0 {
		refined := h.ProcessResultsParallel(ctx, queryVec, queryVec, res, len(res), roaringFilter)
		if refined == nil {
			// Fallback for query vector types without parallel refinement.
			refined = h.extractSearchResults(res, len(res), searchCtx)
		}
		for _, r := range refined {
			if r.Distance <= threshold {
				results = append(results, r)
			}
		}
	}

	_ = time.Since(start)
	_ = maxNodeCount

	return results, nil
}
