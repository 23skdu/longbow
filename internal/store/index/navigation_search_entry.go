package index

import (
	"context"
	"sort"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/tensor"
)

// applyIndexDispatch pins the math backend for one search using ResolveBackend
// rules (dtype + node count). Returns a restore func. Skipped while a
// PushStandard scope is active (temporal search, roadmap.md §2).
func applyIndexDispatch(typeName string, nodeCount int) (restore func()) {
	if mathutil.IsForceStandard() {
		return func() {}
	}
	want := tensor.ResolveBackend(typeName, nodeCount)
	prev := mathutil.GetBackend()
	if want == prev {
		return func() {}
	}
	mathutil.SetBackend(want)
	return func() { mathutil.SetBackend(prev) }
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

// extractSearchResults translates raw traversal candidates to SearchResults, enforcing
// deletion status, bitmap/dense filter constraints, and metadata predicates.
func (h *ArrowHNSW) extractSearchResults(candidates []types.Candidate, k int, searchCtx *ArrowSearchContext) []types.SearchResult {
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Dist < candidates[j].Dist })
	fm := searchCtx.filterMask
	bm := searchCtx.filterBitmap
	limit := k
	if len(candidates) < limit {
		limit = len(candidates)
	}
	results := make([]types.SearchResult, 0, limit)
	for _, c := range candidates {
		if h.IsDeleted(c.ID) {
			continue
		}
		if fm != nil {
			if !fm.allows(c.ID) {
				continue
			}
		} else if bm != nil {
			if !bm.Contains(c.ID) {
				continue
			}
		}
		if searchCtx.predicate != nil && !searchCtx.predicate.IsMatch(c.ID) {
			continue
		}
		score := float32(1.0 / (1.0 + float64(c.Dist)))
		results = append(results, types.SearchResult{
			ID:       types.VectorID(c.ID),
			Distance: c.Dist,
			Score:    score,
		})
		if len(results) >= k {
			break
		}
	}
	return results
}
