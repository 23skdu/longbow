package store

import (
	"context"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *GraphData) ([]Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, false)
	_, err := h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, query)
	if err != nil {
		return nil, err
	}

	// Transfer results from resultSet to a slice
	res := make([]Candidate, ctx.resultSet.Len())
	for i := len(res) - 1; i >= 0; i-- {
		c, _ := ctx.resultSet.Pop()
		res[i] = c
	}
	return res, nil
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []Candidate, m int, _ *GraphData) []Candidate {
	if len(candidates) <= m {
		return candidates
	}

	// Optimization: Limit the scope of the diversity check
	// If limit is set, we only consider the top K candidates for diversity.
	// This avoids O(M * Ef) complexity when Ef is large.
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		// candidates are already sorted by distance (closest first)
		candidates = candidates[:limit]
	}

	// RobustPrune heuristic: select diverse neighbors
	var selected []Candidate
	var remaining []Candidate

	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m)
		}
		selected = ctx.scratchSelected[:0]

		if cap(ctx.scratchRemaining) < len(candidates) {
			ctx.scratchRemaining = make([]Candidate, len(candidates))
		}
		remaining = ctx.scratchRemaining[:len(candidates)]
		copy(remaining, candidates)
	} else {
		selected = make([]Candidate, 0, m)
		remaining = make([]Candidate, len(candidates))
		copy(remaining, candidates)
	}

	// Simple greedy selection for now (can be improved with proper diversity)
	for len(selected) < m && len(remaining) > 0 {
		// Find closest remaining
		bestIdx := 0
		for i := 1; i < len(remaining); i++ {
			if remaining[i].Dist < remaining[bestIdx].Dist {
				bestIdx = i
			}
		}

		selected = append(selected, remaining[bestIdx])
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}

	return selected
}

// Core insertion functions that remain to be refactored in Phase 3

// Insert function moved to insertion_core.go

// ensureTrained function moved to quantization_integration.go
