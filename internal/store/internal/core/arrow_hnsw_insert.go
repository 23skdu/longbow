package core

import (
	"context"
	"github.com/23skdu/longbow/internal/store/types"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *types.GraphData) ([]types.Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, false)
	res, err := h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, query)
	if err != nil {
		return nil, err
	}

	cloned := h.getCandidateSlice(len(res))
	cloned = append(cloned, res...)
	return cloned, nil
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	if len(candidates) <= m {
		return candidates
	}

	// Optimization: Limit the scope of the diversity check
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	// Fast-path for Float32 to lift type declarations out of nested loops
	// types.VectorTypeFloat32 is the design DataType setup.
	// Setup constants to keep speedups or just use types.VectorDataType(1)?
	// Let's check with types.VectorTypeFloat32
	if data.Type == 1 { // VectorTypeFloat32 is usually 1, or just do reflection assert once
		return h.selectNeighborsFloat32(ctx, candidates, m, data)
	}

	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}
		selected = append(selected, cand)
	}

	// Fallback: if selected is empty but candidates were not, take at least one (closest)
	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	// Optional: if SQ8 is enabled and we have very few neighbors, take a few more even if not diverse
	if h.config.SQ8Enabled && len(selected) < m/4 && len(candidates) > len(selected) {
		// Take some more to ensure connectivity
		for _, cand := range candidates {
			found := false
			for _, s := range selected {
				if s.ID == cand.ID {
					found = true
					break
				}
			}
			if !found {
				selected = append(selected, cand)
				if len(selected) >= m/2 {
					break
				}
			}
		}
	}

	return selected
}

// Core insertion functions that remain to be refactored in Phase 3

// selectNeighborsFloat32 is a specialized high-performance diversity check for float32 vectors.
func (h *ArrowHNSW) selectNeighborsFloat32(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	// Optimization: use context scratch buffer to avoid allocations in critical path
	// but ensure isolation by using a local slice if ctx is nil (should not happen in bulk path).
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	var vectorCache map[uint32]any
	if ctx != nil {
		vectorCache = ctx.vectorCache
	} else {
		vectorCache = make(map[uint32]any, len(candidates))
	}

	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1Any, ok := vectorCache[cand.ID]
		var v1 []float32
		if !ok {
			vecAny, _ := data.GetVector(cand.ID)
			if v, ok := vecAny.([]float32); ok {
				v1 = v
				vectorCache[cand.ID] = v
			}
		} else {
			v1, _ = v1Any.([]float32)
		}
		if v1 == nil {
			continue
		}

		for _, sel := range selected {
			v2Any, ok := vectorCache[sel.ID]
			var v2 []float32
			if !ok {
				vecAny, _ := data.GetVector(sel.ID)
				if v, ok := vecAny.([]float32); ok {
					v2 = v
					vectorCache[sel.ID] = v
				}
			} else {
				v2, _ = v2Any.([]float32)
			}
			if v2 == nil {
				continue
			}

			d, err := h.distFunc(v1, v2)

			threshold := cand.Dist
			if h.config.SQ8Enabled {
				threshold *= 1.2
			}


			if err == nil && d > 0 && d < threshold {
				isDiverse = false
				break
			}
		}

		if isDiverse {
			selected = append(selected, cand)
		}
	}

	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	if ctx != nil {
		ctx.scratchSelected = selected
	}
	return selected
}

// Insert function moved to insertion_core.go

// ensureTrained function moved to quantization_integration.go
