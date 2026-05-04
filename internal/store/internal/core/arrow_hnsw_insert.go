package core

import (
	"context"
	"fmt"
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

	if data.Type == types.VectorTypeFloat32 {
		return h.selectNeighborsFloat32(ctx, candidates, m, data)
	}

	// Generic path for other types (Int8, Int16, etc.)
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	if len(candidates) == 0 {
		return nil
	}

	// For non-float32, we use a slower but generic diversity check
	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1Any, err := data.GetVector(cand.ID)
		if err != nil || v1Any == nil {
			continue
		}

		for _, sel := range selected {
			v2Any, err := data.GetVector(sel.ID)
			if err != nil || v2Any == nil {
				continue
			}

			// Compute distance between candidates to check diversity
			var d float32
			var distErr error
			
			switch v1 := v1Any.(type) {
			case []int8:
				d, distErr = h.distFuncInt8(v1, v2Any.([]int8))
			case []int16:
				d, distErr = h.distFuncInt16(v1, v2Any.([]int16))
			case []uint16:
				d, distErr = h.distFuncUint16(v1, v2Any.([]uint16))
			case []int32:
				d, distErr = h.distFuncInt32(v1, v2Any.([]int32))
			case []uint32:
				d, distErr = h.distFuncUint32(v1, v2Any.([]uint32))
			case []int64:
				d, distErr = h.distFuncInt64(v1, v2Any.([]int64))
			case []uint64:
				d, distErr = h.distFuncUint64(v1, v2Any.([]uint64))
			case []float64:
				d, distErr = h.distFuncF64(v1, v2Any.([]float64))
			default:
				distErr = fmt.Errorf("unsupported type")
			}

			if distErr == nil && d < cand.Dist {
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

// selectNeighborsFloat32 is a specialized high-performance diversity check for float32 vectors.
func (h *ArrowHNSW) selectNeighborsFloat32(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
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
