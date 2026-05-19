package core

import (
	"context"
	"github.com/23skdu/longbow/internal/store/types"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *types.GraphData) ([]types.Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, true)
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

	var vectorCache map[uint32]any
	if ctx != nil {
		vectorCache = ctx.vectorCache
	} else {
		vectorCache = make(map[uint32]any, len(candidates))
	}

	// For non-float32, we use a cached diversity check
	// We use specialized loops for each type to avoid type assertions in the hot path
	switch data.Type {
	case types.VectorTypeInt8:
		for _, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]int8)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]int8)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]int8)
				if v2 == nil { continue }
				d, _ := h.distFuncInt8(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeInt16:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]int16)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]int16)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]int16)
				if v2 == nil { continue }
				d, _ := h.distFuncInt16(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeUint16:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]uint16)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]uint16)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]uint16)
				if v2 == nil { continue }
				d, _ := h.distFuncUint16(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeInt32:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]int32)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]int32)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]int32)
				if v2 == nil { continue }
				d, _ := h.distFuncInt32(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeUint32:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]uint32)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]uint32)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]uint32)
				if v2 == nil { continue }
				d, _ := h.distFuncUint32(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeInt64:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]int64)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]int64)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]int64)
				if v2 == nil { continue }
				d, _ := h.distFuncInt64(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeUint64:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]uint64)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]uint64)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]uint64)
				if v2 == nil { continue }
				d, _ := h.distFuncUint64(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeFloat64:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]float64)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]float64)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]float64)
				if v2 == nil { continue }
				d, _ := h.distFuncF64(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeComplex64:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]complex64)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]complex64)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]complex64)
				if v2 == nil { continue }
				d, _ := h.distFuncC64(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	case types.VectorTypeComplex128:
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, ok := vectorCache[cand.ID].([]complex128)
			if !ok {
				vecAny, _ := data.GetVector(cand.ID)
				v1, _ = vecAny.([]complex128)
				if v1 == nil { continue }
				vectorCache[cand.ID] = v1
			}
			for _, sel := range selected {
				v2, _ := vectorCache[sel.ID].([]complex128)
				if v2 == nil { continue }
				d, _ := h.distFuncC128(v1, v2)
				if d < cand.Dist { isDiverse = false; break }
			}
			if isDiverse { selected = append(selected, cand) }
		}
	default:
		// Fallback for unknown types (slow path)
		for _, cand := range candidates {
			if len(selected) >= m { break }
			isDiverse := true
			v1, _ := data.GetVector(cand.ID)
			if v1 == nil { continue }
			for _, sel := range selected {
				v2, _ := data.GetVector(sel.ID)
				if v2 == nil { continue }
				// CORRECT RobustPrune: only reject if an existing neighbor is CLOSER to the candidate than the query is
				// We don't have a distFunc for unknown types, so we fallback to a simple reachability check
				// (in practice this shouldn't happen as all types are registered)
				// For now, just allow all up to M
			}
			if isDiverse { selected = append(selected, cand) }
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


	// Try GPU pruning first if enabled
	if h.gpuEnabled && h.gpuIndex != nil && len(candidates) > 16 {
		candIds := make([]uint32, len(candidates))
		candDists := make([]float32, len(candidates))
		for i, c := range candidates {
			candIds[i] = uint32(c.ID)
			candDists[i] = c.Dist
		}
		
		selectedIds, err := h.pruneNeighborsGPU(candIds, candDists, m)
		if err == nil {
			// Convert back to types.Candidate
			res := make([]types.Candidate, 0, len(selectedIds))
			// We need distances to keep the Candidate structure consistent
			// But for pruning results, we only care about IDs usually.
			// However, HNSW might need them. 
			// For simplicity, we find the original candidate for each ID.
			candMap := make(map[uint32]float32)
			for i := range candidates {
				candMap[uint32(candidates[i].ID)] = candidates[i].Dist
			}
			
			for _, id := range selectedIds {
				res = append(res, types.Candidate{ID: id, Dist: candMap[id]})
			}
			
			if ctx != nil {
				ctx.scratchSelected = res
			}
			return res
		}
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
