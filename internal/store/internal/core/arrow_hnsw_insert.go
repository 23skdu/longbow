package core

import (
	"context"
	"github.com/23skdu/longbow/internal/store/types"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *types.GraphData) ([]types.Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, true)
	var res []types.Candidate
	var err error
	if compF32, ok := computer.(*float32ToFloat32Computer); ok {
		res, err = h.searchLayerFloat32(goCtx, compF32, entryPoint, ef, layer, ctx, data)
	} else {
		res, err = h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, query)
	}
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

	extracted := make([]any, len(candidates))
	for i, cand := range candidates {
		vecAny, _ := data.GetVector(cand.ID)
		extracted[i] = vecAny
	}

	selectedVecs := make([]any, 0, m)

	// For non-float32, we use a cached diversity check
	// We use specialized loops for each type to avoid type assertions in the hot path
	switch data.Type {
	case types.VectorTypeInt8:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int8)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int8)
				d, _ := h.distFuncInt8(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt16:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int16)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int16)
				d, _ := h.distFuncInt16(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint16:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint16)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint16)
				d, _ := h.distFuncUint16(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt32:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int32)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int32)
				d, _ := h.distFuncInt32(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint32:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint32)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint32)
				d, _ := h.distFuncUint32(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int64)
				d, _ := h.distFuncInt64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint64)
				d, _ := h.distFuncUint64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeFloat64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]float64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]float64)
				d, _ := h.distFuncF64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeComplex64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]complex64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]complex64)
				d, _ := h.distFuncC64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeComplex128:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]complex128)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]complex128)
				d, _ := h.distFuncC128(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	default:
		// Fallback for unknown types (slow path)
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1 := extracted[i]
			if v1 == nil {
				continue
			}
			for j := range selected {
				_ = selectedVecs[j]
				// CORRECT RobustPrune: only reject if an existing neighbor is CLOSER to the candidate than the query is
				// We don't have a distFunc for unknown types, so we fallback to a simple reachability check
				// (in practice this shouldn't happen as all types are registered)
				// For now, just allow all up to M
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
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

	extracted := make([][]float32, len(candidates))
	for i, cand := range candidates {
		vecAny, _ := data.GetVector(cand.ID)
		if v, ok := vecAny.([]float32); ok {
			extracted[i] = v
		}
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

	selectedVecs := make([][]float32, 0, m)

	for i, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1 := extracted[i]
		if v1 == nil {
			continue
		}

		for j := range selected {
			v2 := selectedVecs[j]

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
			selectedVecs = append(selectedVecs, v1)
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
