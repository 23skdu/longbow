package index

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/store/types"
)

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
	if data.Type == types.VectorTypeFloat64 {
		return h.selectNeighborsFloat64(ctx, candidates, m, data)
	}
	if data.Type == types.VectorTypeComplex64 {
		return h.selectNeighborsComplex64(ctx, candidates, m, data)
	}
	if data.Type == types.VectorTypeComplex128 {
		return h.selectNeighborsComplex128(ctx, candidates, m, data)
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

	var extracted []any
	if ctx != nil {
		if cap(ctx.scratchExtractedAny) >= len(candidates) {
			extracted = ctx.scratchExtractedAny[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([]any, len(candidates))
			ctx.scratchExtractedAny = extracted
		}
	} else {
		extracted = make([]any, len(candidates))
	}
	for i, cand := range candidates {
		vecAny, _ := data.GetVector(cand.ID)
		extracted[i] = vecAny
	}

	var selectedVecs []any
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsAny[:0]
	} else {
		selectedVecs = make([]any, 0, m)
	}

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
	case types.VectorTypeUint8:
		// uint8 needs its own case for the same reason int8 has one. It used
		// to fall through to the default branch, which is the documented
		// fallback for unknown types and applies no diversity heuristic at
		// all - it keeps the first m candidates in arrival order and does
		// nothing else. A neighbour list pruned that way is a truncation
		// rather than a selection, and the graph does not hold together.
		//
		// Measured on the 3-cluster corpus the recall tests use: a uint8
		// index built through the default branch reached 314-1572 of 4,000
		// nodes from its entry point, mean degree 1.27-6.29, varying run to
		// run on unchanged code. int8 on the same corpus reached 4,000 at
		// degree 16.00 every time. The difference is that int8 has this
		// branch and uint8 did not - the bulk linker prunes ~63k times per
		// 4,000-node uint8 build and never for int8, because only uint8 was
		// taking the distance-computing prune path.
		//
		// The distances were available all along: distFuncUint8 is the native
		// 8-bit kernel and int8Computer.uint8Mode already exists.
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint8)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint8)
				d, _ := h.distFuncUint8(v1, v2)
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
	case types.VectorTypeFloat16:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]float16.Num)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]float16.Num)
				d, _ := h.distFuncF16(v1, v2)
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
	case types.VectorTypeTQ:
		if arena := h.tqArena.Load(); arena != nil && arena.IsEnabled() {
			dim := int(h.dims.Load())
			for _, cand := range candidates {
				if len(selected) >= m {
					break
				}
				// A cache miss must cost a decode, never the candidate.
				// tqVectorAt decodes on demand, and only a vector that cannot
				// be reconstructed at all falls through to "accept", because
				// dropping it here prunes a neighbour list with no geometric
				// information in it. See tqVectorAt.
				v1, ok1 := h.tqVectorAt(arena, cand.ID, dim)
				if !ok1 {
					selected = append(selected, cand)
					continue
				}
				isDiverse := true
				for j := range selected {
					v2, ok2 := h.tqVectorAt(arena, selected[j].ID, dim)
					if ok2 {
						d, _ := h.distFunc(v1[:dim], v2[:dim])
						if d < cand.Dist {
							isDiverse = false
							break
						}
					}
				}
				if isDiverse {
					selected = append(selected, cand)
				}
			}
		} else if p := h.tqDecodeCache.Load(); p != nil {
			dim := int(h.dims.Load())
			for _, cand := range candidates {
				if len(selected) >= m {
					break
				}
				isDiverse := true
				off1 := int(cand.ID) * dim
				if off1+dim > len(p.data) {
					continue
				}
				v1 := p.data[off1 : off1+dim]
				for j := range selected {
					off2 := int(selected[j].ID) * dim
					if off2+dim <= len(p.data) {
						v2 := p.data[off2 : off2+dim]
						d, _ := h.distFunc(v1, v2)
						if d < cand.Dist {
							isDiverse = false
							break
						}
					}
				}
				if isDiverse {
					selected = append(selected, cand)
				}
			}
		} else {
			for _, cand := range candidates {
				if len(selected) >= m {
					break
				}
				selected = append(selected, cand)
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

	// KeepPrunedConnections (Malkov & Yashunin Algorithm 4): if diversity heuristic leaves
	// selected below m, top up from the discarded candidates in distance order.
	if len(selected) < m {
		for _, cand := range candidates {
			if len(selected) >= m {
				break
			}
			alreadySelected := false
			for _, s := range selected {
				if s.ID == cand.ID {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected {
				selected = append(selected, cand)
			}
		}
	}

	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	if ctx != nil {
		ctx.scratchSelected = selected
		ctx.scratchExtractedAny = extracted
		ctx.scratchSelectedVecsAny = selectedVecs
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

	var extracted [][]float32
	if ctx != nil {
		if cap(ctx.scratchExtractedF32) >= len(candidates) {
			extracted = ctx.scratchExtractedF32[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([][]float32, len(candidates))
			ctx.scratchExtractedF32 = extracted
		}
	} else {
		extracted = make([][]float32, len(candidates))
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}

	pd := data.GetPaddedDimsForType(types.VectorTypeFloat32)
	for i, cand := range candidates {
		cID := int(cand.ID) / types.ChunkSize
		cOff := int(cand.ID) % types.ChunkSize
		chunk := data.GetVectorsChunkWithGen(cID, maxGen)
		if chunk != nil {
			start := cOff * pd
			if start+data.Dims <= len(chunk) {
				extracted[i] = chunk[start : start+data.Dims]
			}
		}
	}

	var selectedVecs [][]float32
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsF32[:0]
	} else {
		selectedVecs = make([][]float32, 0, m)
	}

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

	// KeepPrunedConnections (Malkov & Yashunin Algorithm 4): if the diversity
	// heuristic leaves selected below m, top up from the discarded candidates in
	// distance order so the node does not suffer from connection starvation.
	if len(selected) < m {
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			alreadySelected := false
			for _, s := range selected {
				if s.ID == cand.ID {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected && extracted[i] != nil {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, extracted[i])
			}
		}
	}

	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	if ctx != nil {
		ctx.scratchSelected = selected
		ctx.scratchSelectedVecsF32 = selectedVecs
	}
	return selected
}
