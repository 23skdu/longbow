package store

import (
	"context"
	"unsafe"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *GraphData) ([]Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, false)
	res, err := h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, query)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
	if len(candidates) <= m {
		return candidates
	}

	// Optimization: Limit the scope of the diversity check
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	var selected []Candidate
	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m)
		}
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]Candidate, 0, m)
	}

	// Unified toF32 helper
	toF32 := func(v any) []float32 {
		switch vf := v.(type) {
		case []float32:
			return vf
		case []int8:
			if h.quantizer != nil && h.sq8Ready.Load() {
				byteVec := *(*[]byte)(unsafe.Pointer(&vf))
				return h.quantizer.Decode(byteVec)
			}
			res := make([]float32, len(vf))
			for i, val := range vf {
				res[i] = float32(uint8(val))
			}
			return res
		case []uint8:
			if h.quantizer != nil && h.sq8Ready.Load() {
				return h.quantizer.Decode(vf)
			}
			res := make([]float32, len(vf))
			for i, val := range vf {
				res[i] = float32(val)
			}
			return res
		default:
			return nil
		}
	}

	// Cache de-quantized vectors to avoid repeated conversions and data access
	vectorCache := make(map[uint32][]float32, len(candidates))

	// HNSW "Heuristic 2" (Diversity Heuristic)
	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1f, ok := vectorCache[cand.ID]
		if !ok {
			vecAny, _ := data.GetVector(cand.ID)
			v1f = toF32(vecAny)
			vectorCache[cand.ID] = v1f
		}
		if v1f == nil {
			continue
		}

		for _, sel := range selected {
			v2f, ok := vectorCache[sel.ID]
			if !ok {
				vecAny, _ := data.GetVector(sel.ID)
				v2f = toF32(vecAny)
				vectorCache[sel.ID] = v2f
			}
			if v2f == nil {
				continue
			}

			d, err := h.distFunc(v1f, v2f)
			// Diversity Heuristic check: Loosen for SQ8 to allow more edges
			threshold := cand.Dist
			if h.config.SQ8Enabled {
				threshold *= 1.2 // Allow 20% closer neighbors before pruning
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

// Insert function moved to insertion_core.go

// ensureTrained function moved to quantization_integration.go
