package store

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/float16"
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

	// Fast-path for Float32 to lift type declarations out of nested loops
	// types.VectorTypeFloat32 is the design DataType setup.
	// Setup constants to keep speedups or just use types.VectorDataType(1)?
	// Let's check with types.VectorTypeFloat32
	if data.Type == 1 { // VectorTypeFloat32 is usually 1, or just do reflection assert once
		return h.selectNeighborsFloat32(ctx, candidates, m, data)
	}

	selected := make([]Candidate, 0, m)

	// HNSW "Heuristic 2" (Diversity Heuristic)
	vectorCache := make(map[uint32]any, len(candidates))

	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1, ok := vectorCache[cand.ID]
		if !ok {
			vecAny, _ := data.GetVector(cand.ID)
			v1 = vecAny
			// Cast integer types to float32 once when fetched into bypass double-loop builds
			if vInt, ok := vecAny.([]int32); ok {
				v1f := make([]float32, len(vInt))
				for i, val := range vInt { v1f[i] = float32(val) }
				v1 = v1f
			} else if vUint, ok := vecAny.([]uint32); ok {
				v1f := make([]float32, len(vUint))
				for i, val := range vUint { v1f[i] = float32(val) }
				v1 = v1f
			}
			vectorCache[cand.ID] = v1
		}
		if v1 == nil {
			continue
		}

		for _, sel := range selected {
			v2, ok := vectorCache[sel.ID]
			if !ok {
				vecAny, _ := data.GetVector(sel.ID)
				v2 = vecAny
				vectorCache[sel.ID] = v2
			}
			if v2 == nil {
				continue
			}

			var d float32
			var err error

			switch v1Typed := v1.(type) {
			case []float32:
				if v2f, ok := v2.([]float32); ok {
					d, err = h.distFunc(v1Typed, v2f)
				}
			case []float64:
				if v2Typed, ok := v2.([]float64); ok {
					d, err = h.distFuncF64(v1Typed, v2Typed)
				}
			case []float16.Num:
				if v2Typed, ok := v2.([]float16.Num); ok {
					d, err = h.distFuncF16(v1Typed, v2Typed)
				}
			case []complex64:
				if v2Typed, ok := v2.([]complex64); ok {
					d, err = h.distFuncC64(v1Typed, v2Typed)
				}
			case []complex128:
				if v2Typed, ok := v2.([]complex128); ok {
					var df64 float64
					df64, err = h.distFuncC128(v1Typed, v2Typed)
					d = float32(df64)
				}
			case []int8:
				// Fallback to float32
				v1f := make([]float32, len(v1Typed))
				for i, val := range v1Typed {
					v1f[i] = float32(val)
				}
				if v2Typed, ok := v2.([]int8); ok {
					v2f := make([]float32, len(v2Typed))
					for i, val := range v2Typed {
						v2f[i] = float32(val)
					}
					d, err = h.distFunc(v1f, v2f)
				}
			case []uint8:
				// Fallback to SQ8 explicitly if enabled handled elsewhere, but default to float32
				v1f := make([]float32, len(v1Typed))
				for i, val := range v1Typed {
					v1f[i] = float32(val)
				}
				if v2Typed, ok := v2.([]uint8); ok {
					v2f := make([]float32, len(v2Typed))
					for i, val := range v2Typed {
						v2f[i] = float32(val)
					}
					d, err = h.distFunc(v1f, v2f)
				}
			}

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

// selectNeighborsFloat32 is a specialized high-performance diversity check for float32 vectors.
func (h *ArrowHNSW) selectNeighborsFloat32(ctx *ArrowSearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
	// Optimization: use context scratch buffer to avoid allocations in critical path
	// but ensure isolation by using a local slice if ctx is nil (should not happen in bulk path).
	var selected []Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]Candidate, 0, m)
	}

	vectorCache := make(map[uint32][]float32, len(candidates))

	for _, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1, ok := vectorCache[cand.ID]
		if !ok {
			vecAny, _ := data.GetVector(cand.ID)
			if v, ok := vecAny.([]float32); ok {
				v1 = v
				vectorCache[cand.ID] = v
			}
		}
		if v1 == nil {
			continue
		}

		for _, sel := range selected {
			v2, ok := vectorCache[sel.ID]
			if !ok {
				vecAny, _ := data.GetVector(sel.ID)
				if v, ok := vecAny.([]float32); ok {
					v2 = v
					vectorCache[sel.ID] = v
				}
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
