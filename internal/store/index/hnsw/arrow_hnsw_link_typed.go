package hnsw

import (
	"math"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// selectNeighborsFloat64 is a specialized diversity check for float64 vectors using typed scratch slices.
func (h *ArrowHNSW) selectNeighborsFloat64(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	var extracted [][]float64
	if ctx != nil {
		if cap(ctx.scratchExtractedF64) >= len(candidates) {
			extracted = ctx.scratchExtractedF64[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([][]float64, len(candidates))
			ctx.scratchExtractedF64 = extracted
		}
	} else {
		extracted = make([][]float64, len(candidates))
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat64)
	for i, cand := range candidates {
		cID := int(cand.ID) / types.ChunkSize
		cOff := int(cand.ID) % types.ChunkSize
		var chunk []float64
		if maxGen == math.MaxUint64 {
			chunk = data.GetVectorsFloat64ChunkFast(cID)
		} else {
			chunk = data.GetVectorsFloat64ChunkWithGen(cID, maxGen)
		}
		if chunk != nil {
			start := cOff * pd
			if start+data.Dims <= len(chunk) {
				extracted[i] = chunk[start : start+data.Dims]
			}
		}
	}

	var selectedVecs [][]float64
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsF64[:0]
	} else {
		selectedVecs = make([][]float64, 0, m)
	}

	for i, cand := range candidates {
		if len(selected) >= m {
			break
		}
		v1 := extracted[i]
		if v1 == nil {
			continue
		}
		isDiverse := true
		if len(selectedVecs) >= 4 {
			var stackDists [64]float32
			var dists []float32
			if len(selectedVecs) <= len(stackDists) {
				dists = stackDists[:len(selectedVecs)]
			} else {
				dists = make([]float32, len(selectedVecs))
			}
			var err error
			switch h.config.Metric {
			case core.MetricCosine:
				err = simd.CosineDistanceFloat64Batch(v1, selectedVecs, dists)
			case core.MetricDotProduct:
				err = simd.DotProductFloat64Batch(v1, selectedVecs, dists)
				if err == nil {
					for j := range dists {
						dists[j] = -dists[j]
					}
				}
			default:
				err = simd.EuclideanDistanceFloat64Batch(v1, selectedVecs, dists)
			}
			if err == nil {
				for _, d := range dists {
					if d < cand.Dist {
						isDiverse = false
						break
					}
				}
			}
		} else if len(selectedVecs) > 0 {
			for j := range selectedVecs {
				var d float32
				var err error
				switch h.config.Metric {
				case core.MetricCosine:
					d, err = simd.CosineDistanceFloat64(v1, selectedVecs[j])
				case core.MetricDotProduct:
					d, err = simd.DotProductF64(v1, selectedVecs[j])
					d = -d
				default:
					d, err = simd.EuclideanDistanceFloat64(v1, selectedVecs[j])
				}
				if err != nil {
					continue
				}
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
		}
		if isDiverse {
			selected = append(selected, cand)
			selectedVecs = append(selectedVecs, v1)
		}
	}

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
		ctx.scratchExtractedF64 = extracted
		ctx.scratchSelectedVecsF64 = selectedVecs
	}
	return selected
}

// selectNeighborsComplex64 is a specialized diversity check for complex64 vectors using typed scratch slices.
func (h *ArrowHNSW) selectNeighborsComplex64(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	var extracted [][]complex64
	if ctx != nil {
		if cap(ctx.scratchExtractedC64) >= len(candidates) {
			extracted = ctx.scratchExtractedC64[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([][]complex64, len(candidates))
			ctx.scratchExtractedC64 = extracted
		}
	} else {
		extracted = make([][]complex64, len(candidates))
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}
	pd := data.GetPaddedDimsForType(types.VectorTypeComplex64)
	for i, cand := range candidates {
		cID := int(cand.ID) / types.ChunkSize
		cOff := int(cand.ID) % types.ChunkSize
		var chunk []complex64
		if maxGen == math.MaxUint64 {
			chunk = data.GetVectorsComplex64ChunkFast(cID)
		} else {
			chunk = data.GetVectorsComplex64ChunkWithGen(cID, maxGen)
		}
		if chunk != nil {
			start := cOff * pd
			if start+data.Dims <= len(chunk) {
				extracted[i] = chunk[start : start+data.Dims]
			}
		}
	}

	var selectedVecs [][]complex64
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsC64[:0]
	} else {
		selectedVecs = make([][]complex64, 0, m)
	}

	for i, cand := range candidates {
		if len(selected) >= m {
			break
		}
		v1 := extracted[i]
		if v1 == nil {
			continue
		}
		isDiverse := true
		if len(selectedVecs) > 0 {
			var stackDists [64]float32
			var dists []float32
			if len(selectedVecs) <= len(stackDists) {
				dists = stackDists[:len(selectedVecs)]
			} else if ctx != nil && cap(ctx.scratchDists) >= len(selectedVecs) {
				dists = ctx.scratchDists[:len(selectedVecs)]
			} else {
				dists = make([]float32, len(selectedVecs))
			}
			var f32Vecs [][]float32
			if ctx != nil {
				if cap(ctx.batchVecsFloat32) < len(selectedVecs) {
					ctx.batchVecsFloat32 = make([][]float32, len(selectedVecs))
				}
				f32Vecs = ctx.batchVecsFloat32[:len(selectedVecs)]
			}
			switch h.config.Metric {
			case core.MetricCosine:
				_ = simd.CosineDistanceComplex64Batch(v1, selectedVecs, dists, f32Vecs)
			case core.MetricDotProduct:
				_ = simd.DotProductComplex64Batch(v1, selectedVecs, dists, f32Vecs)
				for j := range dists {
					dists[j] = -dists[j]
				}
			default:
				_ = simd.EuclideanDistanceComplex64Batch(v1, selectedVecs, dists, f32Vecs)
			}
			for j := range selectedVecs {
				if dists[j] < cand.Dist {
					isDiverse = false
					break
				}
			}
		}
		if isDiverse {
			selected = append(selected, cand)
			selectedVecs = append(selectedVecs, v1)
		}
	}

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
		ctx.scratchExtractedC64 = extracted
		ctx.scratchSelectedVecsC64 = selectedVecs
	}
	return selected
}

// selectNeighborsComplex128 is a specialized diversity check for complex128 vectors using typed scratch slices.
func (h *ArrowHNSW) selectNeighborsComplex128(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	var extracted [][]complex128
	if ctx != nil {
		if cap(ctx.scratchExtractedC128) >= len(candidates) {
			extracted = ctx.scratchExtractedC128[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([][]complex128, len(candidates))
			ctx.scratchExtractedC128 = extracted
		}
	} else {
		extracted = make([][]complex128, len(candidates))
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}
	pd := data.GetPaddedDimsForType(types.VectorTypeComplex128)
	for i, cand := range candidates {
		cID := int(cand.ID) / types.ChunkSize
		cOff := int(cand.ID) % types.ChunkSize
		var chunk []complex128
		if maxGen == math.MaxUint64 {
			chunk = data.GetVectorsComplex128ChunkFast(cID)
		} else {
			chunk = data.GetVectorsComplex128ChunkWithGen(cID, maxGen)
		}
		if chunk != nil {
			start := cOff * pd
			if start+data.Dims <= len(chunk) {
				extracted[i] = chunk[start : start+data.Dims]
			}
		}
	}

	var selectedVecs [][]complex128
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsC128[:0]
	} else {
		selectedVecs = make([][]complex128, 0, m)
	}

	for i, cand := range candidates {
		if len(selected) >= m {
			break
		}
		v1 := extracted[i]
		if v1 == nil {
			continue
		}
		isDiverse := true
		if len(selectedVecs) > 0 {
			for j := range selectedVecs {
				var d float32
				var err error
				switch h.config.Metric {
				case core.MetricCosine:
					d, err = simd.CosineDistanceComplex128(v1, selectedVecs[j])
				case core.MetricDotProduct:
					d, err = simd.DotProductComplex128(v1, selectedVecs[j])
					d = -d
				default:
					d, err = simd.EuclideanDistanceComplex128(v1, selectedVecs[j])
				}
				if err != nil {
					continue
				}
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
		}
		if isDiverse {
			selected = append(selected, cand)
			selectedVecs = append(selectedVecs, v1)
		}
	}

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
		ctx.scratchExtractedC128 = extracted
		ctx.scratchSelectedVecsC128 = selectedVecs
	}
	return selected
}
