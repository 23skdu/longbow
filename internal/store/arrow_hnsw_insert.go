package store

import (
	"context"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *GraphData) ([]Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, false)
	_, err := h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, nil)
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
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
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
		selected = ctx.scratchSelected[:0]
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

// ensureTrained checks if SQ8 training is needed and performs it if sufficient data is accumulated.
// limitID specifies the max ID to backfill (inclusive).
func (h *ArrowHNSW) ensureTrained(limitID int, extraSamples [][]float32) {
	if h.sq8Ready.Load() {
		return
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	if h.sq8Ready.Load() {
		return
	}

	dims := int(h.dims.Load())
	if h.quantizer == nil {
		h.quantizer = NewScalarQuantizer(dims)
	}

	if h.quantizer.IsTrained() {
		h.sq8Ready.Store(true)
		return
	}

	// Buffer vectors for training
	for _, v := range extraSamples {
		vecCopy := make([]float32, len(v))
		copy(vecCopy, v)
		h.sq8TrainingBuffer = append(h.sq8TrainingBuffer, vecCopy)
	}

	// Check if threshold reached
	threshold := h.config.SQ8TrainingThreshold
	if threshold <= 0 {
		threshold = 1000
	}

	if len(h.sq8TrainingBuffer) >= threshold {
		// Train!
		h.quantizer.Train(h.sq8TrainingBuffer)

		// Backfill existing vectors
		if limitID >= 0 {
			currentData := h.data.Load()
			for i := uint32(0); i <= uint32(limitID); i++ {
				cID := chunkID(i)
				cOff := chunkOffset(i)

				vecChunk := currentData.GetVectorsChunk(cID)
				if vecChunk == nil {
					continue
				}

				f32Stride := currentData.GetPaddedDims()
				sq8Stride := (dims + 63) & ^63

				f32Off := int(cOff) * f32Stride
				if f32Off+dims > len(vecChunk) {
					continue
				}
				srcVec := vecChunk[f32Off : f32Off+dims]

				// Encode to SQ8 chunk
				sq8Chunk := currentData.GetVectorsSQ8Chunk(cID)
				if sq8Chunk != nil {
					sq8Off := int(cOff) * sq8Stride
					if sq8Off+dims <= len(sq8Chunk) {
						dest := sq8Chunk[sq8Off : sq8Off+dims]
						h.quantizer.Encode(srcVec, dest)
					}
				}
			}
		}

		// Clear buffer
		h.sq8TrainingBuffer = nil
		h.sq8Ready.Store(true)
	}
}
