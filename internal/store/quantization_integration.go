package store

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
