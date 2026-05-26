package index

// ensurePQTrained checks if PQ training is needed and performs it when sufficient data is accumulated.
func (h *ArrowHNSW) ensurePQTrained(extraSamples [][]float32) {
	if h.pqTrained.Load() || h.oopqEncoder != nil {
		return
	}

	h.initMu.Lock()
	if h.pqTrained.Load() || h.oopqEncoder != nil {
		h.initMu.Unlock()
		return
	}

	for _, v := range extraSamples {
		vecCopy := make([]float32, len(v))
		copy(vecCopy, v)
		h.pqTrainingBuffer = append(h.pqTrainingBuffer, vecCopy)
	}

	threshold := h.config.PQTrainingThreshold
	if threshold <= 0 {
		threshold = 5000
	}

	if len(h.pqTrainingBuffer) < threshold {
		h.initMu.Unlock()
		return
	}

	buffer := h.pqTrainingBuffer
	h.pqTrainingBuffer = nil
	h.initMu.Unlock()

	if err := h.TrainPQ(buffer); err != nil {
		h.pqTrained.Store(false)
		return
	}
	h.pqTrained.Store(true)
}
