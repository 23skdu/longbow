package store

// getAdaptiveEf returns the efConstruction value to use for the current insertion
// based on the number of nodes already in the graph.
// When AdaptiveEf is enabled, this implements a linear ramp from AdaptiveEfMin
// to full EfConstruction as the graph grows.
func (h *ArrowHNSW) getAdaptiveEf(nodeCount int) int {
	baseEf := h.efConstruction
	
	// If adaptive ef is disabled, return base value
	if !h.config.AdaptiveEf {
		return baseEf
	}
	
	// Determine minimum ef (default: baseEf / 4)
	minEf := h.config.AdaptiveEfMin
	if minEf == 0 {
		minEf = baseEf / 4
		if minEf < 50 {
			minEf = 50 // Absolute minimum for quality
		}
	}
	
	// Determine threshold (default: InitialCapacity / 2)
	threshold := h.config.AdaptiveEfThreshold
	if threshold == 0 {
		threshold = h.config.InitialCapacity / 2
		if threshold < 1000 {
			threshold = 1000 // Minimum threshold
		}
	}
	
	// Linear ramp: minEf -> baseEf over [0, threshold]
	if nodeCount < threshold {
		progress := float64(nodeCount) / float64(threshold)
		currentEf := minEf + int(progress*float64(baseEf-minEf))
		return currentEf
	}
	
	// Past threshold, use full efConstruction
	return baseEf
}
