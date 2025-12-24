package hnsw2

// BatchDistanceCompute computes distances for multiple query-candidate pairs.
// This enables better cache utilization and potential SIMD batching.
func BatchDistanceCompute(queries [][]float32, candidates [][]float32, results []float32) {
	if len(queries) != len(candidates) || len(queries) != len(results) {
		panic("hnsw2: batch distance length mismatch")
	}
	
	// For now, simple loop - can be optimized with SIMD batching later
	for i := range queries {
		results[i] = distanceSIMD(queries[i], candidates[i])
	}
}

// prefetchNode hints to the CPU to prefetch a node's data.
// This can reduce cache misses during graph traversal.
func (h *ArrowHNSW) prefetchNode(id uint32) {
	data := h.data.Load()
	if data == nil || int(id) >= data.Capacity {
		return
	}
	// Touch level to hint cache
	_ = data.Levels[id]
}
