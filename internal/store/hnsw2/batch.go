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
	if int(id) >= len(h.nodes) {
		return
	}
	// Node data will be accessed soon, hint to CPU
	// The actual prefetch is handled by the CPU's hardware prefetcher
	// and Go's memory access patterns
	_ = h.nodes[id].ID
}
