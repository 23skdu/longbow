package store

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


