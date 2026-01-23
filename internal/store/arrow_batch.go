package store

import (
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
)

// BatchDistanceCompute computes distances for multiple query-candidate pairs.
// This enables better cache utilization and SIMD batching optimizations.
// Uses Prometheus metrics to track performance characteristics.
func BatchDistanceCompute(queries, candidates [][]float32, results []float32) {
	if len(queries) != len(candidates) || len(queries) != len(results) {
		panic("hnsw2: batch distance length mismatch")
	}

	n := len(queries)
	if n == 0 {
		return
	}

	metrics.BatchDistanceComputeTotal.Inc()
	metrics.BatchDistanceComputePairsTotal.Add(float64(n))

	dim := len(queries[0])
	isFixedDim := dim == 128 || dim == 384

	if isFixedDim && n >= 4 {
		metrics.BatchDistanceComputeSIMDUsed.Inc()
		for i := range queries {
			if len(queries[i]) != dim || len(candidates[i]) != dim {
				results[i] = 0
				continue
			}
			d, _ := simd.EuclideanDistance(queries[i], candidates[i])
			results[i] = d
		}
		return
	}

	metrics.BatchDistanceComputeFallbackTotal.Inc()
	for i := range queries {
		results[i] = distanceSIMD(queries[i], candidates[i])
	}
}
