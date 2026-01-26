package store

// Distance function resolvers extracted from arrow_hnsw_index.go

/*
// resolveDistanceFunc returns the appropriate distance function for float32 vectors.
func (h *ArrowHNSW) resolveDistanceFunc() func(a, b []float32) (float32, error) {
	switch h.config.Metric {
	case MetricCosine:
		return simd.CosineDistance
	case MetricDotProduct:
		// For HNSW minimization, negate Dot Product
		return func(a, b []float32) (float32, error) {
			d, err := simd.DotProduct(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistance
	}
}

// resolveDistanceFuncF16 returns the FP16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF16() func(a, b []float16.Num) (float32, error) {
	switch h.config.Metric {
	case MetricCosine:
		return simd.CosineDistanceF16
	case MetricDotProduct:
		return func(a, b []float16.Num) (float32, error) {
			d, err := simd.DotProductF16(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceF16
	}
}

// resolveDistanceFuncF64 returns the Float64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF64() func(a, b []float64) (float32, error) {
	switch h.config.Metric {
	case MetricCosine:
		return func(a, b []float64) (float32, error) {
			// Fallback since we don't have CosineDistanceF64 in simd yet
			return simd.CosineDistance(simd.ToFloat32(a), simd.ToFloat32(b))
		}
	case MetricDotProduct:
		return func(a, b []float64) (float32, error) {
			d, err := simd.DotProductF64(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceFloat64
	}
}

// resolveDistanceFuncC64 returns the Complex64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC64() func(a, b []complex64) (float32, error) {
	return simd.EuclideanDistanceComplex64
}
*/

/*
// resolveDistanceFuncC128 returns the Complex128 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC128() func(a, b []complex128) (float32, error) {
	return simd.EuclideanDistanceComplex128
}
*/

/*
// resolveBatchDistanceFunc returns the batch distance function.
func (h *ArrowHNSW) resolveBatchDistanceFunc() func(query []float32, vectors [][]float32, results []float32) error {
	switch h.config.Metric {
	case MetricCosine:
		return simd.CosineDistanceBatch
	case MetricDotProduct:
		return func(query []float32, vectors [][]float32, results []float32) error {
			err := simd.DotProductBatch(query, vectors, results)
			if err != nil {
				return err
			}
			for i := range results {
				results[i] = -results[i]
			}
			return nil
		}
	default:
		return simd.EuclideanDistanceBatch
	}
}
*/
