package simd

import (
	"math"
	"testing"
)

func TestMetricsCorrectness(t *testing.T) {
	// Matrix of types and expected results for a simple case
	// Vector A: [1, 2], Vector B: [2, 1]
	// Euclidean: sqrt((1-2)^2 + (2-1)^2) = sqrt(2) = 1.414...
	// Dot Product: 1*2 + 2*1 = 4
	// Cosine Similarity: 4 / (sqrt(1^2+2^2) * sqrt(2^2+1^2)) = 4 / (sqrt(5)*sqrt(5)) = 4/5 = 0.8
	// Cosine Distance: 1 - 0.8 = 0.2

	expectedEuclidean := float32(math.Sqrt(2))
	expectedDot := float32(4)
	expectedCosine := float32(0.2)

	t.Run("Float32", func(t *testing.T) {
		a, b := []float32{1, 2}, []float32{2, 1}
		d, _ := EuclideanDistance(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProduct(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistance(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})

	t.Run("Int8", func(t *testing.T) {
		a, b := []int8{1, 2}, []int8{2, 1}
		d, _ := EuclideanDistanceInt8(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProductInt8(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistanceInt8(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})

	t.Run("Uint8", func(t *testing.T) {
		a, b := []uint8{1, 2}, []uint8{2, 1}
		d, _ := EuclideanDistanceUint8(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProductUint8(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistanceUint8(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})

	t.Run("Int16", func(t *testing.T) {
		a, b := []int16{1, 2}, []int16{2, 1}
		d, _ := EuclideanDistanceInt16(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProductInt16(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistanceInt16(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})

	t.Run("Float64", func(t *testing.T) {
		a, b := []float64{1, 2}, []float64{2, 1}
		d, _ := EuclideanDistanceFloat64(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProductF64(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistanceFloat64(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})

	t.Run("Complex64", func(t *testing.T) {
		// Complex vectors: A=[1+0i, 2+0i], B=[2+0i, 1+0i]
		// Should match float test for real only
		a, b := []complex64{1, 2}, []complex64{2, 1}
		d, _ := EuclideanDistanceComplex64(a, b)
		if math.Abs(float64(d-expectedEuclidean)) > 1e-6 { t.Errorf("Euclidean mismatch: %v", d) }
		d, _ = DotProductComplex64(a, b)
		if d != expectedDot { t.Errorf("Dot mismatch: %v", d) }
		d, _ = CosineDistanceComplex64(a, b)
		if math.Abs(float64(d-expectedCosine)) > 1e-6 { t.Errorf("Cosine mismatch: %v", d) }
	})
}
