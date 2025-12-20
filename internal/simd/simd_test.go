package simd

import (
	"fmt"
	"math"
	"testing"
)

// Reference implementations for correctness verification
func referenceEuclidean(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return float32(math.Sqrt(float64(sum)))
}

func referenceCosine(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0 // max distance for zero vectors
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

// === CPU Feature Detection Tests ===

func TestCPUFeatureDetection(t *testing.T) {
	features := GetCPUFeatures()

	// Should return valid struct
	if features.Vendor == "" && !features.HasNEON {
		t.Error("CPU vendor should not be empty")
	}

	t.Logf("CPU: %s, AVX2: %v, AVX512: %v, NEON: %v",
		features.Vendor, features.HasAVX2, features.HasAVX512, features.HasNEON)
}

func TestSelectedImplementation(t *testing.T) {
	impl := GetImplementation()
	validImpls := []string{"avx512", "avx2", "neon", "generic"}

	valid := false
	for _, v := range validImpls {
		if impl == v {
			valid = true
			break
		}
	}

	if !valid {
		t.Errorf("Invalid implementation: %s", impl)
	}
	t.Logf("Selected implementation: %s", impl)
}

// === Euclidean Distance Tests ===

func TestEuclideanDistance_Basic(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	expected := referenceEuclidean(a, b)
	result := EuclideanDistance(a, b)

	if !approxEqual(result, expected, 1e-5) {
		t.Errorf("EuclideanDistance(%v, %v) = %v, expected %v", a, b, result, expected)
	}
}

func TestEuclideanDistance_Identical(t *testing.T) {
	a := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	result := EuclideanDistance(a, a)

	if result != 0 {
		t.Errorf("Distance to self should be 0, got %v", result)
	}
}

func TestEuclideanDistance_Zeros(t *testing.T) {
	a := make([]float32, 128)
	b := make([]float32, 128)

	result := EuclideanDistance(a, b)

	if result != 0 {
		t.Errorf("Distance between zero vectors should be 0, got %v", result)
	}
}

func TestEuclideanDistance_VariousDimensions(t *testing.T) {
	dimensions := []int{1, 3, 7, 8, 15, 16, 31, 32, 64, 128, 256, 384, 512, 768, 1024, 1536}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 2.0)

			expected := referenceEuclidean(a, b)
			result := EuclideanDistance(a, b)

			if !approxEqual(result, expected, 1e-3) {
				t.Errorf("dim=%d: got %v, expected %v", dim, result, expected)
			}
		})
	}
}

func TestEuclideanDistance_LengthMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for mismatched lengths")
		}
	}()

	a := []float32{1, 2, 3}
	b := []float32{1, 2}
	EuclideanDistance(a, b)
}

// === Cosine Similarity Tests ===

func TestCosineDistance_Basic(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	expected := referenceCosine(a, b)
	result := CosineDistance(a, b)

	if !approxEqual(result, expected, 1e-5) {
		t.Errorf("CosineDistance(%v, %v) = %v, expected %v", a, b, result, expected)
	}
}

func TestCosineDistance_Identical(t *testing.T) {
	a := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	result := CosineDistance(a, a)

	if !approxEqual(result, 0, 1e-5) {
		t.Errorf("Cosine distance to self should be 0, got %v", result)
	}
}

func TestCosineDistance_Orthogonal(t *testing.T) {
	a := []float32{1, 0, 0, 0}
	b := []float32{0, 1, 0, 0}

	result := CosineDistance(a, b)

	// Orthogonal vectors have cosine similarity 0, distance 1
	if !approxEqual(result, 1.0, 1e-5) {
		t.Errorf("Orthogonal vectors should have distance 1, got %v", result)
	}
}

func TestCosineDistance_Opposite(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{-1, -2, -3, -4}

	result := CosineDistance(a, b)

	// Opposite vectors have cosine similarity -1, distance 2
	if !approxEqual(result, 2.0, 1e-5) {
		t.Errorf("Opposite vectors should have distance 2, got %v", result)
	}
}

func TestCosineDistance_ZeroVector(t *testing.T) {
	a := []float32{0, 0, 0, 0}
	b := []float32{1, 2, 3, 4}

	result := CosineDistance(a, b)

	// Zero vector should return max distance
	if result != 1.0 {
		t.Errorf("Zero vector should have distance 1, got %v", result)
	}
}

func TestCosineDistance_VariousDimensions(t *testing.T) {
	dimensions := []int{1, 3, 7, 8, 15, 16, 31, 32, 64, 128, 256, 384, 512, 768, 1024, 1536}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 2.0)

			expected := referenceCosine(a, b)
			result := CosineDistance(a, b)

			if !approxEqual(result, expected, 1e-3) {
				t.Errorf("dim=%d: got %v, expected %v", dim, result, expected)
			}
		})
	}
}

func TestCosineDistance_LengthMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for mismatched lengths")
		}
	}()

	a := []float32{1, 2, 3}
	b := []float32{1, 2}
	CosineDistance(a, b)
}

// === Dot Product Tests ===

func TestDotProduct_Basic(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	// 1*5 + 2*6 + 3*7 + 4*8 = 5 + 12 + 21 + 32 = 70
	expected := float32(70)
	result := DotProduct(a, b)

	if !approxEqual(result, expected, 1e-5) {
		t.Errorf("DotProduct(%v, %v) = %v, expected %v", a, b, result, expected)
	}
}

func TestDotProduct_VariousDimensions(t *testing.T) {
	dimensions := []int{1, 7, 8, 15, 16, 31, 32, 64, 128, 256, 512, 1024}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 1.0)

			// Reference dot product
			var expected float32
			for i := range a {
				expected += a[i] * b[i]
			}

			result := DotProduct(a, b)

			if !approxEqual(result, expected, 1e-3) {
				t.Errorf("dim=%d: got %v, expected %v", dim, result, expected)
			}
		})
	}
}

// === Benchmarks ===

func BenchmarkEuclideanDistance_128(b *testing.B) {
	v1 := makeTestVector(128, 1.0)
	v2 := makeTestVector(128, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(v1, v2)
	}
}

func BenchmarkEuclideanDistance_384(b *testing.B) {
	v1 := makeTestVector(384, 1.0)
	v2 := makeTestVector(384, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(v1, v2)
	}
}

func BenchmarkEuclideanDistance_768(b *testing.B) {
	v1 := makeTestVector(768, 1.0)
	v2 := makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(v1, v2)
	}
}

func BenchmarkEuclideanDistance_1536(b *testing.B) {
	v1 := makeTestVector(1536, 1.0)
	v2 := makeTestVector(1536, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(v1, v2)
	}
}

func BenchmarkCosineDistance_128(b *testing.B) {
	v1 := makeTestVector(128, 1.0)
	v2 := makeTestVector(128, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineDistance(v1, v2)
	}
}

func BenchmarkCosineDistance_384(b *testing.B) {
	v1 := makeTestVector(384, 1.0)
	v2 := makeTestVector(384, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineDistance(v1, v2)
	}
}

func BenchmarkCosineDistance_768(b *testing.B) {
	v1 := makeTestVector(768, 1.0)
	v2 := makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineDistance(v1, v2)
	}
}

func BenchmarkCosineDistance_1536(b *testing.B) {
	v1 := makeTestVector(1536, 1.0)
	v2 := makeTestVector(1536, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineDistance(v1, v2)
	}
}

func BenchmarkDotProduct_1536(b *testing.B) {
	v1 := makeTestVector(1536, 1.0)
	v2 := makeTestVector(1536, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProduct(v1, v2)
	}
}

// Compare SIMD vs Generic
func BenchmarkEuclidean_Generic_768(b *testing.B) {
	v1 := makeTestVector(768, 1.0)
	v2 := makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		euclideanGeneric(v1, v2)
	}
}

func BenchmarkCosine_Generic_768(b *testing.B) {
	v1 := makeTestVector(768, 1.0)
	v2 := makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cosineGeneric(v1, v2)
	}
}

// === Helpers ===

func makeTestVector(dim int, seed float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed * float32(i+1) * 0.1
	}
	return v
}

func approxEqual(a, b, relTol float32) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	// Use relative tolerance for large values
	maxVal := a
	if b > a {
		maxVal = b
	}
	if maxVal < 0 {
		maxVal = -maxVal
	}
	if maxVal < 1 {
		maxVal = 1
	}
	return diff < relTol*maxVal
}
