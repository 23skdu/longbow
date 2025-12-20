package simd

import (
	"math"
	"math/rand"
	"testing"
)

// =============================================================================
// Cross-platform TDD Tests for FMA-optimized SIMD kernels
// Tests the public API (DotProductFMA, EuclideanDistanceFMA, CosineDistanceFMA)
// =============================================================================

func generateTestVector(n int, seed int64) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, n)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

func refDotProduct(a, b []float32) float32 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum)
}

func refEuclideanSq(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(sum)
}

func refCosineDistance(a, b []float32) float32 {
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	return float32(1.0 - dot/math.Sqrt(normA*normB))
}

// =============================================================================
// DotProductFMA Tests
// =============================================================================

func TestDotProductFMA_Basic(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	expected := refDotProduct(a, b) // expected = 70
	got := DotProductFMA(a, b)

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("DotProductFMA basic: got %v, want %v", got, expected)
	}
}

func TestDotProductFMA_768Dim(t *testing.T) {
	a := generateTestVector(768, 42)
	b := generateTestVector(768, 43)

	expected := refDotProduct(a, b)
	got := DotProductFMA(a, b)

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("DotProductFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestDotProductFMA_1536Dim(t *testing.T) {
	a := generateTestVector(1536, 44)
	b := generateTestVector(1536, 45)

	expected := refDotProduct(a, b)
	got := DotProductFMA(a, b)

	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("DotProductFMA 1536: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestDotProductFMA_EmptyVector(t *testing.T) {
	got := DotProductFMA([]float32{}, []float32{})
	if got != 0 {
		t.Errorf("DotProductFMA empty: got %v, want 0", got)
	}
}

func TestDotProductFMA_LengthMismatch(t *testing.T) {
	got := DotProductFMA([]float32{1, 2, 3}, []float32{1, 2})
	if got != 0 {
		t.Errorf("DotProductFMA mismatch: got %v, want 0", got)
	}
}

// =============================================================================
// EuclideanDistanceFMA Tests
// =============================================================================

func TestEuclideanDistanceFMA_Basic(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{3, 4, 0}

	expected := refEuclideanSq(a, b) // 25 (squared)
	got := EuclideanDistanceFMA(a, b)

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("EuclideanDistanceFMA basic: got %v, want %v", got, expected)
	}
}

func TestEuclideanDistanceFMA_768Dim(t *testing.T) {
	a := generateTestVector(768, 50)
	b := generateTestVector(768, 51)

	expected := refEuclideanSq(a, b)
	got := EuclideanDistanceFMA(a, b)

	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("EuclideanDistanceFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestEuclideanDistanceFMA_Identical(t *testing.T) {
	a := generateTestVector(256, 60)
	got := EuclideanDistanceFMA(a, a)

	if got != 0 {
		t.Errorf("EuclideanDistanceFMA identical: got %v, want 0", got)
	}
}

// =============================================================================
// CosineDistanceFMA Tests
// =============================================================================

func TestCosineDistanceFMA_Basic(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}

	expected := refCosineDistance(a, b) // 1.0 (orthogonal)
	got := CosineDistanceFMA(a, b)

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("CosineDistanceFMA orthogonal: got %v, want %v", got, expected)
	}
}

func TestCosineDistanceFMA_Identical(t *testing.T) {
	a := generateTestVector(384, 70)
	got := CosineDistanceFMA(a, a)

	// Identical vectors should have cosine distance 0
	if math.Abs(float64(got)) > 1e-5 {
		t.Errorf("CosineDistanceFMA identical: got %v, want 0", got)
	}
}

func TestCosineDistanceFMA_768Dim(t *testing.T) {
	a := generateTestVector(768, 80)
	b := generateTestVector(768, 81)

	expected := refCosineDistance(a, b)
	got := CosineDistanceFMA(a, b)

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("CosineDistanceFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestCosineDistanceFMA_ZeroVector(t *testing.T) {
	a := make([]float32, 128)
	b := generateTestVector(128, 90)

	got := CosineDistanceFMA(a, b)

	if got != 1.0 {
		t.Errorf("CosineDistanceFMA zero: got %v, want 1.0", got)
	}
}

// =============================================================================
// Non-aligned length tests (tail processing)
// =============================================================================

func TestFMA_NonAlignedLengths(t *testing.T) {
	sizes := []int{1, 7, 15, 17, 31, 33, 63, 65, 100, 127, 129}

	for _, size := range sizes {
		a := generateTestVector(size, int64(size))
		b := generateTestVector(size, int64(size+1000))

		// Dot product
		expDot := refDotProduct(a, b)
		gotDot := DotProductFMA(a, b)
		if math.Abs(float64(gotDot-expDot)) > 1e-4 {
			t.Errorf("DotProductFMA size=%d: got %v, want %v", size, gotDot, expDot)
		}

		// Euclidean
		expEuc := refEuclideanSq(a, b)
		gotEuc := EuclideanDistanceFMA(a, b)
		if math.Abs(float64(gotEuc-expEuc)) > 1e-4 {
			t.Errorf("EuclideanDistanceFMA size=%d: got %v, want %v", size, gotEuc, expEuc)
		}

		// Cosine
		expCos := refCosineDistance(a, b)
		gotCos := CosineDistanceFMA(a, b)
		if math.Abs(float64(gotCos-expCos)) > 1e-4 {
			t.Errorf("CosineDistanceFMA size=%d: got %v, want %v", size, gotCos, expCos)
		}
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkDotProductFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DotProductFMA(a, v)
	}
}

func BenchmarkDotProductFMA_1536(b *testing.B) {
	a := generateTestVector(1536, 1)
	v := generateTestVector(1536, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DotProductFMA(a, v)
	}
}

func BenchmarkEuclideanDistanceFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistanceFMA(a, v)
	}
}

func BenchmarkCosineDistanceFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CosineDistanceFMA(a, v)
	}
}
