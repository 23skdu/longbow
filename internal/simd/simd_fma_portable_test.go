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

// TestDotProductFMA_Basic runs the test dot product fma_basic operation.
func TestDotProductFMA_Basic(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	expected := refDotProduct(a, b) // expected = 70
	got, err := DotProductFMA(a, b)
	if err != nil {
		t.Errorf("DotProductFMA error: %v", err)
	}

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("DotProductFMA basic: got %v, want %v", got, expected)
	}
}

// TestDotProductFMA_768Dim runs the test dot product fma_768 dim operation.
func TestDotProductFMA_768Dim(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(768, 42)
	b := generateTestVector(768, 43)

	expected := refDotProduct(a, b)
	got, err := DotProductFMA(a, b)
	if err != nil {
		t.Errorf("DotProductFMA error: %v", err)
	}

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("DotProductFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// TestDotProductFMA_1536Dim runs the test dot product fma_1536 dim operation.
func TestDotProductFMA_1536Dim(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(1536, 44)
	b := generateTestVector(1536, 45)

	expected := refDotProduct(a, b)
	got, err := DotProductFMA(a, b)
	if err != nil {
		t.Errorf("DotProductFMA error: %v", err)
	}

	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("DotProductFMA 1536: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// TestDotProductFMA_EmptyVector runs the test dot product fma_empty vector operation.
func TestDotProductFMA_EmptyVector(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	got, _ := DotProductFMA([]float32{}, []float32{})
	if got != 0 {
		t.Errorf("DotProductFMA empty: got %v, want 0", got)
	}
}

// TestDotProductFMA_LengthMismatch runs the test dot product fma_length mismatch operation.
func TestDotProductFMA_LengthMismatch(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	_, err := DotProductFMA([]float32{1, 2, 3}, []float32{1, 2})
	if err == nil {
		t.Errorf("DotProductFMA mismatch: expected error, got nil")
	}
}

// =============================================================================
// EuclideanDistanceFMA Tests
// =============================================================================

// TestEuclideanDistanceFMA_Basic runs the test euclidean distance fma_basic operation.
func TestEuclideanDistanceFMA_Basic(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := []float32{0, 0, 0}
	b := []float32{3, 4, 0}

	expected := refEuclideanSq(a, b) // 25 (squared)
	got, err := EuclideanDistanceFMA(a, b)
	if err != nil {
		t.Errorf("EuclideanDistanceFMA error: %v", err)
	}

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("EuclideanDistanceFMA basic: got %v, want %v", got, expected)
	}
}

// TestEuclideanDistanceFMA_768Dim runs the test euclidean distance fma_768 dim operation.
func TestEuclideanDistanceFMA_768Dim(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(768, 50)
	b := generateTestVector(768, 51)

	expected := refEuclideanSq(a, b)
	got, err := EuclideanDistanceFMA(a, b)
	if err != nil {
		t.Errorf("EuclideanDistanceFMA error: %v", err)
	}

	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("EuclideanDistanceFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// TestEuclideanDistanceFMA_Identical runs the test euclidean distance fma_identical operation.
func TestEuclideanDistanceFMA_Identical(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(256, 60)
	got, _ := EuclideanDistanceFMA(a, a)

	if got != 0 {
		t.Errorf("EuclideanDistanceFMA identical: got %v, want 0", got)
	}
}

// =============================================================================
// CosineDistanceFMA Tests
// =============================================================================

// TestCosineDistanceFMA_Basic runs the test cosine distance fma_basic operation.
func TestCosineDistanceFMA_Basic(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}

	expected := refCosineDistance(a, b) // 1.0 (orthogonal)
	got, err := CosineDistanceFMA(a, b)
	if err != nil {
		t.Errorf("CosineDistanceFMA error: %v", err)
	}

	if math.Abs(float64(got-expected)) > 1e-5 {
		t.Errorf("CosineDistanceFMA orthogonal: got %v, want %v", got, expected)
	}
}

// TestCosineDistanceFMA_Identical runs the test cosine distance fma_identical operation.
func TestCosineDistanceFMA_Identical(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(384, 70)
	got, _ := CosineDistanceFMA(a, a)

	// Identical vectors should have cosine distance 0
	if math.Abs(float64(got)) > 1e-5 {
		t.Errorf("CosineDistanceFMA identical: got %v, want 0", got)
	}
}

// TestCosineDistanceFMA_768Dim runs the test cosine distance fma_768 dim operation.
func TestCosineDistanceFMA_768Dim(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := generateTestVector(768, 80)
	b := generateTestVector(768, 81)

	expected := refCosineDistance(a, b)
	got, err := CosineDistanceFMA(a, b)
	if err != nil {
		t.Errorf("CosineDistanceFMA error: %v", err)
	}

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("CosineDistanceFMA 768: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// TestCosineDistanceFMA_ZeroVector runs the test cosine distance fma_zero vector operation.
func TestCosineDistanceFMA_ZeroVector(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	a := make([]float32, 128)
	b := generateTestVector(128, 90)

	got, _ := CosineDistanceFMA(a, b)

	if got != 1.0 {
		t.Errorf("CosineDistanceFMA zero: got %v, want 1.0", got)
	}
}

// =============================================================================
// Non-aligned length tests (tail processing)
// =============================================================================

// TestFMA_NonAlignedLengths runs the test fma_non aligned lengths operation.
func TestFMA_NonAlignedLengths(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not available")
	}
	sizes := []int{1, 7, 15, 17, 31, 33, 63, 65, 100, 127, 129}

	for _, size := range sizes {
		a := generateTestVector(size, int64(size))
		b := generateTestVector(size, int64(size+1000))

		// Dot product
		expDot := refDotProduct(a, b)
		gotDot, _ := DotProductFMA(a, b)
		if math.Abs(float64(gotDot-expDot)) > 1e-4 {
			t.Errorf("DotProductFMA size=%d: got %v, want %v", size, gotDot, expDot)
		}

		// Euclidean
		expEuc := refEuclideanSq(a, b)
		gotEuc, _ := EuclideanDistanceFMA(a, b)
		if math.Abs(float64(gotEuc-expEuc)) > 1e-4 {
			t.Errorf("EuclideanDistanceFMA size=%d: got %v, want %v", size, gotEuc, expEuc)
		}

		// Cosine
		expCos := refCosineDistance(a, b)
		gotCos, _ := CosineDistanceFMA(a, b)
		if math.Abs(float64(gotCos-expCos)) > 1e-4 {
			t.Errorf("CosineDistanceFMA size=%d: got %v, want %v", size, gotCos, expCos)
		}
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkDotProductFMA_768 runs the benchmark dot product fma_768 operation.
func BenchmarkDotProductFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for b.Loop() {
		_, _ = DotProductFMA(a, v)
	}
}

// BenchmarkDotProductFMA_1536 runs the benchmark dot product fma_1536 operation.
func BenchmarkDotProductFMA_1536(b *testing.B) {
	a := generateTestVector(1536, 1)
	v := generateTestVector(1536, 2)
	b.ResetTimer()
	for b.Loop() {
		_, _ = DotProductFMA(a, v)
	}
}

// BenchmarkEuclideanDistanceFMA_768 runs the benchmark euclidean distance fma_768 operation.
func BenchmarkEuclideanDistanceFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for b.Loop() {
		_, _ = EuclideanDistanceFMA(a, v)
	}
}

// BenchmarkCosineDistanceFMA_768 runs the benchmark cosine distance fma_768 operation.
func BenchmarkCosineDistanceFMA_768(b *testing.B) {
	a := generateTestVector(768, 1)
	v := generateTestVector(768, 2)
	b.ResetTimer()
	for b.Loop() {
		_, _ = CosineDistanceFMA(a, v)
	}
}
