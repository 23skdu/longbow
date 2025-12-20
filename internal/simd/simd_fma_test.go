//go:build amd64

package simd

import (
	"math"
	"math/rand"
	"testing"
	"unsafe"
)

// =============================================================================
// TDD Tests for avo-generated FMA-optimized SIMD kernels
// These tests verify correctness and performance of assembly implementations
// =============================================================================

// Test helper: generate random float32 slice
func generateRandomVectorFMA(n int, seed int64) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, n)
	for i := range v {
		v[i] = r.Float32()*2 - 1 // [-1, 1]
	}
	return v
}

// Reference implementations for verification
func referenceDotProduct(a, b []float32) float32 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum)
}

func referenceEuclidean(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

func referenceCosine(a, b []float32) float32 {
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
// Tests for 32-element FMA kernels (processes 32 floats = 2 ZMM registers)
// =============================================================================

func TestDot32FMA_Correctness(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(32, 42)
	b := generateRandomVectorFMA(32, 43)

	expected := referenceDotProduct(a, b)
	got := dot32FMA(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])))

	// Allow small epsilon for FMA vs separate mul+add
	epsilon := float32(1e-5)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("dot32FMA mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestEuclidean32FMA_Correctness(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(32, 44)
	b := generateRandomVectorFMA(32, 45)

	expected := referenceEuclidean(a, b)
	// Get squared sum, then sqrt in Go
	sumSq := euclidean32FMA(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])))
	got := float32(math.Sqrt(float64(sumSq)))

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("euclidean32FMA mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestCosine32FMA_Correctness(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(32, 46)
	b := generateRandomVectorFMA(32, 47)

	expected := referenceCosine(a, b)
	dot, normA, normB := cosine32FMA(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])))

	var got float32 = 1.0
	if normA > 0 && normB > 0 {
		got = 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
	}

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("cosine32FMA mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// =============================================================================
// Tests for 64-element FMA kernels (processes 64 floats = 4 ZMM registers)
// =============================================================================

func TestDot64FMA_Correctness(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(64, 48)
	b := generateRandomVectorFMA(64, 49)

	expected := referenceDotProduct(a, b)
	got := dot64FMA(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])))

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("dot64FMA mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestEuclidean64FMA_Correctness(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(64, 50)
	b := generateRandomVectorFMA(64, 51)

	expected := referenceEuclidean(a, b)
	sumSq := euclidean64FMA(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])))
	got := float32(math.Sqrt(float64(sumSq)))

	epsilon := float32(1e-4)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("euclidean64FMA mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// =============================================================================
// Integration tests: Full-length vectors using FMA dispatch
// =============================================================================

func TestDotProductFMA_FullVector768(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	// 768 dimensions = common embedding size (OpenAI, etc.)
	a := generateRandomVectorFMA(768, 52)
	b := generateRandomVectorFMA(768, 53)

	expected := referenceDotProduct(a, b)
	got := DotProductFMA(a, b)

	// Larger vectors accumulate more error
	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("DotProductFMA(768) mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestEuclideanDistanceFMA_FullVector1536(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	// 1536 dimensions = common embedding size (OpenAI ada-002)
	a := generateRandomVectorFMA(1536, 54)
	b := generateRandomVectorFMA(1536, 55)

	expected := referenceEuclidean(a, b)
	got := EuclideanDistanceFMA(a, b)

	epsilon := float32(1e-2)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("EuclideanDistanceFMA(1536) mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

func TestCosineDistanceFMA_FullVector768(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	a := generateRandomVectorFMA(768, 56)
	b := generateRandomVectorFMA(768, 57)

	expected := referenceCosine(a, b)
	got := CosineDistanceFMA(a, b)

	epsilon := float32(1e-3)
	if diff := float32(math.Abs(float64(got - expected))); diff > epsilon {
		t.Errorf("CosineDistanceFMA(768) mismatch: got %v, want %v (diff %v)", got, expected, diff)
	}
}

// =============================================================================
// Benchmarks: Compare FMA vs existing implementations
// =============================================================================

func BenchmarkDotProduct_FMA_768(b *testing.B) {
	if !features.HasAVX512 {
		b.Skip("AVX-512 not available")
	}

	v1 := generateRandomVectorFMA(768, 100)
	v2 := generateRandomVectorFMA(768, 101)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = DotProductFMA(v1, v2)
	}
}

func BenchmarkDotProduct_Current_768(b *testing.B) {
	v1 := generateRandomVectorFMA(768, 100)
	v2 := generateRandomVectorFMA(768, 101)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = DotProduct(v1, v2)
	}
}

func BenchmarkEuclidean_FMA_1536(b *testing.B) {
	if !features.HasAVX512 {
		b.Skip("AVX-512 not available")
	}

	v1 := generateRandomVectorFMA(1536, 102)
	v2 := generateRandomVectorFMA(1536, 103)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistanceFMA(v1, v2)
	}
}

func BenchmarkEuclidean_Current_1536(b *testing.B) {
	v1 := generateRandomVectorFMA(1536, 102)
	v2 := generateRandomVectorFMA(1536, 103)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistance(v1, v2)
	}
}

func BenchmarkCosine_FMA_768(b *testing.B) {
	if !features.HasAVX512 {
		b.Skip("AVX-512 not available")
	}

	v1 := generateRandomVectorFMA(768, 104)
	v2 := generateRandomVectorFMA(768, 105)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = CosineDistanceFMA(v1, v2)
	}
}

func BenchmarkCosine_Current_768(b *testing.B) {
	v1 := generateRandomVectorFMA(768, 104)
	v2 := generateRandomVectorFMA(768, 105)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = CosineDistance(v1, v2)
	}
}

// =============================================================================
// Edge case tests
// =============================================================================

func TestFMA_ZeroVector(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	zeros := make([]float32, 64)
	ones := generateRandomVectorFMA(64, 60)

	// Dot product with zero vector should be 0
	dot := DotProductFMA(zeros, ones)
	if dot != 0 {
		t.Errorf("dot product with zero vector: got %v, want 0", dot)
	}

	// Cosine with zero vector should be 1.0 (max distance)
	cos := CosineDistanceFMA(zeros, ones)
	if cos != 1.0 {
		t.Errorf("cosine distance with zero vector: got %v, want 1.0", cos)
	}
}

func TestFMA_IdenticalVectors(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	v := generateRandomVectorFMA(128, 61)

	// Euclidean distance to self should be 0
	euc := EuclideanDistanceFMA(v, v)
	if euc != 0 {
		t.Errorf("euclidean distance to self: got %v, want 0", euc)
	}

	// Cosine distance to self should be 0 (identical = similarity 1.0)
	cos := CosineDistanceFMA(v, v)
	if cos > 1e-6 {
		t.Errorf("cosine distance to self: got %v, want ~0", cos)
	}
}

func TestFMA_NonAlignedLength(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX-512 not available")
	}

	// Test non-power-of-2 lengths that require remainder handling
	for _, size := range []int{33, 65, 100, 127, 257, 500, 1000} {
		a := generateRandomVectorFMA(size, int64(size))
		b := generateRandomVectorFMA(size, int64(size+1))

		expected := referenceDotProduct(a, b)
		got := DotProductFMA(a, b)

		epsilon := float32(1e-3)
		if diff := float32(math.Abs(float64(got - expected))); diff > epsilon*float32(size)/100 {
			t.Errorf("DotProductFMA(%d) mismatch: got %v, want %v (diff %v)", size, got, expected, diff)
		}
	}
}
