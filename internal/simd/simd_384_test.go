package simd

import (
	"math/rand"
	"testing"
)

func TestEuclidean384(t *testing.T) {
	dims := 384
	a := make([]float32, dims)
	b := make([]float32, dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < dims; i++ {
		a[i] = rng.Float32()
		b[i] = rng.Float32()
	}

	expected, err1 := euclideanGeneric(a, b)
	if err1 != nil {
		t.Errorf("euclideanGeneric error: %v", err1)
	}

	// Should route to specialized 384 kernel
	got, err2 := EuclideanDistance(a, b)
	if err2 != nil {
		t.Errorf("EuclideanDistance error: %v", err2)
	}

	// Check closeness
	diff := expected - got
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-4 {
		t.Errorf("Euclidean384 mismatch: expected %v, got %v", expected, got)
	}
}

func TestDot384(t *testing.T) {
	dims := 384
	a := make([]float32, dims)
	b := make([]float32, dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < dims; i++ {
		a[i] = rng.Float32()
		b[i] = rng.Float32()
	}

	expected, err1 := dotGeneric(a, b)
	if err1 != nil {
		t.Errorf("dotGeneric error: %v", err1)
	}

	// Should route to specialized 384 kernel
	got, err2 := DotProduct(a, b)
	if err2 != nil {
		t.Errorf("DotProduct error: %v", err2)
	}

	// Check closeness
	diff := expected - got
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-4 {
		t.Errorf("Dot384 mismatch: expected %v, got %v", expected, got)
	}
}

func BenchmarkEuclidean384(b *testing.B) {
	dims := 384
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistance(a, bb)
	}
}

// Compare against generic (force standard impl by using 385 dims or calling generic directly)
// We can't call generic directly from test package easily as it is unexported?
// Wait, simd.go specific implementations are unexported.
// generic is unexported.
// But we can benchmark length 385 to see the dropoff to generic/unrolled loop.

func BenchmarkEuclidean385_Generic(b *testing.B) {
	dims := 385 // 1 more than 384, should hit fallback
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(a, bb)
	}
}

func BenchmarkDot384(b *testing.B) {
	dims := 384
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DotProduct(a, bb)
	}
}

func BenchmarkDot385_Generic(b *testing.B) {
	dims := 385
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DotProduct(a, bb)
	}
}
