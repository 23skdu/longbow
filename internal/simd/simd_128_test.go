package simd

import (
	"math/rand"
	"testing"
)

func TestEuclidean128(t *testing.T) {
	dims := 128
	a := make([]float32, dims)
	b := make([]float32, dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < dims; i++ {
		a[i] = rng.Float32()
		b[i] = rng.Float32()
	}

	expected := euclideanGeneric(a, b)

	// Should route to specialized 128 kernel
	got := EuclideanDistance(a, b)

	// Check closeness
	diff := expected - got
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-4 {
		t.Errorf("Euclidean128 mismatch: expected %v, got %v", expected, got)
	}
}

func TestDot128(t *testing.T) {
	dims := 128
	a := make([]float32, dims)
	b := make([]float32, dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < dims; i++ {
		a[i] = rng.Float32()
		b[i] = rng.Float32()
	}

	expected := dotGeneric(a, b)

	// Should route to specialized 128 kernel
	got := DotProduct(a, b)

	// Check closeness
	diff := expected - got
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-4 {
		t.Errorf("Dot128 mismatch: expected %v, got %v", expected, got)
	}
}

func BenchmarkEuclidean128(b *testing.B) {
	dims := 128
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistance(a, bb)
	}
}

func BenchmarkDot128(b *testing.B) {
	dims := 128
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProduct(a, bb)
	}
}
