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

	expected, err1 := euclideanGeneric(a, b)
	if err1 != nil {
		t.Errorf("euclideanGeneric error: %v", err1)
	}

	// Should route to specialized 128 kernel
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

	expected, err1 := dotGeneric(a, b)
	if err1 != nil {
		t.Errorf("dotGeneric error: %v", err1)
	}

	// Should route to specialized 128 kernel
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
		t.Errorf("Dot128 mismatch: expected %v, got %v", expected, got)
	}
}

func BenchmarkEuclidean128(b *testing.B) {
	dims := 128
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistance(a, bb)
	}
}

func BenchmarkDot128(b *testing.B) {
	dims := 128
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DotProduct(a, bb)
	}
}
