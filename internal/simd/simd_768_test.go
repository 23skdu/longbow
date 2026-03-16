package simd

import (
	"math"
	"math/rand"
	"testing"
)

func float32Equal(a, b float32) bool {
	return math.Abs(float64(a-b)) < 1e-4
}

func TestEuclidean768(t *testing.T) {
	dims := 768
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

	got, err2 := euclidean768Unrolled4x(a, b)
	if err2 != nil {
		t.Errorf("euclidean768Unrolled4x error: %v", err2)
	}

	if !float32Equal(expected, got) {
		t.Errorf("euclidean768Unrolled4x mismatch: expected %v, got %v", expected, got)
	}
}

func TestEuclidean1536(t *testing.T) {
	dims := 1536
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

	got, err2 := euclidean1536Unrolled4x(a, b)
	if err2 != nil {
		t.Errorf("euclidean1536Unrolled4x error: %v", err2)
	}

	if !float32Equal(expected, got) {
		t.Errorf("euclidean1536Unrolled4x mismatch: expected %v, got %v", expected, got)
	}
}

func TestEuclidean384Unrolled4x(t *testing.T) {
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

	got, err2 := euclidean384Unrolled4x(a, b)
	if err2 != nil {
		t.Errorf("euclidean384Unrolled4x error: %v", err2)
	}

	if !float32Equal(expected, got) {
		t.Errorf("euclidean384Unrolled4x mismatch: expected %v, got %v", expected, got)
	}
}

func BenchmarkEuclidean768(b *testing.B) {
	dims := 768
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclidean768Unrolled4x(a, bb)
	}
}

func BenchmarkEuclidean1536(b *testing.B) {
	dims := 1536
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclidean1536Unrolled4x(a, bb)
	}
}

func BenchmarkEuclidean768Dispatch(b *testing.B) {
	dims := 768
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistance(a, bb)
	}
}

func BenchmarkEuclidean1536Dispatch(b *testing.B) {
	dims := 1536
	a := make([]float32, dims)
	bb := make([]float32, dims)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistance(a, bb)
	}
}
