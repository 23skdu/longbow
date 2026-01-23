package simd

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

func TestEuclideanDistanceBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	dim := 128
	numVectors := 100

	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	results := make([]float32, numVectors)
	err := EuclideanDistanceBatch(query, vectors, results)
	if err != nil {
		t.Fatalf("EuclideanDistanceBatch error: %v", err)
	}

	for i, v := range vectors {
		expected, err := EuclideanDistance(query, v)
		if err != nil {
			t.Errorf("EuclideanDistance error: %v", err)
		}
		if math.Abs(float64(results[i]-expected)) > 1e-5 {
			t.Errorf("Mismatch at index %d: expected %f, got %f", i, expected, results[i])
		}
	}
}

func BenchmarkEuclideanDistanceBatch(b *testing.B) {
	rng := rand.New(rand.NewSource(12345))
	dim := 128
	numVectors := 1000

	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	results := make([]float32, numVectors)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistanceBatch(query, vectors, results)
	}
}

func TestEuclideanDistanceSQ8Batch(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	dim := 128
	numVectors := 100

	query := make([]byte, dim)
	for i := range query {
		query[i] = byte(rng.Intn(256))
	}

	vectors := make([][]byte, numVectors)
	for i := range vectors {
		vectors[i] = make([]byte, dim)
		for j := range vectors[i] {
			vectors[i][j] = byte(rng.Intn(256))
		}
	}

	results := make([]float32, numVectors)
	err := EuclideanDistanceSQ8Batch(query, vectors, results)
	if err != nil {
		t.Fatalf("EuclideanDistanceSQ8Batch error: %v", err)
	}

	for i, v := range vectors {
		expected, err := EuclideanDistanceSQ8(query, v)
		if err != nil {
			t.Errorf("EuclideanDistanceSQ8 error: %v", err)
		}
		if results[i] != float32(expected) {
			t.Errorf("Mismatch at index %d: expected %f, got %f", i, float32(expected), results[i])
		}
	}
}

func BenchmarkEuclideanDistanceSQ8Batch(b *testing.B) {
	rng := rand.New(rand.NewSource(12345))
	dim := 128
	numVectors := 1000

	query := make([]byte, dim)
	for i := range query {
		query[i] = byte(rng.Intn(256))
	}

	vectors := make([][]byte, numVectors)
	for i := range vectors {
		vectors[i] = make([]byte, dim)
		for j := range vectors[i] {
			vectors[i][j] = byte(rng.Intn(256))
		}
	}

	results := make([]float32, numVectors)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistanceSQ8Batch(query, vectors, results)
	}
}

func TestEuclideanDistanceF16Batch(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	dim := 128
	numVectors := 100

	query := make([]float16.Num, dim)
	for i := range query {
		query[i] = float16.New(rng.Float32())
	}

	vectors := make([][]float16.Num, numVectors)
	for i := range vectors {
		vectors[i] = make([]float16.Num, dim)
		for j := range vectors[i] {
			vectors[i][j] = float16.New(rng.Float32())
		}
	}

	results := make([]float32, numVectors)
	err := EuclideanDistanceF16Batch(query, vectors, results)
	if err != nil {
		t.Fatalf("EuclideanDistanceF16Batch error: %v", err)
	}

	for i, v := range vectors {
		expected, err := EuclideanDistanceF16(query, v)
		if err != nil {
			t.Errorf("EuclideanDistanceF16 error: %v", err)
		}
		if math.Abs(float64(results[i]-expected)) > 1e-3 {
			t.Errorf("Mismatch at index %d: expected %f, got %f", i, expected, results[i])
		}
	}
}

func BenchmarkEuclideanDistanceF16Batch(b *testing.B) {
	rng := rand.New(rand.NewSource(12345))
	dim := 128
	numVectors := 1000

	query := make([]float16.Num, dim)
	for i := range query {
		query[i] = float16.New(rng.Float32())
	}

	vectors := make([][]float16.Num, numVectors)
	for i := range vectors {
		vectors[i] = make([]float16.Num, dim)
		for j := range vectors[i] {
			vectors[i][j] = float16.New(rng.Float32())
		}
	}

	results := make([]float32, numVectors)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EuclideanDistanceF16Batch(query, vectors, results)
	}
}
