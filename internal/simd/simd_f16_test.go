package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// referenceEuclideanF16 computes Euclidean distance using float64 for precision.
func referenceEuclideanF16(a, b []float16.Num) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i].Float32()) - float64(b[i].Float32())
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

// referenceCosineF16 computes Cosine distance using float64.
func referenceCosineF16(a, b []float16.Num) float32 {
	var dot, normA, normB float64
	for i := range a {
		va := float64(a[i].Float32())
		vb := float64(b[i].Float32())
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	return float32(1.0 - (dot / (math.Sqrt(normA) * math.Sqrt(normB))))
}

// referenceDotProductF16 computes Dot Product using float64.
func referenceDotProductF16(a, b []float16.Num) float32 {
	var dot float64
	for i := range a {
		dot += float64(a[i].Float32()) * float64(b[i].Float32())
	}
	return float32(dot)
}

func makeTestVectorF16(dim int) []float16.Num {
	res := make([]float16.Num, dim)
	for i := 0; i < dim; i++ {
		res[i] = float16.New(rand.Float32()*2 - 1)
	}
	return res
}

func TestEuclideanDistanceF16(t *testing.T) {
	dims := []int{1, 4, 8, 16, 32, 64, 128, 384, 768, 1536}
	for _, dim := range dims {
		t.Run(fmt.Sprintf("Dim%d", dim), func(t *testing.T) {
			a := makeTestVectorF16(dim)
			b := makeTestVectorF16(dim)

			expected := referenceEuclideanF16(a, b)
			actual, err := EuclideanDistanceF16(a, b)
			if err != nil {
				t.Fatalf("EuclideanDistanceF16 error: %v", err)
			}

			// FP16 has limited precision, so we use a relatively large epsilon
			// but we expect the SIMD vs Generic to match exactly or very closely.
			if math.Abs(float64(expected-actual)) > 1e-3 {
				t.Errorf("dim %d: expected %f, got %f", dim, expected, actual)
			}
		})
	}
}

func TestDotProductF16(t *testing.T) {
	dims := []int{1, 4, 8, 16, 32, 64, 128, 384, 768, 1536}
	for _, dim := range dims {
		t.Run(fmt.Sprintf("Dim%d", dim), func(t *testing.T) {
			a := makeTestVectorF16(dim)
			b := makeTestVectorF16(dim)

			expected := referenceDotProductF16(a, b)
			actual, err := DotProductF16(a, b)
			if err != nil {
				t.Fatalf("DotProductF16 error: %v", err)
			}

			if math.Abs(float64(expected-actual)) > 1e-3 {
				t.Errorf("dim %d: expected %f, got %f", dim, expected, actual)
			}
		})
	}
}

func TestCosineDistanceF16(t *testing.T) {
	dims := []int{1, 4, 8, 16, 32, 64, 128, 384, 768, 1536}
	for _, dim := range dims {
		t.Run(fmt.Sprintf("Dim%d", dim), func(t *testing.T) {
			a := makeTestVectorF16(dim)
			b := makeTestVectorF16(dim)

			expected := referenceCosineF16(a, b)
			actual, err := CosineDistanceF16(a, b)
			if err != nil {
				t.Fatalf("CosineDistanceF16 error: %v", err)
			}

			if math.Abs(float64(expected-actual)) > 2e-3 {
				t.Errorf("dim %d: expected %f, got %f", dim, expected, actual)
			}
		})
	}
}

func BenchmarkEuclideanDistanceF16_384(b *testing.B) {
	dim := 384
	v1 := makeTestVectorF16(dim)
	v2 := makeTestVectorF16(dim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistanceF16(v1, v2)
	}
}

func BenchmarkDotProductF16_384(b *testing.B) {
	dim := 384
	v1 := makeTestVectorF16(dim)
	v2 := makeTestVectorF16(dim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DotProductF16(v1, v2)
	}
}
