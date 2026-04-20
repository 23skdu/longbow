package simd

import (
	"math"
	"math/rand"
	"testing"
	"github.com/stretchr/testify/assert"
)

// Reference implementations using float64 for maximum precision

func refEuclidean[T numeric](a, b []T) float32 {
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func refDot[T numeric](a, b []T) float32 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum)
}

func refCosine[T numeric](a, b []T) float32 {
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	// Clamp to [0, 2] as in implementation
	return float32(math.Max(0, math.Min(2, 1.0-similarity)))
}

type numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

func TestSimdParity_FloatingPoint(t *testing.T) {
	rand.Seed(42)
	dims := []int{1, 3, 4, 7, 8, 15, 16, 31, 32, 128, 512}

	t.Run("Float32", func(t *testing.T) {
		for _, d := range dims {
			a := make([]float32, d)
			b := make([]float32, d)
			for i := 0; i < d; i++ {
				a[i] = rand.Float32() * 10
				b[i] = rand.Float32() * 10
			}

			// Euclidean
			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 1e-4, "Euclidean parity mismatch at dims %d", d)

			// Dot Product
			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 1e-3, "Dot parity mismatch at dims %d", d)

			// Cosine
			got, _ = DispatchDistance(MetricCosine, a, b)
			expected = refCosine(a, b)
			assert.InDelta(t, expected, got, 1e-5, "Cosine parity mismatch at dims %d", d)
		}
	})

	t.Run("Float64", func(t *testing.T) {
		for _, d := range dims {
			a := make([]float64, d)
			b := make([]float64, d)
			for i := 0; i < d; i++ {
				a[i] = rand.Float64() * 10
				b[i] = rand.Float64() * 10
			}

			// Euclidean
			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 1e-4, "Euclidean parity mismatch at dims %d", d)

			// Dot Product
			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 1e-3, "Dot parity mismatch at dims %d", d)

			// Cosine
			got, _ = DispatchDistance(MetricCosine, a, b)
			expected = refCosine(a, b)
			assert.InDelta(t, expected, got, 1e-5, "Cosine parity mismatch at dims %d", d)
		}
	})
}

func TestSimdParity_Integer(t *testing.T) {
	rand.Seed(42)
	dims := []int{4, 8, 16, 31, 128}

	t.Run("Int8", func(t *testing.T) {
		for _, d := range dims {
			a := make([]int8, d)
			b := make([]int8, d)
			for i := 0; i < d; i++ {
				a[i] = int8(rand.Intn(256) - 128)
				b[i] = int8(rand.Intn(256) - 128)
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 1e-5)

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 1e-5)

			got, _ = DispatchDistance(MetricCosine, a, b)
			expected = refCosine(a, b)
			assert.InDelta(t, expected, got, 1e-5)
		}
	})

	t.Run("Uint8", func(t *testing.T) {
		for _, d := range dims {
			a := make([]uint8, d)
			b := make([]uint8, d)
			for i := 0; i < d; i++ {
				a[i] = uint8(rand.Intn(256))
				b[i] = uint8(rand.Intn(256))
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 1e-5)

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 1e-5)

			got, _ = DispatchDistance(MetricCosine, a, b)
			expected = refCosine(a, b)
			assert.InDelta(t, expected, got, 1e-5)
		}
	})

	t.Run("Int16", func(t *testing.T) {
		for _, d := range dims {
			a := make([]int16, d)
			b := make([]int16, d)
			for i := 0; i < d; i++ {
				a[i] = int16(rand.Intn(65536) - 32768)
				b[i] = int16(rand.Intn(65536) - 32768)
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 0.1)

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 0.1)
		}
	})

	t.Run("Uint16", func(t *testing.T) {
		for _, d := range dims {
			a := make([]uint16, d)
			b := make([]uint16, d)
			for i := 0; i < d; i++ {
				a[i] = uint16(rand.Intn(65536))
				b[i] = uint16(rand.Intn(65536))
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 0.1)

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 0.1)
		}
	})

	t.Run("Int32", func(t *testing.T) {
		for _, d := range dims {
			a := make([]int32, d)
			b := make([]int32, d)
			for i := 0; i < d; i++ {
				a[i] = rand.Int31() / 1000
				b[i] = rand.Int31() / 1000
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 1.0) // Larger tolerance for high values

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 1.0)
		}
	})

	t.Run("Int64", func(t *testing.T) {
		for _, d := range dims {
			a := make([]int64, d)
			b := make([]int64, d)
			for i := 0; i < d; i++ {
				a[i] = rand.Int63() / 1000000
				b[i] = rand.Int63() / 1000000
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			assert.InDelta(t, expected, got, 10.0)

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			assert.InDelta(t, expected, got, 10.0)
		}
	})
}
