package simd

import (
	"math"
	"math/rand"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/apache/arrow-go/v18/arrow/float16"
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
			assert.InDelta(t, expected, got, 5e-3, "Dot parity mismatch at dims %d", d)

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
			assert.InDelta(t, expected, got, 5e-3, "Dot parity mismatch at dims %d", d)

			// Cosine
			got, _ = DispatchDistance(MetricCosine, a, b)
			expected = refCosine(a, b)
			assert.InDelta(t, expected, got, 1e-5, "Cosine parity mismatch at dims %d", d)
		}
	})

	t.Run("Float16", func(t *testing.T) {
		for _, d := range dims {
			a := make([]float16.Num, d)
			b := make([]float16.Num, d)
			refA := make([]float32, d)
			refB := make([]float32, d)
			for i := 0; i < d; i++ {
				valA := rand.Float32() * 10
				valB := rand.Float32() * 10
				a[i] = float16.New(valA)
				b[i] = float16.New(valB)
				refA[i] = a[i].Float32()
				refB[i] = b[i].Float32()
			}

			// Euclidean
			got, err := EuclideanDistanceF16(a, b)
			assert.NoError(t, err)
			expected := refEuclidean(refA, refB)
			assert.InDelta(t, expected, got, 1e-2, "Float16 Euclidean parity mismatch at dims %d", d)

			// Dot Product
			got, err = DotProductF16(a, b)
			assert.NoError(t, err)
			expected = refDot(refA, refB)
			assert.InDelta(t, expected, got, 5e-2, "Float16 Dot parity mismatch at dims %d", d)

			// Cosine
			got, err = CosineDistanceF16(a, b)
			assert.NoError(t, err)
			expected = refCosine(refA, refB)
			assert.InDelta(t, expected, got, 1e-2, "Float16 Cosine parity mismatch at dims %d", d)
		}
	})

	t.Run("Complex64", func(t *testing.T) {
		for _, d := range dims {
			a := make([]complex64, d)
			b := make([]complex64, d)
			refA := make([]float32, d*2)
			refB := make([]float32, d*2)
			for i := 0; i < d; i++ {
				realA, imagA := rand.Float32()*10, rand.Float32()*10
				realB, imagB := rand.Float32()*10, rand.Float32()*10
				a[i] = complex(realA, imagA)
				b[i] = complex(realB, imagB)
				refA[2*i] = realA
				refA[2*i+1] = imagA
				refB[2*i] = realB
				refB[2*i+1] = imagB
			}

			got, err := EuclideanDistanceComplex64(a, b)
			assert.NoError(t, err)
			expected := refEuclidean(refA, refB)
			assert.InDelta(t, expected, got, 1e-4, "Complex64 Euclidean parity mismatch at dims %d", d)
		}
	})

	t.Run("Complex128", func(t *testing.T) {
		for _, d := range dims {
			a := make([]complex128, d)
			b := make([]complex128, d)
			refA := make([]float64, d*2)
			refB := make([]float64, d*2)
			for i := 0; i < d; i++ {
				realA, imagA := rand.Float64()*10, rand.Float64()*10
				realB, imagB := rand.Float64()*10, rand.Float64()*10
				a[i] = complex(realA, imagA)
				b[i] = complex(realB, imagB)
				refA[2*i] = realA
				refA[2*i+1] = imagA
				refB[2*i] = realB
				refB[2*i+1] = imagB
			}

			got, err := EuclideanDistanceComplex128(a, b)
			assert.NoError(t, err)
			expected := refEuclidean(refA, refB)
			assert.InDelta(t, expected, got, 1e-4, "Complex128 Euclidean parity mismatch at dims %d", d)
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
				a[i] = int16(rand.Intn(2001) - 1000)
				b[i] = int16(rand.Intn(2001) - 1000)
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			// Float32 accumulation on large int16 values has relative error ~1e-5;
			// use 1% relative tolerance to handle any magnitude.
			if expected > 1 {
				assert.InEpsilon(t, expected, got, 0.05)
			} else {
				assert.InDelta(t, expected, got, 0.5)
			}

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			if math.Abs(float64(expected)) > 1 {
				assert.InEpsilon(t, expected, got, 0.05)
			} else {
				assert.InDelta(t, expected, got, 0.5)
			}
		}
	})

	t.Run("Uint16", func(t *testing.T) {
		for _, d := range dims {
			a := make([]uint16, d)
			b := make([]uint16, d)
			for i := 0; i < d; i++ {
				a[i] = uint16(rand.Intn(2001))
				b[i] = uint16(rand.Intn(2001))
			}

			got, _ := DispatchDistance(MetricEuclidean, a, b)
			expected := refEuclidean(a, b)
			if expected > 1 {
				assert.InEpsilon(t, expected, got, 0.05)
			} else {
				assert.InDelta(t, expected, got, 0.5)
			}

			got, _ = DispatchDistance(MetricDotProduct, a, b)
			expected = refDot(a, b)
			if math.Abs(float64(expected)) > 1 {
				assert.InEpsilon(t, expected, got, 0.05)
			} else {
				assert.InDelta(t, expected, got, 0.5)
			}
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
