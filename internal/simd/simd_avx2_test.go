//go:build amd64

package simd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// saveAndSetAVX2 saves original state and sets AVX2-only mode
func saveAndSetAVX2() func() {
	origFeatures := features
	origImpl := implementation
	features.HasAVX2 = true
	features.HasAVX512 = false
	implementation = "avx2"
	return func() {
		features = origFeatures
		implementation = origImpl
	}
}

func TestAVX2_EuclideanDistance(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	tests := []struct {
		name string
		dim  int
	}{
		{"dim_4", 4}, {"dim_7", 7}, {"dim_8", 8}, {"dim_9", 9},
		{"dim_15", 15}, {"dim_16", 16}, {"dim_31", 31}, {"dim_32", 32},
		{"dim_64", 64}, {"dim_128", 128}, {"dim_256", 256},
		{"dim_384", 384}, {"dim_512", 512}, {"dim_768", 768},
		{"dim_1024", 1024}, {"dim_1536", 1536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := makeTestVector(tt.dim, 1.0)
			b := makeTestVector(tt.dim, 2.0)
			expected := referenceEuclidean(a, b)
			result, err := euclideanAVX2(a, b)
			require.NoError(t, err)
			if !approxEqual(result, expected, 1e-4) {
				t.Errorf("got %v, expected %v", result, expected)
			}
		})
	}
}

func TestAVX2_EuclideanEdgeCases(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	t.Run("identical", func(t *testing.T) {
		a := makeTestVector(128, 1.0)
		res, err := euclideanAVX2(a, a)
		require.NoError(t, err)
		if res != 0 {
			t.Error("distance to self should be 0")
		}
	})

	t.Run("zeros", func(t *testing.T) {
		a, b := make([]float32, 128), make([]float32, 128)
		res, err := euclideanAVX2(a, b)
		require.NoError(t, err)
		if res != 0 {
			t.Error("zero vectors distance should be 0")
		}
	})

	t.Run("negative", func(t *testing.T) {
		a, b := make([]float32, 64), make([]float32, 64)
		for i := range a {
			a[i], b[i] = float32(-i)*0.1, float32(i)*0.1
		}
		expected := referenceEuclidean(a, b)
		res, err := euclideanAVX2(a, b)
		require.NoError(t, err)
		if !approxEqual(res, expected, 1e-4) {
			t.Error("negative values failed")
		}
	})
}

func TestAVX2_CosineDistance(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	tests := []struct {
		name string
		dim  int
	}{
		{"dim_4", 4}, {"dim_7", 7}, {"dim_8", 8}, {"dim_9", 9},
		{"dim_15", 15}, {"dim_16", 16}, {"dim_31", 31}, {"dim_32", 32},
		{"dim_64", 64}, {"dim_128", 128}, {"dim_256", 256},
		{"dim_384", 384}, {"dim_512", 512}, {"dim_768", 768}, {"dim_1536", 1536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := makeTestVector(tt.dim, 1.0)
			b := makeTestVector(tt.dim, 2.0)
			expected := referenceCosine(a, b)
			result, err := cosineAVX2(a, b)
			require.NoError(t, err)
			if !approxEqual(result, expected, 1e-4) {
				t.Errorf("got %v, expected %v", result, expected)
			}
		})
	}
}

func TestAVX2_CosineEdgeCases(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	t.Run("identical", func(t *testing.T) {
		a := makeTestVector(128, 1.0)
		res, err := cosineAVX2(a, a)
		require.NoError(t, err)
		if !approxEqual(res, 0, 1e-5) {
			t.Error("cosine to self should be ~0")
		}
	})

	t.Run("zero_vector", func(t *testing.T) {
		a, b := make([]float32, 128), makeTestVector(128, 1.0)
		res, err := cosineAVX2(a, b)
		require.NoError(t, err)
		if res != 1.0 {
			t.Error("zero vector distance should be 1.0")
		}
	})

	t.Run("orthogonal", func(t *testing.T) {
		a, b := make([]float32, 8), make([]float32, 8)
		a[0], b[1] = 1.0, 1.0
		res, err := cosineAVX2(a, b)
		require.NoError(t, err)
		if !approxEqual(res, 1.0, 1e-5) {
			t.Error("orthogonal should be 1.0")
		}
	})

	t.Run("opposite", func(t *testing.T) {
		a := makeTestVector(64, 1.0)
		b := make([]float32, 64)
		for i := range b {
			b[i] = -a[i]
		}
		res, err := cosineAVX2(a, b)
		require.NoError(t, err)
		if !approxEqual(res, 2.0, 1e-5) {
			t.Error("opposite should be 2.0")
		}
	})
}

func TestAVX2_DotProduct(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	tests := []struct {
		name string
		dim  int
	}{
		{"dim_4", 4}, {"dim_7", 7}, {"dim_8", 8}, {"dim_9", 9},
		{"dim_15", 15}, {"dim_16", 16}, {"dim_31", 31}, {"dim_32", 32},
		{"dim_64", 64}, {"dim_128", 128}, {"dim_256", 256},
		{"dim_512", 512}, {"dim_768", 768}, {"dim_1536", 1536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := makeTestVector(tt.dim, 1.0)
			b := makeTestVector(tt.dim, 1.0)
			var expected float32
			for i := range a {
				expected += a[i] * b[i]
			}
			result, err := dotAVX2(a, b)
			require.NoError(t, err)
			if !approxEqual(result, expected, 1e-3) {
				t.Errorf("got %v, expected %v", result, expected)
			}
		})
	}
}

func TestAVX2_PublicAPI(t *testing.T) {
	restore := saveAndSetAVX2()
	defer restore()

	if GetImplementation() != "avx2" {
		t.Fatalf("expected avx2, got %s", GetImplementation())
	}

	for _, dim := range []int{8, 16, 32, 64, 128, 256, 384, 512, 768, 1536} {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			a, b := makeTestVector(dim, 1.0), makeTestVector(dim, 2.0)
			resE, errE := EuclideanDistance(a, b)
			resC, errC := CosineDistance(a, b)
			require.NoError(t, errE)
			require.NoError(t, errC)
			if !approxEqual(resE, referenceEuclidean(a, b), 1e-4) {
				t.Error("EuclideanDistance failed")
			}
			if !approxEqual(resC, referenceCosine(a, b), 1e-4) {
				t.Error("CosineDistance failed")
			}
		})
	}
}

// Benchmarks
func BenchmarkAVX2_Euclidean_768(b *testing.B) {
	restore := saveAndSetAVX2()
	defer restore()
	v1, v2 := makeTestVector(768, 1.0), makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclideanAVX2(v1, v2)
	}
}

func BenchmarkAVX2_Cosine_768(b *testing.B) {
	restore := saveAndSetAVX2()
	defer restore()
	v1, v2 := makeTestVector(768, 1.0), makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cosineAVX2(v1, v2)
	}
}

func BenchmarkAVX2_Dot_768(b *testing.B) {
	restore := saveAndSetAVX2()
	defer restore()
	v1, v2 := makeTestVector(768, 1.0), makeTestVector(768, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dotAVX2(v1, v2)
	}
}

func BenchmarkAVX2_Euclidean_1536(b *testing.B) {
	restore := saveAndSetAVX2()
	defer restore()
	v1, v2 := makeTestVector(1536, 1.0), makeTestVector(1536, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclideanAVX2(v1, v2)
	}
}

func BenchmarkAVX2_Cosine_1536(b *testing.B) {
	restore := saveAndSetAVX2()
	defer restore()
	v1, v2 := makeTestVector(1536, 1.0), makeTestVector(1536, 2.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cosineAVX2(v1, v2)
	}
}
