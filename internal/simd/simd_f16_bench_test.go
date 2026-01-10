package simd

import (
	"strconv"
	"testing"
)

// BenchmarkEuclideanF16_Comparative runs benchmarks for different vector sizes
// to help measure the impact of SIMD optimizations.
func BenchmarkEuclideanF16_Comparative(b *testing.B) {
	dims := []int{128, 384, 768, 1536}
	for _, dim := range dims {
		v1 := makeTestVectorF16(dim)
		v2 := makeTestVectorF16(dim)

		b.Run("EuclideanF16_"+strconv.Itoa(dim), func(b *testing.B) {
			b.SetBytes(int64(dim * 2)) // 2 bytes per float16
			for i := 0; i < b.N; i++ {
				EuclideanDistanceF16(v1, v2)
			}
		})
	}
}

// BenchmarkCosineF16_Comparative runs benchmarks for different vector sizes.
func BenchmarkCosineF16_Comparative(b *testing.B) {
	dims := []int{128, 384, 768, 1536}
	for _, dim := range dims {
		v1 := makeTestVectorF16(dim)
		v2 := makeTestVectorF16(dim)

		b.Run("CosineF16_"+strconv.Itoa(dim), func(b *testing.B) {
			b.SetBytes(int64(dim * 2))
			for i := 0; i < b.N; i++ {
				CosineDistanceF16(v1, v2)
			}
		})
	}
}
