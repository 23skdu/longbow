package simd

import (
	"math/rand"
	"testing"
)

// branchlessMatchInt64 is a candidate replacement for matchInt64Generic
func branchlessMatchInt64(src []int64, val int64, dst []byte) {
	for i, v := range src {
		if v == val {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func BenchmarkMatchInt64(b *testing.B) {
	size := 1000
	src := make([]int64, size)
	dst := make([]byte, size)
	val := int64(42)

	// Randomize to ensure branch predictor doesn't memorize
	// 50% match rate
	for i := 0; i < size; i++ {
		if rand.Float32() < 0.5 {
			src[i] = val
		} else {
			src[i] = val + 1
		}
	}

	b.Run("Branchy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			branchlessMatchInt64(src, val, dst)
		}
	})

	b.Run("Branchless", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j, v1 := range src {
				// Pure Go approximation of branchless SETCC
				var res byte = 0
				if v1 == val {
					res = 1
				}
				dst[j] = res
			}
		}
	})
}
