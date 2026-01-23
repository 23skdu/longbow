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

func TestMatchInt64Correctness(t *testing.T) {
	src := []int64{0, 1, -1, 42, -9223372036854775808, 9223372036854775807}
	dst := make([]byte, len(src))

	// Test Eq
	matchInt64Generic(src, 42, CompareEq, dst)
	expected := []byte{0, 0, 0, 1, 0, 0}
	for i, v := range expected {
		if dst[i] != v {
			t.Errorf("Eq[%d]: got %d want %d", i, dst[i], v)
		}
	}

	// Test Neq
	matchInt64Generic(src, 42, CompareNeq, dst)
	expectedNeq := []byte{1, 1, 1, 0, 1, 1}
	for i, v := range expectedNeq {
		if dst[i] != v {
			t.Errorf("Neq[%d]: got %d want %d", i, dst[i], v)
		}
	}
}
