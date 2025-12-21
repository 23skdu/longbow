package simd

import (
	"math/rand"
	"testing"
)

func TestMatchInt64(t *testing.T) {
	sizes := []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 100, 1024}
	ops := []CompareOp{CompareEq, CompareNeq, CompareGt, CompareGe, CompareLt, CompareLe}

	for _, size := range sizes {
		src := make([]int64, size)
		dst := make([]byte, size)
		expected := make([]byte, size)

		// Fill randomized
		for i := range src {
			src[i] = int64(rand.Intn(100))
		}
		val := int64(50)

		for _, op := range ops {
			// Compute scalar benchmark
			matchInt64Generic(src, val, op, expected)

			// Compute SIMD
			// Clear dst
			for i := range dst {
				dst[i] = 255
			}
			MatchInt64(src, val, op, dst)

			for i := range src {
				if dst[i] != expected[i] {
					t.Errorf("MatchInt64 size=%d op=%v idx=%d: got %d, want %d (src=%d, val=%d)", size, op, i, dst[i], expected[i], src[i], val)
					// Fail fast for first error per size/op
					break
				}
			}
		}
	}
}

func TestMatchFloat32(t *testing.T) {
	sizes := []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 100, 1024}
	ops := []CompareOp{CompareEq, CompareNeq, CompareGt, CompareGe, CompareLt, CompareLe}

	for _, size := range sizes {
		src := make([]float32, size)
		dst := make([]byte, size)
		expected := make([]byte, size)

		// Fill randomized
		for i := range src {
			src[i] = rand.Float32() * 100
		}
		val := float32(50.0)

		for _, op := range ops {
			// Compute scalar benchmark
			matchFloat32Generic(src, val, op, expected)

			// Compute SIMD
			for i := range dst {
				dst[i] = 255
			}
			MatchFloat32(src, val, op, dst)

			for i := range src {
				if dst[i] != expected[i] {
					t.Errorf("MatchFloat32 size=%d op=%v idx=%d: got %d, want %d (src=%f, val=%f)", size, op, i, dst[i], expected[i], src[i], val)
					break
				}
			}
		}
	}
}
