package simd

import (
	"testing"
)

func TestMatchInt64_Correctness(t *testing.T) {
	src := []int64{10, 20, 30, 40, 50, 10, 50}
	dst := make([]byte, len(src))

	tests := []struct {
		name     string
		val      int64
		op       CompareOp
		expected []byte
	}{
		{"Eq_10", 10, CompareEq, []byte{1, 0, 0, 0, 0, 1, 0}},
		{"Neq_10", 10, CompareNeq, []byte{0, 1, 1, 1, 1, 0, 1}},
		{"Gt_25", 25, CompareGt, []byte{0, 0, 1, 1, 1, 0, 1}},
		{"Ge_30", 30, CompareGe, []byte{0, 0, 1, 1, 1, 0, 1}},
		{"Lt_30", 30, CompareLt, []byte{1, 1, 0, 0, 0, 1, 0}},
		{"Le_30", 30, CompareLe, []byte{1, 1, 1, 0, 0, 1, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Clear dst
			for i := range dst {
				dst[i] = 255
			}
			MatchInt64(src, tc.val, tc.op, dst)
			for i, v := range dst {
				if v != tc.expected[i] {
					t.Errorf("Index %d: expected %d, got %d", i, tc.expected[i], v)
				}
			}
		})
	}
}

func TestMatchFloat32_Correctness(t *testing.T) {
	src := []float32{1.5, 2.5, 3.5, 4.5, 5.5}
	dst := make([]byte, len(src))

	tests := []struct {
		name     string
		val      float32
		op       CompareOp
		expected []byte
	}{
		{"Eq_2.5", 2.5, CompareEq, []byte{0, 1, 0, 0, 0}},
		{"Neq_2.5", 2.5, CompareNeq, []byte{1, 0, 1, 1, 1}},
		{"Gt_3.0", 3.0, CompareGt, []byte{0, 0, 1, 1, 1}},
		{"Ge_3.5", 3.5, CompareGe, []byte{0, 0, 1, 1, 1}},
		{"Lt_3.5", 3.5, CompareLt, []byte{1, 1, 0, 0, 0}},
		{"Le_3.5", 3.5, CompareLe, []byte{1, 1, 1, 0, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range dst {
				dst[i] = 255
			}
			MatchFloat32(src, tc.val, tc.op, dst)
			for i, v := range dst {
				if v != tc.expected[i] {
					t.Errorf("Index %d: expected %d, got %d", i, tc.expected[i], v)
				}
			}
		})
	}
}
