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

func TestMatchFloat32_AVX2_LargeArray(t *testing.T) {
	src8 := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}
	dst8 := make([]byte, len(src8))

	tests := []struct {
		name     string
		val      float32
		op       CompareOp
		expected []byte
	}{
		{"Eq_5.0", 5.0, CompareEq, []byte{0, 0, 0, 0, 1, 0, 0, 0}},
		{"Neq_5.0", 5.0, CompareNeq, []byte{1, 1, 1, 1, 0, 1, 1, 1}},
		{"Gt_4.5", 4.5, CompareGt, []byte{0, 0, 0, 0, 1, 1, 1, 1}},
		{"Ge_5.0", 5.0, CompareGe, []byte{0, 0, 0, 0, 1, 1, 1, 1}},
		{"Lt_5.0", 5.0, CompareLt, []byte{1, 1, 1, 1, 0, 0, 0, 0}},
		{"Le_5.0", 5.0, CompareLe, []byte{1, 1, 1, 1, 1, 0, 0, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range dst8 {
				dst8[i] = 255
			}
			MatchFloat32(src8, tc.val, tc.op, dst8)
			for i, v := range dst8 {
				if v != tc.expected[i] {
					t.Errorf("Index %d: expected %d, got %d", i, tc.expected[i], v)
				}
			}
		})
	}

	src16 := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0}
	dst16 := make([]byte, len(src16))
	expected16 := []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	for i := range dst16 {
		dst16[i] = 255
	}
	MatchFloat32(src16, 5.0, CompareEq, dst16)
	for i, v := range dst16 {
		if v != expected16[i] {
			t.Errorf("Index %d: expected %d, got %d", i, expected16[i], v)
		}
	}

	src9 := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
	dst9 := make([]byte, len(src9))
	expected9 := []byte{0, 0, 0, 0, 1, 0, 0, 0, 0}

	for i := range dst9 {
		dst9[i] = 255
	}
	MatchFloat32(src9, 5.0, CompareEq, dst9)
	for i, v := range dst9 {
		if v != expected9[i] {
			t.Errorf("Index %d: expected %d, got %d", i, expected9[i], v)
		}
	}
}
