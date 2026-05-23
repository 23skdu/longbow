package simd

import (
	"fmt"
	"testing"
)

func TestMatchInt32NEON(t *testing.T) {
	src := []int32{1, 2, 3, 4, 5, 6, 7, 8, 1, 1, 1, 1}
	val := int32(1)
	dst := make([]byte, len(src))

	ops := []struct {
		op   CompareOp
		name string
		want []byte
	}{
		{CompareEq, "Eq", []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}},
		{CompareNeq, "Neq", []byte{0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0}},
		{CompareGt, "Gt", []byte{0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0}}, // src > 1
		{CompareGe, "Ge", []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}, // src >= 1
		{CompareLt, "Lt", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}, // src < 1
		{CompareLe, "Le", []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}}, // src <= 1
	}

	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			for i := range dst {
				dst[i] = 0
			}
			err := MatchInt32(src, val, tc.op, dst)
			if err != nil {
				t.Fatalf("MatchInt32 failed: %v", err)
			}
			for i := range src {
				if dst[i] != tc.want[i] {
					t.Errorf("%s: at index %d, src=%d, val=%d, got %d, want %d", tc.name, i, src[i], val, dst[i], tc.want[i])
				}
			}
		})
	}
}

func TestMatchInt64NEON(t *testing.T) {
	src := []int64{10, 20, 30, 40, 10}
	val := int64(10)
	dst := make([]byte, len(src))

	err := MatchInt64(src, val, CompareEq, dst)
	if err != nil {
		t.Fatalf("MatchInt64 failed: %v", err)
	}
	want := []byte{1, 0, 0, 0, 1}
	for i := range want {
		if dst[i] != want[i] {
			t.Errorf("at index %d, got %d, want %d", i, dst[i], want[i])
		}
	}
}

func TestMatchFloat32NEON(t *testing.T) {
	src := []float32{1.0, 2.0, 3.0, 4.0, 1.0}
	val := float32(2.0)
	dst := make([]byte, len(src))

	err := MatchFloat32(src, val, CompareGt, dst) // src > 2.0
	if err != nil {
		t.Fatalf("MatchFloat32 failed: %v", err)
	}
	want := []byte{0, 0, 1, 1, 0}
	for i := range want {
		if dst[i] != want[i] {
			t.Errorf("at index %d, got %d, want %d", i, dst[i], want[i])
		}
	}
}

func BenchmarkMatchInt32(b *testing.B) {
	sizes := []int{1024, 1024 * 1024}
	for _, size := range sizes {
		src := make([]int32, size)
		dst := make([]byte, size)
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 4))
			for i := 0; i < b.N; i++ {
				_ = MatchInt32(src, 1, CompareEq, dst)
			}
		})
	}
}
