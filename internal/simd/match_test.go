package simd

import (
	"fmt"
	"testing"
)

// TestMatchInt32NEON runs the test match int32 neon operation.
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

// TestMatchInt64NEON runs the test match int64 neon operation.
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

// TestMatchFloat32NEON runs the test match float32 neon operation.
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

// BenchmarkMatchInt32 runs the benchmark match int32 operation.
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

// TestMatchUint16 runs the test match uint16 operation.
func TestMatchUint16(t *testing.T) {
	src := []uint16{1, 2, 3, 1, 5, 1, 7, 8, 1}
	dst := make([]byte, len(src))
	if err := MatchUint16(src, 1, CompareEq, dst); err != nil {
		t.Fatalf("MatchUint16 Eq failed: %v", err)
	}
	wantEq := []byte{1, 0, 0, 1, 0, 1, 0, 0, 1}
	for i := range wantEq {
		if dst[i] != wantEq[i] {
			t.Fatalf("Eq idx %d: got %d want %d", i, dst[i], wantEq[i])
		}
	}
	if err := MatchUint16(src, 1, CompareNeq, dst); err != nil {
		t.Fatalf("MatchUint16 Neq failed: %v", err)
	}
	wantNeq := []byte{0, 1, 1, 0, 1, 0, 1, 1, 0}
	for i := range wantNeq {
		if dst[i] != wantNeq[i] {
			t.Fatalf("Neq idx %d: got %d want %d", i, dst[i], wantNeq[i])
		}
	}
	if err := MatchUint16(src, 1, CompareEq, dst[:2]); err == nil {
		t.Fatal("expected length mismatch error")
	}
}

// BenchmarkMatchUint16 runs the benchmark match uint16 operation.
func BenchmarkMatchUint16(b *testing.B) {
	for _, size := range []int{1024, 65536} {
		src := make([]uint16, size)
		dst := make([]byte, size)
		for i := range src {
			src[i] = uint16(i % 16)
		}
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 2))
			for i := 0; i < b.N; i++ {
				_ = MatchUint16(src, 3, CompareEq, dst)
			}
		})
	}
}
