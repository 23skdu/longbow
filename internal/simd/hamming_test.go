package simd

import (
	"testing"
)

func TestHammingDistanceSIMD(t *testing.T) {
	cases := []struct {
		name     string
		a, b     []uint64
		expected int
	}{
		{
			name:     "empty",
			a:        []uint64{},
			b:        []uint64{},
			expected: 0,
		},
		{
			name:     "identical",
			a:        []uint64{0x1234567890ABCDEF, 0xFEDCBA0987654321},
			b:        []uint64{0x1234567890ABCDEF, 0xFEDCBA0987654321},
			expected: 0,
		},
		{
			name:     "one bit",
			a:        []uint64{0x1, 0x0},
			b:        []uint64{0x0, 0x0},
			expected: 1,
		},
		{
			name:     "all bits",
			a:        []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			b:        []uint64{0x0, 0x0},
			expected: 128,
		},
		{
			name:     "random small",
			a:        []uint64{0x0101010101010101},
			b:        []uint64{0x1010101010101010},
			expected: 16,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := HammingDistance(tc.a, tc.b)
			if got != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, got)
			}
		})
	}
}

func TestHammingDistanceSIMD_Alignments(t *testing.T) {
	// Test various lengths to check loop and tail logic
	for n := 0; n < 10; n++ {
		a := make([]uint64, n)
		b := make([]uint64, n)
		for i := 0; i < n; i++ {
			a[i] = uint64(i)
			b[i] = ^uint64(i)
		}
		expected := n * 64
		got := HammingDistance(a, b)
		if got != expected {
			t.Errorf("len %d: expected %d, got %d", n, expected, got)
		}
	}
}
