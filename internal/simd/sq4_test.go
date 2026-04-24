package simd

import (
	"testing"
)

func TestDotInt4(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected float32
	}{
		{"exact_16", make([]byte, 16), make([]byte, 16), 0},
		{"simple_2", []byte{0x12, 0x34}, []byte{0x12, 0x34}, 30},
		{"tail_3", []byte{0x12, 0x34, 0x56}, []byte{0x12, 0x34, 0x56}, 30 + (6*6 + 5*5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DotProductInt4(tt.a, tt.b)
			if err != nil {
				t.Fatalf("DotProductInt4 failed: %v", err)
			}
			if got != tt.expected {
				t.Errorf("%s: expected %f, got %f", tt.name, tt.expected, got)
			}
		})
	}
}

func FuzzDotInt4(f *testing.F) {
	f.Add([]byte{0x12, 0x34}, []byte{0x12, 0x34})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if len(a) != len(b) {
			return
		}
		_, _ = DotProductInt4(a, b)
	})
}

func TestDotInt2(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected float32
	}{
		{"exact_16", make([]byte, 16), make([]byte, 16), 0},
		{"simple_1", []byte{0x01}, []byte{0x01}, 1},
		{"mixed", []byte{0b11100100}, []byte{0b11100100}, 3*3 + 2*2 + 1*1 + 0*0}, // 9+4+1+0=14
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DotProductInt2(tt.a, tt.b)
			if err != nil {
				t.Fatalf("DotProductInt2 failed: %v", err)
			}
			if got != tt.expected {
				t.Errorf("%s: expected %f, got %f", tt.name, tt.expected, got)
			}
		})
	}
}
