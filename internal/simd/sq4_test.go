package simd

import (
	"testing"
)

func TestDotInt4(t *testing.T) {
	a := []byte{0x12, 0x34} // [2, 1, 4, 3]
	b := []byte{0x12, 0x34} // [2, 1, 4, 3]
	
	// al*bl + ah*bh + al*bl + ah*bh
	// 2*2 + 1*1 + 4*4 + 3*3 = 4 + 1 + 16 + 9 = 30
	expected := float32(30)
	got, _ := DotProductInt4(a, b)
	if got != expected {
		t.Errorf("DotInt4 expected %f, got %f", expected, got)
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
	a := []byte{0x01} // [1, 0, 0, 0]
	b := []byte{0x01} // [1, 0, 0, 0]
	expected := float32(1)
	got, _ := DotProductInt2(a, b)
	if got != expected {
		t.Errorf("DotInt2 expected %f, got %f", expected, got)
	}
}
