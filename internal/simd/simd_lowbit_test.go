//go:build arm64

package simd

import (
	"testing"
	"unsafe"
)

func TestLowBitKernels(t *testing.T) {
	// Test Int4
	// In my assembly, I process high and low nibbles separately.
	// Low nibble (0x0F) and High nibble (>>4)
	// byte 0x12 -> low=2, high=1
	// byte 0x34 -> low=4, high=3
	a4 := []byte{0x12, 0x34}
	b4 := []byte{0x56, 0x78}
	// a_lows: [2, 4], a_highs: [1, 3]
	// b_lows: [6, 8], b_highs: [5, 7]
	// Sum = (2*6) + (4*8) + (1*5) + (3*7) = 12 + 32 + 5 + 21 = 70
	res4 := dotInt4NeonKernel(unsafe.Pointer(&a4[0]), unsafe.Pointer(&b4[0]), 2)
	if res4 != 70 {
		t.Errorf("dotInt4NeonKernel failed: expected 70, got %d", res4)
	}

	// Test Int2
	// byte 0xE4 -> 11 10 01 00 (binary)
	// Element 0 (bits 0-1): 0
	// Element 1 (bits 2-3): 1
	// Element 2 (bits 4-5): 2
	// Element 3 (bits 6-7): 3
	// byte 0x1B -> 00 01 10 11 (binary)
	// Element 0: 3
	// Element 1: 2
	// Element 2: 1
	// Element 3: 0
	a2 := []byte{0xE4}
	b2 := []byte{0x1B}
	// Sum = (0*3) + (1*2) + (2*1) + (3*0) = 0 + 2 + 2 + 0 = 4
	res2 := dotInt2NeonKernel(unsafe.Pointer(&a2[0]), unsafe.Pointer(&b2[0]), 1)
	if res2 != 4 {
		t.Errorf("dotInt2NeonKernel failed: expected 4, got %d", res2)
	}
}
