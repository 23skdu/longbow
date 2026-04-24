package simd

// DotInt4 calculates the dot product of two Int4-packed byte slices.
// Each byte contains two 4-bit values.
func DotInt4(a, b []byte) float32 {
	var sum int32
	for i := 0; i < len(a); i++ {
		// Low nibbles
		al := int32(a[i] & 0x0F)
		bl := int32(b[i] & 0x0F)
		sum += al * bl

		// High nibbles
		ah := int32((a[i] >> 4) & 0x0F)
		bh := int32((b[i] >> 4) & 0x0F)
		sum += ah * bh
	}
	return float32(sum)
}

// DotInt2 calculates the dot product of two Int2-packed byte slices.
// Each byte contains four 2-bit values.
func DotInt2(a, b []byte) float32 {
	var sum int32
	for i := 0; i < len(a); i++ {
		for j := 0; j < 4; j++ {
			shift := uint(j * 2)
			valA := int32((a[i] >> shift) & 0x03)
			valB := int32((b[i] >> shift) & 0x03)
			sum += valA * valB
		}
	}
	return float32(sum)
}
