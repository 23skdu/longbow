package simd

import "math"

// UnpackTQ2 unpacks 2-bit TurboQuant data.
func UnpackTQ2(src []byte, dst []float32, scale, bias float32) {
	if unpackTQ2Impl != nil {
		unpackTQ2Impl(src, dst, scale, bias)
	} else {
		UnpackTQ2Generic(src, dst, scale, bias)
	}
}

// UnpackTQ4 unpacks 4-bit TurboQuant data.
func UnpackTQ4(src []byte, dst []float32, scale, bias float32) {
	if unpackTQ4Impl != nil {
		unpackTQ4Impl(src, dst, scale, bias)
	} else {
		UnpackTQ4Generic(src, dst, scale, bias)
	}
}

// UnpackTQ8 unpacks 8-bit TurboQuant data.
func UnpackTQ8(src []byte, dst []float32, scale, bias float32) {
	if unpackTQ8Impl != nil {
		unpackTQ8Impl(src, dst, scale, bias)
	} else {
		UnpackTQ8Generic(src, dst, scale, bias)
	}
}

func UnpackTQ2Generic(src []byte, dst []float32, scale, bias float32) {
	n := len(dst)
	i := 0
	for ; i <= n-8; i += 8 {
		b0 := src[i/4]
		b1 := src[i/4+1]
		dst[i] = float32(b0&0x03)*scale + bias
		dst[i+1] = float32((b0>>2)&0x03)*scale + bias
		dst[i+2] = float32((b0>>4)&0x03)*scale + bias
		dst[i+3] = float32((b0>>6)&0x03)*scale + bias
		dst[i+4] = float32(b1&0x03)*scale + bias
		dst[i+5] = float32((b1>>2)&0x03)*scale + bias
		dst[i+6] = float32((b1>>4)&0x03)*scale + bias
		dst[i+7] = float32((b1>>6)&0x03)*scale + bias
	}
	for ; i < n; i++ {
		val := (src[i/4] >> (uint(i%4) * 2)) & 0x03
		dst[i] = float32(val)*scale + bias
	}
}

func UnpackTQ4Generic(src []byte, dst []float32, scale, bias float32) {
	n := len(dst)
	i := 0
	for ; i <= n-8; i += 8 {
		b0 := src[i/2]
		b1 := src[i/2+1]
		b2 := src[i/2+2]
		b3 := src[i/2+3]
		dst[i] = float32(b0&0x0F)*scale + bias
		dst[i+1] = float32(b0>>4)*scale + bias
		dst[i+2] = float32(b1&0x0F)*scale + bias
		dst[i+3] = float32(b1>>4)*scale + bias
		dst[i+4] = float32(b2&0x0F)*scale + bias
		dst[i+5] = float32(b2>>4)*scale + bias
		dst[i+6] = float32(b3&0x0F)*scale + bias
		dst[i+7] = float32(b3>>4)*scale + bias
	}
	for ; i < n; i++ {
		var val byte
		if i%2 == 0 {
			val = src[i/2] & 0x0F
		} else {
			val = src[i/2] >> 4
		}
		dst[i] = float32(val)*scale + bias
	}
}

func UnpackTQ8Generic(src []byte, dst []float32, scale, bias float32) {
	n := len(dst)
	i := 0
	for ; i <= n-8; i += 8 {
		dst[i] = float32(src[i])*scale + bias
		dst[i+1] = float32(src[i+1])*scale + bias
		dst[i+2] = float32(src[i+2])*scale + bias
		dst[i+3] = float32(src[i+3])*scale + bias
		dst[i+4] = float32(src[i+4])*scale + bias
		dst[i+5] = float32(src[i+5])*scale + bias
		dst[i+6] = float32(src[i+6])*scale + bias
		dst[i+7] = float32(src[i+7])*scale + bias
	}
	for ; i < n; i++ {
		dst[i] = float32(src[i])*scale + bias
	}
}

// PackTQ2 packs float32 data into 2-bit TurboQuant format.
func PackTQ2(src []float32, dst []byte) {
	if packTQ2Impl != nil {
		packTQ2Impl(src, dst)
	} else {
		PackTQ2Generic(src, dst)
	}
}

// PackTQ4 packs float32 data into 4-bit TurboQuant format.
func PackTQ4(src []float32, dst []byte) {
	if packTQ4Impl != nil {
		packTQ4Impl(src, dst)
	} else {
		PackTQ4Generic(src, dst)
	}
}

// PackTQ8 packs float32 data into 8-bit TurboQuant format.
func PackTQ8(src []float32, dst []byte) {
	if packTQ8Impl != nil {
		packTQ8Impl(src, dst)
	} else {
		PackTQ8Generic(src, dst)
	}
}

func PackTQ2Generic(src []float32, dst []byte) {
	maxVal := float32(3)
	inv2Pi := float32(1.0 / (2 * math.Pi))
	pi32 := float32(math.Pi)
	for i := 0; i < len(src); i += 4 {
		var b byte
		for j := 0; j < 4; j++ {
			if i+j < len(src) {
				norm := (src[i+j] + pi32) * inv2Pi
				if norm < 0 {
					norm = 0
				} else if norm > 1 {
					norm = 1
				}
				q := byte(norm*maxVal + 0.5)
				b |= (q << (uint(j) * 2))
			}
		}
		dst[i/4] = b
	}
}

func PackTQ4Generic(src []float32, dst []byte) {
	maxVal := float32(15)
	inv2Pi := float32(1.0 / (2 * math.Pi))
	pi32 := float32(math.Pi)
	for i := 0; i < len(src); i += 2 {
		norm1 := (src[i] + pi32) * inv2Pi
		if norm1 < 0 {
			norm1 = 0
		} else if norm1 > 1 {
			norm1 = 1
		}
		q1 := byte(norm1*maxVal + 0.5)
		var q2 byte
		if i+1 < len(src) {
			norm2 := (src[i+1] + pi32) * inv2Pi
			if norm2 < 0 {
				norm2 = 0
			} else if norm2 > 1 {
				norm2 = 1
			}
			q2 = byte(norm2*maxVal + 0.5)
		}
		dst[i/2] = q1 | (q2 << 4)
	}
}

func PackTQ8Generic(src []float32, dst []byte) {
	maxVal := float32(255)
	inv2Pi := float32(1.0 / (2 * math.Pi))
	pi32 := float32(math.Pi)
	for i, val := range src {
		norm := (val + pi32) * inv2Pi
		if norm < 0 {
			norm = 0
		} else if norm > 1 {
			norm = 1
		}
		dst[i] = byte(norm*maxVal + 0.5)
	}
}

func l2SquaredTQCorrectionGeneric(query, recon []float32, qjlBits []byte, correction float32, n int) float32 {
	var sum float32
	i := 0
	// 8x unrolling
	for ; i <= n-8; i += 8 {
		bits := qjlBits[i/8]
		for j := 0; j < 8; j++ {
			idx := i + j
			val := recon[idx]
			if (bits>>uint(j))&1 != 0 {
				val += correction
			} else {
				val -= correction
			}
			diff := query[idx] - val
			sum += diff * diff
		}
	}
	// Remainder
	for ; i < n; i++ {
		val := recon[i]
		if (qjlBits[i/8]>>(i%8))&1 != 0 {
			val += correction
		} else {
			val -= correction
		}
		diff := query[i] - val
		sum += diff * diff
	}
	return sum
}
