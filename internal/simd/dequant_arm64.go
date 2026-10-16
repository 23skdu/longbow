//go:build arm64

package simd

import (
	"unsafe"
)

func dequantL2Float32Uint8NEONKernel(q, v unsafe.Pointer, n int, minV, scale float32) float32

func init() {
	dequantL2Float32Uint8Impl = dequantL2Float32Uint8NEON
}

func dequantL2Float32Uint8NEON(q []float32, v []uint8, minV, scale float32) float32 {
	n := len(q)
	if len(v) < n {
		n = len(v)
	}
	if n < 8 {
		return dequantL2Float32Uint8Generic(q[:n], v[:n], minV, scale)
	}
	// #nosec G103 -- SIMD kernel pointer passing requires unsafe.Pointer
	return dequantL2Float32Uint8NEONKernel(unsafe.Pointer(&q[0]), unsafe.Pointer(&v[0]), n, minV, scale)
}
