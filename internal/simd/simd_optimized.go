package simd

import (
	"unsafe"
)

// euclideanComplex64Optimized uses unsafe casting to reuse float32 SIMD kernels
func euclideanComplex64Optimized(a, b []complex64) float32 {
	if len(a) != len(b) {
		panic("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0
	}
	// Cast to float32 slice (len*2)
	vfA := unsafe.Slice((*float32)(unsafe.Pointer(&a[0])), len(a)*2)
	vfB := unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)*2)

	// Call the function pointer directly to avoid wrapper overhead
	return euclideanDistanceImpl(vfA, vfB)
}
