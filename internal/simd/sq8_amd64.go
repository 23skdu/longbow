//go:build amd64
// +build amd64

package simd

import (
	"unsafe"
)

// init runs after simd.go's init (lexical order usually, but safer to rely on internal check)
// But since we are in same package, order is file name based. simd.go comes before sq8_amd64.go
func init() {
	if features.HasAVX2 {
		euclideanSQ8Impl = euclideanSQ8AVX2
	}
}

func euclideanSQ8AVX2(a, b []byte) (int32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanSQ8AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])), len(a)), nil
}

/*
func euclideanSQ8AVX512(a, b []byte) (int32, error) {
	// Use AVX2 kernel as fallback (AVX512 assembly is in separate file)
	return euclideanSQ8AVX2(a, b)
}
*/

// euclideanSQ8AVX2Kernel is now in all_kernels_stubs_amd64.go
