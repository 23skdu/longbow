//go:build amd64

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
	return euclideanSQ8AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func euclideanSQ8AVX512(a, b []byte) (int32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX512 {
		return euclideanSQ8AVX2(a, b)
	}
	return euclideanSQ8AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

//go:noescape
func euclideanSQ8AVX2Kernel(a, b unsafe.Pointer, n int) int32

//go:noescape
func euclideanSQ8AVX512Kernel(a, b unsafe.Pointer, n int) int32
