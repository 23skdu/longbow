//go:build arm64

package simd

import (
	"errors"
	"unsafe"
)

func init() {
	if features.HasNEON {
		euclideanSQ8Impl = euclideanSQ8NEON
	}
}

//go:noescape
func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32

func euclideanSQ8NEON(a, b []byte) (int32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanSQ8NEONKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}
