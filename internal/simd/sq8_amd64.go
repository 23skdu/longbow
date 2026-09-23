//go:build amd64
// +build amd64

package simd

import (
	"unsafe"
)

func euclideanSQ8AVX2(a, b []byte) (int32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanSQ8AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])), len(a)), nil // #nosec G103
}
