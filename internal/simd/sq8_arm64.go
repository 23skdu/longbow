//go:build arm64

package simd

import "unsafe"

func init() {
	if features.HasNEON {
		euclideanSQ8Impl = euclideanSQ8NEON
	}
}

//go:noescape
func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32

func euclideanSQ8NEON(a, b []byte) int32 {
	return EuclideanSQ8Generic(a, b)
}
