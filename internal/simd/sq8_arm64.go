//go:build arm64

package simd

func init() {
	// Stub disabled until implemented
	// if features.HasNEON {
	// 	euclideanSQ8Impl = euclideanSQ8NEON
	// }
}

// //go:noescape
// func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32

// func euclideanSQ8NEON(a, b []byte) int32 {
// 	if len(a) != len(b) || len(a) == 0 {
// 		return 0
// 	}
// 	return euclideanSQ8NEONKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
// }
