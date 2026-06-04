//go:build amd64

package tensor

// gemm4x8KernelPacked computes C[0:4][0:8] += A[0:4][0:k] * Bpacked[0:k][0:8]
// for float32 row-major matrices, where Bpacked is K×8 contiguous (stride = 8).
//
// a:     A panel (4 rows × k cols, stride = lda elements)
// b:     B panel packed K×8 contiguous (stride = 8 elements, 32 bytes)
// c:     C tile (stride = ldc elements, accumulated in place)
// k:     inner dimension
// lda:    leading dimension of A (stride between A rows in float32 elements)
// ldc:   leading dimension of C (stride between C rows in float32 elements)
//
//go:noescape
func gemm4x8KernelPacked(a, b, c uintptr, k int, lda int, ldc int)
