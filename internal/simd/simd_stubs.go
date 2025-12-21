//go:build !amd64

package simd

// Stubs for non-AMD64 architectures to satisfy simd.go references

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) {
	matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) {
	matchFloat32Generic(src, val, op, dst)
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) {
	matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) {
	matchFloat32Generic(src, val, op, dst)
}
