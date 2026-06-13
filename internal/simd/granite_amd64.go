//go:build amd64 && granite
// +build amd64,granite

package simd

import "github.com/apache/arrow-go/v18/arrow/float16"

// Granite Rapids AMX-FP16/COMPLEX kernel Go wrappers.
// These extend Emerald Rapids features with AMX-FP16 (TDPFP16PS)
// for native float16 tile matrix multiply and AMX-COMPLEX for
// complex number tile operations.
//
// Real AMX-FP16 assembly kernels (TDPFP16PS, etc.) should be
// implemented in granite_amd64.s when available.
// Until then, these delegate to generic implementations.

func euclideanF16AMX(a, b []float16.Num) (float32, error) {
	return euclideanF16Unrolled4x(a, b)
}

func dotF16AMX(a, b []float16.Num) (float32, error) {
	return dotF16Unrolled4x(a, b)
}

func matMulF16AMX(a, b []float16.Num, m, n, k int, dst []float16.Num) {
	fa := make([]float32, len(a))
	fb := make([]float32, len(b))
	fdst := make([]float32, len(dst))
	float16ToFloat32Generic(a, fa)
	float16ToFloat32Generic(b, fb)
	matMulGeneric(fa, fb, m, n, k, fdst)
	for i := range dst {
		dst[i] = float16.New(fdst[i])
	}
}
