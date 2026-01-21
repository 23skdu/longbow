//go:build !amd64

package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func euclideanF16AVX2(a, b []float16.Num) (float32, error)   { return euclideanF16Unrolled4x(a, b) }
func euclideanF16AVX512(a, b []float16.Num) (float32, error) { return euclideanF16Unrolled4x(a, b) }
func dotF16AVX2(a, b []float16.Num) (float32, error)         { return dotF16Unrolled4x(a, b) }
func dotF16AVX512(a, b []float16.Num) (float32, error)       { return dotF16Unrolled4x(a, b) }
func cosineF16AVX2(a, b []float16.Num) (float32, error)      { return cosineF16Unrolled4x(a, b) }
func cosineF16AVX512(a, b []float16.Num) (float32, error)    { return cosineF16Unrolled4x(a, b) }
