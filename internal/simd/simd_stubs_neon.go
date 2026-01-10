//go:build !arm64

package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func euclideanF16NEON(a, b []float16.Num) float32 { return euclideanF16Unrolled4x(a, b) }
func dotF16NEON(a, b []float16.Num) float32       { return dotF16Unrolled4x(a, b) }
