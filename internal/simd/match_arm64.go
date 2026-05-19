//go:build arm64
// +build arm64

package simd

import (
	"unsafe"
)

//go:noescape
func matchInt64NeonKernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchInt32NeonKernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat32NeonKernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat64NeonKernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int)
