// +build amd64,!nosimd

package query

import (
	"unsafe"
)

// fastPathInt32EqualAVX2Kernel compares an int32 slice with a scalar using AVX2.
// Returns a bitmask (1 bit per element).
//go:noescape
func fastPathInt32EqualAVX2Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)

// fastPathFloat32EqualAVX2Kernel compares a float32 slice with a scalar using AVX2.
//go:noescape
func fastPathFloat32EqualAVX2Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)

// fastPathBoolAVX2Kernel applies bitwise NOT or identity to an Arrow bit-packed buffer.
//go:noescape
func fastPathBoolAVX2Kernel(src unsafe.Pointer, nBytes int, negate bool, result unsafe.Pointer)

// fastPathStringEqualAVX2Kernel compares a string array's data with a target string.
//go:noescape
func fastPathStringEqualAVX2Kernel(offsets unsafe.Pointer, data unsafe.Pointer, n int, target unsafe.Pointer, targetLen int, result unsafe.Pointer)

// fastPathInt64EqualAVX2Kernel compares an int64 slice with a scalar using AVX2.
//go:noescape
func fastPathInt64EqualAVX2Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)

// fastPathFloat64EqualAVX2Kernel compares a float64 slice with a scalar using AVX2.
//go:noescape
func fastPathFloat64EqualAVX2Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)
