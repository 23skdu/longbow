//go:build amd64 && !nosimd
// +build amd64,!nosimd

package query

import (
	"unsafe"
)

// fastPathInt32EqualAVX512Kernel compares an int32 slice with a scalar using AVX-512.
//go:noescape
func fastPathInt32EqualAVX512Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)

// fastPathFloat32EqualAVX512Kernel compares a float32 slice with a scalar using AVX-512.
//go:noescape
func fastPathFloat32EqualAVX512Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)

// fastPathInt64EqualAVX512Kernel compares an int64 slice with a scalar using AVX-512.
//go:noescape
func fastPathInt64EqualAVX512Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)

// fastPathFloat64EqualAVX512Kernel compares a float64 slice with a scalar using AVX-512.
//go:noescape
func fastPathFloat64EqualAVX512Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)
