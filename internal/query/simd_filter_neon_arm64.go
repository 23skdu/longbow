//go:build arm64 && !noasm

package query

import "unsafe"

// fastPathInt64EqualNEONKernel compares src[0:n] with val and writes results to result[0:n]
// result[i] will be 0xFFFFFFFFFFFFFFFF if src[i] == val, 0 otherwise.
//go:noescape
func fastPathInt64EqualNEONKernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)

// fastPathInt32EqualNEONKernel compares src[0:n] with val and writes results to result[0:n]
// result[i] will be 0xFFFFFFFF if src[i] == val, 0 otherwise.
//go:noescape
func fastPathInt32EqualNEONKernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)

// fastPathFloat64EqualNEONKernel compares src[0:n] with val and writes results to result[0:n]
//go:noescape
func fastPathFloat64EqualNEONKernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)

// fastPathFloat32EqualNEONKernel compares src[0:n] with val and writes results to result[0:n]
//go:noescape
func fastPathFloat32EqualNEONKernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)
