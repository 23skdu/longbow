//go:build !arm64 || noasm

package query

import "unsafe"

func fastPathInt64EqualNEONKernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	panic("NEON not supported on this platform")
}

func fastPathInt32EqualNEONKernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	panic("NEON not supported on this platform")
}

func fastPathFloat64EqualNEONKernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	panic("NEON not supported on this platform")
}

func fastPathFloat32EqualNEONKernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	panic("NEON not supported on this platform")
}
