//go:build amd64

package simd

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasPOPCNT {
		hammingImpl = hammingAVX2
	}
}

//go:noescape
func hammingAVX2Kernel(a, b unsafe.Pointer, n int) int

func hammingAVX2(a, b []uint64) int {
	if len(a) == 0 {
		return 0
	}
	return hammingAVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}
