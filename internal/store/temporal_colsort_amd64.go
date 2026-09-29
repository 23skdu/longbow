//go:build amd64

package store

import "golang.org/x/sys/cpu"

// temporalHasAVX2 reports whether the CPU implements the AVX2 subset used by
// the columnar temporal lower bound.
var temporalHasAVX2 = cpu.X86.HasAVX2

//go:noescape
func temporalLowerBoundAVX2Asm(ts []int64, x int64) int

// temporalLowerBoundAVX2 returns the index of the first timestamp >= x.
func temporalLowerBoundAVX2(ts []int64, x int64) int {
	return temporalLowerBoundAVX2Asm(ts, x)
}
