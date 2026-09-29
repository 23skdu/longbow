//go:build !amd64

package store

// temporalHasAVX2 reports whether the CPU implements the AVX2 subset used by
// the columnar temporal lower bound. There is no AVX2 assembly kernel outside
// amd64, so the portable scalar search is always used.
var temporalHasAVX2 = false

// temporalLowerBoundAVX2 returns the index of the first timestamp >= x.
func temporalLowerBoundAVX2(ts []int64, x int64) int {
	return lowerBoundScalar(ts, x)
}
