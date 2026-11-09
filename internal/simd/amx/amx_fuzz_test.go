package amx

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// f32le encodes floats as a little-endian []byte blob so they can travel
// through the fuzzing engine, which only accepts scalars, strings and []byte.
func f32le(vals ...float32) []byte {
	buf := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(v))
	}
	return buf
}

// decodeF32 interprets the blob as little-endian float32 values, ignoring any
// trailing bytes that do not form a complete 4-byte element.
func decodeF32(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	for i := 0; i < n; i++ {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return out
}

// FuzzAMXVectorOps asserts the vector-kernel contract shared by the stub and
// the darwin implementation: no error for bounded inputs, deterministic
// results, untouched operands, and exact arithmetic identities (zero vector,
// self-distance, swap symmetry).
func FuzzAMXVectorOps(f *testing.F) {
	// Seeds: valid pairs, empty operands, zeros, identical operands,
	// non-trivial floats and wide magnitude ranges.
	f.Add(f32le(1, 2, 3, 4), f32le(5, 6, 7, 8))
	f.Add(f32le(), f32le())
	f.Add(f32le(), f32le(1, 2, 3))
	f.Add(f32le(0, 0, 0), f32le(0, 0, 0))
	f.Add(f32le(1.5, -2.5, 0.1), f32le(1.5, -2.5, 0.1))
	f.Add(f32le(0.1, 0.2, 0.3), f32le(1, 2, 3))
	f.Add(f32le(1), f32le(1))
	f.Add(f32le(-1e6, 1e6), f32le(1e-6, -1e-6))

	f.Fuzz(func(t *testing.T, rawA, rawB []byte) {
		// Bound the decoded vectors to 1024 elements (4 KiB each).
		if len(rawA) > 4096 || len(rawB) > 4096 {
			return
		}
		a := decodeF32(rawA)
		b := decodeF32(rawB)

		// NaN is out of range (exact identities cannot hold), and operands
		// are magnitude-capped so every product, difference and partial sum
		// stays finite: no overflow to Inf, hence no Inf-Inf = NaN.
		for _, v := range a {
			fv := float64(v)
			if math.IsNaN(fv) || fv > 1e17 || fv < -1e17 {
				return
			}
		}
		for _, v := range b {
			fv := float64(v)
			if math.IsNaN(fv) || fv > 1e17 || fv < -1e17 {
				return
			}
		}

		// The darwin implementation reads b[0:len(a)]; a shorter b would be
		// an out-of-range pairing for it.
		if len(a) > 0 && len(b) < len(a) {
			return
		}

		a0 := slices.Clone(a)
		b0 := slices.Clone(b)

		dot1, err1 := DotAMX(a, b)
		require.NoError(t, err1)
		dot2, err2 := DotAMX(a, b)
		require.NoError(t, err2)
		require.Equal(t, dot1, dot2, "DotAMX must be deterministic")

		l21, l2err1 := L2AMX(a, b)
		require.NoError(t, l2err1)
		l22, l2err2 := L2AMX(a, b)
		require.NoError(t, l2err2)
		require.Equal(t, l21, l22, "L2AMX must be deterministic")

		require.Equal(t, a0, a, "input A must not be mutated")
		require.Equal(t, b0, b, "input B must not be mutated")

		// Empty first operand: both the stub and the darwin path return
		// exactly zero without touching the other operand.
		if len(a) == 0 {
			require.Equal(t, float32(0), dot1, "empty input must yield 0")
		}

		// Zero first vector: every product term is exactly zero.
		if len(a) > 0 {
			allZero := true
			for _, v := range a {
				if v != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				require.Equal(t, float32(0), dot1, "dot with zero vector must be 0")
			}
		}

		// Squared distance is a sum of squares: never negative.
		require.False(t, l21 < 0, "L2 distance must be non-negative, got %v", l21)

		// A vector is at zero distance from itself.
		l2Same, l2sameErr := L2AMX(a, a)
		require.NoError(t, l2sameErr)
		require.Equal(t, float32(0), l2Same, "L2 of a vector with itself must be 0")

		// Equal-length operands: dot and L2 are symmetric (products and
		// squared differences are bit-wise commutative, same accumulation
		// order on both platforms).
		if len(a) == len(b) {
			dotSwap, dotSwapErr := DotAMX(b, a)
			require.NoError(t, dotSwapErr)
			require.Equal(t, dot1, dotSwap, "DotAMX must be symmetric")

			l2Swap, l2SwapErr := L2AMX(b, a)
			require.NoError(t, l2SwapErr)
			require.Equal(t, l21, l2Swap, "L2AMX must be symmetric")
		}
	})
}
