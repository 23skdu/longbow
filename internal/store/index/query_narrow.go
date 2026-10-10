package index

import "math"

// Query narrowing for the integer element types.
//
// The query arrives over the wire as []float32 while the corpus it is compared
// against lives in the dataset's own integer domain. The conversion between the
// two is a truncating cast in the Go language, and a float-to-integer conversion
// whose value is outside the destination type is implementation-defined: on
// amd64 and arm64 it yields the "integer indefinite" value, so a query
// component of 1e9 against an int8 dataset becomes -128. That is not a
// rounding error, it is a sign flip on an arbitrary component, and it silently
// returns neighbours that have nothing to do with the query.
//
// It also made the benchmark matrix meaningless for the narrow types. See
// docs/roadmap.md item 1 (R32).
//
// This file defines the conversion so that it is total, defined, and
// in-domain-preserving: a component already inside the destination range is
// rounded to the nearest representable value and nothing else changes, so a
// client that sends queries in the corpus's domain - the documented contract -
// sees no behavioural difference at all. Only components that would have been
// implementation-defined are pulled back to the range edge.

const (
	// narrowInt8Max etc. are the largest int8 value, 127. The type's minimum,
	// -128, is excluded from the symmetric range on purpose: -128 has no
	// positive counterpart, so allowing it would let the narrow query range be
	// asymmetric and make the rounding below unrepresentable for scale factors
	// derived from it.
	narrowInt8Max = 127.0
	narrowInt8Min = -127.0

	narrowUint8Max = 255.0
	narrowUint8Min = 0.0

	narrowInt16Max = 32767.0
	narrowInt16Min = -32767.0

	narrowUint16Max = 65535.0
	narrowUint16Min = 0.0

	narrowInt32Max = math.MaxInt32
	narrowInt32Min = math.MinInt32

	narrowUint32Max = math.MaxUint32
	narrowUint32Min = 0.0
)

// narrowRound clamps v into [lo, hi] and rounds it to the nearest integer,
// halves away from zero. math.Round is used rather than a bare cast because
// truncation biases every in-domain fractional component downward, which for a
// normalized embedding is a systematic bias in the query direction rather than
// noise.
func narrowRound(v, lo, hi float32) float32 {
	if v <= lo {
		return lo
	}
	if v >= hi {
		return hi
	}
	r := float32(math.Round(float64(v)))
	if r <= lo {
		return lo
	}
	if r >= hi {
		return hi
	}
	return r
}

func narrowInt8(v float32) int8 {
	return int8(narrowRound(v, narrowInt8Min, narrowInt8Max)) // #nosec G115 -- narrowRound clamps into [-127,127]
}

func narrowUint8(v float32) uint8 {
	return uint8(narrowRound(v, narrowUint8Min, narrowUint8Max)) // #nosec G115 -- narrowRound clamps into [0,255]
}

func narrowInt16(v float32) int16 {
	return int16(narrowRound(v, narrowInt16Min, narrowInt16Max)) // #nosec G115 -- narrowRound clamps into [-32767,32767]
}

func narrowUint16(v float32) uint16 {
	return uint16(narrowRound(v, narrowUint16Min, narrowUint16Max)) // #nosec G115 -- narrowRound clamps into [0,65535]
}

func narrowInt32(v float32) int32 {
	return int32(narrowRound(v, narrowInt32Min, narrowInt32Max)) // #nosec G115 -- the clamp is exact for int32
}

func narrowUint32(v float32) uint32 {
	r := narrowRound(v, narrowUint32Min, narrowUint32Max) // #nosec G115 -- the clamp is exact for uint32
	if r < 0 {
		return 0
	}
	return uint32(r) // #nosec G115 -- r is in [0, MaxUint32]
}

// narrowFloatsToInt8 converts a float32 query into the int8 domain.
func narrowFloatsToInt8(src []float32, dst []int8) {
	for i, v := range src {
		dst[i] = narrowInt8(v)
	}
}

// narrowFloatsToUint8 converts a float32 query into the uint8 domain.
func narrowFloatsToUint8(src []float32, dst []uint8) {
	for i, v := range src {
		dst[i] = narrowUint8(v)
	}
}

// narrowFloatsToInt16 converts a float32 query into the int16 domain.
func narrowFloatsToInt16(src []float32, dst []int16) {
	for i, v := range src {
		dst[i] = narrowInt16(v)
	}
}

// narrowFloatsToUint16 converts a float32 query into the uint16 domain.
func narrowFloatsToUint16(src []float32, dst []uint16) {
	for i, v := range src {
		dst[i] = narrowUint16(v)
	}
}

// narrowFloatsToInt32 converts a float32 query into the int32 domain.
func narrowFloatsToInt32(src []float32, dst []int32) {
	for i, v := range src {
		dst[i] = narrowInt32(v)
	}
}

// narrowFloatsToUint32 converts a float32 query into the uint32 domain.
func narrowFloatsToUint32(src []float32, dst []uint32) {
	for i, v := range src {
		dst[i] = narrowUint32(v)
	}
}

// The resize helpers below grow a pooled query buffer to exactly n elements.
// They replace the `buf = buf[:0]` + append loop they used to stand in for:
// append grows geometrically, so a search that alternated between two dimension
// counts reallocated on every other call, while an exact resize reuses the
// buffer whenever it is already large enough and never copies in the common
// case where the dimension count did not change.

func resizeInt8(b []int8, n int) []int8 {
	if cap(b) < n {
		return make([]int8, n)
	}
	return b[:n]
}

func resizeUint8(b []uint8, n int) []uint8 {
	if cap(b) < n {
		return make([]uint8, n)
	}
	return b[:n]
}

func resizeInt16(b []int16, n int) []int16 {
	if cap(b) < n {
		return make([]int16, n)
	}
	return b[:n]
}

func resizeUint16(b []uint16, n int) []uint16 {
	if cap(b) < n {
		return make([]uint16, n)
	}
	return b[:n]
}

func resizeInt32(b []int32, n int) []int32 {
	if cap(b) < n {
		return make([]int32, n)
	}
	return b[:n]
}

func resizeUint32(b []uint32, n int) []uint32 {
	if cap(b) < n {
		return make([]uint32, n)
	}
	return b[:n]
}
