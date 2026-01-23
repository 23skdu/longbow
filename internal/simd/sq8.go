package simd

import "errors"

var (
	// Function pointer for SQ8 distance
	euclideanSQ8Impl func(a, b []byte) (int32, error)
)

func init() {
	euclideanSQ8Impl = EuclideanSQ8Generic
	// Architecture specific overrides will be set in their respective files or here if exported?
	// Actually, usually we set the global variable in init() but we need to know features.
	// features are in simd.go.
	// We can update the pointer in a wrapper init or expose a SetUp function.
	// Note: simd.go likely has an init() that runs. We should ensure order or do it lazily?
	// Easier: Just let init() in this file set default, and platform specific files use init to override
	// provided they run after features calculation.
	// BUT features calculation is in simd.go init().
	// Go init order is file name lexical? No.
	// Safe way: call setup in simd.go or usage lazy load.
	// Let's use lazy initialization or rely on simd package init.
	// Actually simd package init calculates features.
	// We can just use a function that checks implementation string or features struct.
}

// EuclideanDistanceSQ8 computes the Euclidean distance between two uint8 vectors.
// It returns the squared Euclidean distance as an int32 to avoid overflow and expensive sqrt.
// The actual float distance would be scale * scale * distance.
// Arguments:
//
//	a, b: Quantized vectors (uint8)
//
// Returns:
//
//	Squared L2 distance (int32)
func EuclideanDistanceSQ8(a, b []byte) (int32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	// Direct Call to function pointer
	return euclideanSQ8Impl(a, b)
}

func EuclideanSQ8Generic(a, b []byte) (int32, error) {
	var sum int32
	i := 0
	// Unroll 8x
	for ; i <= len(a)-8; i += 8 {
		d0 := int32(a[i+0]) - int32(b[i+0])
		d1 := int32(a[i+1]) - int32(b[i+1])
		d2 := int32(a[i+2]) - int32(b[i+2])
		d3 := int32(a[i+3]) - int32(b[i+3])
		d4 := int32(a[i+4]) - int32(b[i+4])
		d5 := int32(a[i+5]) - int32(b[i+5])
		d6 := int32(a[i+6]) - int32(b[i+6])
		d7 := int32(a[i+7]) - int32(b[i+7])
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3 + d4*d4 + d5*d5 + d6*d6 + d7*d7
	}
	// Tail
	for ; i < len(a); i++ {
		d := int32(a[i]) - int32(b[i])
		sum += d * d
	}
	return sum, nil
}

// QuantizeSQ8 converts a float32 vector to uint8 using min/max bounds.
// dst must be pre-allocated with correct length.
func QuantizeSQ8(src []float32, dst []byte, minVal, maxVal float32) {
	scale := 255.0 / (maxVal - minVal)
	if maxVal == minVal {
		scale = 0
	}

	for i, v := range src {
		val := (v - minVal) * scale
		if val < 0 {
			val = 0
		}
		if val > 255 {
			val = 255
		}
		dst[i] = byte(val)
	}
}

// ComputeBounds calculates min and max values of a vector.
func ComputeBounds(vec []float32) (minVal, maxVal float32) {
	if len(vec) == 0 {
		return 0, 0
	}
	minVal = vec[0]
	maxVal = vec[0]
	for _, v := range vec[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal
}
