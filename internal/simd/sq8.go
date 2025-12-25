package simd



// EuclideanDistanceSQ8 computes the Euclidean distance between two uint8 vectors.
// It returns the squared Euclidean distance as an int32 to avoid overflow and expensive sqrt.
// The actual float distance would be scale * scale * distance.
// Arguments:
//   a, b: Quantized vectors (uint8)
// Returns:
//   Squared L2 distance (int32)
func EuclideanDistanceSQ8(a, b []byte) int32 {
	if len(a) != len(b) {
		panic("simd: vector length mismatch")
	}
	// TODO: Add AVX2/NEON implementation
	return euclideanSQ8Generic(a, b)
}

func euclideanSQ8Generic(a, b []byte) int32 {
	var sum int32
	for i := 0; i < len(a); i++ {
		d := int32(a[i]) - int32(b[i])
		sum += d * d
	}
	return sum
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
func ComputeBounds(vec []float32) (float32, float32) {
	if len(vec) == 0 {
		return 0, 0
	}
	minVal := vec[0]
	maxVal := vec[0]
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
