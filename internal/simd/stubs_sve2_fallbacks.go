//go:build !arm64

package simd

func euclideanSVE2(a, b []float32) (float32, error) {
	return EuclideanDistance(a, b)
}

func l2SquaredSVE2(a, b []float32) float32 {
	d, _ := L2Squared(a, b)
	return d
}

func dotSVE2(a, b []float32) float32 {
	d, _ := DotProduct(a, b)
	return d
}

func cosineSVE2(a, b []float32) (float32, error) {
	return CosineDistance(a, b)
}
