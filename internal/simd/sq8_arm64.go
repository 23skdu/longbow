//go:build arm64

package simd

func init() {
	if features.HasNEON {
		euclideanSQ8Impl = euclideanSQ8NEON
	}
}

func euclideanSQ8NEON(a, b []byte) int32 {
	return EuclideanSQ8Generic(a, b)
}
