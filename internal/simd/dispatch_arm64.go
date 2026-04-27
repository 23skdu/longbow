//go:build arm64

package simd

// ARM64-specific dispatch table entries
func init() {
	dispatchTable["neon"] = &ImplementationDispatch{
		EuclideanDistance:          euclideanNEON,
		CosineDistance:             cosineNEON,
		DotProduct:                 dotNEON,
		EuclideanDistanceBatch:     euclideanBatchNEON,
		CosineDistanceBatch:        cosineBatchNEON,
		DotProductBatch:            dotBatchNEON,

		EuclideanDistance128:  euclidean128NEON,
		EuclideanDistance384:  euclidean384NEON,
		EuclideanDistance768:  euclidean768NEON,
		EuclideanDistance1024: euclidean1024NEON,
		EuclideanDistance1536: euclidean1536NEON,
		EuclideanDistance3072: euclidean3072NEON,

		DotProduct128:  dot128NEON,
		DotProduct384:  dot384NEON,
		DotProduct768:  dot768NEON,
		DotProduct1024: dot1024NEON,
		DotProduct1536: dot1536NEON,
		DotProduct3072: dot3072NEON,
	}
}
