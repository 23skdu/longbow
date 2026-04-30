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

		// Conversions
		Int8ToFloat32:   int8ToFloat32NEON,
		Uint8ToFloat32:  uint8ToFloat32NEON,
		Int16ToFloat32:  int16ToFloat32NEON,
		Uint16ToFloat32: uint16ToFloat32NEON,
		Int32ToFloat32:  int32ToFloat32NEON,
		Uint32ToFloat32: uint32ToFloat32NEON,
		Float16ToFloat32: float16ToFloat32NEON,

		// Activations
		Sigmoid: sigmoidNEON,
		Softmax: softmaxNEON,
		Exp:     expNEON,
		Log:     logNEON,
	}
}
