//go:build amd64

package simd

// x86_64-specific dispatch table - AVX2 only (AVX512 disabled for now)
func init() {
	dispatchTable["avx2"].EuclideanDistance = euclideanAVX2
	dispatchTable["avx2"].CosineDistance = cosineAVX2
	dispatchTable["avx2"].DotProduct = dotAVX2
	dispatchTable["avx2"].EuclideanDistanceBatch = euclideanBatchGeneric
	dispatchTable["avx2"].CosineDistanceBatch = cosineBatchGeneric
	dispatchTable["avx2"].DotProductBatch = dotBatchGeneric
	dispatchTable["avx2"].EuclideanDistanceBatchFlat = euclideanBatchFlatGeneric

	dispatchTable["avx2"].EuclideanDistance128 = euclidean128Unrolled4x
	dispatchTable["avx2"].EuclideanDistance384 = euclidean384AVX2
	dispatchTable["avx2"].EuclideanDistance768 = euclidean768AVX2
	dispatchTable["avx2"].EuclideanDistance1024 = euclidean1024Blocked
	dispatchTable["avx2"].EuclideanDistance1536 = euclidean1536AVX2
	dispatchTable["avx2"].EuclideanDistance3072 = euclidean3072Blocked

	dispatchTable["avx2"].DotProduct128 = dot128Unrolled4x
	dispatchTable["avx2"].DotProduct384 = dotGeneric
	dispatchTable["avx2"].DotProduct768 = dotGeneric
	dispatchTable["avx2"].DotProduct1024 = dotAVX2
	dispatchTable["avx2"].DotProduct1536 = dotAVX2
	dispatchTable["avx2"].DotProduct3072 = DotProductFloat32Blocked
	dispatchTable["avx2"].Sum = sumAVX2
	dispatchTable["avx2"].Max = maxAVX2
	dispatchTable["avx2"].Min = minAVX2
	dispatchTable["avx2"].Exp = expAVX2
	dispatchTable["avx2"].Log = logAVX2
	dispatchTable["avx2"].Softmax = softmaxAVX2
	dispatchTable["avx2"].Sigmoid = sigmoidAVX2
}
