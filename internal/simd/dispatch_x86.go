//go:build amd64

package simd

// x86_64-specific dispatch table - AVX2 only (AVX512 disabled for now)
func init() {
	// Core distance functions - AVX2 optimized
	dispatchTable["avx2"].EuclideanDistance = euclideanAVX2
	dispatchTable["avx2"].CosineDistance = cosineAVX2
	dispatchTable["avx2"].DotProduct = dotAVX2

	// Fallbacks for complex or batch operations
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
	dispatchTable["avx2"].DotProduct1024 = dotGeneric
	dispatchTable["avx2"].DotProduct1536 = dotGeneric
	dispatchTable["avx2"].DotProduct3072 = DotProductFloat32Blocked

	// Use generic for all reductions and activations on AVX2 for now (stubs in asm)
	dispatchTable["avx2"].Sum = sumFloat32Generic
	dispatchTable["avx2"].Max = maxFloat32Generic
	dispatchTable["avx2"].Min = minFloat32Generic
	dispatchTable["avx2"].Exp = expGeneric
	dispatchTable["avx2"].Log = logGeneric
	dispatchTable["avx2"].Sin = sinFloat32Generic
	dispatchTable["avx2"].Cos = cosFloat32Generic
	dispatchTable["avx2"].Atan2 = atan2Float32Generic
	dispatchTable["avx2"].Softmax = softmaxGeneric
	dispatchTable["avx2"].Sigmoid = sigmoidGeneric
	dispatchTable["avx2"].ArgMax = argMaxGeneric
	dispatchTable["avx2"].ArgMin = argMinGeneric

	dispatchTable["avx2"].ManhattanDistance = ManhattanDistanceFloat32
	dispatchTable["avx2"].ChebyshevDistance = ChebyshevDistanceFloat32
	dispatchTable["avx2"].BrayCurtisDistance = BrayCurtisDistanceFloat32

	// Type conversion (mostly generic for now)
	dispatchTable["avx2"].Int8ToFloat32 = int8ToFloat32Generic
	dispatchTable["avx2"].Uint8ToFloat32 = uint8ToFloat32Generic
	dispatchTable["avx2"].Int16ToFloat32 = int16ToFloat32Generic
	dispatchTable["avx2"].Uint16ToFloat32 = uint16ToFloat32Generic
	dispatchTable["avx2"].Int32ToFloat32 = int32ToFloat32Generic
	dispatchTable["avx2"].Uint32ToFloat32 = uint32ToFloat32Generic
	dispatchTable["avx2"].Float16ToFloat32 = float16ToFloat32Generic
}
