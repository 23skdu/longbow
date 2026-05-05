package simd
import "testing"
func TestStubs_Coverage(t *testing.T) {
  // Calling matchInt64AVX2
  _ = matchInt64AVX2(nil, 0, 0, nil)
  // Calling matchFloat32AVX2
  _ = matchFloat32AVX2(nil, 0, 0, nil)
  // Calling matchFloat64AVX2
  _ = matchFloat64AVX2(nil, 0, 0, nil)
  // Calling matchFloat64AVX512
  _ = matchFloat64AVX512(nil, 0, 0, nil)
  // Calling matchInt64AVX512
  _ = matchInt64AVX512(nil, 0, 0, nil)
  // Calling matchFloat32AVX512
  _ = matchFloat32AVX512(nil, 0, 0, nil)
  // Calling adcBatchAVX2
  _ = adcBatchAVX2(nil, nil, 0, nil)
  // Calling adcBatchAVX512
  _ = adcBatchAVX512(nil, nil, 0, nil)
  // Calling adcBatchVNNI
  _ = adcBatchVNNI(nil, nil, 0, nil)
  // Calling euclideanAVX2
  _, _ = euclideanAVX2(nil, nil)
  // Calling euclideanAVX512
  _, _ = euclideanAVX512(nil, nil)
  // Calling cosineAVX2
  _, _ = cosineAVX2(nil, nil)
  // Calling cosineAVX512
  _, _ = cosineAVX512(nil, nil)
  // Calling dotAVX2
  _, _ = dotAVX2(nil, nil)
  // Calling dotAVX512
  _, _ = dotAVX512(nil, nil)
  // Calling euclidean384AVX512
  _, _ = euclidean384AVX512(nil, nil)
  // Calling euclidean768AVX512
  _, _ = euclidean768AVX512(nil, nil)
  // Calling euclidean1024AVX512
  _, _ = euclidean1024AVX512(nil, nil)
  // Calling euclidean1536AVX512
  _, _ = euclidean1536AVX512(nil, nil)
  // Calling euclidean3072AVX512
  _, _ = euclidean3072AVX512(nil, nil)
  // Calling euclidean384AVX2
  _, _ = euclidean384AVX2(nil, nil)
  // Calling euclidean768AVX2
  _, _ = euclidean768AVX2(nil, nil)
  // Calling euclidean1024AVX2
  _, _ = euclidean1024AVX2(nil, nil)
  // Calling euclidean1536AVX2
  _, _ = euclidean1536AVX2(nil, nil)
  // Calling euclidean3072AVX2
  _, _ = euclidean3072AVX2(nil, nil)
  // Calling dot384AVX512
  _, _ = dot384AVX512(nil, nil)
  // Calling dot768AVX512
  _, _ = dot768AVX512(nil, nil)
  // Calling dot1024AVX512
  _, _ = dot1024AVX512(nil, nil)
  // Calling dot1536AVX512
  _, _ = dot1536AVX512(nil, nil)
  // Calling dot3072AVX512
  _, _ = dot3072AVX512(nil, nil)
  // Calling dot384AVX2
  _, _ = dot384AVX2(nil, nil)
  // Calling dot768AVX2
  _, _ = dot768AVX2(nil, nil)
  // Calling dot1024AVX2
  _, _ = dot1024AVX2(nil, nil)
  // Calling dot1536AVX2
  _, _ = dot1536AVX2(nil, nil)
  // Calling dot3072AVX2
  _, _ = dot3072AVX2(nil, nil)
  // Calling euclideanBatchAVX2
  _ = euclideanBatchAVX2(nil, nil, nil)
  // Calling euclideanBatchAVX512
  _ = euclideanBatchAVX512(nil, nil, nil)
  // Calling dotBatchAVX2
  _ = dotBatchAVX2(nil, nil, nil)
  // Calling dotBatchAVX512
  _ = dotBatchAVX512(nil, nil, nil)
  // Calling cosineBatchAVX2
  _ = cosineBatchAVX2(nil, nil, nil)
  // Calling cosineBatchAVX512
  _ = cosineBatchAVX512(nil, nil, nil)
  // Calling euclideanVerticalBatchAVX2
  _ = euclideanVerticalBatchAVX2(nil, nil, nil)
  // Calling euclideanVerticalBatchAVX512
  _ = euclideanVerticalBatchAVX512(nil, nil, nil)
  prefetchNTA(0)
  // Calling dotFloat64AVX512
  _, _ = dotFloat64AVX512(nil, nil)
  // Calling haversineBatchAVX2
  haversineBatchAVX2(0, 0, nil, 0, nil)
  // Calling euclideanFloat64AVX512
  _, _ = euclideanFloat64AVX512(nil, nil)
  // Calling euclideanInt8AVX2
  _, _ = euclideanInt8AVX2(nil, nil)
  // Calling euclideanInt16AVX2
  _, _ = euclideanInt16AVX2(nil, nil)
  // Calling euclideanUint16AVX2
  _, _ = euclideanUint16AVX2(nil, nil)
  // Calling dotInt16AVX2
  _, _ = dotInt16AVX2(nil, nil)
  // Calling dotUint16AVX2
  _, _ = dotUint16AVX2(nil, nil)
  // Calling l2SquaredAVX2
  _, _ = l2SquaredAVX2(nil, nil)
  // Calling l2SquaredAVX512
  _, _ = l2SquaredAVX512(nil, nil)
  // Calling euclideanSQ8BatchAVX2
  _ = euclideanSQ8BatchAVX2(nil, nil, nil)
  // Calling euclideanSQ8BatchAVX512
  _ = euclideanSQ8BatchAVX512(nil, nil, nil)
  // Calling euclideanF16BatchAVX2
  _ = euclideanF16BatchAVX2(nil, nil, nil)
  // Calling euclideanF16BatchAVX512
  _ = euclideanF16BatchAVX512(nil, nil, nil)
  // Calling matchInt32AVX2
  _ = matchInt32AVX2(nil, 0, 0, nil)
  // Calling matchInt32AVX512
  _ = matchInt32AVX512(nil, 0, 0, nil)
  // Calling andBytesAVX2
  andBytesAVX2(nil, nil)
  // Calling orBytesAVX2
  orBytesAVX2(nil, nil)
  // Calling isAllZerosAVX2
  _ = isAllZerosAVX2(nil)
  // Calling dotFloat64AVX2
  _, _ = dotFloat64AVX2(nil, nil)
  // Calling euclideanFloat64AVX2
  _, _ = euclideanFloat64AVX2(nil, nil)
  // Calling dotInt4AVX512
  _, _ = dotInt4AVX512(nil, nil)
  // Calling dotInt4AVX2
  _, _ = dotInt4AVX2(nil, nil)
  // Calling dotInt2AVX512
  _, _ = dotInt2AVX512(nil, nil)
  // Calling dotInt2AVX2
  _, _ = dotInt2AVX2(nil, nil)
  // Calling euclidean16AVX512Wrapper
  _, _ = euclidean16AVX512Wrapper(nil, nil)
  // Calling cosine16AVX512Wrapper
  _, _ = cosine16AVX512Wrapper(nil, nil)
  // Calling andBytesAVX512
  andBytesAVX512(nil, nil)
  // Calling orBytesAVX512
  orBytesAVX512(nil, nil)
  // Calling isAllZerosAVX512
  _ = isAllZerosAVX512(nil)
  // Calling euclideanInt8AVX512
  _, _ = euclideanInt8AVX512(nil, nil)
  // Calling euclideanInt16AVX512
  _, _ = euclideanInt16AVX512(nil, nil)
  // Calling euclideanUint16AVX512
  _, _ = euclideanUint16AVX512(nil, nil)
  // Calling dotInt16AVX512
  _, _ = dotInt16AVX512(nil, nil)
  // Calling dotUint16AVX512
  _, _ = dotUint16AVX512(nil, nil)
  // Calling int8ToFloat32AVX2
  int8ToFloat32AVX2(nil, nil)
  // Calling uint8ToFloat32AVX2
  uint8ToFloat32AVX2(nil, nil)
  // Calling int16ToFloat32AVX2
  int16ToFloat32AVX2(nil, nil)
  // Calling uint16ToFloat32AVX2
  uint16ToFloat32AVX2(nil, nil)
  // Calling int32ToFloat32AVX2
  int32ToFloat32AVX2(nil, nil)
  // Calling uint32ToFloat32AVX2
  uint32ToFloat32AVX2(nil, nil)
  // Calling float16ToFloat32AVX2
  float16ToFloat32AVX2(nil, nil)
  // Calling int8ToFloat32AVX512
  int8ToFloat32AVX512(nil, nil)
  // Calling uint8ToFloat32AVX512
  uint8ToFloat32AVX512(nil, nil)
  // Calling int16ToFloat32AVX512
  int16ToFloat32AVX512(nil, nil)
  // Calling uint16ToFloat32AVX512
  uint16ToFloat32AVX512(nil, nil)
  // Calling int32ToFloat32AVX512
  int32ToFloat32AVX512(nil, nil)
  // Calling uint32ToFloat32AVX512
  uint32ToFloat32AVX512(nil, nil)
  // Calling float16ToFloat32AVX512
  float16ToFloat32AVX512(nil, nil)
  // Calling sigmoidAVX2
  sigmoidAVX2(nil, nil)
  // Calling softmaxAVX2
  softmaxAVX2(nil, nil)
  // Calling expAVX2
  expAVX2(nil, nil)
  // Calling logAVX2
  logAVX2(nil, nil)
  // Calling sigmoidAVX512
  sigmoidAVX512(nil, nil)
  // Calling softmaxAVX512
  softmaxAVX512(nil, nil)
  // Calling expAVX512
  expAVX512(nil, nil)
  // Calling logAVX512
  logAVX512(nil, nil)
  // Calling sumAVX2
  _ = sumAVX2(nil)
  // Calling maxAVX2
  _ = maxAVX2(nil)
  // Calling minAVX2
  _ = minAVX2(nil)
  // Calling sinAVX2
  sinAVX2(nil, nil)
  // Calling cosAVX2
  cosAVX2(nil, nil)
  // Calling atan2AVX2
  atan2AVX2(nil, nil, nil)
  // Calling matMulAVX2Go
  matMulAVX2Go(nil, nil, 0, 0, 0, nil)
  // Calling ManhattanDistanceFloat32AVX2
  _, _ = ManhattanDistanceFloat32AVX2(nil, nil)
  // Calling ChebyshevDistanceFloat32AVX2
  _, _ = ChebyshevDistanceFloat32AVX2(nil, nil)
  // Calling BrayCurtisDistanceFloat32AVX2
  _, _ = BrayCurtisDistanceFloat32AVX2(nil, nil)
}
