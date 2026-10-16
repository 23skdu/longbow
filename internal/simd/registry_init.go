package simd

// registerDynamicKernels registers active kernels into the dynamic Registry.
func registerDynamicKernels(impl string) {
	// Float32 Euclidean
	Registry.Register(MetricEuclidean, DataTypeFloat32, 0, euclideanDistanceImpl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 128, euclideanDistance128Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 384, euclideanDistance384Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 768, euclideanDistance768Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1024, euclideanDistance1024Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1536, euclideanDistance1536Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 3072, euclideanDistance3072Impl)

	// Float32 Cosine & Dot Product
	Registry.Register(MetricCosine, DataTypeFloat32, 0, cosineDistanceImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 0, dotProductImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 128, dotProduct128Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 384, dotProduct384Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 768, dotProduct768Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1024, dotProduct1024Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1536, dotProduct1536Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 3072, dotProduct3072Impl)

	// Float16 (Support both native and unrolled paths)
	Registry.Register(MetricEuclidean, DataTypeFloat16, 0, euclideanDistanceF16Impl)
	Registry.Register(MetricCosine, DataTypeFloat16, 0, cosineDistanceF16Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat16, 0, dotProductF16Impl)

	// Complex Numbers (Unrolled Baselines)
	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanDistanceComplex64Impl)
	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanDistanceComplex128Impl)

	// Baseline Fallbacks for all other types
	Registry.Register(MetricEuclidean, DataTypeInt8, 0, euclideanDistanceInt8Impl)
	Registry.Register(MetricEuclidean, DataTypeInt8, 384, euclideanDistanceInt8Impl)
	Registry.Register(MetricEuclidean, DataTypeInt8, 768, euclideanDistanceInt8Impl)
	Registry.Register(MetricEuclidean, DataTypeInt8, 1024, euclideanDistanceInt8Impl)
	Registry.Register(MetricL2Squared, DataTypeInt8, 0, l2SquaredInt8Impl)
	Registry.Register(MetricCosine, DataTypeInt8, 0, cosineDistanceInt8Impl)
	Registry.Register(MetricDotProduct, DataTypeInt8, 0, dotProductInt8Impl)
	Registry.Register(MetricDotProduct, DataTypeInt8, 384, dotProductInt8Impl)
	Registry.Register(MetricDotProduct, DataTypeInt8, 768, dotProductInt8Impl)
	Registry.Register(MetricDotProduct, DataTypeInt8, 1024, dotProductInt8Impl)

	Registry.Register(MetricEuclidean, DataTypeInt16, 0, euclideanDistanceInt16Impl)
	Registry.Register(MetricCosine, DataTypeInt16, 0, CosineDistanceInt16)
	Registry.Register(MetricDotProduct, DataTypeInt16, 0, dotProductInt16Impl)

	Registry.Register(MetricEuclidean, DataTypeInt32, 0, EuclideanDistanceInt32)
	Registry.Register(MetricCosine, DataTypeInt32, 0, CosineDistanceInt32)
	Registry.Register(MetricDotProduct, DataTypeInt32, 0, DotProductInt32)

	Registry.Register(MetricEuclidean, DataTypeInt64, 0, EuclideanDistanceInt64)
	Registry.Register(MetricCosine, DataTypeInt64, 0, CosineDistanceInt64)
	Registry.Register(MetricDotProduct, DataTypeInt64, 0, DotProductInt64)

	Registry.Register(MetricEuclidean, DataTypeUint8, 0, euclideanDistanceUint8Impl)
	if impl != "generic" {
		Registry.Register(MetricEuclidean, DataTypeUint8, 384, euclideanDistanceUint8Impl)
		Registry.Register(MetricEuclidean, DataTypeUint8, 768, euclideanDistanceUint8Impl)
		Registry.Register(MetricEuclidean, DataTypeUint8, 1024, euclideanDistanceUint8Impl)
	} else {
		Registry.Register(MetricEuclidean, DataTypeUint8, 384, Euclidean384Uint8)
		Registry.Register(MetricEuclidean, DataTypeUint8, 768, Euclidean768Uint8)
		Registry.Register(MetricEuclidean, DataTypeUint8, 1024, Euclidean1024Uint8)
	}
	Registry.Register(MetricL2Squared, DataTypeUint8, 0, l2SquaredUint8Impl)
	Registry.Register(MetricCosine, DataTypeUint8, 0, cosineDistanceUint8Impl)
	Registry.Register(MetricDotProduct, DataTypeUint8, 0, dotProductUint8Impl)
	if impl != "generic" {
		Registry.Register(MetricDotProduct, DataTypeUint8, 384, dotProductUint8Impl)
		Registry.Register(MetricDotProduct, DataTypeUint8, 768, dotProductUint8Impl)
		Registry.Register(MetricDotProduct, DataTypeUint8, 1024, dotProductUint8Impl)
	} else {
		Registry.Register(MetricDotProduct, DataTypeUint8, 384, Dot384Uint8)
		Registry.Register(MetricDotProduct, DataTypeUint8, 768, Dot768Uint8)
		Registry.Register(MetricDotProduct, DataTypeUint8, 1024, Dot1024Uint8)
	}

	Registry.Register(MetricEuclidean, DataTypeUint16, 0, euclideanDistanceUint16Impl)
	Registry.Register(MetricEuclidean, DataTypeUint16, 384, Euclidean384Uint16)
	Registry.Register(MetricEuclidean, DataTypeUint16, 768, Euclidean768Uint16)
	Registry.Register(MetricEuclidean, DataTypeUint16, 1024, Euclidean1024Uint16)
	Registry.Register(MetricCosine, DataTypeUint16, 0, CosineDistanceUint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 0, dotProductUint16Impl)
	Registry.Register(MetricDotProduct, DataTypeUint16, 384, Dot384Uint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 768, Dot768Uint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 1024, Dot1024Uint16)

	Registry.Register(MetricEuclidean, DataTypeUint32, 0, EuclideanDistanceUint32)
	Registry.Register(MetricEuclidean, DataTypeUint32, 384, Euclidean384Uint32)
	Registry.Register(MetricEuclidean, DataTypeUint32, 768, Euclidean768Uint32)
	Registry.Register(MetricEuclidean, DataTypeUint32, 1024, Euclidean1024Uint32)
	Registry.Register(MetricCosine, DataTypeUint32, 0, CosineDistanceUint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 0, DotProductUint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 384, Dot384Uint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 768, Dot768Uint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 1024, Dot1024Uint32)

	Registry.Register(MetricEuclidean, DataTypeUint64, 0, EuclideanDistanceUint64)
	Registry.Register(MetricEuclidean, DataTypeUint64, 384, Euclidean384Uint64)
	Registry.Register(MetricEuclidean, DataTypeUint64, 768, Euclidean768Uint64)
	Registry.Register(MetricEuclidean, DataTypeUint64, 1024, Euclidean1024Uint64)
	Registry.Register(MetricCosine, DataTypeUint64, 0, CosineDistanceUint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 0, DotProductUint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 384, Dot384Uint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 768, Dot768Uint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 1024, Dot1024Uint64)

	Registry.Register(MetricEuclidean, DataTypeFloat64, 0, EuclideanDistanceFloat64)
	Registry.Register(MetricEuclidean, DataTypeFloat64, 384, Euclidean384Float64)
	Registry.Register(MetricEuclidean, DataTypeFloat64, 768, Euclidean768Float64)
	Registry.Register(MetricCosine, DataTypeFloat64, 0, CosineDistanceFloat64)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 0, DotProductF64)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 384, Dot384Float64)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 768, Dot768Float64)
	Registry.Register(MetricL2Squared, DataTypeFloat64, 0, L2SquaredFloat64)
	Registry.Register(MetricL2Squared, DataTypeFloat64, 384, L2Squared384Float64)
	Registry.Register(MetricL2Squared, DataTypeFloat64, 768, L2Squared768Float64)

	// complex64: Euclidean uses the optimized dispatch-set version (cast to float32 SIMD)
	Registry.Register(MetricCosine, DataTypeComplex64, 0, CosineDistanceComplex64)
	Registry.Register(MetricDotProduct, DataTypeComplex64, 0, DotProductComplex64)
	// complex128: Euclidean uses the optimized dispatch-set version (cast to float64 SIMD)
	Registry.Register(MetricCosine, DataTypeComplex128, 0, CosineDistanceComplex128)
	Registry.Register(MetricDotProduct, DataTypeComplex128, 0, DotProductComplex128)
}
