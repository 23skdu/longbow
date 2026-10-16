package index

// DistanceComputer defines the interface for specialized distance computation.
type DistanceComputer interface {
	ComputeSingle(id uint32) (float32, error)
	ComputeBatch(ids []uint32, dst []float32) ([]float32, error)
	Prefetch(id uint32)
}

// Compile-time interface satisfaction checks across all concrete computer implementations.
var (
	_ DistanceComputer = (*pqComputer)(nil)
	_ DistanceComputer = (*tqComputer)(nil)
	_ DistanceComputer = (*float32ToSQ8Computer)(nil)
	_ DistanceComputer = (*float32Computer)(nil)
	_ DistanceComputer = (*float32ToFloat32Computer)(nil)
	_ DistanceComputer = (*sharedFloat32Computer)(nil)
	_ DistanceComputer = (*int8Computer)(nil)
	_ DistanceComputer = (*sharedInt8Computer)(nil)
	_ DistanceComputer = (*complex64Computer)(nil)
	_ DistanceComputer = (*complex128Computer)(nil)
	_ DistanceComputer = (*float16Computer)(nil)
	_ DistanceComputer = (*float64Computer)(nil)
	_ DistanceComputer = (*int16Computer)(nil)
	_ DistanceComputer = (*uint16Computer)(nil)
	_ DistanceComputer = (*int32Computer)(nil)
	_ DistanceComputer = (*uint32Computer)(nil)
	_ DistanceComputer = (*int64Computer)(nil)
	_ DistanceComputer = (*uint64Computer)(nil)
)
