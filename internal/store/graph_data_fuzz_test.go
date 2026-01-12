//go:build go1.18

package store

import (
	"testing"
)

func FuzzGraphData_AllocationStress(f *testing.F) {
	// Seed with valid types and capacities
	f.Add(int(VectorTypeFloat32), 128, 1024)
	f.Add(int(VectorTypeInt8), 64, 2048)
	f.Add(int(VectorTypeComplex128), 1536, 512)

	f.Fuzz(func(t *testing.T, typeInt int, dims int, cap int) {
		if dims <= 0 || dims > 3072 || cap <= 0 || cap > 10000 {
			return
		}
		dt := VectorDataType(typeInt % 13) // Map to valid range
		if dt < 0 {
			dt = VectorTypeFloat32
		}

		hnswConfig := DefaultArrowHNSWConfig()
		hnsw := NewArrowHNSW(nil, hnswConfig, nil)

		gd := NewGraphData(cap, dims, false, false, 0, false, false, false, dt)

		// Stress allocation
		for i := 0; i < (cap+ChunkSize-1)/ChunkSize; i++ {
			gd = hnsw.ensureChunk(gd, uint32(i), 0, dims)
		}

		// Cleanup (partial)
		_ = gd.Close()
	})
}
