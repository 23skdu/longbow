package core

import "math"

// VectorID is a unique identifier for a vector in the system.
// It is physically a uint32, allowing for ~4 billion vectors per shard.
type VectorID uint32

// Location maps a VectorID to a physical location in a Dataset (Batch + Row).
type Location struct {
	BatchIdx int
	RowIdx   int
}

// PackLocation packs a Location into a uint64 for atomic storage.
// It assumes batchIdx and rowIdx fit within 32 bits, which is generally safe.
// This matches the logic previously in internal/store/location_store.go
func PackLocation(loc Location) uint64 {
	return uint64(uint32(loc.BatchIdx))<<32 | uint64(uint32(loc.RowIdx))
}

// UnpackLocation unpacks a uint64 into a Location.
func UnpackLocation(val uint64) Location {
	return Location{
		BatchIdx: int(int32(val >> 32)),
		RowIdx:   int(int32(val)),
	}
}

// Uint32FromFloat32 bits helper (commonly used in packing)
func PackBytesToFloat32s(b []byte) []float32 {
	n := len(b)
	numFloats := (n + 3) / 4
	floats := make([]float32, numFloats)

	for i := 0; i < numFloats; i++ {
		var bits uint32
		validBytes := 4
		if (i+1)*4 > n {
			validBytes = n - i*4
		}

		for j := 0; j < validBytes; j++ {
			bits |= uint32(b[i*4+j]) << (8 * j)
		}
		floats[i] = math.Float32frombits(bits)
	}
	return floats
}
