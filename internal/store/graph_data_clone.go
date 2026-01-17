package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Clone creates a deep copy of GraphData with new capacity.
// This implementation is optimized for high-dimensional vectors by using
// bulk memory copies for metadata arrays instead of element-wise atomic loads,
// significantly reducing overhead during Grow operations.
//
// Note: While this uses copy() which is not strictly atomic in the `sync/atomic` sense,
// on 64-bit architectures aligned uint64 copies are atomic (no tearing).
// Concurrent updates (0 -> offset) might still be missed, which is an existing
// race condition inherent to lock-free growth, usually mitigated by caller synchronization.
func (gd *GraphData) Clone(minCap, targetDims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled, float16Enabled, packedAdjacencyEnabled bool) *GraphData {
	// 1. Calculate new capacity and chunks
	numChunks := (minCap + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	// 2. Calculate padded dimensions for new targetDims
	elementSize := gd.Type.ElementSize()
	rawBytes := targetDims * elementSize
	paddedBytes := (rawBytes + 63) & ^63
	paddedDims := targetDims
	if elementSize > 0 {
		paddedDims = paddedBytes / elementSize
	}

	// 3. Create new GraphData shell
	// We reuse the existing SlabArena pointers. Since SlabArena is append-only and
	// we only copy offsets, the data remains valid.
	newGD := &GraphData{
		Capacity:     numChunks * ChunkSize,
		Dims:         targetDims,
		PaddedDims:   paddedDims,
		PQDims:       pqDims,
		Type:         gd.Type,
		SlabArena:    gd.SlabArena,
		Uint8Arena:   gd.Uint8Arena,
		Int8Arena:    gd.Int8Arena,
		Int16Arena:   gd.Int16Arena,
		Uint16Arena:  gd.Uint16Arena,
		Float32Arena: gd.Float32Arena,
		Float16Arena: gd.Float16Arena,
		Float64Arena: gd.Float64Arena,

		Uint32Arena: gd.Uint32Arena,
		Int32Arena:  gd.Int32Arena,
		Uint64Arena: gd.Uint64Arena,
		Int64Arena:  gd.Int64Arena,

		Complex64Arena:  gd.Complex64Arena,
		Complex128Arena: gd.Complex128Arena,

		// Pre-allocate slices with new capacity
		Levels: make([]uint64, numChunks),
	}

	// 4. Allocate Vector metadata arrays
	if targetDims > 0 {
		newGD.Vectors = make([]uint64, numChunks)
		if float16Enabled {
			newGD.VectorsF16 = make([]uint64, numChunks)
		}
		if sq8Enabled {
			newGD.VectorsSQ8 = make([]uint64, numChunks)
		}
		if pqEnabled && pqDims > 0 {
			newGD.VectorsPQ = make([]uint64, numChunks)
		}
		if bqEnabled {
			newGD.VectorsBQ = make([]uint64, numChunks)
		}
		if gd.Type == VectorTypeComplex64 {
			newGD.VectorsC64 = make([]uint64, numChunks)
		}
		if gd.Type == VectorTypeComplex128 {
			newGD.VectorsC128 = make([]uint64, numChunks)
		}
	}

	// 5. Allocate Neighbor/Count/Version metadata arrays
	for i := 0; i < ArrowMaxLayers; i++ {
		newGD.Neighbors[i] = make([]uint64, numChunks)
		newGD.Counts[i] = make([]uint64, numChunks)
		newGD.Versions[i] = make([]uint64, numChunks)
	}

	// 6. Bulk Copy Metadata
	// Helper for bulk copy of uint64 slices
	copySlice := func(dst, src []uint64) {
		n := len(src)
		if len(dst) < n {
			n = len(dst)
		}
		if n > 0 {
			copy(dst[:n], src[:n])
		}
	}

	// Levels
	copySlice(newGD.Levels, gd.Levels)

	// Vectors
	if len(gd.Vectors) > 0 && len(newGD.Vectors) > 0 {
		copySlice(newGD.Vectors, gd.Vectors)
	}

	// Auxiliary Vector Arrays
	if len(gd.VectorsF16) > 0 && len(newGD.VectorsF16) > 0 {
		copySlice(newGD.VectorsF16, gd.VectorsF16)
	}
	if len(gd.VectorsSQ8) > 0 && len(newGD.VectorsSQ8) > 0 {
		copySlice(newGD.VectorsSQ8, gd.VectorsSQ8)
	}
	if len(gd.VectorsPQ) > 0 && len(newGD.VectorsPQ) > 0 {
		copySlice(newGD.VectorsPQ, gd.VectorsPQ)
	}
	if len(gd.VectorsBQ) > 0 && len(newGD.VectorsBQ) > 0 {
		copySlice(newGD.VectorsBQ, gd.VectorsBQ)
	}
	if len(gd.VectorsC64) > 0 && len(newGD.VectorsC64) > 0 {
		copySlice(newGD.VectorsC64, gd.VectorsC64)
	}
	if len(gd.VectorsC128) > 0 && len(newGD.VectorsC128) > 0 {
		copySlice(newGD.VectorsC128, gd.VectorsC128)
	}

	// Neighbors, Counts, Versions
	for i := 0; i < ArrowMaxLayers; i++ {
		copySlice(newGD.Neighbors[i], gd.Neighbors[i])
		copySlice(newGD.Counts[i], gd.Counts[i])
		copySlice(newGD.Versions[i], gd.Versions[i])

		// Packed Neighbors (Pointer copy)
		if packedAdjacencyEnabled && gd.PackedNeighbors[i] != nil {
			newGD.PackedNeighbors[i] = gd.PackedNeighbors[i]
		}
	}

	return newGD
}

// ensureF16Arena makes sure the Float16Arena exists, creating it if necessary.
// This is relevant when upgrading to F16 enabled during runtime.
func (gd *GraphData) ensureF16Arena() {
	if gd.Float16Arena == nil && gd.SlabArena != nil {
		gd.Float16Arena = memory.NewTypedArena[float16.Num](gd.SlabArena)
	}
}
