package types

import (
	"sync/atomic"
)

// StoreNeighborsChunk stores a chunk of neighbors for testing.
// It allocates space in the Arena and copies the data.
func (gd *GraphData) StoreNeighborsChunk(layer int, chunkID uint32, chunk []uint32) {
	if chunk == nil {
		return
	}
	// Expand if needed (assuming layer exists)
	if layer >= len(gd.Neighbors) || int(chunkID) >= len(gd.Neighbors[layer]) {
		return
	}

	// Copy data to new slice
	dst := make([]uint32, len(chunk))
	copy(dst, chunk)

	gd.Neighbors[layer][chunkID] = dst
}

// StoreCountsChunk stores a chunk of counts for testing.
func (gd *GraphData) StoreCountsChunk(layer int, chunkID uint32, chunk []int32) {
	if chunk == nil {
		return
	}
	if layer >= len(gd.Counts) || int(chunkID) >= len(gd.Counts[layer]) {
		return
	}
	dst := make([]int32, len(chunk))
	copy(dst, chunk)
	gd.Counts[layer][chunkID] = dst
}

// StoreVersionsChunk stores a chunk of versions for testing.
func (gd *GraphData) StoreVersionsChunk(layer int, chunkID uint32, chunk []uint32) {
	if chunk == nil {
		return
	}

	if layer >= len(gd.Versions) || int(chunkID) >= len(gd.Versions[layer]) {
		return
	}
	dst := make([]uint32, len(chunk))
	copy(dst, chunk)
	gd.Versions[layer][chunkID] = dst
}

// StoreVectorsChunk stores a chunk of vectors for testing.
// It assumes dims matches context, though here we just copy bytes/floats.
// Actually, vectors are float32.
func (gd *GraphData) StoreVectorsChunk(chunkID uint32, chunk []float32) {
	if chunk == nil {
		return
	}
	// Check bounds
	if int(chunkID) >= len(gd.Vectors) {
		return
	}

	// Alloc
	numWords := len(chunk)
	ref, _ := gd.Float32Arena.AllocSlice(numWords)
	slice := gd.Float32Arena.Get(ref)
	copy(slice, chunk)

	gd.Vectors[chunkID] = slice
}

// StoreSQ8Chunk stores a chunk of SQ8 vectors for testing.
func (gd *GraphData) StoreSQ8Chunk(chunkID uint32, chunk []byte) {
	if chunk == nil {
		return
	}
	numBytes := len(chunk)
	ref, _ := gd.Uint8Arena.AllocSlice(numBytes)
	slice := gd.Uint8Arena.Get(ref)
	copy(slice, chunk)
	atomic.StoreUint64(&gd.VectorsSQ8[chunkID], ref.Offset)
}

// StorePQChunk stores a chunk of PQ vectors for testing.
func (gd *GraphData) StorePQChunk(chunkID uint32, chunk []byte) {
	if chunk == nil {
		return
	}
	numBytes := len(chunk)
	ref, _ := gd.Uint8Arena.AllocSlice(numBytes)
	slice := gd.Uint8Arena.Get(ref)
	copy(slice, chunk)
	atomic.StoreUint64(&gd.VectorsPQ[chunkID], ref.Offset)
}
