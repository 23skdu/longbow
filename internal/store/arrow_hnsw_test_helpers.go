package store

import (
	"sync/atomic"
)

// StoreNeighborsChunk stores a chunk of neighbors for testing.
// It allocates space in the Arena and copies the data.
func (gd *GraphData) StoreNeighborsChunk(layer int, chunkID uint32, chunk *[]uint32) {
	if chunk == nil {
		return
	}
	// Expand if needed
	for layer >= len(gd.Neighbors) {
		return
	}
	numWords := len(*chunk)
	ref, _ := gd.Uint32Arena.AllocSlice(numWords) // Alloc header+data
	slice := gd.Uint32Arena.Get(ref)
	copy(slice, *chunk) // Copy data

	atomic.StoreUint64(&gd.Neighbors[layer][chunkID], ref.Offset)
}

// StoreCountsChunk stores a chunk of counts for testing.
func (gd *GraphData) StoreCountsChunk(layer int, chunkID uint32, chunk *[]int32) {
	if chunk == nil {
		return
	}
	numWords := len(*chunk)
	ref, _ := gd.Uint32Arena.AllocSlice(numWords)
	slice := gd.Uint32Arena.Get(ref)
	// Copy int32 as uint32
	dst := slice
	for i, v := range *chunk {
		dst[i] = uint32(v)
	}
	atomic.StoreUint64(&gd.Counts[layer][chunkID], ref.Offset)
}

// StoreVersionsChunk stores a chunk of versions for testing.
func (gd *GraphData) StoreVersionsChunk(layer int, chunkID uint32, chunk *[]uint32) {
	if chunk == nil {
		return
	}
	numWords := len(*chunk)
	ref, _ := gd.Uint32Arena.AllocSlice(numWords)
	slice := gd.Uint32Arena.Get(ref)
	copy(slice, *chunk)
	atomic.StoreUint64(&gd.Versions[layer][chunkID], ref.Offset)
}

// StoreSQ8Chunk stores a chunk of SQ8 vectors for testing.
func (gd *GraphData) StoreSQ8Chunk(chunkID uint32, chunk *[]byte) {
	if chunk == nil {
		return
	}
	numBytes := len(*chunk)
	ref, _ := gd.Uint8Arena.AllocSlice(numBytes)
	slice := gd.Uint8Arena.Get(ref)
	copy(slice, *chunk)
	atomic.StoreUint64(&gd.VectorsSQ8[chunkID], ref.Offset)
}

// StorePQChunk stores a chunk of PQ vectors for testing.
func (gd *GraphData) StorePQChunk(chunkID uint32, chunk *[]byte) {
	if chunk == nil {
		return
	}
	numBytes := len(*chunk)
	ref, _ := gd.Uint8Arena.AllocSlice(numBytes)
	slice := gd.Uint8Arena.Get(ref)
	copy(slice, *chunk)
	atomic.StoreUint64(&gd.VectorsPQ[chunkID], ref.Offset)
}

// EnsureChunk ensures that chunks for the given ID are allocated.
// This matches the logic needed by GraphData tests.
func (gd *GraphData) EnsureChunk(cID uint32, dims int) {
	// Allocate Vectors SQ8 (if needed, but default false)
	// Allocate Vectors PQ (if needed)
	// For now, minimal alloc akin to ensureChunk in ArrowHNSW but simpler

	// Neighbors
	if cID < uint32(len(gd.Neighbors[0])) { // Check layer 0
		off := atomic.LoadUint64(&gd.Neighbors[0][cID])
		if off == 0 {
			// Allocate
			numWords := int(ChunkSize * MaxNeighbors)
			ref, _ := gd.Uint32Arena.AllocSlice(numWords)
			atomic.StoreUint64(&gd.Neighbors[0][cID], ref.Offset)
		}
	}

	// Counts
	if cID < uint32(len(gd.Counts[0])) {
		off := atomic.LoadUint64(&gd.Counts[0][cID])
		if off == 0 {
			ref, _ := gd.Int32Arena.AllocSlice(ChunkSize)
			atomic.StoreUint64(&gd.Counts[0][cID], ref.Offset)
		}
	}

	// Versions
	if cID < uint32(len(gd.Versions[0])) {
		off := atomic.LoadUint64(&gd.Versions[0][cID])
		if off == 0 {
			ref, _ := gd.Uint32Arena.AllocSlice(ChunkSize)
			atomic.StoreUint64(&gd.Versions[0][cID], ref.Offset)
		}
	}

	// Levels
	if int(cID) < len(gd.Levels) {
		off := atomic.LoadUint64(&gd.Levels[cID])
		if off == 0 {
			ref, _ := gd.Uint8Arena.AllocSlice(ChunkSize)
			atomic.StoreUint64(&gd.Levels[cID], ref.Offset)
		}
	}
}
