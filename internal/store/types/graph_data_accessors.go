package types

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"math"
	"sync/atomic"
	"unsafe"
)

func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	return g.GetVectorsChunkWithGen(chunkID, math.MaxUint64)
}

// GetVectorsChunkWithGen returns the vector chunk for the given ID with generation isolation.
// Uses non-atomic offset read for performance. Chunk offset arrays are written once by
// EnsureChunk and stable during search; the arena handles concurrent safety internally.
func (g *GraphData) GetVectorsChunkWithGen(chunkID int, maxGen uint64) []float32 {
	// Try arena first (off-heap, GC-free)
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	// Fallback to legacy slice
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsChunkFast returns the vector chunk using a non-atomic offset read.
// Safe because chunk offset arrays are written once by EnsureChunk and are
// GetVectorsChunkFast returns the vector chunk using a non-atomic offset read.
// This is an optimization for search threads. For committed data (no generation isolation).
func (g *GraphData) GetVectorsChunkFast(chunkID int) []float32 {
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsChunkFastWithGen returns the vector chunk using an atomic offset read
// with generation isolation.
func (g *GraphData) GetVectorsChunkFastWithGen(chunkID int, maxGen uint64) []float32 {
	if g.Float32Arena != nil && chunkID < len(g.VectorsF32) {
		pd := g.GetPaddedDimsForType(VectorTypeFloat32)
		offset := atomic.LoadUint64(&g.VectorsF32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

// GetVectorsTQChunkFast returns a TurboQuant chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsTQChunkFast(chunkID int) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		offset := atomic.LoadUint64(&g.VectorsTQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)}) // #nosec G115
	}
	return nil
}

// GetVectorsFloat64ChunkFast returns a float64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsFloat64ChunkFast(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat64)
		offset := atomic.LoadUint64(&g.VectorsFloat64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

// GetVectorsComplex64ChunkFast returns a complex64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsComplex64ChunkFast(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex64)
		offset := atomic.LoadUint64(&g.VectorsComplex64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

// GetVectorsComplex128ChunkFast returns a complex128 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsComplex128ChunkFast(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex128)
		offset := atomic.LoadUint64(&g.VectorsComplex128Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex128Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

// GetVectorsInt64ChunkFast returns an int64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt64ChunkFast(chunkID int) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		offset := atomic.LoadUint64(&g.VectorsInt64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint64ChunkFast returns a uint64 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint64ChunkFast(chunkID int) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		offset := atomic.LoadUint64(&g.VectorsUint64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsInt32ChunkFast returns an int32 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt32ChunkFast(chunkID int) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		offset := atomic.LoadUint64(&g.VectorsInt32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint32ChunkFast returns a uint32 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint32ChunkFast(chunkID int) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		offset := atomic.LoadUint64(&g.VectorsUint32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsInt8ChunkFast returns an int8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt8ChunkFast(chunkID int) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt8)
		offset := atomic.LoadUint64(&g.VectorsInt8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint8ChunkFast returns a uint8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint8ChunkFast(chunkID int) []uint8 {
	chunk := g.GetVectorsInt8ChunkFast(chunkID)
	if chunk == nil {
		return nil
	}
	ptr := unsafe.Pointer(&chunk[0])               // #nosec G103
	return unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
}

// GetVectorsInt16ChunkFast returns an int16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsInt16ChunkFast(chunkID int) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt16)
		offset := atomic.LoadUint64(&g.VectorsInt16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsUint16ChunkFast returns a uint16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsUint16ChunkFast(chunkID int) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint16)
		offset := atomic.LoadUint64(&g.VectorsUint16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsF16ChunkFast returns a float16 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsF16ChunkFast(chunkID int) []float16.Num {
	if chunkID < len(g.VectorsF16) && g.Float16Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat16)
		offset := atomic.LoadUint64(&g.VectorsF16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float16Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}) // #nosec G115
	}
	return nil
}

// GetVectorsSQ8ChunkFast returns an SQ8 chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsSQ8ChunkFast(chunkID int) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		offset := atomic.LoadUint64(&g.VectorsSQ8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}) // #nosec G115
	}
	return nil
}

// GetVectorsBQChunkFast returns a BQ chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsBQChunkFast(chunkID int) []uint64 {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		chunkLen := ChunkSize * numWordsPerNode
		if chunkLen == 0 {
			return nil
		}
		offset := atomic.LoadUint64(&g.VectorsBQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		})
	}
	return nil
}

// GetVectorsPQChunkFast returns a PQ chunk using a non-atomic offset read.
func (g *GraphData) GetVectorsPQChunkFast(chunkID int) []byte {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsPQ) && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode
		offset := atomic.LoadUint64(&g.VectorsPQ[chunkID])
		if offset == 0 {
			return nil
		}
		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		})
		if len(chunk) == 0 {
			return nil
		}
		ptr := unsafe.Pointer(&chunk[0])              // #nosec G103
		return unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103
	}
	return nil
}

// GetCountsChunkFast returns a counts chunk using a non-atomic offset read.
func (g *GraphData) GetCountsChunkFast(layer, chunkID int) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) && g.Int32Arena != nil {
		offset := atomic.LoadUint64(&g.Counts[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
	}
	return nil
}

// GetNeighborsChunkFast returns a neighbors chunk using a non-atomic offset read.
func (g *GraphData) GetNeighborsChunkFast(layer, chunkID int) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		if offset == 0 && g.OnNeighborsMiss != nil {
			_ = g.OnNeighborsMiss(layer)
			offset = atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		}
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * MaxNeighbors), Cap: uint32(ChunkSize * MaxNeighbors)}) // #nosec G115
	}
	return nil
}

// GetVersionsChunkFast returns a versions chunk using a non-atomic offset read.
func (g *GraphData) GetVersionsChunkFast(layer, chunkID int) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Versions[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.Get(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}) // #nosec G115
	}
	return nil
}

// PackedSize returns the byte size of a TurboQuant packed vector.

func (g *GraphData) GetVectorsTQChunk(chunkID int) []byte {
	return g.GetVectorsTQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsTQChunkWithGen(chunkID int, maxGen uint64) []byte {
	if chunkID < len(g.VectorsTQ) && g.Uint8Arena != nil {
		stride := g.PackedSize()
		offset := atomic.LoadUint64(&g.VectorsTQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * stride), Cap: uint32(ChunkSize * stride)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsFloat64Chunk returns a chunk of float64 vectors.
func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	return g.GetVectorsFloat64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsFloat64ChunkWithGen(chunkID int, maxGen uint64) []float64 {
	if chunkID < len(g.VectorsFloat64Offsets) && g.Float64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeFloat64)
		offset := atomic.LoadUint64(&g.VectorsFloat64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

// GetVectorsComplex64Chunk returns a chunk of complex64 vectors.
func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	return g.GetVectorsComplex64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsComplex64ChunkWithGen(chunkID int, maxGen uint64) []complex64 {
	if chunkID < len(g.VectorsComplex64Offsets) && g.Complex64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex64)
		offset := atomic.LoadUint64(&g.VectorsComplex64Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

// GetVectorsComplex128Chunk returns a chunk of complex128 vectors.
func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	return g.GetVectorsComplex128ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsComplex128ChunkWithGen(chunkID int, maxGen uint64) []complex128 {
	if chunkID < len(g.VectorsComplex128Offsets) && g.Complex128Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeComplex128)
		offset := atomic.LoadUint64(&g.VectorsComplex128Offsets[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Complex128Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

// GetVectorsInt64Chunk returns a chunk of int64 vectors.
func (g *GraphData) GetVectorsInt64Chunk(chunkID int) []int64 {
	return g.GetVectorsInt64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt64ChunkWithGen(chunkID int, maxGen uint64) []int64 {
	if chunkID < len(g.VectorsInt64) && g.Int64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		offset := atomic.LoadUint64(&g.VectorsInt64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsUint64Chunk returns a chunk of uint64 vectors.
func (g *GraphData) GetVectorsUint64Chunk(chunkID int) []uint64 {
	return g.GetVectorsUint64ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint64ChunkWithGen(chunkID int, maxGen uint64) []uint64 {
	if chunkID < len(g.VectorsUint64) && g.Uint64Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		offset := atomic.LoadUint64(&g.VectorsUint64[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsInt32Chunk returns a chunk of int32 vectors.
func (g *GraphData) GetVectorsInt32Chunk(chunkID int) []int32 {
	return g.GetVectorsInt32ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt32ChunkWithGen(chunkID int, maxGen uint64) []int32 {
	if chunkID < len(g.VectorsInt32) && g.Int32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		offset := atomic.LoadUint64(&g.VectorsInt32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVectorsUint32Chunk returns a chunk of uint32 vectors.
func (g *GraphData) GetVectorsUint32Chunk(chunkID int) []uint32 {
	return g.GetVectorsUint32ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint32ChunkWithGen(chunkID int, maxGen uint64) []uint32 {
	if chunkID < len(g.VectorsUint32) && g.Uint32Arena != nil {
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		offset := atomic.LoadUint64(&g.VectorsUint32[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * pd), Cap: uint32(ChunkSize * pd)}, maxGen) // #nosec G115
	}
	return nil
}

// GetPaddedDims returns the padded dimension for the primary vector type.

func (g *GraphData) GetVectorsSQ8Chunk(chunkID int) []byte {
	return g.GetVectorsSQ8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsSQ8ChunkWithGen(chunkID int, maxGen uint64) []byte {
	if chunkID < len(g.VectorsSQ8) && g.Uint8Arena != nil {
		paddedDims := (g.Dims + 63) & ^63
		offset := atomic.LoadUint64(&g.VectorsSQ8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint8Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsBQChunk(chunkID int) []uint64 {
	return g.GetVectorsBQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsBQChunkWithGen(chunkID int, maxGen uint64) []uint64 {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWordsPerNode := paddedDims / 64
		chunkLen := ChunkSize * numWordsPerNode
		if chunkLen == 0 {
			return nil
		}

		offset := atomic.LoadUint64(&g.VectorsBQ[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		}, maxGen)
	}
	return nil
}

// GetVectorsPQChunk returns the PQ vectors chunk for the given ID.
func (g *GraphData) GetVectorsPQChunk(chunkID int) []byte {
	return g.GetVectorsPQChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsPQChunkWithGen(chunkID int, maxGen uint64) []byte {
	if g.Uint64Arena != nil && chunkID < len(g.VectorsPQ) && g.PQM > 0 {
		numWordsPerNode := (g.PQM + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		offset := atomic.LoadUint64(&g.VectorsPQ[chunkID])
		if offset == 0 {
			return nil
		}
		chunk := g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		}, maxGen)

		if len(chunk) == 0 {
			return nil
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])              // #nosec G103
		return unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103
	}
	return nil
}

func (g *GraphData) GetCountsChunk(layer, chunkID int) []int32 {
	return g.GetCountsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetCountsChunkWithGen(layer, chunkID int, maxGen uint64) []int32 {
	if layer < len(g.Counts) && chunkID < len(g.Counts[layer]) && g.Int32Arena != nil {
		offset := atomic.LoadUint64(&g.Counts[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetNeighborsChunk(layer, chunkID int) []uint32 {
	return g.GetNeighborsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetNeighborsChunkWithGen(layer, chunkID int, maxGen uint64) []uint32 {
	if layer < len(g.Neighbors) && chunkID < len(g.Neighbors[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		if offset == 0 && g.OnNeighborsMiss != nil {
			_ = g.OnNeighborsMiss(layer)
			offset = atomic.LoadUint64(&g.Neighbors[layer][chunkID])
		}
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * MaxNeighbors), Cap: uint32(ChunkSize * MaxNeighbors)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVersionsChunk(layer, chunkID int) []uint32 {
	return g.GetVersionsChunkWithGen(layer, chunkID, math.MaxUint64)
}

func (g *GraphData) GetVersionsChunkWithGen(layer, chunkID int, maxGen uint64) []uint32 {
	if layer < len(g.Versions) && chunkID < len(g.Versions[layer]) && g.Uint32Arena != nil {
		offset := atomic.LoadUint64(&g.Versions[layer][chunkID])
		if offset == 0 {
			return nil
		}
		return g.Uint32Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize), Cap: uint32(ChunkSize)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsInt8Chunk(chunkID int) []int8 {
	return g.GetVectorsInt8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt8ChunkWithGen(chunkID int, maxGen uint64) []int8 {
	if chunkID < len(g.VectorsInt8) && g.Int8Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt8)
		offset := atomic.LoadUint64(&g.VectorsInt8[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int8Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsUint8Chunk(chunkID int) []uint8 {
	return g.GetVectorsUint8ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint8ChunkWithGen(chunkID int, maxGen uint64) []uint8 {
	chunk := g.GetVectorsInt8ChunkWithGen(chunkID, maxGen)
	if chunk == nil {
		return nil
	}
	ptr := unsafe.Pointer(&chunk[0])               // #nosec G103
	return unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
}

func (g *GraphData) GetVectorsInt16Chunk(chunkID int) []int16 {
	return g.GetVectorsInt16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsInt16ChunkWithGen(chunkID int, maxGen uint64) []int16 {
	if chunkID < len(g.VectorsInt16) && g.Int16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
		offset := atomic.LoadUint64(&g.VectorsInt16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Int16Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsUint16Chunk(chunkID int) []uint16 {
	return g.GetVectorsUint16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsUint16ChunkWithGen(chunkID int, maxGen uint64) []uint16 {
	if chunkID < len(g.VectorsUint16) && g.Uint16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
		return g.Uint16Arena.GetWithGeneration(memory.SliceRef{Offset: g.VectorsUint16[chunkID], Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

func (g *GraphData) GetVectorsF16Chunk(chunkID int) []float16.Num {
	return g.GetVectorsF16ChunkWithGen(chunkID, math.MaxUint64)
}

func (g *GraphData) GetVectorsF16ChunkWithGen(chunkID int, maxGen uint64) []float16.Num {
	if chunkID < len(g.VectorsF16) && g.Float16Arena != nil {
		paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
		offset := atomic.LoadUint64(&g.VectorsF16[chunkID])
		if offset == 0 {
			return nil
		}
		return g.Float16Arena.GetWithGeneration(memory.SliceRef{Offset: offset, Len: uint32(ChunkSize * paddedDims), Cap: uint32(ChunkSize * paddedDims)}, maxGen) // #nosec G115
	}
	return nil
}

// GetVector returns the vector for the given ID.

func (g *GraphData) GetLevelsChunk(chunkID int) []uint32 {
	if chunkID < len(g.Levels) {
		return g.Levels[chunkID]
	}
	return nil
}

// AcquireReader pins the GraphData against premature typed-arena release.
// Bracket any read path that accesses the typed-arena fields (Int8Arena,
// Float32Arena, etc.) with AcquireReader/​ReleaseReader so a concurrent
// compareAndSwapData-driven Release() waits until the read completes.
// Cheap: a single atomic add. Hot-path readers (search, GetNeighbors) should
// also use it; the contention window is short.
