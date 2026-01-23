package store

import (
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Vector access methods extracted from arrow_hnsw_graph.go

func (gd *GraphData) GetVectorsChunk(chunkID uint32) []float32 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.Vectors[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Float32Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsInt8Chunk(chunkID uint32) []int8 {
	if int(chunkID) >= len(gd.VectorsInt8) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsInt8[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Int8Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsInt16Chunk(chunkID uint32) []int16 {
	if int(chunkID) >= len(gd.VectorsInt16) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsInt16[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Int16Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsInt32Chunk(chunkID uint32) []int32 {
	if int(chunkID) >= len(gd.VectorsInt32) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsInt32[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Int32Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsFloat64Chunk(chunkID uint32) []float64 {
	if int(chunkID) >= len(gd.VectorsFloat64) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsFloat64[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Float64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsComplex64Chunk(chunkID uint32) []complex64 {
	if int(chunkID) >= len(gd.VectorsC64) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsC64[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Complex64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsComplex128Chunk(chunkID uint32) []complex128 {
	if int(chunkID) >= len(gd.VectorsC128) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsC128[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Complex128Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorComplex64(id uint32) []complex64 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsComplex64Chunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.GetPaddedDims()
	return chunk[start : start+gd.Dims]
}

func (gd *GraphData) GetVectorComplex128(id uint32) []complex128 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsComplex128Chunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.GetPaddedDims()
	return chunk[start : start+gd.Dims]
}

func (gd *GraphData) GetVectorsF16Chunk(chunkID uint32) []float16.Num {
	// Try VectorsF16 (Aux) first
	if int(chunkID) < len(gd.VectorsF16) {
		if offset := atomic.LoadUint64(&gd.VectorsF16[chunkID]); offset != 0 {
			stride := gd.GetPaddedDimsForType(VectorTypeFloat16)
			return gd.Float16Arena.Get(memory.SliceRef{
				Offset: offset,
				Len:    uint32(ChunkSize * stride),
				Cap:    uint32(ChunkSize * stride),
			})
		}
	}
	// Try Primary Vectors if Type is Float16
	if gd.Type == VectorTypeFloat16 && int(chunkID) < len(gd.Vectors) {
		if offset := atomic.LoadUint64(&gd.Vectors[chunkID]); offset != 0 {
			stride := gd.GetPaddedDims()
			return gd.Float16Arena.Get(memory.SliceRef{
				Offset: offset,
				Len:    uint32(ChunkSize * stride),
				Cap:    uint32(ChunkSize * stride),
			})
		}
	}
	return nil
}

func (gd *GraphData) GetVectorsSQ8Chunk(chunkID uint32) []byte {
	if int(chunkID) >= len(gd.VectorsSQ8) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsSQ8[chunkID]); offset != 0 {
		stride := (gd.Dims + 63) & ^63 // Padded to 64-byte alignment
		return gd.Uint8Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsPQChunk(chunkID uint32) []byte {
	if int(chunkID) >= len(gd.VectorsPQ) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsPQ[chunkID]); offset != 0 {
		// PQ chunks store M bytes per vector, where M is PQ subvector count
		stride := gd.PQDims
		return gd.Uint8Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsBQChunk(chunkID uint32) []uint64 {
	if int(chunkID) >= len(gd.VectorsBQ) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsBQ[chunkID]); offset != 0 {
		// BQ uses 64-bit words, typically 2 words per 128-dim vector
		wordsPerVector := (gd.Dims + 127) / 128 // Round up for 128-dim blocks
		stride := wordsPerVector
		return gd.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorBQ(id uint32) []uint64 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsBQChunk(cID)
	if chunk == nil {
		return nil
	}
	wordsPerVector := (gd.Dims + 127) / 128 // Round up for 128-dim blocks
	start := int(cOff) * wordsPerVector
	return chunk[start : start+wordsPerVector]
}

func (gd *GraphData) GetVectorPQ(id uint32) []byte {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsPQChunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.PQDims
	return chunk[start : start+gd.PQDims]
}

func (gd *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsSQ8Chunk(cID)
	if chunk == nil {
		return nil
	}
	stride := (gd.Dims + 63) & ^63
	start := int(cOff) * stride
	return chunk[start : start+gd.Dims]
}

func (gd *GraphData) GetVectorsUint16Chunk(chunkID uint32) []uint16 {
	if int(chunkID) >= len(gd.VectorsUint16) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsUint16[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Uint16Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsUint32Chunk(chunkID uint32) []uint32 {
	if int(chunkID) >= len(gd.VectorsUint32) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsUint32[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Uint32Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsUint64Chunk(chunkID uint32) []uint64 {
	if int(chunkID) >= len(gd.VectorsUint64) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsUint64[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Uint64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsInt64Chunk(chunkID uint32) []int64 {
	if int(chunkID) >= len(gd.VectorsInt64) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsInt64[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Int64Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorsUint8Chunk(chunkID uint32) []uint8 {
	if int(chunkID) >= len(gd.VectorsUint8) {
		return nil
	}
	if offset := atomic.LoadUint64(&gd.VectorsUint8[chunkID]); offset != 0 {
		stride := gd.GetPaddedDims()
		return gd.Uint8Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(ChunkSize * stride),
			Cap:    uint32(ChunkSize * stride),
		})
	}
	return nil
}

func (gd *GraphData) GetVectorF16(id uint32) []float16.Num {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsF16Chunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.GetPaddedDims()
	return chunk[start : start+gd.Dims]
}
