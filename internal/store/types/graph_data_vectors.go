package types

import (
	"fmt"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"math"
	"sync/atomic"
	"unsafe"
)

func (g *GraphData) SetVectorPQ(id uint32, code []byte) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsPQ) {
		m := g.PQM
		if len(code) != m {
			return fmt.Errorf("PQ code length mismatch: expected %d, got %d", m, len(code))
		}

		numWordsPerNode := (m + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsPQ[cID],
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		})

		if len(chunk) == 0 {
			return fmt.Errorf("PQ chunk is empty (arena %p, offset %d)", g.Uint64Arena, g.VectorsPQ[cID])
		}

		// Cast uint64 to byte slice
		ptr := unsafe.Pointer(&chunk[0])                    // #nosec G103
		byteChunk := unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103

		start := cOff * m
		if start+m <= len(byteChunk) {
			copy(byteChunk[start:start+m], code)
			return nil
		}
	}
	return fmt.Errorf("failed to set PQ vector for id %d", id)
}

func (g *GraphData) GetVector(id uint32) (any, error) {
	return g.GetVectorWithGen(id, math.MaxUint64)
}

func (g *GraphData) GetVectorWithGen(id uint32, maxGen uint64) (any, error) {
	if g.Dims <= 0 {
		return nil, fmt.Errorf("vector type mismatch or uninitialized for ID %d (dims is 0)", id)
	}

	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	// Based on type, get the appropriate chunk
	// Only supporting float32 and float16 for now in this generic method
	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID && g.SQ8Enabled && atomic.LoadUint32(&g.SQ8Ready) == 1 {
		chunk := g.GetVectorsSQ8ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	}

	switch g.Type {
	case VectorTypeUint8:
		chunk := g.GetVectorsInt8ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			ptr := unsafe.Pointer(&chunk[0])                   // #nosec G103
			u8Chunk := unsafe.Slice((*uint8)(ptr), len(chunk)) // #nosec G103
			return u8Chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt8:
		chunk := g.GetVectorsInt8ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt8)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt16:
		chunk := g.GetVectorsInt16ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint16:
		chunk := g.GetVectorsUint16ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint16)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt32:
		chunk := g.GetVectorsInt32ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint32:
		chunk := g.GetVectorsUint32ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint32)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeInt64:
		chunk := g.GetVectorsInt64ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeInt64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeUint64:
		chunk := g.GetVectorsUint64ChunkWithGen(cID, maxGen)
		if chunk == nil {
			return nil, nil
		}
		pd := g.GetPaddedDimsForType(VectorTypeUint64)
		start := cOff * pd
		if start+g.Dims <= len(chunk) {
			return chunk[start : start+g.Dims], nil
		}
	case VectorTypeFloat32:
		chunk := g.GetVectorsChunkWithGen(cID, maxGen)
		if chunk != nil {
			pd := g.GetPaddedDimsForType(VectorTypeFloat32)
			start := cOff * pd
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat64:
		chunk := g.GetVectorsFloat64ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex64:
		chunk := g.GetVectorsComplex64ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeComplex128:
		chunk := g.GetVectorsComplex128ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeFloat16:
		chunk := g.GetVectorsF16ChunkWithGen(cID, maxGen)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				return chunk[start : start+g.Dims], nil
			}
		}
	case VectorTypeTQ:
		chunk := g.GetVectorsTQChunkWithGen(cID, maxGen)
		if chunk != nil {
			stride := g.PackedSize()
			start := cOff * stride
			if start+stride <= len(chunk) {
				return chunk[start : start+stride], nil
			}
		}
	}

	return nil, fmt.Errorf("vector type mismatch or uninitialized for ID %d (type %v)", id, g.Type)
}

func (g *GraphData) SetVector(id uint32, vec any) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	switch v := vec.(type) {
	case []float32:
		if g.Type == VectorTypeFloat16 {
			// Convert to Float16
			chunk := g.GetVectorsF16Chunk(cID)
			if chunk != nil {
				paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
				start := cOff * paddedDims
				if start+len(v) <= len(chunk) {
					for i, val := range v {
						chunk[start+i] = float16.New(val)
					}
				}
			}
			return nil
		}
		chunk := g.GetVectorsChunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat32)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				// Zero-Copy Optimization: Skip copy if source and destination are the same memory
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []float16.Num:
		chunk := g.GetVectorsF16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []float64:
		chunk := g.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeFloat64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				dest := chunk[start : start+len(v)]
				if len(dest) > 0 && len(v) > 0 && &dest[0] == &v[0] {
					return nil
				}
				copy(dest, v)
			}
		}
	case []complex64:
		chunk := g.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []complex128:
		chunk := g.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeComplex128)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
		// Compute and store magnitude for triangle-inequality pruning
		var sum float64
		for _, val := range v {
			sum += real(val)*real(val) + imag(val)*imag(val)
		}
		g.SetComplex128Magnitude(id, math.Sqrt(sum))
	case []uint8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint8)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				// Copy []uint8 to []int8 chunk via unsafe cast
				uint8Chunk := *(*[]uint8)(unsafe.Pointer(&chunk)) // #nosec G103
				copy(uint8Chunk[start:start+len(v)], v)
			}
		}
	case []int8:
		chunk := g.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt8)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int16:
		chunk := g.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint16:
		chunk := g.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint16)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int64:
		chunk := g.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint64:
		chunk := g.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint64)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []int32:
		chunk := g.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeInt32)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	case []uint32:
		chunk := g.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			paddedDims := g.GetPaddedDimsForType(VectorTypeUint32)
			start := cOff * paddedDims
			if start+len(v) <= len(chunk) {
				copy(chunk[start:start+len(v)], v)
			}
		}
	}
	return nil
}

// GetComplex128Magnitude returns the pre-computed L2 magnitude for a complex128 vector.

func (g *GraphData) GetComplex128Magnitude(id uint32) float64 {
	if int(id) < len(g.Complex128Magnitudes) {
		return g.Complex128Magnitudes[id]
	}
	return 0
}

// SetComplex128Magnitude stores the L2 magnitude for a complex128 vector.
func (g *GraphData) SetComplex128Magnitude(id uint32, mag float64) {
	if int(id) < len(g.Complex128Magnitudes) {
		g.Complex128Magnitudes[id] = mag
	}
}

// SetVectorsBatch sets multiple vectors in the same chunk efficiently.
// This is optimized for bulk insertion where vectors belong to the same chunk.
func (g *GraphData) SetVectorsBatch(startID uint32, vecs [][]float32) error {
	if len(vecs) == 0 {
		return nil
	}

	// Get chunk info for first vector
	startChunk := int(startID) / ChunkSize
	chunk := g.GetVectorsChunk(startChunk)
	if chunk == nil {
		return fmt.Errorf("chunk %d not found", startChunk)
	}

	dims := g.Dims
	if dims == 0 {
		return fmt.Errorf("dimensions not set")
	}

	// Batch copy all vectors to chunk
	for i, vec := range vecs {
		id := startID + uint32(i)
		cID := int(id) / ChunkSize
		cOff := int(id) % ChunkSize

		// Ensure we're in the same chunk
		if cID != startChunk {
			// Different chunk - use regular SetVector
			if err := g.SetVector(id, vec); err != nil {
				return err
			}
			continue
		}

		start := cOff * dims
		if start+len(vec) <= len(chunk) {
			if len(vec) > 0 {
				simd.MemcpyNTA(unsafe.Pointer(&chunk[start]), unsafe.Pointer(&vec[0]), len(vec)*4) // #nosec G103
			}
		}
	}

	return nil
}

func (g *GraphData) GetVectorPQ(id uint32) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	chunk := g.GetVectorsPQChunk(cID)
	if chunk == nil {
		return nil
	}

	m := g.PQM
	if m == 0 {
		return nil
	}

	start := cOff * m
	if start+m <= len(chunk) {
		return chunk[start : start+m]
	}
	return nil
}

func (g *GraphData) GetVectorPQWithGen(id uint32, maxGen uint64) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsPQ) {
		m := g.PQM
		numWordsPerNode := (m + 7) / 8
		numWords := ChunkSize * numWordsPerNode

		offset := atomic.LoadUint64(&g.VectorsPQ[cID])
		chunk := g.Uint64Arena.GetWithGeneration(memory.SliceRef{
			Offset: offset,
			Len:    uint32(numWords), // #nosec G115
			Cap:    uint32(numWords), // #nosec G115
		}, maxGen)

		if len(chunk) == 0 {
			return nil
		}

		ptr := unsafe.Pointer(&chunk[0])                    // #nosec G103
		byteChunk := unsafe.Slice((*byte)(ptr), numWords*8) // #nosec G103

		start := cOff * m
		if start+m <= len(byteChunk) {
			return byteChunk[start : start+m]
		}
	}
	return nil
}

func (g *GraphData) GetVectorBQ(id uint32) ([]uint64, error) {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWords := paddedDims / 64
		chunkLen := ChunkSize * numWords

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsBQ[cID],
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		})

		start := cOff * numWords
		if start+numWords <= len(chunk) {
			return chunk[start : start+numWords], nil
		}
	}
	return nil, fmt.Errorf("BQ vector not found for id %d", id)
}

func (g *GraphData) SetVectorBQ(id uint32, vec []uint64) error {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint64Arena != nil && cID < len(g.VectorsBQ) {
		paddedDims := (g.Dims + 63) & ^63
		numWords := paddedDims / 64
		chunkLen := ChunkSize * numWords

		chunk := g.Uint64Arena.Get(memory.SliceRef{
			Offset: g.VectorsBQ[cID],
			Len:    uint32(chunkLen), // #nosec G115
			Cap:    uint32(chunkLen), // #nosec G115
		})

		start := cOff * numWords
		if start+len(vec) <= len(chunk) {
			copy(chunk[start:start+len(vec)], vec)
			return nil
		}
	}
	return fmt.Errorf("failed to set BQ vector for id %d", id)
}

func (g *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := int(id) / ChunkSize
	cOff := int(id) % ChunkSize

	if g.Uint8Arena != nil && len(g.VectorsSQ8) > cID {
		chunk := g.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			paddedDims := (g.Dims + 63) & ^63
			start := cOff * paddedDims
			if start+g.Dims <= len(chunk) {
				// Return a copy to be safe, or just the slice?
				// Disk writer wants to write it, so slice is fine.
				return chunk[start : start+g.Dims]
			}
		}
	}
	return nil
}

// GetLevelsChunk returns the level chunk for the given ID.
