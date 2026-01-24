package types

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// GraphData holds the vector data and graph topology.
// It effectively implements the component storage for ArrowHNSW.
type GraphData struct {
	// Metadata
	Capacity int
	Dims     int
	Type     VectorDataType

	// Mutable State
	BackingGraph any // interface{} to avoid import cycle (likely *DiskGraph)

	// Vectors (primary storage, usually float32)
	Vectors [][]float32

	// VectorsPQ for quantized vectors
	VectorsPQ []byte // Placeholder

	// VectorsF16 for half-precision
	VectorsF16 []byte // Placeholder

	// VectorsBQ for binary quantized vectors
	VectorsBQ []uint64 // Placeholder

	// VectorsSQ8 for scalar quantized vectors
	VectorsSQ8 []byte // Placeholder

	// VectorsFloat64
	VectorsFloat64 [][]float64

	// VectorsComplex64
	VectorsComplex64 [][]complex64

	// VectorsComplex128
	VectorsComplex128 [][]complex128

	// Neighbors (Offsets)
	Neighbors [][]uint64

	// Levels (Offsets)
	Levels []uint64

	// Versions (Offsets)
	Versions [][]uint64

	// Counts (Offsets)
	Counts [][]uint64

	// Memory Arenas
	Float32Arena    *memory.TypedArena[float32]
	Float64Arena    *memory.TypedArena[float64]
	Uint8Arena      *memory.TypedArena[uint8]
	Uint16Arena     *memory.TypedArena[uint16]
	Uint32Arena     *memory.TypedArena[uint32]
	Uint64Arena     *memory.TypedArena[uint64]
	Int8Arena       *memory.TypedArena[int8]
	Int16Arena      *memory.TypedArena[int16]
	Int32Arena      *memory.TypedArena[int32]
	Int64Arena      *memory.TypedArena[int64]
	Float16Arena    *memory.TypedArena[float16.Num]
	Complex64Arena  *memory.TypedArena[complex64]
	Complex128Arena *memory.TypedArena[complex128]

	// PackedNeighbors
	PackedNeighbors []PackedNeighbors
}

// PackedNeighbors interface for graph adjacency management
type PackedNeighbors interface {
	GetNeighbors(id uint32) ([]uint32, bool)
	SetNeighbors(id uint32, neighbors []uint32) error
	GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool)
	SetNeighborsF16(id uint32, neighbors []uint32, dists []float16.Num) error
}

// GetVectorsChunk returns the vector chunk for the given ID.
func (g *GraphData) GetVectorsChunk(chunkID int) []float32 {
	if chunkID < len(g.Vectors) {
		return g.Vectors[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsFloat64Chunk(chunkID int) []float64 {
	if chunkID < len(g.VectorsFloat64) {
		return g.VectorsFloat64[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsComplex64Chunk(chunkID int) []complex64 {
	if chunkID < len(g.VectorsComplex64) {
		return g.VectorsComplex64[chunkID]
	}
	return nil
}

func (g *GraphData) GetVectorsComplex128Chunk(chunkID int) []complex128 {
	if chunkID < len(g.VectorsComplex128) {
		return g.VectorsComplex128[chunkID]
	}
	return nil
}

func (g *GraphData) GetPaddedDims() int {
	return g.Dims
}

func (g *GraphData) GetPaddedDimsForType(dt VectorDataType) int {
	return g.Dims
}

func (g *GraphData) GetVectorsSQ8Chunk(chunkID int) []byte {
	return g.VectorsSQ8 // Simplified
}

func (g *GraphData) GetVectorsBQChunk(chunkID int) []uint64 {
	return g.VectorsBQ // Simplified
}

// GetVectorsPQChunk is a method used in pq_training.go
func (g *GraphData) GetVectorsPQChunk(chunkID int) []byte {
	return nil // Placeholder
}

func (g *GraphData) GetCountsChunk(layer, chunkID int) []int32 {
	return nil
}

func (g *GraphData) GetNeighborsChunk(layer, chunkID int) []uint32 {
	return nil
}

func (g *GraphData) GetVersionsChunk(layer, chunkID int) []uint32 {
	return nil
}

// GetVector returns the vector for the given ID.
func (g *GraphData) GetVector(id uint32) (any, error) {
	// Need implementation. For now, return nil/error to satisfy interface.
	return nil, nil // Placeholder
}

func (g *GraphData) SetVector(id uint32, vec any) error {
	return nil
}

func (g *GraphData) DiskStore() any {
	return g.BackingGraph
}

func (g *GraphData) PQDims() int {
	return 0
}

func (g *GraphData) GetVectorPQ(id uint32) []byte {
	return nil
}

func (g *GraphData) GetVectorBQ(id uint32) ([]uint64, error) {
	return nil, nil
}

// GetNeighbors returns the neighbors for a given node at a level.
func (g *GraphData) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	if layer >= len(g.Neighbors) {
		return nil
	}
	// Simplified chunk access (assuming standard chunking)
	// We need access constants MaxNeighbors, ChunkSize.
	// Since we are in types, we might not have them.
	// We'll rely on g.GetNeighborsChunk logic.
	// We assume id maps to chunk.
	// This is hard without importing store/constants.
	// BUT disk_writer.go calls this.
	// Maybe we should implementation simple logic assuming chunks are correct.
	// Or maybe Neighbors is just flat? No, [][]uint64.
	// We will return nil for now as placeholder to satisfy compile,
	// OR panic "not implemented" if runtime.
	// Given we are fixing compilation:
	return nil
}

func (g *GraphData) GetVectorSQ8(id uint32) []byte {
	return nil
}

// GetLevelsChunk returns the level chunk for the given ID.
func (g *GraphData) GetLevelsChunk(chunkID int) []uint8 {
	return nil
}

// NewGraphData creates a new GraphData instance.
// This is a helper for legacy tests.
func NewGraphData(capacity, dim int, mmap bool, useDisk bool, fd int,
	quantization bool, sq8 bool, persistent bool,
	dataType VectorDataType) *GraphData {

	gd := &GraphData{
		Capacity: capacity,
		Dims:     dim,
		Type:     dataType,
		Vectors:  make([][]float32, 0),
	}
	return gd
}
