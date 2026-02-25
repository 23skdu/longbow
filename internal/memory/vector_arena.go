package memory

import (
	"errors"
	"sync"
	"unsafe"
)

// VectorArena is a specialized arena for vector storage that integrates with the slab arena
// and provides vector-specific allocation methods with proper alignment for SIMD operations.
type VectorArena struct {
	arena *SlabArena
	mu    sync.Mutex
}

// NewVectorArena creates a new vector arena with the specified slab size.
func NewVectorArena(slabSizeBytes int) *VectorArena {
	return &VectorArena{
		arena: NewSlabArena(slabSizeBytes),
	}
}

// AllocVector allocates a vector of the specified dimension and type.
// Returns a global offset and a typed slice that points to the off-heap memory.
func (va *VectorArena) AllocVector(dim int, vecType VectorType) (uint64, interface{}, error) {
	va.mu.Lock()
	defer va.mu.Unlock()

	// Calculate size based on vector type
	size := dim * vecType.size()
	if size <= 0 {
		return 0, nil, errors.New("invalid vector dimension or type")
	}

	// Allocate from slab arena
	offset, err := va.arena.Alloc(size)
	if err != nil {
		return 0, nil, err
	}

	// Get pointer to the allocated memory
	ptr := va.arena.GetPointer(offset)

	// Create typed slice based on vector type
	var slice interface{}
	switch vecType {
	case VectorTypeFloat32:
		slice = unsafe.Slice((*float32)(ptr), dim)
	case VectorTypeFloat16:
		slice = unsafe.Slice((*uint16)(ptr), dim)
	case VectorTypeInt8:
		slice = unsafe.Slice((*int8)(ptr), dim)
	case VectorTypeComplex64:
		slice = unsafe.Slice((*complex64)(ptr), dim)
	default:
		return 0, nil, errors.New("unsupported vector type")
	}

	return offset, slice, nil
}

// GetVector returns a typed slice for the vector at the specified offset.
func (va *VectorArena) GetVector(offset uint64, dim int, vecType VectorType) (interface{}, error) {
	if offset == 0 {
		return nil, errors.New("invalid offset")
	}

	// Get the memory from arena
	ptr := va.arena.GetPointer(offset)
	if ptr == nil {
		return nil, errors.New("vector not found")
	}

	// Create typed slice based on vector type
	switch vecType {
	case VectorTypeFloat32:
		return unsafe.Slice((*float32)(ptr), dim), nil
	case VectorTypeFloat16:
		return unsafe.Slice((*uint16)(ptr), dim), nil
	case VectorTypeInt8:
		return unsafe.Slice((*int8)(ptr), dim), nil
	case VectorTypeComplex64:
		return unsafe.Slice((*complex64)(ptr), dim), nil
	default:
		return nil, errors.New("unsupported vector type")
	}
}

// Free releases all memory back to the slab arena.
func (va *VectorArena) Free() {
	va.mu.Lock()
	defer va.mu.Unlock()
	va.arena.Free()
}

// VectorType defines the supported vector data types.
type VectorType int

const (
	// VectorTypeFloat32 is a 32-bit floating point vector
	VectorTypeFloat32 VectorType = iota
	// VectorTypeFloat16 is a 16-bit floating point vector
	VectorTypeFloat16
	// VectorTypeInt8 is an 8-bit integer vector
	VectorTypeInt8
	// VectorTypeComplex64 is a complex 64-bit vector
	VectorTypeComplex64
)

// size returns the size in bytes for the vector type.
func (vt VectorType) size() int {
	switch vt {
	case VectorTypeFloat32:
		return 4
	case VectorTypeFloat16:
		return 2
	case VectorTypeInt8:
		return 1
	case VectorTypeComplex64:
		return 8 // complex64 is two float32s
	default:
		return 0
	}
}

// VectorID is an alias for uint32 to represent vector identifiers.
type VectorID uint32

// Stats returns statistics about the vector arena usage.
func (va *VectorArena) Stats() ArenaStats {
	return va.arena.Stats()
}

// RegisterArena registers the vector arena with the global registry.
func (va *VectorArena) RegisterArena() {
	RegisterArena(va.arena)
}
