package memory

import (
	"unsafe"
)

// TypedArena wraps a SlabArena to provide typed slice access.
type TypedArena[T any] struct {
	arena *SlabArena
}

func NewTypedArena[T any](arena *SlabArena) *TypedArena[T] {
	return &TypedArena[T]{
		arena: arena,
	}
}

func (ta *TypedArena[T]) AllocSlice(count int) (SliceRef, error) {
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	totalBytes := count * elemSize

	offset, err := ta.arena.Alloc(totalBytes)
	if err != nil {
		return SliceRef{}, err
	}

	return SliceRef{
		Offset: offset,
		Len:    uint32(count),
		Cap:    uint32(count),
	}, nil
}

// Get retrieves a typed slice from the arena using a SliceRef.
func (ta *TypedArena[T]) Get(ref SliceRef) []T {
	if ref.Offset == 0 || ref.Len == 0 {
		return nil
	}

	var zero T
	elemSize := uint32(unsafe.Sizeof(zero))
	byteSlice := ta.arena.Get(ref.Offset, ref.Len*elemSize)
	if byteSlice == nil || len(byteSlice) == 0 {
		return nil
	}

	// Use unsafe.Slice instead of deprecated reflect.SliceHeader
	ptr := unsafe.Pointer(&byteSlice[0])
	return unsafe.Slice((*T)(ptr), ref.Len)
}
