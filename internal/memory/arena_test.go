package memory

import (
	"sync"
	"testing"
	"unsafe"
)

func TestSlabArena_Alloc(t *testing.T) {
	// 1024 byte slabs
	arena := NewSlabArena(1024)

	// Alloc 100 bytes
	off1, err := arena.Alloc(100)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Alloc 500 bytes
	off2, err := arena.Alloc(500)
	if err != nil {
		t.Fatalf("Alloc 2 failed: %v", err)
	}

	// Alloc 600 bytes (should trigger new slab as 100+500+600 > 1024)
	off3, err := arena.Alloc(600)
	if err != nil {
		t.Fatalf("Alloc 3 failed: %v", err)
	}

	// Verify offsets are distinct
	if off1 == off2 || off1 == off3 || off2 == off3 {
		t.Errorf("Offsets should be distinct: %d, %d, %d", off1, off2, off3)
	}

	// Check slab IDs (implied by offset logic, usually high bits)
	// Assuming simple sequential offsets for now or specific packing logic
	// If we use simple 64-bit offsets:
	// slab1: 0..1024
	// slab2: 1024..2048
	// OR (SlabID << 32) | Offset

	// Let's verify data isolation
	data1 := arena.Get(off1, 100)
	data1[0] = 0xAA

	data2 := arena.Get(off2, 500)
	data2[0] = 0xBB

	if data1[0] != 0xAA {
		t.Error("Data1 corruption")
	}
	if data2[0] != 0xBB {
		t.Error("Data2 corruption")
	}
}

func TestTypedArena_Slice(t *testing.T) {
	slabArena := NewSlabArena(4096) // 4KB
	floatArena := NewTypedArena[float32](slabArena)

	// Allocate slice of 10 floats
	header, err := floatArena.AllocSlice(10)
	if err != nil {
		t.Fatalf("AllocSlice failed: %v", err)
	}

	// Get access
	slice := floatArena.Get(header)
	if len(slice) != 10 {
		t.Fatalf("Expected len 10, got %d", len(slice))
	}

	// Write data
	for i := 0; i < 10; i++ {
		slice[i] = float32(i) * 1.5
	}

	// Verify data
	sliceRead := floatArena.Get(header)
	for i := 0; i < 10; i++ {
		expected := float32(i) * 1.5
		if sliceRead[i] != expected {
			t.Errorf("Idx %d: expected %f, got %f", i, expected, sliceRead[i])
		}
	}
}

func TestArena_Concurrency(t *testing.T) {
	arena := NewSlabArena(1024 * 1024) // 1MB
	var wg sync.WaitGroup

	// 50 concurrent allocators
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				size := 128
				off, err := arena.Alloc(size)
				if err != nil {
					t.Errorf("Routine %d iter %d alloc failed: %v", id, j, err)
					return
				}

				// Write check
				data := arena.Get(off, size)
				data[0] = byte(id)
				data[size-1] = byte(id)
			}
		}(i)
	}
	wg.Wait()
}

func TestSliceHeader_Safety(t *testing.T) {
	// Verify that SliceRef fits in uint64 (packing offset + len)
	// We want SliceRef to be compact.
	// If SliceRef is a struct { Offset uint64, Len int }, it's 16 bytes.
	// For HNSW neighbors, we have many of them.
	// Ideally SliceRef is just uint64 if we can assume max size?
	// Or we keep it 16 bytes.

	ref := SliceRef{Offset: 12345, Len: 100}
	if unsafe.Sizeof(ref) > 16 {
		t.Errorf("SliceRef too large: %d", unsafe.Sizeof(ref))
	}
}
