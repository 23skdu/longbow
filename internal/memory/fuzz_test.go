package memory

import (
	"testing"
)

func FuzzSlabArena_Alloc(f *testing.F) {
	// Seed corpus
	f.Add(1)
	f.Add(1024)
	f.Add(1024 * 1024)
	f.Add(16 * 1024 * 1024)
	f.Add(16*1024*1024 + 1) // Boundary

	f.Fuzz(func(t *testing.T, size int) {
		if size <= 0 {
			return // Alloc expects positive size (or handles 0)
		}

		slabSize := 1024 * 1024 // 1MB Slab
		arena := NewSlabArena(slabSize)

		off, err := arena.Alloc(size)
		if size > slabSize {
			if err == nil {
				t.Errorf("Expected error for alloc size %d > slab size %d", size, slabSize)
			}
			return
		}

		if err != nil {
			t.Errorf("Alloc failed for valid size %d: %v", size, err)
			return
		}

		if off == 0 {
			t.Errorf("Alloc returned 0 offset for valid size %d", size)
		}

		// Verify we can access the memory
		slice := arena.Get(off, size)
		if slice == nil {
			t.Errorf("Get returned nil for offset %d size %d", off, size)
			return
		}
		if len(slice) != size {
			t.Errorf("Get returned slice length %d, want %d", len(slice), size)
		}

		// Write to verify bounds (cheap check)
		slice[0] = 0xAA
		slice[len(slice)-1] = 0xBB
	})
}

func FuzzTypedArena_AllocSlice(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(100)
	f.Add(100000)

	f.Fuzz(func(t *testing.T, length int) {
		if length < 0 {
			return
		}

		// Use small slab to force multiple slabs potentially
		slabSize := 4096
		slabArena := NewSlabArena(slabSize)
		// Int32 Arena (4 bytes per elem)
		intArena := NewTypedArena[int32](slabArena)

		sizeBytes := length * 4
		if sizeBytes > slabSize {
			// Expect error? Or TypedArena handles large allocs?
			// SlabArena Alloc errors if size > slabSize.
			_, err := intArena.AllocSlice(length)
			if err == nil {
				t.Logf("Warning: AllocSlice succeeded for sizeBytes %d > slabSize %d? (Maybe arena implementation changed)", sizeBytes, slabSize)
			}
			// We mostly fuzz for panics here
			return
		}

		ref, err := intArena.AllocSlice(length)
		if err != nil {
			t.Errorf("AllocSlice failed for length %d: %v", length, err)
			return
		}

		if length > 0 && ref.Len != uint32(length) {
			t.Errorf("SliceRef length mismatch: got %d, want %d", ref.Len, length)
		}

		slice := intArena.Get(ref)
		if slice == nil {
			if length == 0 {
				return
			}
			t.Errorf("Get returned nil for valid ref: %+v", ref)
			return
		}

		if len(slice) != length {
			t.Errorf("Go slice length %d != requested %d", len(slice), length)
		}
	})
}
