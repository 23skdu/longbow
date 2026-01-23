package memory

import (
	"sync"
	"testing"
)

func FuzzCompactableArena_Operations(f *testing.F) {
	// Seed corpus
	f.Add(uint8(10), uint8(5), uint8(100))
	f.Add(uint8(50), uint8(25), uint8(200))

	f.Fuzz(func(t *testing.T, numAllocs, keepEvery, sliceSize uint8) {
		if numAllocs == 0 || keepEvery == 0 || sliceSize == 0 {
			return
		}

		arena := NewCompactableArena[uint32](1 * 1024 * 1024)

		// Allocate slices
		var refs []SliceRef
		for i := uint8(0); i < numAllocs; i++ {
			ref, err := arena.AllocSlice(int(sliceSize))
			if err != nil {
				// Allocation failed (out of space), that's ok
				break
			}

			// Write pattern
			data := arena.Get(ref)
			for j := range data {
				data[j] = uint32(i)*1000 + uint32(j)
			}

			refs = append(refs, ref)
		}

		if len(refs) == 0 {
			return
		}

		// Keep every Nth slice
		liveRefs := make([]SliceRef, 0)
		for i := 0; i < len(refs); i += int(keepEvery) {
			liveRefs = append(liveRefs, refs[i])
		}

		if len(liveRefs) == 0 {
			return
		}

		// Compact
		newRefs, stats, err := arena.Compact(liveRefs)
		if err != nil {
			t.Errorf("compaction failed: %v", err)
			return
		}

		// Verify invariants
		if len(newRefs) != len(liveRefs) {
			t.Errorf("newRefs length mismatch: got %d, want %d", len(newRefs), len(liveRefs))
		}

		if stats.LiveDataCopied < 0 {
			t.Errorf("negative live data copied: %d", stats.LiveDataCopied)
		}

		if stats.BytesReclaimed < 0 {
			t.Errorf("negative bytes reclaimed: %d", stats.BytesReclaimed)
		}

		if stats.FragmentationPct < 0 || stats.FragmentationPct > 100 {
			t.Errorf("invalid fragmentation percentage: %.2f", stats.FragmentationPct)
		}
	})
}

func FuzzCompactableArena_ConcurrentAccess(f *testing.F) {
	f.Add(uint8(5), uint8(10))

	f.Fuzz(func(t *testing.T, numGoroutines, allocsPerGoroutine uint8) {
		if numGoroutines == 0 || allocsPerGoroutine == 0 {
			return
		}

		arena := NewCompactableArena[uint64](4 * 1024 * 1024)

		var wg sync.WaitGroup
		wg.Add(int(numGoroutines))

		for g := uint8(0); g < numGoroutines; g++ {
			go func(id uint8) {
				defer wg.Done()

				for i := uint8(0); i < allocsPerGoroutine; i++ {
					ref, err := arena.AllocSlice(100)
					if err != nil {
						// Out of space, that's ok
						return
					}

					// Write and read
					data := arena.Get(ref)
					data[0] = uint64(id)*1000 + uint64(i)

					// Read it back
					readData := arena.Get(ref)
					if readData[0] != uint64(id)*1000+uint64(i) {
						t.Errorf("data mismatch")
						return
					}
				}
			}(g)
		}

		wg.Wait()
	})
}
