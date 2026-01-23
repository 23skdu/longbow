package memory

import (
	"sync"
	"testing"
)

func FuzzSlabPool_ConcurrentOperations(f *testing.F) {
	// Seed corpus with various operation patterns
	f.Add(uint8(10), uint8(5), uint8(3))
	f.Add(uint8(100), uint8(50), uint8(10))
	f.Add(uint8(255), uint8(128), uint8(64))

	f.Fuzz(func(t *testing.T, numGoroutines, allocsPerGoroutine, releaseFreq uint8) {
		if numGoroutines == 0 || allocsPerGoroutine == 0 {
			return
		}

		pool := newSlabPool(4 * 1024 * 1024)
		pool.maxPooled = 50

		var wg sync.WaitGroup
		wg.Add(int(numGoroutines))

		for g := uint8(0); g < numGoroutines; g++ {
			go func() {
				defer wg.Done()

				slabs := make([][]byte, 0, allocsPerGoroutine)

				for i := uint8(0); i < allocsPerGoroutine; i++ {
					// Allocate
					slab := pool.Get()
					if cap(slab) != 4*1024*1024 {
						t.Errorf("unexpected slab capacity: %d", cap(slab))
						return
					}

					// Write to it to ensure it's valid
					slab[0] = byte(i)
					slab[len(slab)-1] = byte(i)

					slabs = append(slabs, slab)

					// Periodically release some slabs
					if releaseFreq > 0 && i%releaseFreq == 0 && len(slabs) > 0 {
						pool.Put(slabs[0])
						slabs = slabs[1:]
					}
				}

				// Release all remaining slabs
				for _, slab := range slabs {
					pool.Put(slab)
				}
			}()
		}

		wg.Wait()

		// Verify final state
		if pool.ActiveCount() != 0 {
			t.Errorf("expected 0 active slabs, got %d", pool.ActiveCount())
		}
	})
}

func FuzzSlabPool_ReleaseUnused(f *testing.F) {
	f.Add(uint8(50), uint8(10))
	f.Add(uint8(100), uint8(50))

	f.Fuzz(func(t *testing.T, numSlabs, maxPooled uint8) {
		if numSlabs == 0 || maxPooled == 0 {
			return
		}

		pool := newSlabPool(4 * 1024 * 1024)
		pool.maxPooled = int64(maxPooled)

		// Allocate and return slabs
		slabs := make([][]byte, numSlabs)
		for i := range slabs {
			slabs[i] = pool.Get()
		}
		for _, slab := range slabs {
			pool.Put(slab)
		}

		initialPooled := pool.PooledCount()

		// Release unused
		released := pool.ReleaseUnused()

		// Verify invariants
		if released < 0 {
			t.Errorf("negative release count: %d", released)
		}

		finalPooled := pool.PooledCount()
		if finalPooled > initialPooled {
			t.Errorf("pooled count increased after release: %d -> %d", initialPooled, finalPooled)
		}

		if pool.ActiveCount() != 0 {
			t.Errorf("active count should be 0, got %d", pool.ActiveCount())
		}
	})
}
