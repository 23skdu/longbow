package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockFreeNeighbors_ConcurrentReads verifies that multiple readers can access
// neighbor lists concurrently without locks and get consistent results.
func TestLockFreeNeighbors_ConcurrentReads(t *testing.T) {
	list := NewLockFreeNeighborList()

	// Initialize with some neighbors
	initialNeighbors := []uint32{1, 2, 3, 4, 5}
	list.Update(initialNeighbors)

	// Spawn 100 concurrent readers
	var wg sync.WaitGroup
	numReaders := 100
	readCount := atomic.Int64{}
	errors := atomic.Int64{}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				neighbors := list.Read()
				readCount.Add(1)

				// Verify correctness
				if len(neighbors) != len(initialNeighbors) {
					errors.Add(1)
					return
				}
				for k, n := range neighbors {
					if n != initialNeighbors[k] {
						errors.Add(1)
						return
					}
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(0), errors.Load(), "No errors should occur during concurrent reads")
	assert.Equal(t, int64(numReaders*1000), readCount.Load(), "All reads should complete")
}

// TestLockFreeNeighbors_ReadWriteConcurrency tests that reads and writes can happen
// concurrently without data races or corruption.
func TestLockFreeNeighbors_ReadWriteConcurrency(t *testing.T) {
	list := NewLockFreeNeighborList()

	// Initialize
	list.Update([]uint32{1, 2, 3})

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					neighbors := list.Read()
					// Verify neighbors are always valid (non-nil and reasonable length)
					if neighbors != nil {
						assert.LessOrEqual(t, len(neighbors), 100, "Neighbor list should be reasonable size")
					}
				}
			}
		}()
	}

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				// Create new neighbor list
				newNeighbors := make([]uint32, (id+1)*2)
				for k := range newNeighbors {
					newNeighbors[k] = uint32(id*100 + k)
				}
				list.Update(newNeighbors)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

// TestLockFreeNeighbors_MemoryOrdering verifies that memory ordering is correct
// and updates are visible to all readers.
func TestLockFreeNeighbors_MemoryOrdering(t *testing.T) {
	list := NewLockFreeNeighborList()

	// Sequence of updates
	sequences := [][]uint32{
		{1},
		{1, 2},
		{1, 2, 3},
		{1, 2, 3, 4},
		{1, 2, 3, 4, 5},
	}

	for _, seq := range sequences {
		list.Update(seq)

		// Verify all readers see the update
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				neighbors := list.Read()
				require.Equal(t, len(seq), len(neighbors), "All readers should see updated length")
				for j, n := range neighbors {
					require.Equal(t, seq[j], n, "All readers should see correct values")
				}
			}()
		}
		wg.Wait()
	}
}

// TestLockFreeNeighbors_RaceDetection is specifically designed to catch races
// under the -race detector.
func TestLockFreeNeighbors_RaceDetection(t *testing.T) {
	list := NewLockFreeNeighborList()
	list.Update([]uint32{1, 2, 3, 4, 5})

	var wg sync.WaitGroup

	// Aggressive concurrent access
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if id%2 == 0 {
				// Reader
				for j := 0; j < 1000; j++ {
					neighbors := list.Read()
					_ = neighbors
				}
			} else {
				// Writer
				for j := 0; j < 100; j++ {
					newNeighbors := make([]uint32, id%10+1)
					for k := range newNeighbors {
						newNeighbors[k] = uint32(k)
					}
					list.Update(newNeighbors)
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestLockFreeNeighbors_NoMemoryLeaks verifies that old neighbor slices are
// properly garbage collected.
func TestLockFreeNeighbors_NoMemoryLeaks(t *testing.T) {
	list := NewLockFreeNeighborList()

	// Perform many updates
	for i := 0; i < 10000; i++ {
		neighbors := make([]uint32, 100)
		for j := range neighbors {
			neighbors[j] = uint32(j)
		}
		list.Update(neighbors)
	}

	// Force GC
	runtime.GC()

	// Memory should not grow unbounded
	// This is a basic check - more sophisticated leak detection would use pprof
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("Alloc after 10k updates: %d MB", m.Alloc/1024/1024)

	// Should be less than 10MB for this test
	assert.Less(t, m.Alloc, uint64(10*1024*1024), "Memory should not leak")
}

// BenchmarkNeighborAccess_LockFree measures lock-free read performance
func BenchmarkNeighborAccess_LockFree(b *testing.B) {
	list := NewLockFreeNeighborList()
	neighbors := make([]uint32, 50)
	for i := range neighbors {
		neighbors[i] = uint32(i)
	}
	list.Update(neighbors)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		n := list.Read()
		_ = n[0] // Use the result
	}
}

// BenchmarkNeighborAccess_Locked measures traditional RWMutex read performance
func BenchmarkNeighborAccess_Locked(b *testing.B) {
	var mu sync.RWMutex
	neighbors := make([]uint32, 50)
	for i := range neighbors {
		neighbors[i] = uint32(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mu.RLock()
		n := neighbors
		_ = n[0]
		mu.RUnlock()
	}
}

// BenchmarkNeighborAccess_LockFree_Parallel measures lock-free performance under contention
func BenchmarkNeighborAccess_LockFree_Parallel(b *testing.B) {
	list := NewLockFreeNeighborList()
	neighbors := make([]uint32, 50)
	for i := range neighbors {
		neighbors[i] = uint32(i)
	}
	list.Update(neighbors)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := list.Read()
			_ = n[0]
		}
	})
}

// BenchmarkNeighborAccess_Locked_Parallel measures RWMutex performance under contention
func BenchmarkNeighborAccess_Locked_Parallel(b *testing.B) {
	var mu sync.RWMutex
	neighbors := make([]uint32, 50)
	for i := range neighbors {
		neighbors[i] = uint32(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			n := neighbors
			_ = n[0]
			mu.RUnlock()
		}
	})
}

// TestLockFreeNeighbors_UpdateCorrectness verifies that updates are applied correctly
func TestLockFreeNeighbors_UpdateCorrectness(t *testing.T) {
	list := NewLockFreeNeighborList()

	testCases := []struct {
		name      string
		neighbors []uint32
	}{
		{"empty", []uint32{}},
		{"single", []uint32{42}},
		{"small", []uint32{1, 2, 3, 4, 5}},
		{"medium", make([]uint32, 50)},
		{"large", make([]uint32, 500)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize test data
			for i := range tc.neighbors {
				tc.neighbors[i] = uint32(i * 10)
			}

			// Update
			list.Update(tc.neighbors)

			// Read back
			result := list.Read()

			// Verify
			require.Equal(t, len(tc.neighbors), len(result))
			for i, n := range result {
				assert.Equal(t, tc.neighbors[i], n)
			}
		})
	}
}

// TestLockFreeNeighbors_ConcurrentUpdates verifies that concurrent updates
// don't corrupt the data structure.
func TestLockFreeNeighbors_ConcurrentUpdates(t *testing.T) {
	list := NewLockFreeNeighborList()

	var wg sync.WaitGroup
	numWriters := 10
	updatesPerWriter := 100

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < updatesPerWriter; j++ {
				neighbors := make([]uint32, id+1)
				for k := range neighbors {
					neighbors[k] = uint32(id*1000 + j*10 + k)
				}
				list.Update(neighbors)
			}
		}(i)
	}

	wg.Wait()

	// Final read should succeed without panic
	result := list.Read()
	assert.NotNil(t, result)
}
