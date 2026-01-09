package store

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHNSW_GrowthRace reproduces the "lost chunk allocation" bug.
// It runs multiple inserters that target IDs across different chunks.
// Concurrently, it forces Grow() operations (implicitly via inserts).
func TestHNSW_GrowthRace(t *testing.T) {
	// Setup
	cfg := DefaultArrowHNSWConfig()
	cfg.InitialCapacity = 100 // Start small to force frequent Grows
	cfg.M = 16
	cfg.EfConstruction = 100

	h := NewArrowHNSW(nil, cfg, nil)
	// Manually set dims to > 0 so we don't hit the "initMu" path for first insert
	h.dims.Store(128)

	// Create "vectors" implicitly by just calling Insert with dummy ID.
	// We need to ensure GetVector works?
	// Insert checks getVector unless we use InsertWithVector?
	// The bug is in InsertWithVector / ensureChunk logic.
	// So we can mock the vector retrieval or just pass a vector.

	vectorSize := 128
	dummyVec := make([]float32, vectorSize)
	for i := range dummyVec {
		dummyVec[i] = rand.Float32()
	}

	var wg sync.WaitGroup
	numWorkers := 10
	insertsPerWorker := 1000

	// We want to force reallocation.
	// IDs will range from 0 to numWorkers * insertsPerWorker

	// Track successful inserts
	var successCount atomic.Int32
	var failures atomic.Int32

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Stagger start
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

			for i := 0; i < insertsPerWorker; i++ {
				// ID calculation: spread out to hit different chunks?
				// Or sequential to force continuous growth?
				// Sequential per worker, but interleaved globally.
				id := uint32(workerID*insertsPerWorker + i)

				// Insert
				err := h.InsertWithVector(id, dummyVec, h.generateLevel())
				if err != nil {
					// We might expect some errors if alloc fails, but ideally nil
					failures.Add(1)

				} else {
					successCount.Add(1)
				}

				// Verify immediately?
				// The bug is: Insert returns success, but the vector is not in the graph data
				// because the chunk it wrote to was discarded.

				// We can check if "GetNeighbors" or accessing the vector works.
				// Since we don't have GetVector implemented fully without dataset,
				// we can check internal structure or check if "data.Vectors[chunk]" is nil?
				// But that's internal.

				// Better: Run a "reader" concurrently that checks recently inserted IDs?
				// Or just check all at the end.

				if i%10 == 0 {
					runtime.Gosched() // Yield to encourage race
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	t.Logf("Inserted %d items in %v (Failures: %d)", successCount.Load(), duration, failures.Load())

	// Verification Phase
	// Iterate all IDs and ensure their vector chunk is present and not nil
	data := h.data.Load()
	missingCount := 0

	for w := 0; w < numWorkers; w++ {
		for i := 0; i < insertsPerWorker; i++ {
			id := uint32(w*insertsPerWorker + i)

			cID := chunkID(id)
			cOff := chunkOffset(id)
			_ = cOff // Only checking chunk existence for now

			// Check if chunk exists (Levels/Neighbors)
			// Vectors are not stored in GraphData anymore (unless SQ8/PQ).
			// So checking Levels/Neighbors is the correct way to verify allocation.
			if data.GetLevelsChunk(cID) == nil {
				missingCount++
				if missingCount < 5 {
					t.Logf("Missing Levels Chunk for ID %d", id)
				}
			}
		}
	}

	if missingCount > 0 {
		t.Fatalf("Consistency Check Failed: Stuck/Lost allocations for %d items", missingCount)
	}

	// Also require no failures during insert
	require.Equal(t, int32(0), failures.Load(), "Inserts should not fail")
}
