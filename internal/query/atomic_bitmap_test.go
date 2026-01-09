package query_test

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/stretchr/testify/assert"
)

func TestAtomicBitset_Basic(t *testing.T) {
	ab := query.NewAtomicBitset()
	assert.False(t, ab.Contains(1), "Should be empty")

	ab.Set(1)
	assert.True(t, ab.Contains(1), "Should contain 1")
	assert.Equal(t, uint64(1), ab.Count())

	ab.Set(100)
	assert.True(t, ab.Contains(100), "Should contain 100")
	assert.Equal(t, uint64(2), ab.Count())

	ab.Clear(1)
	assert.False(t, ab.Contains(1), "Should not contain 1")
	assert.True(t, ab.Contains(100), "Should contain 100")
	assert.Equal(t, uint64(1), ab.Count())

	arr := ab.ToUint32Array()
	assert.Equal(t, []uint32{100}, arr)
}

func TestAtomicBitset_Concurrent(t *testing.T) {
	ab := query.NewAtomicBitset()
	const workers = 10
	const opsPerWorker = 1000

	var wg sync.WaitGroup
	wg.Add(workers * 2) // Writers and Readers

	// Concurrent Writers
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < opsPerWorker; j++ {
				val := rng.Intn(1000)
				if j%2 == 0 {
					ab.Set(val)
				} else {
					ab.Clear(val)
				}
			}
		}(i)
	}

	// Concurrent Readers
	reads := atomic.Int64{}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				ab.Contains(j)
				reads.Add(1)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(workers*opsPerWorker), reads.Load())
}

func TestAtomicBitset_SnapshotIsolation(t *testing.T) {
	ab := query.NewAtomicBitset()
	ab.Set(1)

	// Create a "snapshot" by cloning (which just loads the pointer currently or does deep copy?)
	// AtomicBitset.Clone currently does deep copy.
	// But let's check reader behavior.
	// In strict Wait-Free reading, there is no "Snapshot" object exposed to user unless they call Clone.
	// But internally, Contains loads a pointer.

	// Let's verify that Set doesn't corrupt state.
	ab.Set(2)
	assert.True(t, ab.Contains(1))
	assert.True(t, ab.Contains(2))

	clone := ab.Clone()
	assert.True(t, clone.Contains(1))
	assert.True(t, clone.Contains(2))

	ab.Clear(1)
	assert.False(t, ab.Contains(1))
	assert.True(t, ab.Contains(2))

	// Clone should remain unchanged
	assert.True(t, clone.Contains(1))
}
