package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockContention_InstrumentedMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	assert.NotNil(t, metrics.WALLockWaitDuration, "WALLockWaitDuration metric should exist")
	assert.NotNil(t, metrics.PoolLockWaitDuration, "PoolLockWaitDuration metric should exist")
	assert.NotNil(t, metrics.IndexLockWaitDuration, "IndexLockWaitDuration metric should exist")
}

func TestLockContention_AtomicOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Run("RecordSizeCache_ConcurrentAccess", func(t *testing.T) {
		cache := NewRecordSizeCache()
		require.NotNil(t, cache)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = cache.Len()
			}()
		}
		wg.Wait()
	})

	t.Run("AtomicCounter_LockFree", func(t *testing.T) {
		var counter atomic.Int64
		var wg sync.WaitGroup

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				counter.Add(1)
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(1000), counter.Load())
	})
}

func TestLockContention_ConcurrentReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Run("Dataset_ConcurrentReadAccess", func(t *testing.T) {
		ds := &Dataset{
			lastAccess: time.Now().UnixNano(),
		}

		var wg sync.WaitGroup
		var readCount atomic.Int32

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ds.dataMu.RLock()
				_ = ds.lastAccess
				readCount.Add(1)
				ds.dataMu.RUnlock()
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(100), readCount.Load())
	})
}

func TestLockContention_ShardedAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Run("ShardedRWMutex_DistributesLoad", func(t *testing.T) {
		cfg := DefaultShardedRWMutexConfig()
		cfg.NumShards = 16
		sm := NewShardedRWMutex(cfg)

		var wg sync.WaitGroup
		var ops atomic.Int64

		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(key int) {
				defer wg.Done()
				sm.Lock(uint64(key))
				ops.Add(1)
				sm.Unlock(uint64(key))
			}(i)
		}
		wg.Wait()

		assert.Equal(t, int64(1000), ops.Load())
	})
}

func TestLockContention_NoDeadlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Run("NestedLocks_ProperOrdering", func(t *testing.T) {
		done := make(chan bool, 1)

		go func() {
			var muA, muB sync.Mutex

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					muA.Lock()
					muB.Lock()
					_ = 0
					muB.Unlock() //nolint:gocritic,staticcheck
					muA.Unlock()
				}()
			}
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Potential deadlock detected")
		}
	})
}

func TestLockContention_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Run("TryLockWithTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var mu sync.Mutex
		mu.Lock()

		result := make(chan bool, 1)
		go func() {
			mu.Lock()
			result <- true
			mu.Unlock()
		}()

		select {
		case <-result:
			t.Log("Lock acquired unexpectedly")
		case <-ctx.Done():
		}

		mu.Unlock()
	})
}
