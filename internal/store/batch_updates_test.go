package store

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchNeighborUpdater_Basic(t *testing.T) {
	flushCount := atomic.Int32{}
	updateCount := atomic.Int32{}

	cfg := BatchNeighborUpdaterConfig{
		MaxBatchSize: 10,
		OnFlush: func(updates []BatchNeighborUpdate) error {
			flushCount.Add(1)
			updateCount.Add(int32(len(updates)))
			return nil
		},
	}

	bu := NewBatchNeighborUpdater(cfg)
	defer func() {
		if err := bu.Stop(); err != nil {
			t.Logf("failed to stop BatchNeighborUpdater: %v", err)
		}
	}()

	// Add 5 updates (should not flush immediately)
	for i := 0; i < 5; i++ {
		bu.Add(BatchNeighborUpdate{NodeID: uint32(i), Layer: 0, Neighbors: []uint32{1, 2}})
	}

	assert.Equal(t, int32(0), flushCount.Load())
	assert.Equal(t, 5, bu.PendingCount())

	// Add 5 more updates (should trigger flush)
	for i := 5; i < 10; i++ {
		bu.Add(BatchNeighborUpdate{NodeID: uint32(i), Layer: 0, Neighbors: []uint32{1, 2}})
	}

	// Flush is asynchronous or synchronous depending on implementation,
	// but here it's called synchronously when max reached.
	assert.Equal(t, int32(1), flushCount.Load())
	assert.Equal(t, int32(10), updateCount.Load())
	assert.Equal(t, 0, bu.PendingCount())
}

func TestBatchNeighborUpdater_Interval(t *testing.T) {
	flushCount := atomic.Int32{}

	cfg := BatchNeighborUpdaterConfig{
		MaxBatchSize:  100,
		FlushInterval: 50 * time.Millisecond,
		OnFlush: func(updates []BatchNeighborUpdate) error {
			flushCount.Add(1)
			return nil
		},
	}

	bu := NewBatchNeighborUpdater(cfg)
	defer func() {
		if err := bu.Stop(); err != nil {
			t.Logf("failed to stop BatchNeighborUpdater: %v", err)
		}
	}()

	bu.Add(BatchNeighborUpdate{NodeID: 1, Layer: 0, Neighbors: []uint32{}})

	assert.Equal(t, int32(0), flushCount.Load())

	// Wait for interval
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), flushCount.Load())
}

func TestBatchNeighborUpdater_Stop(t *testing.T) {
	flushCount := atomic.Int32{}

	cfg := BatchNeighborUpdaterConfig{
		MaxBatchSize: 100,
		OnFlush: func(updates []BatchNeighborUpdate) error {
			flushCount.Add(1)
			return nil
		},
	}

	bu := NewBatchNeighborUpdater(cfg)
	bu.Add(BatchNeighborUpdate{NodeID: 1, Layer: 0, Neighbors: []uint32{}})

	assert.Equal(t, int32(0), flushCount.Load())

	err := bu.Stop()
	require.NoError(t, err)

	assert.Equal(t, int32(1), flushCount.Load())
}
