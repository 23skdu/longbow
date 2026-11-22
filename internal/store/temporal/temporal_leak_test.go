package temporal

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countTemporalIngestWorkers() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			stacks := string(buf[:n])
			return strings.Count(stacks, "TemporalIndex).runIngestWorker") +
				strings.Count(stacks, "TemporalIndex).startIngestWorker")
		}
		buf = make([]byte, len(buf)*2)
	}
}

func waitForZeroTemporalWorkers(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if countTemporalIngestWorkers() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected 0 temporal ingest worker goroutines, found %d", countTemporalIngestWorkers())
}

func TestTemporalIndex_NoGoroutineLeakWhenIdle(t *testing.T) {
	waitForZeroTemporalWorkers(t, time.Second)
	baseline := runtime.NumGoroutine()

	const count = 64
	indices := make([]*TemporalIndex, count)
	for i := 0; i < count; i++ {
		indices[i] = NewTemporalIndex(8)
	}

	assert.Equal(t, 0, countTemporalIngestWorkers(), "idle TemporalIndex instances must not spawn worker goroutines")
	assert.LessOrEqual(t, runtime.NumGoroutine(), baseline+4, "creating idle indices should not grow goroutine count")

	for _, ti := range indices {
		require.NoError(t, ti.Close())
	}
}

func TestTemporalIndex_AsyncWorkerDrainsAndExits(t *testing.T) {
	waitForZeroTemporalWorkers(t, time.Second)

	const numIndices = 24
	const batchSize = 32
	baseTs := time.Now().UnixNano()

	indices := make([]*TemporalIndex, numIndices)
	for i := 0; i < numIndices; i++ {
		ti := NewTemporalIndex(4)
		indices[i] = ti

		ids := make([]uint64, batchSize)
		norms := make([]float32, batchSize)
		timestamps := make([]int64, batchSize)
		for j := 0; j < batchSize; j++ {
			ids[j] = uint64(i*1000 + j + 1)
			norms[j] = float32(j + 1)
			timestamps[j] = baseTs + int64(j)
		}
		require.NoError(t, ti.AddBatchNorms(ids, norms, timestamps, 4))
	}

	// Workers run on demand during async ingestion and exit without requiring Close().
	waitForZeroTemporalWorkers(t, 2*time.Second)

	for i, ti := range indices {
		tree := ti.temporalTree.Load()
		require.NotNil(t, tree, fmt.Sprintf("index %d missing tree", i))
		require.Eventually(t, func() bool {
			return tree.Len() == batchSize
		}, time.Second, 5*time.Millisecond)

		res, err := ti.SearchRange(context.Background(), baseTs, baseTs+batchSize, 10)
		require.NoError(t, err)
		assert.NotEmpty(t, res)
		require.NoError(t, ti.Close())
	}

	waitForZeroTemporalWorkers(t, time.Second)
}

func TestTemporalIndex_ConcurrentAsyncToggleAndClose(t *testing.T) {
	waitForZeroTemporalWorkers(t, time.Second)

	for round := 0; round < 10; round++ {
		ti := NewTemporalIndex(4)
		var wg sync.WaitGroup

		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				ids := []uint64{uint64(worker*100 + 1), uint64(worker*100 + 2)}
				vecs := [][]float32{{1, 0, 0, 0}, {0, 2, 0, 0}}
				norms := []float32{1, 2}
				ts := []int64{int64(worker + 1), int64(worker + 2)}
				for iter := 0; iter < 20; iter++ {
					_ = ti.AddBatch(ids, vecs, ts, nil)
					_ = ti.AddBatchNorms(ids, norms, ts, 4)
					if iter%5 == 0 {
						ti.SetAsyncIngestion(iter%10 == 0)
					}
				}
			}(w)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			_ = ti.Close()
		}()

		wg.Wait()
	}

	waitForZeroTemporalWorkers(t, 2*time.Second)
}
