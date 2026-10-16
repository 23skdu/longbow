package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createNumaTestBatch(t *testing.T) arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	return b.NewRecordBatch()
}

func TestNUMAIndexQueues_RoutingAndStealing(t *testing.T) {
	vs := &VectorStore{
		indexQueue:      NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig()),
		numaIndexQueues: make([]*IndexJobQueueLockFree, 2),
		stopChan:        make(chan struct{}),
	}
	vs.numaIndexQueues[0] = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())
	vs.numaIndexQueues[1] = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())

	defer func() {
		close(vs.stopChan)
		vs.indexQueue.Stop()
		vs.numaIndexQueues[0].Stop()
		vs.numaIndexQueues[1].Stop()
	}()

	rec := createNumaTestBatch(t)
	defer rec.Release()

	// 1. Enqueue job for node 0
	job0 := types.IndexJob{
		DatasetName: "ds0",
		Record:      rec,
		BatchIdx:    0,
		NodeID:      0,
	}
	require.True(t, vs.EnqueueIndexJob(job0))
	assert.Equal(t, 1, vs.numaIndexQueues[0].Len())
	assert.Equal(t, 0, vs.numaIndexQueues[1].Len())
	assert.Equal(t, 0, vs.indexQueue.Len())
	assert.Equal(t, 1, vs.TotalIndexQueueDepth())

	// 2. Enqueue job for node 1
	job1 := types.IndexJob{
		DatasetName: "ds1",
		Record:      rec,
		BatchIdx:    1,
		NodeID:      1,
	}
	require.True(t, vs.EnqueueIndexJob(job1))
	assert.Equal(t, 1, vs.numaIndexQueues[0].Len())
	assert.Equal(t, 1, vs.numaIndexQueues[1].Len())
	assert.Equal(t, 2, vs.TotalIndexQueueDepth())

	// 3. Pop with worker pinned to node 0 should get job0 first
	popWorker0 := func() (types.IndexJob, bool) {
		workerNodeID := 0
		if workerNodeID >= 0 && workerNodeID < len(vs.numaIndexQueues) && vs.numaIndexQueues[workerNodeID] != nil {
			if j, ok := vs.numaIndexQueues[workerNodeID].Pop(); ok {
				return j, true
			}
		}
		if vs.indexQueue != nil {
			if j, ok := vs.indexQueue.Pop(); ok {
				return j, true
			}
		}
		for i, q := range vs.numaIndexQueues {
			if i != workerNodeID && q != nil {
				if j, ok := q.Pop(); ok {
					return j, true
				}
			}
		}
		return types.IndexJob{}, false
	}

	got0, ok := popWorker0()
	require.True(t, ok)
	assert.Equal(t, "ds0", got0.DatasetName)
	assert.Equal(t, 0, vs.numaIndexQueues[0].Len())
	assert.Equal(t, 1, vs.numaIndexQueues[1].Len())

	// 4. Worker 0 popping again should steal job1 from node 1
	got1, ok := popWorker0()
	require.True(t, ok)
	assert.Equal(t, "ds1", got1.DatasetName)
	assert.Equal(t, 0, vs.numaIndexQueues[1].Len())
	assert.Equal(t, 0, vs.TotalIndexQueueDepth())
}

func TestNUMAIndexQueues_FallbackToGlobal(t *testing.T) {
	vs := &VectorStore{
		indexQueue:      NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig()),
		numaIndexQueues: make([]*IndexJobQueueLockFree, 2),
		stopChan:        make(chan struct{}),
	}
	vs.numaIndexQueues[0] = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())
	vs.numaIndexQueues[1] = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())

	defer func() {
		close(vs.stopChan)
		vs.indexQueue.Stop()
		vs.numaIndexQueues[0].Stop()
		vs.numaIndexQueues[1].Stop()
	}()

	rec := createNumaTestBatch(t)
	defer rec.Release()

	// Enqueue with invalid NodeID (-1) should land in global queue
	jobGlobal := types.IndexJob{
		DatasetName: "ds_global",
		Record:      rec,
		BatchIdx:    99,
		NodeID:      -1,
	}
	require.True(t, vs.EnqueueIndexJob(jobGlobal))
	assert.Equal(t, 0, vs.numaIndexQueues[0].Len())
	assert.Equal(t, 0, vs.numaIndexQueues[1].Len())
	assert.Equal(t, 1, vs.indexQueue.Len())
	assert.Equal(t, 1, vs.TotalIndexQueueDepth())
}

func TestNUMAIndexQueues_ConcurrentEnqueueDequeue(t *testing.T) {
	vs := &VectorStore{
		indexQueue:      NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig()),
		numaIndexQueues: make([]*IndexJobQueueLockFree, 4),
		stopChan:        make(chan struct{}),
	}
	for i := range vs.numaIndexQueues {
		vs.numaIndexQueues[i] = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())
	}

	defer func() {
		close(vs.stopChan)
		vs.indexQueue.Stop()
		for _, q := range vs.numaIndexQueues {
			q.Stop()
		}
	}()

	rec := createNumaTestBatch(t)
	defer rec.Release()

	numWorkers := 4
	numJobs := 500
	var wg sync.WaitGroup

	// Enqueue jobs across nodes concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numJobs; i++ {
			job := types.IndexJob{
				DatasetName: "test_ds",
				Record:      rec,
				BatchIdx:    i,
				NodeID:      i % len(vs.numaIndexQueues),
			}
			vs.EnqueueIndexJob(job)
		}
	}()

	// Dequeue jobs concurrently across workers
	var consumedJobs sync.WaitGroup
	var count atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for w := 0; w < numWorkers; w++ {
		consumedJobs.Add(1)
		go func(nodeID int) {
			defer consumedJobs.Done()
			for {
				if count.Load() >= int64(numJobs) {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
					// Pop from nodeID or steal
					var j types.IndexJob
					var ok bool
					if nodeID < len(vs.numaIndexQueues) {
						j, ok = vs.numaIndexQueues[nodeID].Pop()
					}
					if !ok {
						j, ok = vs.indexQueue.Pop()
					}
					if !ok {
						for other := range vs.numaIndexQueues {
							if other != nodeID {
								j, ok = vs.numaIndexQueues[other].Pop()
								if ok {
									break
								}
							}
						}
					}
					if ok && j.Record != nil {
						count.Add(1)
					} else {
						time.Sleep(1 * time.Millisecond)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	consumedJobs.Wait()
	assert.Equal(t, int64(numJobs), count.Load())
}
