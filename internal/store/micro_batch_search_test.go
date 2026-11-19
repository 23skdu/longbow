package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMicroBatchCoalescer verifies small concurrent search requests are
// coalesced into micro-batches and returned to their respective callers (roadmap C3).
func TestMicroBatchCoalescer(t *testing.T) {
	const (
		totalQueries = 32
		maxBatch     = 8
	)

	var maxObservedBatch int
	var mu sync.Mutex
	coalescer := NewMicroBatchCoalescer(maxBatch, 5*time.Millisecond, 64, func(_ context.Context, batch []BatchSearchRequest) {
		mu.Lock()
		if len(batch) > maxObservedBatch {
			maxObservedBatch = len(batch)
		}
		mu.Unlock()
		for _, req := range batch {
			id := VectorID(int(req.Query[0])) // #nosec G115
			req.ResultCh <- []SearchResult{{ID: id, Score: req.Query[0]}}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coalescer.Start(ctx)
	defer coalescer.Stop()

	var wg sync.WaitGroup
	errs := make(chan error, totalQueries)
	for i := range totalQueries {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := coalescer.Submit(ctx, []float32{float32(idx)}, 1, nil, SearchOptions{})
			if err != nil {
				errs <- err
				return
			}
			if len(res) != 1 || int(res[0].ID) != idx {
				errs <- context.DeadlineExceeded
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.GreaterOrEqual(t, coalescer.BatchesDispatched(), uint64(1))
	require.Less(t, coalescer.BatchesDispatched(), uint64(totalQueries),
		"concurrent small queries should coalesce into fewer batches than totalQueries")
	mu.Lock()
	require.GreaterOrEqual(t, maxObservedBatch, 2, "expected at least one coalesced micro-batch")
	mu.Unlock()
}
