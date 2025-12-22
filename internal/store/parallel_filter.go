package store

import (
	"context"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// filterResult holds a filtered record with its original index for ordering
type filterResult struct {
	index int
	rec   arrow.RecordBatch
	err   error
}

// filterRecordsParallel processes records through filterRecord using a worker pool
// while preserving original order in the output channel.
func (s *VectorStore) filterRecordsParallel(ctx context.Context, recs []arrow.RecordBatch, filters []Filter) <-chan arrow.RecordBatch {
	outCh := make(chan arrow.RecordBatch)

	if len(recs) == 0 {
		close(outCh)
		return outCh
	}

	// Worker count: min(numCPU, len(recs))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(recs) {
		numWorkers = len(recs)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Job channel for distributing work
	type job struct {
		index int
		rec   arrow.RecordBatch
	}
	jobsCh := make(chan job, len(recs))
	resultsCh := make(chan filterResult, len(recs))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobsCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				filtered, err := s.filterRecord(ctx, j.rec, filters)
				resultsCh <- filterResult{index: j.index, rec: filtered, err: err}
			}
		}()
	}

	// Send jobs
	go func() {
		for i, rec := range recs {
			select {
			case <-ctx.Done():
				close(jobsCh)
				return
			case jobsCh <- job{index: i, rec: rec}:
			}
		}
		close(jobsCh)
	}()

	// Collect results and order them
	go func() {
		defer close(outCh)

		// Wait for all workers to finish
		go func() {
			wg.Wait()
			close(resultsCh)
		}()

		// Collect all results
		results := make([]filterResult, len(recs))
		received := make([]bool, len(recs))
		nextToSend := 0

		for res := range resultsCh {
			results[res.index] = res
			received[res.index] = true

			// Send in order as results become available
			for nextToSend < len(recs) && received[nextToSend] {
				r := results[nextToSend]
				// Skip errors and empty results
				if r.err == nil && r.rec != nil && r.rec.NumRows() > 0 {
					select {
					case <-ctx.Done():
						r.rec.Release()
						return
					case outCh <- r.rec:
					}
				} else if r.rec != nil && r.rec.NumRows() == 0 {
					// Release empty records
					r.rec.Release()
				}
				nextToSend++
			}
		}
	}()

	return outCh
}
