package store

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

// FilterFunc is the signature for batch filtering functions
type FilterFunc func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error)

// PipelineResult holds a filtered record with its original index for ordering
type PipelineResult struct {
	Record arrow.RecordBatch
	Index  int
}

// PipelineStats tracks pipeline processing statistics
type PipelineStats struct {
	BatchesProcessed int64
	BatchesFiltered  int64
	ErrorCount       int64
}

// DoGetPipeline implements pipelined processing for DoGet operations.
// Workers filter batches in parallel while the main thread writes,
// hiding compute latency behind network I/O.
type DoGetPipeline struct {
	numWorkers int
	bufferSize int

	// Statistics
	stats PipelineStats

	// Lifecycle
	mu      sync.Mutex
	running bool
}

// NewDoGetPipeline creates a new pipeline with the specified number of workers.
// If workers is 0, defaults to 4. If bufferSize is 0, defaults to workers * 2.
func NewDoGetPipeline(workers, bufferSize int) *DoGetPipeline {
	if workers <= 0 {
		workers = 4
	}
	if bufferSize <= 0 {
		bufferSize = workers * 2
	}

	return &DoGetPipeline{
		numWorkers: workers,
		bufferSize: bufferSize,
	}
}

// NumWorkers returns the number of worker goroutines
func (p *DoGetPipeline) NumWorkers() int {
	return p.numWorkers
}

// Stats returns current pipeline statistics
func (p *DoGetPipeline) Stats() PipelineStats {
	return PipelineStats{
		BatchesProcessed: atomic.LoadInt64(&p.stats.BatchesProcessed),
		BatchesFiltered:  atomic.LoadInt64(&p.stats.BatchesFiltered),
		ErrorCount:       atomic.LoadInt64(&p.stats.ErrorCount),
	}
}

// workItem represents a batch to be processed
type workItem struct {
	index  int
	record arrow.RecordBatch
}

// processedItem represents a completed filter operation
type processedItem struct {
	index  int
	record arrow.RecordBatch
	err    error
}

// Process starts pipelined processing of batches.
// Returns a channel of ordered results and an error channel.
// Results are guaranteed to arrive in the same order as input batches.
func (p *DoGetPipeline) Process(
	ctx context.Context,
	batches []arrow.RecordBatch,
	filterFn FilterFunc,
) (results <-chan PipelineResult, errs <-chan error) {
	resultCh := make(chan PipelineResult, p.bufferSize)
	errCh := make(chan error, 1)

	if len(batches) == 0 {
		close(resultCh)
		close(errCh)
		return resultCh, errCh
	}

	go p.run(ctx, batches, filterFn, resultCh, errCh)

	return resultCh, errCh
}

// run executes the pipeline processing
func (p *DoGetPipeline) run(
	ctx context.Context,
	batches []arrow.RecordBatch,
	filterFn FilterFunc,
	resultCh chan<- PipelineResult,
	errCh chan<- error,
) {
	defer close(resultCh)
	defer close(errCh)

	// Work distribution channel
	workCh := make(chan workItem, p.bufferSize)
	// Processed items channel (unordered from workers)
	processedCh := make(chan processedItem, p.bufferSize)

	// Context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start workers
	var workerWg sync.WaitGroup
	for i := 0; i < p.numWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			p.worker(ctx, filterFn, workCh, processedCh)
		}()
	}

	// Close processedCh when all workers done
	go func() {
		workerWg.Wait()
		close(processedCh)
	}()

	// Feed work items (producer)
	go func() {
		defer close(workCh)
		for i, batch := range batches {
			select {
			case <-ctx.Done():
				return
			case workCh <- workItem{index: i, record: batch}:
			}
		}
	}()

	// Reorder buffer to maintain sequence
	// Key insight: workers may complete out of order, but we must emit in order
	reorderBuffer := make(map[int]processedItem)
	nextToEmit := 0
	var firstErr error

	for item := range processedCh {
		if item.err != nil && firstErr == nil {
			firstErr = item.err
			atomic.AddInt64(&p.stats.ErrorCount, 1)
			cancel() // Stop processing on first error
			continue
		}

		if item.err != nil {
			// Already have an error, skip
			if item.record != nil {
				item.record.Release()
			}
			continue
		}

		// Buffer this item
		reorderBuffer[item.index] = item

		// Emit all consecutive items starting from nextToEmit
		for {
			if buffered, ok := reorderBuffer[nextToEmit]; ok {
				delete(reorderBuffer, nextToEmit)

				if firstErr == nil && buffered.record != nil {
					select {
					case <-ctx.Done():
						buffered.record.Release()
						firstErr = ctx.Err()
					case resultCh <- PipelineResult{Record: buffered.record, Index: nextToEmit}:
						atomic.AddInt64(&p.stats.BatchesProcessed, 1)
					}
				} else if buffered.record != nil {
					buffered.record.Release()
				}
				nextToEmit++
			} else {
				break
			}
		}
	}

	// Clean up any remaining buffered records
	for _, item := range reorderBuffer {
		if item.record != nil {
			item.record.Release()
		}
	}

	if firstErr != nil {
		errCh <- firstErr
	}
}

// worker processes items from workCh and sends results to processedCh
func (p *DoGetPipeline) worker(
	ctx context.Context,
	filterFn FilterFunc,
	workCh <-chan workItem,
	processedCh chan<- processedItem,
) {
	for work := range workCh {
		select {
		case <-ctx.Done():
			// Context cancelled, drain remaining work
			processedCh <- processedItem{index: work.index, err: ctx.Err()}
			continue
		default:
		}

		// Execute filter
		filtered, err := filterFn(ctx, work.record)
		atomic.AddInt64(&p.stats.BatchesFiltered, 1)

		processedCh <- processedItem{
			index:  work.index,
			record: filtered,
			err:    err,
		}
	}
}

// Stop gracefully shuts down the pipeline.
// Note: For request-scoped pipelines, prefer context cancellation.
func (p *DoGetPipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
}

// DoGetPipelinePool manages a pool of reusable pipelines
type DoGetPipelinePool struct {
	pool     sync.Pool
	workers  int
	bufferSz int
}

// NewDoGetPipelinePool creates a pool of pipelines
func NewDoGetPipelinePool(workers, bufferSize int) *DoGetPipelinePool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if bufferSize <= 0 {
		bufferSize = workers * 2
	}

	return &DoGetPipelinePool{
		workers:  workers,
		bufferSz: bufferSize,
		pool: sync.Pool{
			New: func() interface{} {
				return NewDoGetPipeline(workers, bufferSize)
			},
		},
	}
}

// Get retrieves a pipeline from the pool
func (pp *DoGetPipelinePool) Get() *DoGetPipeline {
	return pp.pool.Get().(*DoGetPipeline)
}

// Put returns a pipeline to the pool
func (pp *DoGetPipelinePool) Put(p *DoGetPipeline) {
	pp.pool.Put(p)
}
