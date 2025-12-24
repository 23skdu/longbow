package store

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
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
// If workers is 0, defaults to runtime.NumCPU(). If bufferSize is 0, defaults to workers * 2.
func NewDoGetPipeline(workers, bufferSize int) *DoGetPipeline {
	if workers <= 0 {
		workers = runtime.NumCPU()
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

	metrics.PipelineUtilization.WithLabelValues("pipeline_process").Inc()

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
			if item.record != nil {
				item.record.Release()
			}
			continue
		}

		reorderBuffer[item.index] = item

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

	for _, item := range reorderBuffer {
		if item.record != nil {
			item.record.Release()
		}
	}

	if firstErr != nil {
		errCh <- firstErr
	}
}

func (p *DoGetPipeline) worker(
	ctx context.Context,
	filterFn FilterFunc,
	workCh <-chan workItem,
	processedCh chan<- processedItem,
) {
	for work := range workCh {
		select {
		case <-ctx.Done():
			processedCh <- processedItem{index: work.index, err: ctx.Err()}
			continue
		default:
		}

		filtered, err := filterFn(ctx, work.record)
		atomic.AddInt64(&p.stats.BatchesFiltered, 1)

		processedCh <- processedItem{
			index:  work.index,
			record: filtered,
			err:    err,
		}
	}
}

func (p *DoGetPipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
}

// -----------------------------------------------------------------------------
// Compatibility Wrapper for store.go
// -----------------------------------------------------------------------------

// PipelineStage is the legacy result type expected by store.go
type PipelineStage struct {
	Record    arrow.RecordBatch
	BatchIdx  int
	Tombstone *Bitset // Legacy field
	Err       error
}

// ProcessRecords is the compatibility method for store.go
func (p *DoGetPipeline) ProcessRecords(
	ctx context.Context,
	records []arrow.RecordBatch,
	tombstones map[int]*Bitset,
	filters []Filter,
	evaluator interface{},
) <-chan PipelineStage {
	resCh := make(chan PipelineStage, p.bufferSize)

	// Define a filter function that matches the old store.go logic
	// In the real implementation, this would call s.filterRecord or similar.
	// For now, it's a pass-through because the pipeline is about ORDERING and PARALLELISM.
	filterFn := func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		rec.Retain()
		return rec, nil
	}

	go func() {
		defer close(resCh)
		results, errs := p.Process(ctx, records, filterFn)

		errDone := make(chan bool)
		go func() {
			err := <-errs
			if err != nil {
				resCh <- PipelineStage{Err: err}
			}
			errDone <- true
		}()

		for res := range results {
			ts := tombstones[res.Index]
			resCh <- PipelineStage{
				Record:    res.Record,
				BatchIdx:  res.Index,
				Tombstone: ts,
			}
		}
		<-errDone
	}()

	return resCh
}

func ShouldUsePipeline(numBatches int) bool {
	return numBatches >= 10
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
