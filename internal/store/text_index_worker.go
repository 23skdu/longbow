package store

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// textIndexJob is one record batch whose configured text columns still need to
// be indexed for hybrid search.
//
// baseRowID is the dataset-global row offset the batch was published at, so
// the worker can assign stable document IDs without re-walking the batch list.
type textIndexJob struct {
	ds        *Dataset
	batch     arrow.RecordBatch
	baseRowID uint32
}

// textIndexQueue decouples text/BM25 indexing from vector ingestion.
//
// Tokenising every text cell and rewriting the inverted index dictionaries is
// orders of magnitude more expensive than publishing the record it describes,
// and it used to run inline on the ingestion worker while holding the dataset
// write lock. That made a pure-vector ingest pay for hybrid search it was not
// even configured to serve: profiling attributed 82.95% of total process
// allocation and 41.94% of mutex lock delay to BM25InvertedIndex.Add.
//
// The queue is bounded so a backlog of unindexed text cannot pin unbounded
// Arrow record batches in memory; when it is full the batch is indexed inline
// instead, which is slower but never loses work.
type textIndexQueue struct {
	jobs chan textIndexJob

	// stop is closed once the queue has been drained, to retire the workers.
	// It is separate from the store's stop channel because the drain has to
	// happen *before* the store signals shutdown - otherwise the workers exit
	// first and nothing is left to drain them.
	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once

	// dropped counts jobs that had to be indexed inline because the queue was
	// saturated. Exposed for tests and diagnostics.
	dropped atomic.Int64

	// inflight counts jobs a worker has popped but not yet finished. Queue
	// length alone does not describe the state of the pipeline: a job that has
	// been dequeued is still being indexed.
	inflight atomic.Int64
}

const (
	textIndexQueueDepth = 64

	// defaultTextIndexDrainBudget bounds how long shutdown waits for queued
	// text to be indexed before releasing it unprocessed.
	defaultTextIndexDrainBudget = 30 * time.Second
)

// enqueueTextIndexing hands a freshly ingested batch to the background text
// indexer.
//
// The batch is retained for the duration of the job: the caller only holds a
// reference through ds.Records, and the worker may run long after ingestion has
// moved on.
func (s *VectorStore) enqueueTextIndexing(ds *Dataset, rec arrow.RecordBatch, baseRowID uint32) {
	if s == nil || ds == nil || rec == nil {
		return
	}
	if !s.hybridSearchConfig.Enabled || len(s.hybridSearchConfig.TextColumns) == 0 {
		return
	}
	if ds.BM25Index == nil && ds.BM25ArenaIndex == nil {
		return
	}

	job := textIndexJob{ds: ds, batch: rec, baseRowID: baseRowID}

	s.textQueueOnce.Do(func() {
		s.textQueue = &textIndexQueue{
			jobs: make(chan textIndexJob, textIndexQueueDepth),
			stop: make(chan struct{}),
		}
		s.startTextIndexersLocked(runtime.NumCPU())
	})

	// Shutting down or shut down: index inline. The background workers are
	// gone (or going), so a queued job would only be released unread.
	select {
	case <-s.textQueue.stop:
		s.indexTextColumnsForHybridSearch(ds, rec, baseRowID)
		return
	default:
	}

	rec.Retain()
	select {
	case s.textQueue.jobs <- job:
		s.textQueue.inflight.Add(1)
	default:
		// Saturated: index inline so the documents are never silently lost.
		// This is the backpressure signal that the batch is arriving faster
		// than text can be indexed, and it is bounded by ingestion itself.
		s.textQueue.dropped.Add(1)
		rec.Release()
		s.indexTextColumnsForHybridSearch(ds, rec, baseRowID)
	}
}

// startTextIndexersLocked spawns the text indexing workers. Callers must not
// hold textMu.
func (s *VectorStore) startTextIndexersLocked(n int) {
	if n <= 0 {
		n = 1
	}
	if n > textIndexQueueDepth {
		n = textIndexQueueDepth
	}
	for i := 0; i < n; i++ {
		s.textQueue.wg.Add(1)
		go func() {
			defer s.textQueue.wg.Done()
			s.runTextIndexer(s.ctx)
		}()
	}
}

// runTextIndexer drains text indexing jobs until the queue is retired.
func (s *VectorStore) runTextIndexer(ctx context.Context) {
	for {
		select {
		case <-s.textQueue.stop:
			return
		case <-s.stopChan:
			return
		case <-ctx.Done():
			return
		case job := <-s.textQueue.jobs:
			s.processTextIndexJob(job)
		}
	}
}

// processTextIndexJob indexes one batch and always releases the retained record.
func (s *VectorStore) processTextIndexJob(job textIndexJob) {
	defer job.batch.Release()
	defer s.textQueue.inflight.Add(-1)

	if job.ds == nil {
		return
	}
	// A dataset can be closed between enqueue and drain; indexing into a torn
	// down index would resurrect BM25 state that Close just released.
	if job.ds.evicting.Load() {
		return
	}
	s.indexTextColumnsForHybridSearch(job.ds, job.batch, job.baseRowID)
}

// drainTextIndexing waits for every queued text job to be indexed, then stops
// the workers.
//
// It must be called before the workers are signalled to stop: they select on
// the store's stop channel, and once they have exited nothing drains the queue
// and every retained record in it is dropped without a Release.
//
// Callers that only want the queue empty - tests, or a caller that will keep
// using the store - should use waitForTextIndexing instead.
func (s *VectorStore) drainTextIndexing() {
	if s == nil || s.textQueue == nil {
		return
	}
	s.waitForTextIndexing(time.Duration(s.textIndexDrainBudget.Load()))
	s.stopTextIndexers()
}

// waitForTextIndexing blocks until the text index queue has drained. It exists
// for shutdown ordering and for tests that assert on searchability without
// polling.
//
// It can only make progress while the workers are running; callers must not
// have signalled them to stop.
func (s *VectorStore) waitForTextIndexing(budget time.Duration) {
	if s == nil || s.textQueue == nil {
		return
	}
	// A zero budget means "do not wait". shutdown relies on this: if the
	// workers have already exited the queue can never empty, and spinning on
	// it would hang the process. Anything left over is released by
	// stopTextIndexers instead.
	if budget <= 0 {
		return
	}

	deadline := time.Now().Add(budget)
	for len(s.textQueue.jobs) > 0 || s.textQueue.inflight.Load() > 0 {
		if time.Now().After(deadline) {
			return
		}
		runtime.Gosched()
	}
}

// stopTextIndexers shuts the workers down and releases anything still queued.
func (s *VectorStore) stopTextIndexers() {
	if s == nil || s.textQueue == nil {
		return
	}
	s.textQueue.stopOnce.Do(func() {
		close(s.textQueue.stop)
		s.textQueue.wg.Wait()

		// The workers have exited, so nothing else will drain the queue. Every
		// remaining job holds a Retain on its Arrow record; dropping it on the
		// floor would leak the whole batch.
		for {
			select {
			case job := <-s.textQueue.jobs:
				job.batch.Release()
			default:
				return
			}
		}
	})
}

// textIndexQueueDropped reports how many batches bypassed the queue because it
// was saturated.
func (s *VectorStore) textIndexQueueDropped() int64 {
	if s == nil || s.textQueue == nil {
		return 0
	}
	return s.textQueue.dropped.Load()
}
