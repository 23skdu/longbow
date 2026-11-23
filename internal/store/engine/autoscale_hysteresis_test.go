package engine

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func newHysteresisTestStore(t *testing.T) *VectorStore {
	t.Helper()
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testWorkerCounts(s *VectorStore) (ingestion, indexing int) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	return len(s.ingestionWorkerCancels), len(s.indexingWorkerCancels)
}

// TestCanScaleDownIngestion runs the test can scale down ingestion operation.
func TestCanScaleDownIngestion(t *testing.T) {
	cases := []struct {
		pending, waiters int64
		cur, min         int
		want             bool
	}{
		{0, 0, 5, 4, true},
		{1, 0, 5, 4, false},
		{0, 1, 5, 4, false},
		{0, 0, 4, 4, false},
		{0, 0, 3, 4, false},
		{5, 2, 10, 4, false},
	}
	for _, c := range cases {
		got := CanScaleDownIngestion(c.pending, c.waiters, c.cur, c.min)
		if got != c.want {
			t.Errorf("pending=%d waiters=%d cur=%d min=%d: got %v want %v", c.pending, c.waiters, c.cur, c.min, got, c.want)
		}
	}
}

// TestCanScaleDownIndexing runs the test can scale down indexing operation.
func TestCanScaleDownIndexing(t *testing.T) {
	if !CanScaleDownIndexing(0, 0, 5, 4) {
		t.Fatal("expected true")
	}
	if CanScaleDownIndexing(1, 0, 5, 4) {
		t.Fatal("expected false with pending")
	}
	if CanScaleDownIndexing(0, 1, 5, 4) {
		t.Fatal("expected false with lock waiter")
	}
}

// TestPendingScaleDownWorkIncludesIndexQueueDepth verifies that queued index
// jobs are counted as pending work even before workers mark them in flight.
func TestPendingScaleDownWorkIncludesIndexQueueDepth(t *testing.T) {
	s := newHysteresisTestStore(t)
	rec := createTestRecordBatch(t, 1)
	defer rec.Release()

	if !s.EnqueueIndexJob(IndexJob{
		DatasetName: "autoscale_index_queue",
		Record:      rec,
		BatchIdx:    0,
		CreatedAt:   time.Now(),
	}) {
		t.Fatal("failed to enqueue index job")
	}
	pending, _ := s.pendingScaleDownWork()
	if pending < 1 {
		t.Fatalf("queued index job missing from pending work: %d", pending)
	}
}

// TestDatasetLockWaiterInstrumentation verifies that contended read and write
// lock helpers publish waiters to the autoscaler busy signal.
func TestDatasetLockWaiterInstrumentation(t *testing.T) {
	s := newHysteresisTestStore(t)
	ds := NewDataset("autoscale_lock_instrumentation", nil)

	ds.dataMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.lockDatasetForWrite(ds)
		s.unlockDatasetForWrite(ds)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for s.datasetLockWaiters.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.datasetLockWaiters.Load() == 0 {
		ds.dataMu.Unlock()
		<-done
		t.Fatal("write lock contention was not published")
	}
	ds.dataMu.Unlock()
	<-done

	if waiters := s.datasetLockWaiters.Load(); waiters != 0 {
		t.Fatalf("waiter count did not return to zero: %d", waiters)
	}
}

// FuzzCanScaleDownIngestion runs the fuzz can scale down ingestion operation.
func FuzzCanScaleDownIngestion(f *testing.F) {
	f.Add(int64(0), int64(0), 5, 4)
	f.Add(int64(10), int64(2), 2, 4)
	f.Fuzz(func(t *testing.T, pending, waiters int64, cur, min int) {
		if pending < 0 {
			pending = -pending
		}
		if waiters < 0 {
			waiters = -waiters
		}
		if cur < 0 {
			cur = -cur
		}
		if min < 0 {
			min = -min
		}
		got := CanScaleDownIngestion(pending, waiters, cur, min)
		if pending > 0 && got {
			t.Fatal("must not scale down with pending work")
		}
		if waiters > 0 && got {
			t.Fatal("must not scale down with lock waiters")
		}
		if cur <= min && got {
			t.Fatal("must not scale below minimum")
		}
	})
}

// TestAutoscalerHysteresisWithContendedDatasetLock verifies that a contended
// dataset lock is treated as pending work during a scale-down tick.
func TestAutoscalerHysteresisWithContendedDatasetLock(t *testing.T) {
	s := newHysteresisTestStore(t)
	s.StartIngestionWorkers(MinIngestionWorkers + 2)
	s.StartIndexingWorkers(MinIndexingWorkers + 2)
	defer func() {
		ingestion, indexing := testWorkerCounts(s)
		s.StopIngestionWorkers(ingestion)
		s.StopIndexingWorkers(indexing)
	}()

	ds := NewDataset("autoscale_lock", nil)
	s.datasets.Store(&map[string]*Dataset{ds.Name: ds})

	ds.dataMu.Lock()
	s.datasetLockWaiters.Add(1)
	waiterDone := make(chan struct{})
	waiterAcquired := make(chan struct{}, 1)
	go func() {
		defer close(waiterDone)
		ds.dataMu.Lock()
		waiterAcquired <- struct{}{}
		ds.dataMu.Unlock()
	}()
	time.Sleep(10 * time.Millisecond)

	pending, waiters := s.pendingScaleDownWork()
	ingestion, indexing := testWorkerCounts(s)
	if waiters < 1 {
		t.Fatalf("expected lock waiter signal, got %d (pending=%d)", waiters, pending)
	}
	if CanScaleDownIngestion(pending, waiters, ingestion, MinIngestionWorkers) {
		t.Fatal("ingestion must not scale down while a dataset lock is contended")
	}
	if CanScaleDownIndexing(pending, waiters, indexing, MinIndexingWorkers) {
		t.Fatal("indexing must not scale down while a dataset lock is contended")
	}

	s.datasetLockWaiters.Add(-1)
	ds.dataMu.Unlock()
	<-waiterDone
	select {
	case <-waiterAcquired:
	default:
		t.Fatal("dataset lock waiter never acquired the lock")
	}

	pending, waiters = s.pendingScaleDownWork()
	ingestion, _ = testWorkerCounts(s)
	if !CanScaleDownIngestion(pending, waiters, ingestion, MinIngestionWorkers) {
		t.Fatalf("ingestion should scale down after lock release: pending=%d waiters=%d", pending, waiters)
	}
}

// TestStartIngestionAutoscalerSurvivesScaleUp verifies that a scale-up tick
// does not terminate the autoscaler before it can later scale back down.
func TestStartIngestionAutoscalerSurvivesScaleUp(t *testing.T) {
	s := newHysteresisTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartIngestionWorkers(MinIngestionWorkers + 1)
	s.StartIndexingWorkers(MinIndexingWorkers)
	s.StartIngestionAutoscaler(ctx)
	defer func() {
		ingestion, indexing := testWorkerCounts(s)
		s.StopIngestionWorkers(ingestion)
		s.StopIndexingWorkers(indexing)
	}()

	rec := createTestRecordBatch(t, 1)
	dsName := "autoscale_queue_survives_scale_up"
	s.PrewarmDataset(dsName, rec.Schema())
	ds, ok := s.getDataset(dsName)
	if !ok {
		t.Fatal("dataset was not prewarmed")
	}
	// Hold the dataset write lock while the queue fills so workers park on the
	// lock instead of draining the burst before the autoscaler's first tick.
	ds.dataMu.Lock()
	queueLocked := true
	defer func() {
		if queueLocked {
			ds.dataMu.Unlock()
		}
	}()
	for i := 0; i < 120; i++ {
		rec.Retain()
		if !s.ingestionQueue.Push(IngestionJob{DS: ds, Batch: rec}) {
			rec.Release()
			t.Fatal("failed to fill ingestion queue")
		}
	}

	baseline := MinIngestionWorkers + 1
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		ingestion, _ := testWorkerCounts(s)
		if ingestion > baseline {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ingestion, _ := testWorkerCounts(s); ingestion <= baseline {
		t.Fatalf("autoscaler did not scale up: workers=%d baseline=%d", ingestion, baseline)
	}
	ds.dataMu.Unlock()
	queueLocked = false

	// Once the burst drains the same goroutine must still be alive to scale
	// back down. The old implementation returned after the first scale-up.
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		ingestion, _ := testWorkerCounts(s)
		if ingestion == baseline {
			rec.Release()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	rec.Release()
	t.Fatal("autoscaler stopped after scale-up; workers never scaled back down")
}

// TestStartIndexingAutoscalerRespectsMinimum verifies the autoscaler never
// drains the indexing pool below MinIndexingWorkers.
func TestStartIndexingAutoscalerRespectsMinimum(t *testing.T) {
	s := newHysteresisTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartIndexingAutoscaler(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.workerMu.Lock()
		count := len(s.indexingWorkerCancels)
		s.workerMu.Unlock()
		if count == MinIndexingWorkers {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("indexing autoscaler did not settle at the configured minimum")
}
