package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func newHysteresisTestStore() *VectorStore {
	return NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
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
	s := newHysteresisTestStore()
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
	go func() {
		defer close(waiterDone)
		ds.dataMu.Lock()
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

	pending, waiters = s.pendingScaleDownWork()
	ingestion, _ = testWorkerCounts(s)
	if !CanScaleDownIngestion(pending, waiters, ingestion, MinIngestionWorkers) {
		t.Fatalf("ingestion should scale down after lock release: pending=%d waiters=%d", pending, waiters)
	}
}

// TestStartIndexingAutoscalerRespectsMinimum verifies the autoscaler never
// drains the indexing pool below MinIndexingWorkers.
func TestStartIndexingAutoscalerRespectsMinimum(t *testing.T) {
	s := newHysteresisTestStore()
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
