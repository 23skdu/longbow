package store

// Roadmap Item 31: LONGBOW_BENCH_SEED pins the ingest pipeline's worker pools,
// because the number of workers decides when a row is published and which
// index job it rides in - and both decide the graph.

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func TestDeterministicIngestPinsWorkerPools(t *testing.T) {
	saved := DeterministicIngestEnabled()
	SetDeterministicIngest(true)
	t.Cleanup(func() { SetDeterministicIngest(saved) })

	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	t.Cleanup(func() {
		vs.StopIndexingWorkers(len(vs.indexingWorkerCancels))
		vs.StopIngestionWorkers(len(vs.ingestionWorkerCancels))
	})

	// Repeated calls matter: the minimum-worker ticker and the autoscaler both
	// re-enter these, and a clamp that only held on the first call would let
	// the pool grow back within a tick.
	vs.StartIndexingWorkers(8)
	vs.StartIndexingWorkers(8)
	if got := len(vs.indexingWorkerCancels); got != 1 {
		t.Errorf("deterministic mode runs %d index workers, want exactly 1: more than one pops index jobs out of send order, and AddBatch assigns node IDs by position", got)
	}

	// 0 is the "one per CPU" spelling, which is what main.go passes.
	vs.StartIngestionWorkers(0)
	vs.StartIngestionWorkers(8)
	if got := len(vs.ingestionWorkerCancels); got != 1 {
		t.Errorf("deterministic mode runs %d ingestion workers, want exactly 1: a row's global ID depends on which worker publishes its batch first", got)
	}

	// The autoscaler's entry point must not resize the pools.
	vs.AdjustWorkerCounts(8, 8)
	if got := len(vs.indexingWorkerCancels); got != 1 {
		t.Errorf("AdjustWorkerCounts grew the index pool to %d under deterministic mode", got)
	}
	if got := len(vs.ingestionWorkerCancels); got != 1 {
		t.Errorf("AdjustWorkerCounts grew the ingestion pool to %d under deterministic mode", got)
	}
}

func TestWorkerPoolsAreNotPinnedOutsideDeterministicMode(t *testing.T) {
	saved := DeterministicIngestEnabled()
	SetDeterministicIngest(false)
	t.Cleanup(func() { SetDeterministicIngest(saved) })

	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	t.Cleanup(func() {
		vs.StopIndexingWorkers(len(vs.indexingWorkerCancels))
		vs.StopIngestionWorkers(len(vs.ingestionWorkerCancels))
	})

	vs.StartIndexingWorkers(3)
	if got := len(vs.indexingWorkerCancels); got != 3 {
		t.Errorf("normal mode started %d index workers, want 3: the deterministic clamp must not apply when the seed is unset", got)
	}

	vs.StartIngestionWorkers(3)
	if got := len(vs.ingestionWorkerCancels); got != 3 {
		t.Errorf("normal mode started %d ingestion workers, want 3", got)
	}
}
