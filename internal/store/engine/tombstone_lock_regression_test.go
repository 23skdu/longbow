package engine

import (
	"testing"
	"time"
)

func TestCorpusForWaitsForTombstoneLock(t *testing.T) {
	rec := buildP3TextBatch(0, 4, func(i int) string { return "doc" })
	defer rec.Release()

	ds := NewDataset("corpus_tombstone_lock", rec.Schema())
	ds.Records.Append(rec)
	var store VectorStore
	store.datasets.Store(&map[string]*Dataset{ds.Name: ds})
	manager := NewAdvancedTextSearchManager(&store)

	ds.TombstoneMu.Lock()
	done := make(chan error, 1)
	go func() {
		err := manager.CorpusFor(ds.Name)
		done <- err
	}()

	select {
	case err := <-done:
		ds.TombstoneMu.Unlock()
		t.Fatalf("corpusFor completed while TombstoneMu was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	ds.TombstoneMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("corpusFor failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("corpusFor did not complete after TombstoneMu was released")
	}
}

func TestGetTombstonesReturnsStableSnapshot(t *testing.T) {
	ds := NewDataset("tombstone_snapshot", nil)
	setTombstone(ds, 0, 1)
	snapshot := ds.GetTombstones()

	setTombstone(ds, 1, 2)
	if _, ok := snapshot[1]; ok {
		t.Fatal("tombstone snapshot map changed after a later write")
	}

	ds.ResetTombstones()
	if !snapshot[0].Contains(1) {
		t.Fatal("tombstone snapshot was invalidated by ResetTombstones")
	}
}
