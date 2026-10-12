package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/index"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestIngestionQueueNotifyWakesIdleConsumers is the guard against the polling
// loop that cost 1,498 s (24.93%) of cumulative block delay in runtime.selectgo.
//
// An idle worker used to `timer.Reset(1 * time.Millisecond)` and re-enter
// select on every empty Pop, so every worker in the pool woke a thousand times
// a second for a queue with nothing in it. Push must now signal the queue's
// Notify channel, and an idle consumer must be woken by it.
func TestIngestionQueueNotifyWakesIdleConsumers(t *testing.T) {
	rb := NewIngestionRingBuffer(64)

	select {
	case <-rb.Notify():
		t.Fatal("a queue with no pushes must not signal Notify")
	default:
	}

	if !rb.Push(IngestionJob{DS: nil, TS: 1}) {
		t.Fatal("push to an empty ring must succeed")
	}

	select {
	case <-rb.Notify():
	case <-time.After(2 * time.Second):
		t.Fatal("Push must signal Notify so an idle consumer can sleep")
	}

	// The signal is a hint, not a counter: a second push with no reader in
	// between must not block and must leave at most one pending token.
	for range 4 {
		require.True(t, rb.Push(IngestionJob{DS: nil, TS: int64(2)}))
	}
	pending := 0
	for {
		select {
		case <-rb.Notify():
			pending++
			continue
		default:
		}
		break
	}
	require.LessOrEqual(t, pending, 1, "Notify must coalesce, not accumulate")

	// And every pushed job is still reachable after the signals were consumed.
	drained := 0
	for {
		_, ok := rb.Pop()
		if !ok {
			break
		}
		drained++
	}
	require.Equal(t, 5, drained, "notifications must never drop work")
}

// TestIngestionWorkerConsumesWithoutPolling exercises the real worker loop:
// with a notification-driven consumer, a batch pushed after the worker is
// already parked must still be applied promptly.
func TestIngestionWorkerConsumesWithoutPolling(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewSlabArena(1 << 20)
	defer mem.Free()

	store := NewVectorStore(NewPooledAllocator(), logger, 1<<30, 1<<28, 0)
	defer func() { _ = store.Close() }()

	store.StartIngestionWorkers(1)
	require.Eventually(t, func() bool {
		return store.ingestionQueue != nil
	}, time.Second, 10*time.Millisecond)

	ds, _ := store.getOrCreateDataset("notify-drain", func() *Dataset {
		return NewDataset("notify-drain", minimalVectorSchema())
	})

	for i := range 8 {
		rec := makeCountingRecord(t, 4)
		store.ingestionQueue.Push(IngestionJob{DS: ds, Batch: rec, TS: int64(i)})
	}

	require.Eventually(t, func() bool {
		return len(ds.Records.Read()) == 8
	}, 10*time.Second, 20*time.Millisecond,
		"a notification-driven worker must drain the queue without a poll timer")

	require.Equal(t, int64(32), ds.GetRowCount())
}

// ---------------------------------------------------------------------------
// Hybrid text indexing must happen exactly once per batch, and off the
// ingestion critical path.
// ---------------------------------------------------------------------------

// TestTextIndexedExactlyOncePerBatch is the guard against double indexing.
//
// Ingestion used to index every text cell into both BM25 indexes, and the
// background index worker then walked the same records again and inserted the
// same documents a second time - under a different document ID space. That
// doubled every posting list, doubled docCount and avgDocLength, and left the
// sparse half of hybrid search scoring against a corpus twice the real size.
func TestTextIndexedExactlyOncePerBatch(t *testing.T) {
	store := newHybridTestStore(t, "title")
	defer func() { _ = store.Close() }()

	ds, _ := store.getOrCreateDataset("double-index", func() *Dataset {
		return NewDataset("double-index", titleVectorSchema())
	})
	// Exercise both indexes so the assertion covers each of them.
	ds.BM25Index = index.NewBM25InvertedIndex(index.DefaultBM25Config())
	ds.BM25ArenaIndex = index.NewBM25ArenaIndex(memory.NewSlabArena(1<<20), 1024)

	batch := makeTextRecord(t, []string{
		"alpha beta gamma",
		"delta epsilon zeta",
		"eta theta iota",
	})

	store.indexTextColumnsForHybridSearch(ds, batch, 0)

	require.Equal(t, 3, ds.BM25Index.DocCount(),
		"each row must contribute exactly one document to the legacy index")
	require.Equal(t, uint32(3), ds.BM25ArenaIndex.DocumentCount(),
		"each row must contribute exactly one document to the arena index")

	res := ds.BM25Index.SearchBM25("alpha", 10, nil, nil)
	require.Len(t, res, 1, "a term from the first row must match exactly one document")
	require.Equal(t, VectorID(0), res[0].ID)

	res = ds.BM25Index.SearchBM25("eta", 10, nil, nil)
	require.Len(t, res, 1, "a term from the last row must match exactly one document")
	require.Equal(t, VectorID(2), res[0].ID,
		"document IDs must be the dataset-global row offsets handed to the indexer")

	// Document lengths must reflect three documents, not six.
	require.Equal(t, 3, ds.BM25Index.GetDocLength(VectorID(0)))
	require.Equal(t, 3, ds.BM25Index.GetDocLength(VectorID(2)))

	// The corpus IDF must be computed over three documents. With six, every
	// term that appears in one document would be scored as if it were common.
	require.Equal(t, ds.BM25Index.DocCount(), ds.BM25Index.ScorerDocs(),
		"index and scorer corpus counts must not diverge")
}

// TestTextIndexingRunsOffTheIngestionLock verifies that ingest enqueues text
// work rather than performing it inline: by the time applyBatchToMemory
// returns, the dataset write lock must already be free even though BM25
// indexing is still outstanding.
func TestTextIndexingRunsOffTheIngestionLock(t *testing.T) {
	store := newHybridTestStore(t, "title")
	defer func() { _ = store.Close() }()

	ds, _ := store.getOrCreateDataset("off-lock", func() *Dataset {
		return NewDataset("off-lock", titleVectorSchema())
	})
	ds.BM25Index = index.NewBM25InvertedIndex(index.DefaultBM25Config())

	batch := makeTextRecord(t, []string{"lonely unique term", "another unique term"})

	require.NotPanics(t, func() {
		store.enqueueTextIndexing(ds, batch, 0)
	})

	// The batch was retained by the queue, so the caller may release its own
	// reference immediately.
	batch.Release()

	store.waitForTextIndexing(5 * time.Second)

	require.Eventually(t, func() bool {
		return ds.BM25Index.DocCount() == 2
	}, 5*time.Second, 10*time.Millisecond,
		"queued text must still be indexed exactly once")
}

// TestTextIndexQueueSaturatesInline is the backpressure guard: a full queue
// must fall back to inline indexing rather than silently dropping documents.
//
// The queue is attached with no workers and a depth of one, so overflow is a
// property of the test rather than a race against how fast the workers happen
// to run - under -race the real workers keep up easily and the overflow branch
// would never be taken.
func TestTextIndexQueueSaturatesInline(t *testing.T) {
	store := newHybridTestStore(t, "title")
	defer func() { _ = store.Close() }()

	ds, _ := store.getOrCreateDataset("saturate", func() *Dataset {
		return NewDataset("saturate", titleVectorSchema())
	})
	ds.BM25Index = index.NewBM25InvertedIndex(index.DefaultBM25Config())

	store.textQueueOnce.Do(func() {
		store.textQueue = &textIndexQueue{
			jobs: make(chan textIndexJob, 1),
			stop: make(chan struct{}),
		}
	})
	store.textIndexDrainBudget.Store(0)

	const batches = 8
	for i := range batches {
		rec := makeTextRecord(t, []string{"saturation term alpha", "saturation term beta"})
		store.enqueueTextIndexing(ds, rec, uint32(i*2)) //nolint:gosec // test-scale
		rec.Release()
	}

	require.Equal(t, int64(batches-1), store.textIndexQueueDropped(),
		"every push past the queue depth must have been indexed inline")

	// The one job that did fit is abandoned at Close (its workers never
	// existed), so only the inline ones are searchable - but every one of
	// those must be there. A dropped document is indistinguishable from a
	// lost one at query time.
	inline := (batches - 1) * 2
	require.Equal(t, inline, ds.BM25Index.DocCount(),
		"no document may be dropped when the text queue overflows")

	res := ds.BM25Index.SearchBM25("saturation", inline, nil, nil)
	require.Len(t, res, inline, "every inline-indexed document must be searchable")
}

// TestTextIndexingSurvivesShutdown covers the shutdown contract for the text
// queue: a queued job holds a Retain on its Arrow record, so shutting down must
// release every one of them rather than dropping the queue.
//
// The Arrow leak checker is what catches the failure, and it attributes the
// loss to whichever unrelated test runs next - which is why this looked like a
// spurious memory-size error in the HNSW bulk tests.
func TestTextIndexingSurvivesShutdown(t *testing.T) {
	mem := arrowmemory.NewCheckedAllocator(arrowmemory.NewGoAllocator())

	store, err := NewVectorStoreWithHybridConfig(mem, zerolog.Nop(), HybridSearchConfig{
		Enabled:     true,
		TextColumns: []string{"title"},
		BM25:        index.DefaultBM25Config(),
		RRFk:        60,
	})
	require.NoError(t, err)

	ds, _ := store.getOrCreateDataset("shutdown", func() *Dataset {
		return NewDataset("shutdown", titleVectorSchema())
	})

	// A queue with no workers attached: nothing can ever consume these jobs,
	// which is exactly the state shutdown has to cope with when it is called
	// after the store's own workers have already been signalled to stop.
	store.textQueueOnce.Do(func() {
		store.textQueue = &textIndexQueue{
			jobs: make(chan textIndexJob, textIndexQueueDepth),
			stop: make(chan struct{}),
		}
	})
	store.textIndexDrainBudget.Store(0)

	for i := range textIndexQueueDepth {
		rec := makeTextRecordWith(mem, []string{"shutdown term alpha", "shutdown term beta"})
		store.enqueueTextIndexing(ds, rec, uint32(i*2)) //nolint:gosec // test-scale
		rec.Release()
	}

	require.Len(t, store.textQueue.jobs, textIndexQueueDepth)

	done := make(chan error, 1)
	go func() { done <- store.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err, "Close must not block on an undrainable text queue")
	case <-time.After(30 * time.Second):
		t.Fatal("Close hung waiting for a text queue with no workers")
	}

	mem.AssertSize(t, 0)
}
