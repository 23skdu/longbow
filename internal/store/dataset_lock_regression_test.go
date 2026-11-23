package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// nestedReadIndex models an index whose search workers re-acquire the
// dataset read lock, as ArrowHNSW does from processResultsParallelInternal.
type nestedReadIndex struct {
	types.VectorIndexer
	dataset *Dataset
	entered chan struct{}
	release chan struct{}
}

func (i *nestedReadIndex) GetDimension() uint32 { return 1 }
func (i *nestedReadIndex) Close() error         { return nil }

func (i *nestedReadIndex) SearchVectors(context.Context, any, int, []core.Filter, any) ([]types.SearchResult, error) {
	close(i.entered)
	<-i.release
	i.dataset.RLockData()
	defer i.dataset.RUnlockData()
	return nil, nil
}

// TestDatasetSearchLockDoesNotStarveWriters pins the fix for the soak stall:
// callers must release the dataset read lock before invoking an index that
// re-acquires it internally. Otherwise a pending writer blocks both locks.
func TestDatasetSearchLockDoesNotStarveWriters(t *testing.T) {
	newFakeStore := func(t *testing.T) (*VectorStore, *Dataset, *nestedReadIndex) {
		t.Helper()
		s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
		t.Cleanup(func() { _ = s.Close() })

		ds := &Dataset{
			Name:    "lock-regression",
			Records: NewLockFreeSlice[arrow.RecordBatch](),
		}
		idx := &nestedReadIndex{
			dataset: ds,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		ds.Index = idx
		s.UpdateDatasets(func(m map[string]*Dataset) { m[ds.Name] = ds })
		return s, ds, idx
	}

	// Shared dataset vector search path (federated/multimodal fan-out).
	t.Run("SearchDatasetVectors", func(t *testing.T) {
		s, _, idx := newFakeStore(t)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := s.SearchDatasetVectors(ctx, "lock-regression", []float32{1}, 1, nil)
			done <- err
		}()
		<-idx.entered

		writerAcquired := make(chan struct{})
		go func() {
			// Wait until the search is inside the index, then queue a writer.
			idx.dataset.dataMu.Lock()
			close(writerAcquired)
			time.Sleep(20 * time.Millisecond)
			idx.dataset.dataMu.Unlock()
		}()
		requireWriterAcquired(t, writerAcquired)

		close(idx.release)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("SearchDatasetVectors deadlocked with a pending dataset writer")
		}
	})

	// VectorSearch DoAction path used by AgentSoak and normal queries.
	t.Run("VectorSearchAction", func(t *testing.T) {
		s, _, idx := newFakeStore(t)
		body, err := json.Marshal(query.VectorSearchRequest{
			Dataset: "lock-regression",
			Vector:  []float32{1},
			K:       1,
		})
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			done <- s.HandleVectorSearchAction(&flight.Action{Type: "VectorSearch", Body: body}, &mockVectorSearchDoActionServer{})
		}()
		<-idx.entered

		writerAcquired := make(chan struct{})
		go func() {
			idx.dataset.dataMu.Lock()
			close(writerAcquired)
			time.Sleep(20 * time.Millisecond)
			idx.dataset.dataMu.Unlock()
		}()
		requireWriterAcquired(t, writerAcquired)

		close(idx.release)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("VectorSearch action deadlocked with a pending dataset writer")
		}
	})
}

func requireWriterAcquired(t *testing.T, acquired <-chan struct{}) {
	t.Helper()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("dataset writer could not acquire the lock while search was in flight")
	}
}
