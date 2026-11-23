package store

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// lifecycleTestSearcher is a deterministic in-memory VectorSearcher.
type lifecycleTestSearcher struct {
	id    lbtypes.VectorID
	dim   int
	calls atomic.Int64
}

func (m *lifecycleTestSearcher) Search(ctx context.Context, _ []float32, _ int, _ SearchOptions) ([]lbtypes.SearchResult, error) {
	m.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []lbtypes.SearchResult{{ID: m.id, Score: 1}}, nil
}

func (m *lifecycleTestSearcher) GetDimension() int { return m.dim }

func TestFederatedRouterRemoveAndRefresh(t *testing.T) {
	router := NewFederatedQueryRouter()
	first := &lifecycleTestSearcher{id: 1, dim: 4}
	require.NoError(t, router.RegisterCollection("lifecycle", first, &CollectionInfo{
		Name: "lifecycle", Tags: []string{"stale"},
	}))
	require.Equal(t, 1, router.SearcherCount())

	require.True(t, router.RemoveCollection("lifecycle"))
	require.Equal(t, 0, router.SearcherCount())
	_, ok := router.registry.GetCollection("lifecycle")
	require.False(t, ok)
	results, err := router.Search(context.Background(), []float32{1, 2}, []string{"lifecycle"}, 1)
	require.NoError(t, err)
	require.Empty(t, results)

	second := &lifecycleTestSearcher{id: 2, dim: 4}
	router.RefreshCollection("lifecycle", second, &CollectionInfo{
		Description: "persisted", VectorCount: 11,
	})
	info, ok := router.registry.GetCollection("lifecycle")
	require.True(t, ok)
	require.Equal(t, "lifecycle", info.Name)
	require.Equal(t, 4, info.Dimension, "dimension must come from the refreshed searcher")
	require.Equal(t, 11, info.VectorCount)

	results, err = router.Search(context.Background(), []float32{1, 2}, []string{"lifecycle"}, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, lbtypes.VectorID(2), results[0].ID, "stale searcher must no longer serve results")

	third := &lifecycleTestSearcher{id: 3, dim: 8}
	router.RefreshCollection("lifecycle", third, &CollectionInfo{VectorCount: 12})
	info, ok = router.registry.GetCollection("lifecycle")
	require.True(t, ok)
	require.Equal(t, "persisted", info.Description, "refresh without descriptive fields must preserve them")
	require.Equal(t, 8, info.Dimension)
	results, err = router.Search(context.Background(), []float32{1, 2}, []string{"lifecycle"}, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, lbtypes.VectorID(3), results[0].ID)
}

func TestFederatedStoreSyncReconcilesDropAndRecreate(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	const name = "federated_lifecycle_ds"
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	newDS := func() *Dataset { return NewDataset(name, schema) }

	_, created := s.getOrCreateDataset(name, newDS)
	require.True(t, created)
	router := s.GetFederatedRouter()
	s.syncDatasetToFederatedRouter(router, []string{name})
	require.Equal(t, 1, router.SearcherCount())

	require.NoError(t, s.DropDataset(ctx, name))
	require.Equal(t, 0, router.SearcherCount(), "drop must remove the stale searcher")
	_, ok := router.registry.GetCollection(name)
	require.False(t, ok)

	_, created = s.getOrCreateDataset(name, newDS)
	require.True(t, created)
	s.federatedDirty.Add(1)
	s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
	require.Equal(t, 1, router.SearcherCount(), "recreate must refresh the registration")
	searcher, ok := router.searcherFor(name)
	require.True(t, ok)
	storeSearcher, ok := searcher.(*storeDatasetSearcher)
	require.True(t, ok)
	require.Equal(t, name, storeSearcher.dataset)
	require.Same(t, s, storeSearcher.store)
}

// TestFederatedRouterConcurrentRefreshAndSearch runs continuous searches while
// collections are removed and refreshed. Searcher maps are copy-on-write, so
// searchers captured before a refresh remain safe to use.
func TestFederatedRouterConcurrentRefreshAndSearch(t *testing.T) {
	router := NewFederatedQueryRouter()
	stable := &lifecycleTestSearcher{id: 99, dim: 2}
	router.UpsertCollection("stable", stable, &CollectionInfo{Name: "stable"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				results, err := router.Search(ctx, []float32{1, 2}, []string{"stable"}, 1)
				if err != nil || len(results) != 1 || results[0].ID != 99 {
					failures.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			router.RefreshCollection("stable", &lifecycleTestSearcher{id: 99, dim: 2}, &CollectionInfo{
				Name: "stable", VectorCount: i,
			})
			temp := fmt.Sprintf("temp-%d", i%2)
			if i%2 == 0 {
				router.UpsertCollection(temp, &lifecycleTestSearcher{id: lbtypes.VectorID(i)}, &CollectionInfo{Name: temp})
			} else {
				router.RemoveCollection(temp)
			}
			runtime.Gosched()
		}
	}()

	wg.Wait()
	require.Zero(t, failures.Load(), "searches must succeed while membership changes")
}
