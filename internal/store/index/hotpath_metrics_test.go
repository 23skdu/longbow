package index

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rejectingPredicate admits every id but one in sixteen, so the traversal skips
// the candidates it evaluates and the per candidate nodes-skipped counter fires.
//
// The predicate only gates the result set, not the traversal frontier, so the
// graph is walkable regardless of which ids the predicate rejects. Admitting the
// graph entry point is still load bearing for a different reason: searchLayer*
// pushes the entry point into the result set only when it passes the filter and
// the predicate, so a search whose entry point is rejected can legitimately
// return no entry-point result, and this test asserts on returned results. The
// graph is built in parallel, so the entry point differs from build to build;
// admitting it keeps the search deterministic. A one-in-sixteen rejection rate
// keeps the nodes-skipped series moving without discarding most candidates.
type rejectingPredicate struct{ entryPoint uint32 }

func (p rejectingPredicate) IsMatch(id uint32) bool { return id%16 != 0 || id == p.entryPoint }
func (p rejectingPredicate) MatchBatch(ids []uint32, dst []byte) {
	for i, id := range ids {
		if p.IsMatch(id) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

// hotpathSearch returns the filter and the options the metric tests drive the
// index with. The filter is the index's own filter widened to admit the entry
// point, so the predicate admits a node the search is also allowed to return;
// see rejectingPredicate for why that precondition has to hold.
func hotpathSearch(idx *ArrowHNSW, filter *roaring.Bitmap) (*roaring.Bitmap, types.SearchOptions) {
	entryPoint := idx.GetMetadataSnapshot().EntryPoint
	widened := filter.Clone()
	widened.Add(entryPoint)
	return widened, types.SearchOptions{Predicate: rejectingPredicate{entryPoint: entryPoint}}
}

// awaitHotpathCounters publishes the hotpath accumulators synchronously and
// re-reads cond until it holds or the bound expires, so a broken flusher fails
// the test instead of slowing the suite. It replaces require.Eventually, which
// evaluates the condition on its own goroutine and returns without waiting for
// it: a condition that flushes would leave a goroutine draining the shared
// accumulators into the next test's read of the same series.
func awaitHotpathCounters(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics.FlushHotpathCounters()
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			require.Fail(t, msg)
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// buildHotpathIndex returns an index over n random float32 vectors together with
// a query vector and the filter that admits the first pct of the ids.
func buildHotpathIndex(tb testing.TB, name string, n, dims, pct int) (*ArrowHNSW, []float32, *roaring.Bitmap) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	r := rand.New(rand.NewSource(7)) // #nosec G404

	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}
	rec := MakeBatchTestRecord(mem, dims, vecs)
	rec.Retain()
	tb.Cleanup(rec.Release)

	ds := NewMockDataset(name, rec.Schema())
	ds.Records = append(ds.Records, rec)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	cfg.M = 16
	cfg.EfConstruction = 100
	idx := NewArrowHNSW(ds, &cfg, nil)

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(tb, err)

	filter := roaring.New()
	for i := 0; i < n*pct/100; i++ {
		filter.Add(uint32(i)) // #nosec G115
	}

	q := make([]float32, dims)
	for j := range q {
		q[j] = float32(r.NormFloat64())
	}
	return idx, q, filter
}

// TestHotpathMetrics_SearchExportsShardedCounters drives a real filtered and
// predicated search through every converted call site and checks that the
// values the search produced reach the Prometheus counters after a flush.
func TestHotpathMetrics_SearchExportsShardedCounters(t *testing.T) {
	const (
		name   = "hotpath_export_ds"
		n      = 2000
		dims   = 64
		search = 4
	)

	idx, q, filter := buildHotpathIndex(t, name, n, dims, 50)
	filter, opts := hotpathSearch(idx, filter)

	// Publish whatever an earlier test left pending so the baselines are exact.
	metrics.FlushHotpathCounters()
	skippedBefore := testutil.ToFloat64(metrics.HNSWNodesSkippedTotal.WithLabelValues(name))
	preFilteredBefore := testutil.ToFloat64(metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(name))
	chunksBefore := testutil.ToFloat64(metrics.CacheBlockedTraversalChunksTotal)
	poolGetBefore := testutil.ToFloat64(metrics.HNSWSearchPoolGetTotal)

	for i := 0; i < search; i++ {
		res, err := idx.SearchVectorsWithBitmap(context.Background(), q, 10, filter, opts)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	}

	awaitHotpathCounters(t, func() bool {
		return testutil.ToFloat64(metrics.HNSWNodesSkippedTotal.WithLabelValues(name)) > skippedBefore &&
			testutil.ToFloat64(metrics.CacheBlockedTraversalChunksTotal) > chunksBefore &&
			testutil.ToFloat64(metrics.HNSWSearchPoolGetTotal) > poolGetBefore
	}, fmt.Sprintf("a flushed filtered and predicated search must publish the increments it produced: skipped %v>%v chunks %v>%v poolGet %v>%v",
		testutil.ToFloat64(metrics.HNSWNodesSkippedTotal.WithLabelValues(name)), skippedBefore,
		testutil.ToFloat64(metrics.CacheBlockedTraversalChunksTotal), chunksBefore,
		testutil.ToFloat64(metrics.HNSWSearchPoolGetTotal), poolGetBefore))

	assert.Equal(t, float64(search), testutil.ToFloat64(metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(name))-preFilteredBefore,
		"every filtered search must be counted exactly once")

	// An empty filter short-circuits before the traversal and must be counted on
	// the early exit series instead.
	earlyExitBefore := testutil.ToFloat64(metrics.HNSWFilterEarlyExitTotal.WithLabelValues(name))
	empty := roaring.New()
	res, err := idx.SearchVectorsWithBitmap(context.Background(), q, 10, empty, opts)
	require.NoError(t, err)
	assert.Empty(t, res)

	metrics.FlushHotpathCounters()
	assert.Equal(t, earlyExitBefore+1, testutil.ToFloat64(metrics.HNSWFilterEarlyExitTotal.WithLabelValues(name)))
}

// TestHotpathMetrics_ConcurrentSearchesAreExactlyCounted runs many searches
// in parallel, so every per-P shard is written by more than one goroutine, and
// pins the exported total to the number of searches performed.
func TestHotpathMetrics_ConcurrentSearchesAreExactlyCounted(t *testing.T) {
	const (
		name    = "hotpath_exact_ds"
		n       = 2000
		dims    = 64
		workers = 8
		rounds  = 10
	)

	idx, q, filter := buildHotpathIndex(t, name, n, dims, 50)
	filter, opts := hotpathSearch(idx, filter)

	// Publish whatever an earlier test left pending so the baselines are exact.
	metrics.FlushHotpathCounters()
	preFilteredBefore := testutil.ToFloat64(metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(name))
	skippedBefore := testutil.ToFloat64(metrics.HNSWNodesSkippedTotal.WithLabelValues(name))

	// A flusher running for the whole test makes the drain race the producers
	// instead of only happening once at the end.
	flusherDone := make(chan struct{})
	stopFlusher := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-stopFlusher:
				return
			default:
				metrics.FlushHotpathCounters()
			}
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if _, err := idx.SearchVectorsWithBitmap(context.Background(), q, 10, filter, opts); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stopFlusher)
	<-flusherDone
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	metrics.FlushHotpathCounters()
	total := float64(workers * rounds)
	assert.Equal(t, total, testutil.ToFloat64(metrics.HNSWPreFilteredSearchesTotal.WithLabelValues(name))-preFilteredBefore,
		"every search must be counted exactly once, with no loss and no duplication")
	assert.Greater(t, testutil.ToFloat64(metrics.HNSWNodesSkippedTotal.WithLabelValues(name)), skippedBefore)
}

// TestHotpathMetrics_HandlesAreResolvedOnce pins the hoisting: the dataset
// label is resolved on the first call and reused afterwards, so the query path
// never performs a Prometheus label lookup.
func TestHotpathMetrics_HandlesAreResolvedOnce(t *testing.T) {
	h := &ArrowHNSW{name: "hotpath_handle_ds"}
	_ = h.hotpath.NodesSkippedCounter(h.name)
	first := h.hotpath.NodesSkippedCounter(h.name)
	assert.Same(t, first, h.hotpath.NodesSkippedCounter(h.name))
	assert.Same(t, first, h.hotpath.NodesSkippedCounter(h.name))

	preFirst := h.hotpath.PreFilteredCounter(h.name)
	assert.Same(t, preFirst, h.hotpath.PreFilteredCounter(h.name))
	exitFirst := h.hotpath.FilterEarlyExitCounter(h.name)
	assert.Same(t, exitFirst, h.hotpath.FilterEarlyExitCounter(h.name))

	// The constant label sets are resolved at package init.
	assert.NotNil(t, hotpathEarlyTerminationBudget)
	assert.NotNil(t, hotpathBranchLocationFound)
	assert.NotNil(t, hotpathBranchLocationMiss)
	assert.NotNil(t, hotpathBranchFilterMatch)
	assert.NotNil(t, hotpathBranchFilterMiss)
}
