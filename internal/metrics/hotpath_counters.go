package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// hotpathFlushInterval is the publish cadence of the accumulators that front
// the query hot path.
const hotpathFlushInterval = DefaultShardedFlushInterval

// Sharded accumulators that front the query hot path.
//
// Every accumulator publishes into the child collector of the Prometheus
// counter that already exported the family, so the sharded twin is transparent
// to a dashboard: the family name, its label names and the series values are
// exactly the ones the previous inline WithLabelValues call produced. A counter
// may therefore be written through either the inline collector or its sharded
// twin, and the exported value stays exact and monotonic.
//
// The handles are resolved once, off the query path: constant label sets are
// resolved in init, label sets that depend on the dataset are resolved on first
// use and then read with a single atomic load (see index.hotpathMetrics).
var (
	// Plain counters with no label set.
	HNSWSearchPoolGetSharded  = NewShardedCounter(HNSWSearchPoolGetTotal, ShardedCounterOptions{Name: "longbow_hnsw_search_pool_get_total", FlushInterval: hotpathFlushInterval})
	HNSWSearchPoolPutSharded  = NewShardedCounter(HNSWSearchPoolPutTotal, ShardedCounterOptions{Name: "longbow_hnsw_search_pool_put_total", FlushInterval: hotpathFlushInterval})
	HnswContextCheckSharded   = NewShardedCounter(HnswContextCheckTotal, ShardedCounterOptions{Name: "longbow_hnsw_context_check_total", FlushInterval: hotpathFlushInterval})
	PrefetchOperationsSharded = NewShardedCounter(PrefetchOperationsTotal, ShardedCounterOptions{Name: "longbow_prefetch_operations_total", FlushInterval: hotpathFlushInterval})

	CacheBlockedTraversalChunksSharded = NewShardedCounter(CacheBlockedTraversalChunksTotal, ShardedCounterOptions{Name: "longbow_cache_blocked_traversal_chunks_total", FlushInterval: hotpathFlushInterval})

	// Labelled counters of the search traversal and of the result pool.
	HnswBranchPredictionSharded    = newHotpathCounterVec("longbow_hnsw_branch_prediction_total", HnswBranchPredictionTotal)
	HNSWNodesSkippedSharded        = newHotpathCounterVec("longbow_hnsw_nodes_skipped_total", HNSWNodesSkippedTotal)
	HnswSearchEarlyExitsSharded    = newHotpathCounterVec("longbow_hnsw_search_early_exits_total", HnswSearchEarlyExitsTotal)
	HNSWEarlyTerminationSharded    = newHotpathCounterVec("longbow_hnsw_early_termination_total", HNSWEarlyTerminationTotal)
	HNSWPreFilteredSearchesSharded = newHotpathCounterVec("longbow_hnsw_prefiltered_searches_total", HNSWPreFilteredSearchesTotal)
	HNSWFilterEarlyExitSharded     = newHotpathCounterVec("longbow_filter_early_exit_total", HNSWFilterEarlyExitTotal)
	SearchResultPoolGetSharded     = newHotpathCounterVec("longbow_search_result_pool_get_total", SearchResultPoolGetTotal)
	SearchResultPoolHitsSharded    = newHotpathCounterVec("longbow_search_result_pool_hits_total", SearchResultPoolHitsTotal)
	SearchResultPoolPutSharded     = newHotpathCounterVec("longbow_search_result_pool_put_total", SearchResultPoolPutTotal)
)

// newHotpathCounterVec binds a sharded accumulator per label combination to an
// already registered counter vector. The resolver returns the child of vec that
// the inline call site would have used, so both write into the same series.
func newHotpathCounterVec(name string, vec *prometheus.CounterVec) *ShardedCounterVec {
	return NewShardedCounterVec(vec, func(labelValues []string) prometheus.Counter {
		return vec.WithLabelValues(labelValues...)
	}, ShardedCounterOptions{Name: name, FlushInterval: hotpathFlushInterval})
}

// hotpathAccumulator is the publish surface shared by ShardedCounter and
// ShardedCounterVec.
type hotpathAccumulator interface {
	Flush() float64
}

// hotpathAccumulators is the ordered set of accumulators driven by the shared
// flusher. It is a package level slice, so the set is process wide and
// independent of how many store instances exist.
var hotpathAccumulators = []hotpathAccumulator{
	HNSWSearchPoolGetSharded,
	HNSWSearchPoolPutSharded,
	HnswContextCheckSharded,
	PrefetchOperationsSharded,
	CacheBlockedTraversalChunksSharded,
	HnswBranchPredictionSharded,
	HNSWNodesSkippedSharded,
	HnswSearchEarlyExitsSharded,
	HNSWEarlyTerminationSharded,
	HNSWPreFilteredSearchesSharded,
	HNSWFilterEarlyExitSharded,
	SearchResultPoolGetSharded,
	SearchResultPoolHitsSharded,
	SearchResultPoolPutSharded,
}

// hotpathState holds the single flusher goroutine of the process. A store
// acquires it on start and releases it on shutdown, and the goroutine only
// exists while at least one holder is alive: it is never started per store
// instance and never left behind after the last release.
var hotpathState struct {
	mu      sync.Mutex
	refs    int
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// StartHotpathFlushers acquires the shared hotpath flusher and starts it on the
// first acquisition. It is reference counted, so a process that creates and
// shuts down several stores keeps exactly one flusher alive and publishes the
// pending values of the last one on shutdown. Each call must be paired with a
// call to StopHotpathFlushers.
//
// The flusher deliberately does not inherit the context of the first caller: its
// lifetime is the reference count, so the shutdown of one store must not stop
// the publishing of the stores that are still alive.
func StartHotpathFlushers() {
	hotpathState.mu.Lock()
	defer hotpathState.mu.Unlock()

	hotpathState.refs++
	if hotpathState.running {
		return
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	hotpathState.cancel = cancel
	hotpathState.done = done
	hotpathState.running = true

	go runHotpathFlusher(runCtx, done)
}

// StopHotpathFlushers releases one acquisition. The last release stops the
// flusher, flushing every pending value before it returns. It is a no-op once
// every acquisition has been released.
func StopHotpathFlushers(ctx context.Context) error {
	hotpathState.mu.Lock()
	if hotpathState.refs == 0 {
		hotpathState.mu.Unlock()
		FlushHotpathCounters()
		return nil
	}
	hotpathState.refs--
	if hotpathState.refs > 0 || !hotpathState.running {
		hotpathState.mu.Unlock()
		return nil
	}
	cancel, done := hotpathState.cancel, hotpathState.done
	hotpathState.cancel, hotpathState.done = nil, nil
	hotpathState.running = false
	hotpathState.mu.Unlock()

	cancel()
	if done == nil {
		FlushHotpathCounters()
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// The flusher finishes on its own and still publishes everything.
		FlushHotpathCounters()
		return ctx.Err()
	}
}

// HotpathFlushersRunning reports whether the shared flusher is publishing.
func HotpathFlushersRunning() bool {
	hotpathState.mu.Lock()
	defer hotpathState.mu.Unlock()
	return hotpathState.running
}

// runHotpathFlusher publishes every accumulator on a shared ticker. A single
// goroutine drains all of them instead of one goroutine per counter, and the
// accumulators document Flush as safe to call concurrently, so a store that
// releases the flusher while another one acquires it can never double count.
func runHotpathFlusher(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer FlushHotpathCounters()

	ticker := time.NewTicker(hotpathFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			FlushHotpathCounters()
		}
	}
}

// FlushHotpathCounters drains every hotpath accumulator and publishes the
// aggregate into the wrapped Prometheus counters. It returns the published
// value. The store lifecycle runs it from the shared flusher, and a caller that
// has to observe a hotpath metric without waiting for the next tick can use it
// to publish synchronously, which is what the metric assertions in the tests
// do.
func FlushHotpathCounters() float64 {
	var total float64
	for _, acc := range hotpathAccumulators {
		total += acc.Flush()
	}
	return total
}

// HotpathPending returns the accumulated but unpublished value of every
// hotpath accumulator.
func HotpathPending() float64 {
	var total float64
	for _, acc := range hotpathAccumulators {
		switch a := acc.(type) {
		case *ShardedCounter:
			total += a.Pending()
		case *ShardedCounterVec:
			a.children.Range(func(_, child any) bool {
				total += child.(*ShardedCounter).Pending()
				return true
			})
		}
	}
	return total
}
