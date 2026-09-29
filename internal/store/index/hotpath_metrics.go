package index

import (
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// Handles of the hot path counters whose label set is a compile time constant.
// They are resolved once, when the package is initialised, so the traversal
// loop only touches the per-P accumulator of the calling goroutine and never
// performs a Prometheus label lookup.
var (
	// hotpathBranchLocationFound counts candidates whose location was resolved.
	hotpathBranchLocationFound = metrics.HnswBranchPredictionSharded.For("location_found")
	// hotpathBranchLocationMiss counts candidates without a resolvable location.
	hotpathBranchLocationMiss = metrics.HnswBranchPredictionSharded.For("location_miss")
	// hotpathBranchFilterMatch counts candidates admitted by a metadata filter.
	hotpathBranchFilterMatch = metrics.HnswBranchPredictionSharded.For("filter_match")
	// hotpathBranchFilterMiss counts candidates rejected by a metadata filter.
	hotpathBranchFilterMiss = metrics.HnswBranchPredictionSharded.For("filter_miss")

	// hotpathEarlyTerminationBudget counts searches that exhausted the visited
	// node budget.
	hotpathEarlyTerminationBudget = metrics.HNSWEarlyTerminationSharded.For("budget_exceeded")
)

// hotpathMetrics holds the label resolved sharded accumulators of one index.
// The label is the dataset name, which is fixed when the index is built, so the
// child of the counter vector is resolved on first use and afterwards read with
// a single atomic load.
type hotpathMetrics struct {
	nodesSkipped    atomic.Pointer[metrics.ShardedCounter]
	preFiltered     atomic.Pointer[metrics.ShardedCounter]
	filterEarlyExit atomic.Pointer[metrics.ShardedCounter]
}

// NodesSkippedCounter returns the accumulator of
// longbow_hnsw_nodes_skipped_total for this index.
func (m *hotpathMetrics) NodesSkippedCounter(name string) *metrics.ShardedCounter {
	if c := m.nodesSkipped.Load(); c != nil {
		return c
	}
	// The label lookup is memoized, so a racing writer stores the very same
	// accumulator and the loser simply reads it back.
	c := metrics.HNSWNodesSkippedSharded.For(name)
	if m.nodesSkipped.CompareAndSwap(nil, c) {
		return c
	}
	return m.nodesSkipped.Load()
}

// PreFilteredCounter returns the accumulator of
// longbow_hnsw_prefiltered_searches_total for this index.
func (m *hotpathMetrics) PreFilteredCounter(name string) *metrics.ShardedCounter {
	if c := m.preFiltered.Load(); c != nil {
		return c
	}
	c := metrics.HNSWPreFilteredSearchesSharded.For(name)
	if m.preFiltered.CompareAndSwap(nil, c) {
		return c
	}
	return m.preFiltered.Load()
}

// FilterEarlyExitCounter returns the accumulator of
// longbow_filter_early_exit_total for this index.
func (m *hotpathMetrics) FilterEarlyExitCounter(name string) *metrics.ShardedCounter {
	if c := m.filterEarlyExit.Load(); c != nil {
		return c
	}
	c := metrics.HNSWFilterEarlyExitSharded.For(name)
	if m.filterEarlyExit.CompareAndSwap(nil, c) {
		return c
	}
	return m.filterEarlyExit.Load()
}
