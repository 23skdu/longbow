package store

// Tests for the temporal result cache counters (roadmap R11a).
//
// The cache maintained hits, misses and evictions and read none of them, so
// "the temporal cache is thrashing" could only be inferred from latency. The
// failure this guards against is two:
//
//   - the counters existing but not being observable, which is how the original
//     bug survived; and
//   - expiry and capacity eviction being conflated, which would make the counter
//     observable and still useless, because the two need opposite remedies.

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testResults() []lbtypes.SearchResult {
	return []lbtypes.SearchResult{{ID: 1, Distance: 0.5}}
}

func TestCacheCountsHitsAndMisses(t *testing.T) {
	c := NewTemporalResultCache(10)
	c.SetDatasetLabel("cache_hits_misses")

	c.Get("a") // absent
	c.Set("a", testResults(), time.Minute)
	c.Get("a") // present

	s := c.Stats()
	if s.Misses != 1 {
		t.Errorf("Misses = %d, want 1", s.Misses)
	}
	if s.Hits != 1 {
		t.Errorf("Hits = %d, want 1", s.Hits)
	}
	if s.Expiries != 0 {
		t.Errorf("Expiries = %d, want 0", s.Expiries)
	}
	if s.Evictions != 0 {
		t.Errorf("Evictions = %d, want 0", s.Evictions)
	}
}

// The whole reason the counters were split. An expired entry and a full LRU both
// produce a miss, but one is fixed by a longer TTL and the other by a bigger cache.
func TestExpiryAndCapacityEvictionAreCountedSeparately(t *testing.T) {
	c := NewTemporalResultCache(2)
	c.SetDatasetLabel("cache_split")

	c.Set("a", testResults(), time.Nanosecond)
	c.Set("b", testResults(), time.Hour)
	time.Sleep(time.Millisecond)
	c.Get("a") // expired

	s := c.Stats()
	if s.Expiries != 1 {
		t.Errorf("Expiries = %d, want 1 (TTL expiry must not count as capacity eviction)", s.Expiries)
	}
	if s.Evictions != 0 {
		t.Errorf("Evictions = %d, want 0 (nothing has been pushed out of a full LRU yet)", s.Evictions)
	}
	if s.Misses != 1 {
		t.Errorf("Misses = %d, want 1", s.Misses)
	}

	// Fill capacity, then exceed it. Note the expiry above already removed "a"
	// from the LRU, so one more insert is needed to reach capacity before a
	// fourth can push anything out.
	c.Set("c", testResults(), time.Hour) // back to capacity: b, c
	c.Set("d", testResults(), time.Hour) // exceeds capacity, evicts b
	s = c.Stats()
	if s.Evictions != 1 {
		t.Errorf("Evictions = %d, want 1", s.Evictions)
	}
	if s.Expiries != 1 {
		t.Errorf("Expiries = %d, want 1 (capacity eviction must not count as an expiry)", s.Expiries)
	}
}

func TestStatsReportsResidentEntries(t *testing.T) {
	c := NewTemporalResultCache(10)
	c.SetDatasetLabel("cache_entries")

	if s := c.Stats(); s.Entries != 0 {
		t.Errorf("Entries = %d, want 0", s.Entries)
	}
	c.Set("a", testResults(), time.Hour)
	c.Set("b", testResults(), time.Hour)
	if s := c.Stats(); s.Entries != 2 {
		t.Errorf("Entries = %d, want 2", s.Entries)
	}
}

// An unlabelled cache must not vanish from the scrape; it reports under a
// reserved label rather than being silently dropped.
func TestUnlabelledCacheReportsUnderUnknown(t *testing.T) {
	c := NewTemporalResultCache(10)
	c.Set("a", testResults(), time.Hour)
	c.Get("a")

	if n := testutil.ToFloat64(metrics.TemporalCacheHitsTotal.WithLabelValues("unknown")); n != 1 {
		t.Errorf("hits{dataset=\"unknown\"} = %v, want 1", n)
	}
}

func TestPrometheusCountersCarryTheDatasetLabel(t *testing.T) {
	c := NewTemporalResultCache(1)
	c.SetDatasetLabel("labelled_ds")

	c.Get("miss")                        // miss
	c.Set("a", testResults(), time.Hour) // resident
	c.Set("b", testResults(), time.Hour) // evicts a

	label := "labelled_ds"
	if n := testutil.ToFloat64(metrics.TemporalCacheMissesTotal.WithLabelValues(label)); n != 1 {
		t.Errorf("misses = %v, want 1", n)
	}
	if n := testutil.ToFloat64(metrics.TemporalCacheEvictionsTotal.WithLabelValues(label)); n != 1 {
		t.Errorf("evictions = %v, want 1", n)
	}
	if n := testutil.ToFloat64(metrics.TemporalCacheEntries.WithLabelValues(label)); n != 1 {
		t.Errorf("entries = %v, want 1", n)
	}
}

// The metric that motivated R11a must actually move. The pre-existing
// longbow_temporal_tree_cache_hit_ratio was registered, on a dashboard, and never
// set - it reported 0 forever. Pin that the new counters are wired, not declared.
func TestCountersAreWiredNotMerelyDeclared(t *testing.T) {
	c := NewTemporalResultCache(10)
	c.SetDatasetLabel("wiring_ds")

	before := testutil.ToFloat64(metrics.TemporalCacheHitsTotal.WithLabelValues("wiring_ds"))
	c.Set("a", testResults(), time.Hour)
	c.Get("a")
	after := testutil.ToFloat64(metrics.TemporalCacheHitsTotal.WithLabelValues("wiring_ds"))

	if after-before != 1 {
		t.Errorf("hit counter moved by %v across one hit, want 1; counter is declared but not wired", after-before)
	}
}

// Changing the label mid-flight routes subsequent counts to the new series rather
// than rewriting history on the old one.
func TestRelabellingRoutesSubsequentCountsToANewSeries(t *testing.T) {
	c := NewTemporalResultCache(10)
	c.SetDatasetLabel("before_ds")
	c.Set("a", testResults(), time.Hour)
	c.Get("a")

	c.SetDatasetLabel("after_ds")
	c.Get("a")

	if n := testutil.ToFloat64(metrics.TemporalCacheHitsTotal.WithLabelValues("before_ds")); n != 1 {
		t.Errorf("hits{before_ds} = %v, want 1", n)
	}
	if n := testutil.ToFloat64(metrics.TemporalCacheHitsTotal.WithLabelValues("after_ds")); n != 1 {
		t.Errorf("hits{after_ds} = %v, want 1", n)
	}
}

// Concurrency: counters must be exact under contention, not approximate.
func TestCountersAreExactUnderConcurrentAccess(t *testing.T) {
	c := NewTemporalResultCache(64)
	c.SetDatasetLabel("concurrent_ds")

	const workers, ops = 8, 200
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < ops; i++ {
				key := string(rune('a' + (i+w)%26))
				c.Get(key)
				c.Set(key, testResults(), time.Hour)
			}
		}(w)
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	s := c.Stats()
	if s.Hits+s.Misses != workers*ops {
		t.Errorf("hits(%d)+misses(%d) = %d, want %d; a counter was lost under concurrency",
			s.Hits, s.Misses, s.Hits+s.Misses, workers*ops)
	}
}
