package store

import (
"testing"

"github.com/23skdu/longbow/internal/metrics"
"github.com/prometheus/client_golang/prometheus/testutil"
"github.com/stretchr/testify/assert"
)

func TestObservability_ShardedLocking(t *testing.T) {
sm := NewShardedMap()
sm.Set("test-dataset", &Dataset{Name: "test-dataset"})
_, _ = sm.Get("test-dataset")

count := testutil.CollectAndCount(metrics.ShardLockWaitDuration)
assert.Greater(t, count, 0, "Expected shard lock metrics to be recorded")
}

func TestObservability_WALBufferPool(t *testing.T) {
// Capture counter values BEFORE operations to handle global metric pollution
// from other tests (counters are cumulative and can't be reset)
getBefore := testutil.ToFloat64(metrics.WalBufferPoolOperations.WithLabelValues("get"))
putBefore := testutil.ToFloat64(metrics.WalBufferPoolOperations.WithLabelValues("put"))

pool := newWALBufferPool()
buf := pool.Get()
pool.Put(buf)

// Verify delta (increment of 1) rather than absolute value
getAfter := testutil.ToFloat64(metrics.WalBufferPoolOperations.WithLabelValues("get"))
assert.Equal(t, 1.0, getAfter-getBefore, "Expected 1 Get operation (delta)")

putAfter := testutil.ToFloat64(metrics.WalBufferPoolOperations.WithLabelValues("put"))
assert.Equal(t, 1.0, putAfter-putBefore, "Expected 1 Put operation (delta)")
}

func TestObservability_HNSWZeroCopy(t *testing.T) {
ds := &Dataset{Name: "test-vector-ds"}
idx := NewHNSWIndex(ds)

idx.RegisterReader()
val := testutil.ToFloat64(metrics.HnswActiveReaders.WithLabelValues("test-vector-ds"))
assert.Equal(t, 1.0, val, "Expected 1 active reader")

idx.UnregisterReader()
val = testutil.ToFloat64(metrics.HnswActiveReaders.WithLabelValues("test-vector-ds"))
assert.Equal(t, 0.0, val, "Expected 0 active readers")
}
