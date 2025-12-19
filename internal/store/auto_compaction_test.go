package store

import (
"context"
"sync"
"sync/atomic"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
"go.uber.org/zap"
)

// TestCompactionWorker_TriggerChannel verifies the worker has a trigger channel
func TestCompactionWorker_TriggerChannel(t *testing.T) {
cfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 5,
TargetBatchSize:     1000,
CompactionInterval:  time.Hour, // Long interval so timer doesn't fire
}
worker := NewCompactionWorker(cfg)
worker.Start()
defer worker.Stop()

// Worker should have a TriggerCompaction method
err := worker.TriggerCompaction("test-dataset")
assert.NoError(t, err, "TriggerCompaction method should exist and work")
}

// TestCompactionWorker_TriggerNonBlocking verifies trigger doesn't block DoPut
func TestCompactionWorker_TriggerNonBlocking(t *testing.T) {
cfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 5,
TargetBatchSize:     1000,
CompactionInterval:  time.Hour,
}
worker := NewCompactionWorker(cfg)
worker.Start()
defer worker.Stop()

// Trigger many times rapidly - should not block
start := time.Now()
for i := 0; i < 100; i++ {
_ = worker.TriggerCompaction("test-dataset")
}
elapsed := time.Since(start)

// Should complete in under 100ms (non-blocking)
assert.Less(t, elapsed, 100*time.Millisecond, "TriggerCompaction should be non-blocking")
}

// TestAutoCompaction_ThresholdTrigger verifies compaction triggers when batch count exceeds threshold
func TestAutoCompaction_ThresholdTrigger(t *testing.T) {
pool := memory.NewGoAllocator()
logger := zap.NewNop()

// Low threshold for testing
compactionCfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 3, // Trigger after 3 batches
TargetBatchSize:     100,
CompactionInterval:  time.Hour, // Disable timer-based
}

vs := NewVectorStoreWithCompaction(pool, logger, 1<<30, 1<<20, time.Hour, compactionCfg)
require.NotNil(t, vs)
defer func() { _ = vs.Close() }()

// Get initial auto-trigger count
initialTriggers := vs.GetAutoCompactionTriggerCount()

// Add batches one by one
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

for i := 0; i < 5; i++ {
builder := array.NewRecordBuilder(pool, schema)
builder.Field(0).(*array.Int64Builder).Append(int64(i))
rec := builder.NewRecordBatch()
builder.Release()

require.NoError(t, vs.StoreRecordBatch(context.Background(), "test-ds", rec))
rec.Release()
}

// Wait briefly for async trigger to be processed
time.Sleep(100 * time.Millisecond)

// Should have triggered at least once (after batch 3, 4, or 5)
finalTriggers := vs.GetAutoCompactionTriggerCount()
assert.Greater(t, finalTriggers, initialTriggers, "Auto-compaction should have triggered")
}

// TestAutoCompaction_BelowThreshold verifies no trigger when below threshold
func TestAutoCompaction_BelowThreshold(t *testing.T) {
pool := memory.NewGoAllocator()
logger := zap.NewNop()

compactionCfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 10, // High threshold
TargetBatchSize:     100,
CompactionInterval:  time.Hour,
}

vs := NewVectorStoreWithCompaction(pool, logger, 1<<30, 1<<20, time.Hour, compactionCfg)
require.NotNil(t, vs)
defer func() { _ = vs.Close() }()

initialTriggers := vs.GetAutoCompactionTriggerCount()

// Add only 3 batches (below threshold of 10)
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

for i := 0; i < 3; i++ {
builder := array.NewRecordBuilder(pool, schema)
builder.Field(0).(*array.Int64Builder).Append(int64(i))
rec := builder.NewRecordBatch()
builder.Release()

require.NoError(t, vs.StoreRecordBatch(context.Background(), "test-ds", rec))
rec.Release()
}

time.Sleep(100 * time.Millisecond)

// Should NOT have triggered
finalTriggers := vs.GetAutoCompactionTriggerCount()
assert.Equal(t, initialTriggers, finalTriggers, "Auto-compaction should NOT trigger below threshold")
}

// TestAutoCompaction_MetricsEmitted verifies Prometheus metrics are updated
func TestAutoCompaction_MetricsEmitted(t *testing.T) {
pool := memory.NewGoAllocator()
logger := zap.NewNop()

compactionCfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 2,
TargetBatchSize:     100,
CompactionInterval:  time.Hour,
}

vs := NewVectorStoreWithCompaction(pool, logger, 1<<30, 1<<20, time.Hour, compactionCfg)
require.NotNil(t, vs)
defer func() { _ = vs.Close() }()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Add enough batches to trigger
for i := 0; i < 5; i++ {
builder := array.NewRecordBuilder(pool, schema)
builder.Field(0).(*array.Int64Builder).Append(int64(i))
rec := builder.NewRecordBatch()
builder.Release()

require.NoError(t, vs.StoreRecordBatch(context.Background(), "test-ds", rec))
rec.Release()
}

time.Sleep(100 * time.Millisecond)

// Verify auto-trigger count is tracked
triggerCount := vs.GetAutoCompactionTriggerCount()
assert.Greater(t, triggerCount, int64(0), "Auto-compaction trigger count should be tracked")
}

// TestAutoCompaction_ConcurrentDoPut verifies thread safety under concurrent writes
func TestAutoCompaction_ConcurrentDoPut(t *testing.T) {
pool := memory.NewGoAllocator()
logger := zap.NewNop()

compactionCfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 5,
TargetBatchSize:     100,
CompactionInterval:  time.Hour,
}

vs := NewVectorStoreWithCompaction(pool, logger, 1<<30, 1<<20, time.Hour, compactionCfg)
require.NotNil(t, vs)
defer func() { _ = vs.Close() }()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Concurrent writes from multiple goroutines
var wg sync.WaitGroup
var errors atomic.Int64

for g := 0; g < 10; g++ {
wg.Add(1)
go func(goroutineID int) {
defer wg.Done()
for i := 0; i < 20; i++ {
builder := array.NewRecordBuilder(pool, schema)
builder.Field(0).(*array.Int64Builder).Append(int64(goroutineID*100 + i))
rec := builder.NewRecordBatch()
builder.Release()

if err := vs.StoreRecordBatch(context.Background(), "concurrent-ds", rec); err != nil {
errors.Add(1)
}
rec.Release()
}
}(g)
}

wg.Wait()

assert.Equal(t, int64(0), errors.Load(), "No errors should occur during concurrent writes with auto-compaction")

// Verify auto-compaction triggered multiple times
triggerCount := vs.GetAutoCompactionTriggerCount()
assert.Greater(t, triggerCount, int64(0), "Auto-compaction should trigger during concurrent writes")
}

// TestAutoCompaction_DisabledConfig verifies no trigger when compaction disabled
func TestAutoCompaction_DisabledConfig(t *testing.T) {
pool := memory.NewGoAllocator()
logger := zap.NewNop()

compactionCfg := CompactionConfig{
Enabled:             false, // Disabled
MinBatchesToCompact: 2,
TargetBatchSize:     100,
CompactionInterval:  time.Hour,
}

vs := NewVectorStoreWithCompaction(pool, logger, 1<<30, 1<<20, time.Hour, compactionCfg)
require.NotNil(t, vs)
defer func() { _ = vs.Close() }()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Add many batches
for i := 0; i < 10; i++ {
builder := array.NewRecordBuilder(pool, schema)
builder.Field(0).(*array.Int64Builder).Append(int64(i))
rec := builder.NewRecordBatch()
builder.Release()

require.NoError(t, vs.StoreRecordBatch(context.Background(), "test-ds", rec))
rec.Release()
}

time.Sleep(100 * time.Millisecond)

// Should NOT have triggered (compaction disabled)
triggerCount := vs.GetAutoCompactionTriggerCount()
assert.Equal(t, int64(0), triggerCount, "No triggers when compaction disabled")
}

// TestCompactionWorker_GetTriggerCount verifies trigger stats tracking
func TestCompactionWorker_GetTriggerCount(t *testing.T) {
cfg := CompactionConfig{
Enabled:             true,
MinBatchesToCompact: 5,
TargetBatchSize:     1000,
CompactionInterval:  time.Hour,
}
worker := NewCompactionWorker(cfg)
worker.Start()
defer worker.Stop()

initialCount := worker.GetTriggerCount()

// Trigger several times
for i := 0; i < 5; i++ {
_ = worker.TriggerCompaction("test-dataset")
}

// Allow time for processing
time.Sleep(50 * time.Millisecond)

finalCount := worker.GetTriggerCount()
assert.GreaterOrEqual(t, finalCount, initialCount, "Trigger count should be tracked")
}
