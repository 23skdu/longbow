package store

import (
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/prometheus/client_golang/prometheus"
dto "github.com/prometheus/client_model/go"
"github.com/stretchr/testify/require"

"github.com/23skdu/longbow/internal/metrics"
)

// getGaugeValue extracts current value from a Prometheus Gauge
func getGaugeValue(g prometheus.Gauge) float64 {
var m dto.Metric
if err := g.Write(&m); err != nil {
return 0
}
return m.GetGauge().GetValue()
}

// createAdaptiveTestBatch creates a minimal record batch for testing
func createAdaptiveTestBatch(t *testing.T, numRows int) arrow.RecordBatch {
t.Helper()
mem := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
}, nil)

builder := array.NewRecordBuilder(mem, schema)
defer builder.Release()

idBuilder := builder.Field(0).(*array.Int64Builder)
vecBuilder := builder.Field(1).(*array.ListBuilder)
vecValueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < numRows; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
vecValueBuilder.Append(float32(i) * 0.1)
}

rec := builder.NewRecordBatch()
return rec
}

func TestWalAdaptiveIntervalMs_EmittedOnCalculateInterval(t *testing.T) {
// Reset metric to known state
metrics.WalAdaptiveIntervalMs.Set(0)

cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)

// Calculate interval with a known write rate
writeRate := 500.0 // 500 writes/sec
interval := calc.CalculateInterval(writeRate)

// Metric should be updated with the calculated interval in milliseconds
metricValue := getGaugeValue(metrics.WalAdaptiveIntervalMs)

require.Greater(t, interval, time.Duration(0), "interval should be positive")
require.Greater(t, metricValue, 0.0, "WalAdaptiveIntervalMs should be emitted")
require.InDelta(t, float64(interval.Milliseconds()), metricValue, 1.0,
"metric should match calculated interval")
}

func TestWalWriteRatePerSecond_EmittedOnRecordWrite(t *testing.T) {
// Reset metric to known state
metrics.WalWriteRatePerSecond.Set(0)

tracker := NewWriteRateTracker(1 * time.Second)

// Record multiple writes to build up rate
for i := 0; i < 100; i++ {
tracker.RecordWrite()
}

// Force rate update by waiting slightly
time.Sleep(15 * time.Millisecond)
tracker.RecordWrite()

// Metric should be updated with write rate
metricValue := getGaugeValue(metrics.WalWriteRatePerSecond)

require.Greater(t, metricValue, 0.0, "WalWriteRatePerSecond should be emitted")
}

func TestWalBatcher_RecordWriteCalledOnWrite(t *testing.T) {
// This test verifies that WALBatcher.Write() calls rateTracker.RecordWrite()
// We check this indirectly via metrics emission
metrics.WalWriteRatePerSecond.Set(0)

tmpDir := t.TempDir()

cfg := &WALBatcherConfig{
FlushInterval: 50 * time.Millisecond,
MaxBatchSize:  100,
Adaptive:      NewAdaptiveWALConfig(),
}

w := NewWALBatcher(tmpDir, cfg)
defer func() { _ = w.Stop() }()

// Create a minimal record batch for testing
rec := createAdaptiveTestBatch(t, 10)
defer rec.Release()

// Write to WAL
err := w.Write(rec, "test-batch")
require.NoError(t, err)

// Wait for rate tracking to update
time.Sleep(50 * time.Millisecond)

// Record another write to trigger rate calculation
rec2 := createAdaptiveTestBatch(t, 10)
defer rec2.Release()
err = w.Write(rec2, "test-batch-2")
require.NoError(t, err)

// Metric should have been updated
metricValue := getGaugeValue(metrics.WalWriteRatePerSecond)
require.Greater(t, metricValue, 0.0,
"WalWriteRatePerSecond should be emitted after WALBatcher.Write()")
}

func TestGetCurrentInterval_EmitsMetrics(t *testing.T) {
// Reset metrics
metrics.WalAdaptiveIntervalMs.Set(0)
metrics.WalWriteRatePerSecond.Set(0)

tmpDir := t.TempDir()

cfg := &WALBatcherConfig{
FlushInterval: 50 * time.Millisecond,
MaxBatchSize:  100,
Adaptive:      NewAdaptiveWALConfig(),
}

w := NewWALBatcher(tmpDir, cfg)
defer func() { _ = w.Stop() }()

// Simulate writes to build rate
for i := 0; i < 5; i++ {
rec := createAdaptiveTestBatch(t, 10)
err := w.Write(rec, "test")
require.NoError(t, err)
rec.Release()
time.Sleep(5 * time.Millisecond)
}

// Call GetCurrentInterval
interval := w.GetCurrentInterval()

// Both metrics should be emitted
intervalMs := getGaugeValue(metrics.WalAdaptiveIntervalMs)
writeRate := getGaugeValue(metrics.WalWriteRatePerSecond)

require.Greater(t, interval, time.Duration(0), "interval should be positive")
require.Greater(t, intervalMs, 0.0, "WalAdaptiveIntervalMs should be emitted")
require.InDelta(t, float64(interval.Milliseconds()), intervalMs, 1.0)
require.GreaterOrEqual(t, writeRate, 0.0, "WalWriteRatePerSecond should be set")
}
