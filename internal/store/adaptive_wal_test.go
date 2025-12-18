package store

import (
"testing"
"time"

"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

func TestNewAdaptiveWALConfig(t *testing.T) {
cfg := NewAdaptiveWALConfig()
require.NotNil(t, cfg)
assert.Equal(t, 1*time.Millisecond, cfg.MinInterval)
assert.Equal(t, 100*time.Millisecond, cfg.MaxInterval)
assert.Equal(t, 5*time.Millisecond, cfg.TargetLatency)
assert.True(t, cfg.Enabled)
}

func TestAdaptiveWALConfig_Validate(t *testing.T) {
tests := []struct {
name    string
cfg     AdaptiveWALConfig
wantErr bool
}{
{"valid config", AdaptiveWALConfig{MinInterval: 1 * time.Millisecond, MaxInterval: 100 * time.Millisecond, TargetLatency: 5 * time.Millisecond, Enabled: true}, false},
{"min greater than max", AdaptiveWALConfig{MinInterval: 100 * time.Millisecond, MaxInterval: 1 * time.Millisecond, TargetLatency: 5 * time.Millisecond, Enabled: true}, true},
{"zero min interval", AdaptiveWALConfig{MinInterval: 0, MaxInterval: 100 * time.Millisecond, TargetLatency: 5 * time.Millisecond, Enabled: true}, true},
{"zero target latency", AdaptiveWALConfig{MinInterval: 1 * time.Millisecond, MaxInterval: 100 * time.Millisecond, TargetLatency: 0, Enabled: true}, true},
{"disabled skips validation", AdaptiveWALConfig{Enabled: false}, false},
}
for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
err := tt.cfg.Validate()
if tt.wantErr {
assert.Error(t, err)
} else {
assert.NoError(t, err)
}
})
}
}

func TestAdaptiveIntervalCalculator_HighLoad(t *testing.T) {
cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)
interval := calc.CalculateInterval(1000)
assert.LessOrEqual(t, interval, 10*time.Millisecond)
assert.GreaterOrEqual(t, interval, cfg.MinInterval)
}

func TestAdaptiveIntervalCalculator_LowLoad(t *testing.T) {
cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)
interval := calc.CalculateInterval(1)
assert.GreaterOrEqual(t, interval, 50*time.Millisecond)
assert.LessOrEqual(t, interval, cfg.MaxInterval)
}

func TestAdaptiveIntervalCalculator_ZeroLoad(t *testing.T) {
cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)
interval := calc.CalculateInterval(0)
assert.Equal(t, cfg.MaxInterval, interval)
}

func TestAdaptiveIntervalCalculator_BoundsRespected(t *testing.T) {
cfg := AdaptiveWALConfig{MinInterval: 5 * time.Millisecond, MaxInterval: 50 * time.Millisecond, TargetLatency: 10 * time.Millisecond, Enabled: true}
calc := NewAdaptiveIntervalCalculator(cfg)
for _, rate := range []float64{0, 1, 10, 100, 1000, 10000} {
interval := calc.CalculateInterval(rate)
assert.GreaterOrEqual(t, interval, cfg.MinInterval)
assert.LessOrEqual(t, interval, cfg.MaxInterval)
}
}

func TestAdaptiveIntervalCalculator_SmoothTransition(t *testing.T) {
cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)
var prevInterval time.Duration
rates := []float64{10, 50, 100, 200, 500, 1000}
for i, rate := range rates {
interval := calc.CalculateInterval(rate)
if i > 0 {
assert.LessOrEqual(t, interval, prevInterval)
}
prevInterval = interval
}
}

func TestWriteRateTracker_RecordsWrites(t *testing.T) {
tracker := NewWriteRateTracker(1 * time.Second)
for i := 0; i < 100; i++ {
tracker.RecordWrite()
}
rate := tracker.GetRate()
assert.GreaterOrEqual(t, rate, float64(0))
}

func TestWriteRateTracker_RateDecays(t *testing.T) {
tracker := NewWriteRateTracker(100 * time.Millisecond)
for i := 0; i < 50; i++ {
tracker.RecordWrite()
}
rateInitial := tracker.GetRate()
time.Sleep(150 * time.Millisecond)
rateLater := tracker.GetRate()
assert.LessOrEqual(t, rateLater, rateInitial)
}

func TestWALBatcherConfig_WithAdaptive(t *testing.T) {
cfg := WALBatcherConfig{
FlushInterval: 10 * time.Millisecond,
MaxBatchSize:  100,
Adaptive:      NewAdaptiveWALConfig(),
}
assert.True(t, cfg.Adaptive.Enabled)
assert.Equal(t, 1*time.Millisecond, cfg.Adaptive.MinInterval)
}

func TestWALBatcher_AdaptiveMode(t *testing.T) {
tmpDir := t.TempDir()
cfg := WALBatcherConfig{
FlushInterval: 10 * time.Millisecond,
MaxBatchSize:  100,
Adaptive:      NewAdaptiveWALConfig(),
}
batcher := NewWALBatcher(tmpDir, &cfg)
require.NotNil(t, batcher)
assert.True(t, batcher.IsAdaptiveEnabled())
interval := batcher.GetCurrentInterval()
assert.GreaterOrEqual(t, interval, cfg.Adaptive.MinInterval)
assert.LessOrEqual(t, interval, cfg.Adaptive.MaxInterval)
}

func TestWALBatcher_AdaptiveDisabled(t *testing.T) {
tmpDir := t.TempDir()
cfg := WALBatcherConfig{
FlushInterval: 10 * time.Millisecond,
MaxBatchSize:  100,
Adaptive:      AdaptiveWALConfig{Enabled: false},
}
batcher := NewWALBatcher(tmpDir, &cfg)
assert.False(t, batcher.IsAdaptiveEnabled())
assert.Equal(t, cfg.FlushInterval, batcher.GetCurrentInterval())
}

func BenchmarkAdaptiveIntervalCalculation(b *testing.B) {
cfg := NewAdaptiveWALConfig()
calc := NewAdaptiveIntervalCalculator(cfg)
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = calc.CalculateInterval(float64(i % 10000))
}
}

func BenchmarkWriteRateTracking(b *testing.B) {
tracker := NewWriteRateTracker(1 * time.Second)
b.ResetTimer()
for i := 0; i < b.N; i++ {
tracker.RecordWrite()
if i%100 == 0 {
_ = tracker.GetRate()
}
}
}
