package memory

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/rs/zerolog"
)

// MemStatsReader interfaces runtime.ReadMemStats for testing
type MemStatsReader interface {
	ReadMemStats(m *runtime.MemStats)
}

type defaultMemStatsReader struct{}

func (d *defaultMemStatsReader) ReadMemStats(m *runtime.MemStats) {
	runtime.ReadMemStats(m)
}

// GCTuner adjusts GOGC dynamically based on memory usage.
type GCTuner struct {
	limitBytes int64
	highGOGC   int
	lowGOGC    int

	reader MemStatsReader
	logger *zerolog.Logger

	// State to avoid thrashing
	currentGOGC int
	mu          sync.Mutex
}

// NewGCTuner creates a tuner. limitBytes should be close to container memory limit.
func NewGCTuner(limitBytes int64, highGOGC, lowGOGC int, logger *zerolog.Logger) *GCTuner {
	if highGOGC <= 0 {
		highGOGC = 100
	}
	if lowGOGC <= 0 {
		lowGOGC = 10
	}
	if lowGOGC > highGOGC {
		lowGOGC = highGOGC
	}

	return &GCTuner{
		limitBytes:  limitBytes,
		highGOGC:    highGOGC,
		lowGOGC:     lowGOGC,
		reader:      &defaultMemStatsReader{},
		logger:      logger,
		currentGOGC: 100,
	}
}

// Start runs the tuner loop until context is canceled.
func (t *GCTuner) Start(ctx context.Context, interval time.Duration) {
	// Set hard limit first
	if t.limitBytes > 0 {
		debug.SetMemoryLimit(t.limitBytes)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var m runtime.MemStats
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reader.ReadMemStats(&m)
			t.tune(m.HeapInuse)
		}
	}
}

func (t *GCTuner) tune(heapInUse uint64) {
	if t.limitBytes <= 0 {
		return
	}

	ratio := float64(heapInUse) / float64(t.limitBytes)
	metrics.GCTunerHeapUtilization.Set(ratio)

	var targetGOGC int

	// Simple Logic:
	// < 50% usage -> HighGOGC (Relaxed GC)
	// > 90% usage -> LowGOGC (Aggressive GC)
	// In-between  -> Linear interpolation
	switch {
	case ratio < 0.5:
		targetGOGC = t.highGOGC
	case ratio > 0.9:
		targetGOGC = t.lowGOGC
	default:
		// Interpolate: 0.5 -> High, 0.9 -> Low
		// Slope = (Low - High) / (0.9 - 0.5)
		slope := float64(t.lowGOGC-t.highGOGC) / 0.4
		targetGOGC = t.highGOGC + int(slope*(ratio-0.5))
	}

	// Clamp
	if targetGOGC < t.lowGOGC {
		targetGOGC = t.lowGOGC
	}
	if targetGOGC > t.highGOGC {
		targetGOGC = t.highGOGC
	}

	t.mu.Lock()
	if targetGOGC != t.currentGOGC {
		// Only set if changed significantly (e.g. > 5 difference) to avoid noise
		diff := targetGOGC - t.currentGOGC
		if diff < -5 || diff > 5 {
			debug.SetGCPercent(targetGOGC)
			t.currentGOGC = targetGOGC
			metrics.GCTunerTargetGOGC.Set(float64(targetGOGC))
		}
	}
	t.mu.Unlock()
}
