package memory

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
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

	IsAggressive bool
	arenas       []*SlabArena
	mu           sync.RWMutex

	reader MemStatsReader
	logger *zerolog.Logger

	// State to avoid thrashing
	currentGOGC     int
	lastUtilization atomic.Uint64 // 0..1000 representing 0.0..1.0 ratio
}

// NewGCTuner creates a tuner. limitBytes should be close to container memory limit.
func NewGCTuner(limitBytes int64, highGOGC, lowGOGC int, logger *zerolog.Logger) *GCTuner {
	if highGOGC <= 0 {
		highGOGC = 80
	}
	if lowGOGC <= 0 {
		lowGOGC = 10
	}
	if lowGOGC > highGOGC {
		lowGOGC = highGOGC
	}

	tuner := &GCTuner{
		limitBytes:  limitBytes,
		highGOGC:    highGOGC,
		lowGOGC:     lowGOGC,
		reader:      &defaultMemStatsReader{},
		logger:      logger,
		currentGOGC: debug.SetGCPercent(-1), // Get current GOGC without changing it
	}

	if tuner.logger != nil {
		tuner.logger.Info().
			Int64("limitBytes", limitBytes).
			Int("highGOGC", highGOGC).
			Int("lowGOGC", lowGOGC).
			Int("initialGOGC", tuner.currentGOGC).
			Msg("GCTuner initialized")
	}

	return tuner
}

// RegisterArena adds an arena to be tracked by the tuner.
func (t *GCTuner) RegisterArena(a *SlabArena) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.arenas = append(t.arenas, a)
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

	t.mu.RLock()
	aggressive := t.IsAggressive
	unusedArenaMemory := int64(0)
	if aggressive {
		// Use both registered and global arenas
		arenas := t.arenas
		global := GetGlobalArenas()

		seen := make(map[*SlabArena]bool)
		for _, a := range arenas {
			seen[a] = true
			stats := a.Stats()
			unused := stats.TotalCapacity - stats.UsedBytes
			if unused > 0 {
				unusedArenaMemory += unused
			}
		}
		for _, a := range global {
			if seen[a] {
				continue
			}
			stats := a.Stats()
			unused := stats.TotalCapacity - stats.UsedBytes
			if unused > 0 {
				unusedArenaMemory += unused
			}
		}
	}
	t.mu.RUnlock()

	// Effective Usage = Total Heap Inuse - Memory reserved by arenas but not actually used.
	// We want to be aggressive when ACTUAL data (active nodes + overhead) approaches limit,
	// but NOT when just reserved slabs approach limit (those can be freed/compacted).
	effectiveInUse := int64(heapInUse) - unusedArenaMemory
	if effectiveInUse < 0 {
		effectiveInUse = 0
	}

	ratio := float64(effectiveInUse) / float64(t.limitBytes)
	if aggressive && ratio > 0.7 {
		if t.logger != nil {
			t.logger.Warn().Float64("ratio", ratio).Int64("effective", effectiveInUse).Msg("High effective heap utilization")
		}
	}
	metrics.GCTunerHeapUtilization.Set(ratio)
	t.lastUtilization.Store(uint64(ratio * 1000))

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
		// In aggressive mode we might want smaller threshold? Let's stick to 2.
		threshold := 5
		if aggressive {
			threshold = 2
		}

		diff := targetGOGC - t.currentGOGC
		if diff < -threshold || diff > threshold {
			debug.SetGCPercent(targetGOGC)
			t.currentGOGC = targetGOGC
			metrics.GCTunerTargetGOGC.Set(float64(targetGOGC))
		}
	}
	t.mu.Unlock()
}

// GetUtilizationRatio returns the last measured memory utilization ratio (0.0 to 1.0+).
func (t *GCTuner) GetUtilizationRatio() float64 {
	return float64(t.lastUtilization.Load()) / 1000.0
}
