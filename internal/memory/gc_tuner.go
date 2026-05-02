package memory

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/gpu/types"
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

// GCTuner adjusts GOGC dynamically based on memory usage and GPU utilization.
type GCTuner struct {
	limitBytes int64
	highGOGC   int
	lowGOGC    int

	IsAggressive       bool
	EnableGPUTuning    bool
	GPUUtilizationHigh float32 // Threshold for "high" GPU utilization (0-100)
	GPUUtilizationLow  float32 // Threshold for "low" GPU utilization (0-100)
	arenas             []*SlabArena
	mu                 sync.RWMutex

	reader MemStatsReader
	logger *zerolog.Logger

	// State to avoid thrashing
	currentGOGC        int
	lastUtilization    atomic.Uint64 // 0..1000 representing 0.0..1.0 ratio
	lastGPUUtilization atomic.Uint32 // 0..1000 representing 0.0..100.0%

	// Allocation rate tracking for burst mode
	lastTotalAlloc uint64
	lastAllocTime  time.Time
	allocRate      atomic.Uint64 // Bytes per second
	isBursting     atomic.Bool   // True if currently in burst mode
}

// NewGCTuner creates a tuner. limitBytes should be close to container memory limit.
// GPU tuning is enabled by default when a GPU is available.
func NewGCTuner(limitBytes int64, highGOGC, lowGOGC int, logger *zerolog.Logger) *GCTuner {
	if highGOGC <= 0 {
		highGOGC = 150
	}
	if lowGOGC <= 0 {
		lowGOGC = 100
	}
	if lowGOGC > highGOGC {
		lowGOGC = highGOGC
	}

	tuner := &GCTuner{
		limitBytes:         limitBytes,
		highGOGC:           highGOGC,
		lowGOGC:            lowGOGC,
		reader:             &defaultMemStatsReader{},
		logger:             logger,
		currentGOGC:        debug.SetGCPercent(-1),
		EnableGPUTuning:    types.GetDeviceCount() > 0,
		GPUUtilizationHigh: 60.0,
		GPUUtilizationLow:  20.0,
	}

	if tuner.logger != nil {
		tuner.logger.Info().
			Int64("limitBytes", limitBytes).
			Int("highGOGC", highGOGC).
			Int("lowGOGC", lowGOGC).
			Int("initialGOGC", tuner.currentGOGC).
			Bool("gpuTuning", tuner.EnableGPUTuning).
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

	// Use 500ms interval for faster response
	if interval == 0 {
		interval = 500 * time.Millisecond
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
			t.tune(&m, t.IsAggressive)
		}
	}
}

func (t *GCTuner) tune(m *runtime.MemStats, aggressive bool) {
	if t.limitBytes <= 0 {
		return
	}

	// Calculate allocation rate
	now := time.Now()
	if !t.lastAllocTime.IsZero() {
		duration := now.Sub(t.lastAllocTime).Seconds()
		if duration > 0 {
			diff := m.TotalAlloc - t.lastTotalAlloc
			rate := uint64(float64(diff) / duration)
			t.allocRate.Store(rate)
		}
	}
	t.lastTotalAlloc = m.TotalAlloc
	t.lastAllocTime = now

	heapInUse := m.HeapInuse


	t.mu.RLock()
	totalArenaCapacity := int64(0)
	unusedArenaMemory := int64(0)
	if aggressive {
		// Use both registered and global arenas
		arenas := t.arenas
		global := GetGlobalArenas()

		seen := make(map[*SlabArena]bool)
		for _, a := range arenas {
			seen[a] = true
			stats := a.Stats()
			totalArenaCapacity += stats.TotalCapacity
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
			totalArenaCapacity += stats.TotalCapacity
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
	effectiveInUse := int64(heapInUse) - unusedArenaMemory // #nosec G115
	if effectiveInUse < 0 {
		effectiveInUse = 0
	}

	ratio := float64(effectiveInUse) / float64(t.limitBytes)
	
	// Burst mode detection: if alloc rate > 512MB/s and we are using > 60% of heap,
	// or if rate > 1GB/s, we are likely in a heavy ingestion phase.
	allocRate := t.allocRate.Load()
	isBurst := (allocRate > 512*1024*1024 && ratio > 0.6) || (allocRate > 1024*1024*1024)
	t.isBursting.Store(isBurst)

	// Get GPU utilization if enabled
	var gpuUtilization float32
	if t.EnableGPUTuning {
		if util, err := types.GetGlobalGPUUtilization(); err == nil {
			gpuUtilization = util
			t.lastGPUUtilization.Store(uint32(util * 10)) // Store as 0-1000
		}
	}

	// Arena-aware tuning: if arena usage >70% of heap, set GOGC=50
	arenaRatio := float64(totalArenaCapacity) / float64(heapInUse)
	var targetGOGC int

	if aggressive && arenaRatio > 0.7 {
		// Arena-dominated heap: be very aggressive
		targetGOGC = t.lowGOGC
		if t.logger != nil {
			t.logger.Warn().
				Float64("arenaRatio", arenaRatio).
				Int64("totalArenaCapacity", totalArenaCapacity).
				Uint64("heapInUse", heapInUse).
				Int("targetGOGC", targetGOGC).
				Msg("Arena-dominated heap detected, setting aggressive GOGC")
		}
	} else if t.EnableGPUTuning && gpuUtilization >= t.GPUUtilizationHigh {
		// GPU is highly utilized - reduce GOGC to reduce CPU overhead
		// Since GPU is doing the heavy lifting, CPU is less critical
		targetGOGC = t.lowGOGC
		if t.logger != nil {
			t.logger.Debug().
				Float32("gpuUtilization", gpuUtilization).
				Int("targetGOGC", targetGOGC).
				Msg("High GPU utilization detected, reducing GOGC")
		}
	} else if t.EnableGPUTuning && gpuUtilization <= t.GPUUtilizationLow {
		// GPU is underutilized - we can afford more GC overhead
		// CPU is doing more work, so be less aggressive with GC
		targetGOGC = t.highGOGC
	} else {
		// Standard logic based on heap utilization
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
	}

	// Relax for burst mode if we have significant headroom (< 70% utilization)
	if isBurst && ratio < 0.7 {
		targetGOGC += 50
	}

	if aggressive && ratio > 0.7 {
		if t.logger != nil {
			t.logger.Warn().Float64("ratio", ratio).Int64("effective", effectiveInUse).Msg("High effective heap utilization")
		}
	}
	metrics.GCTunerHeapUtilization.Set(ratio)
	t.lastUtilization.Store(uint64(ratio * 1000))

	// Update GPU utilization metric
	if t.EnableGPUTuning {
		metrics.GCTunerGPUUtilization.Set(float64(gpuUtilization))
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
		threshold := 10
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

// IsBursting returns true if the tuner has detected a heavy ingestion burst.
func (t *GCTuner) IsBursting() bool {
	return t.isBursting.Load()
}
