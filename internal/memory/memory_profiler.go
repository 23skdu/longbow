package memory

import (
	"runtime"
	"sync/atomic"
	"time"
)

type MemoryProfiler struct {
	startTime   time.Time
	allocations int64
	allocBytes  int64
	frees       int64
	freedBytes  int64
	peakUsage   int64
	lastGCTime  time.Time
	gcCount     uint32
}

var globalProfiler atomic.Pointer[MemoryProfiler]

func init() {
	globalProfiler.Store(&MemoryProfiler{
		startTime: time.Now(),
	})
}

func GetProfiler() *MemoryProfiler {
	return globalProfiler.Load()
}

func (mp *MemoryProfiler) RecordAllocation(size int64) {
	atomic.AddInt64(&mp.allocations, 1)
	atomic.AddInt64(&mp.allocBytes, size)

	current := atomic.LoadInt64(&mp.allocBytes) - atomic.LoadInt64(&mp.freedBytes)
	if current > mp.peakUsage {
		atomic.StoreInt64(&mp.peakUsage, current)
	}
}

func (mp *MemoryProfiler) RecordFree(size int64) {
	atomic.AddInt64(&mp.frees, 1)
	atomic.AddInt64(&mp.freedBytes, size)
}

func (mp *MemoryProfiler) GetStats() MemoryStats {
	return MemoryStats{
		Duration:         time.Since(mp.startTime),
		TotalAllocations: atomic.LoadInt64(&mp.allocations),
		TotalAllocBytes:  atomic.LoadInt64(&mp.allocBytes),
		TotalFrees:       atomic.LoadInt64(&mp.frees),
		TotalFreedBytes:  atomic.LoadInt64(&mp.freedBytes),
		PeakUsage:        atomic.LoadInt64(&mp.peakUsage),
		CurrentUsage:     atomic.LoadInt64(&mp.allocBytes) - atomic.LoadInt64(&mp.freedBytes),
		GCCount:          atomic.LoadUint32(&mp.gcCount),
		LastGC:           mp.lastGCTime,
	}
}

type MemoryStats struct {
	Duration         time.Duration
	TotalAllocations int64
	TotalAllocBytes  int64
	TotalFrees       int64
	TotalFreedBytes  int64
	PeakUsage        int64
	CurrentUsage     int64
	GCCount          uint32
	LastGC           time.Time
}

func (ms *MemoryStats) AllocationRate() float64 {
	if ms.Duration == 0 {
		return 0
	}
	return float64(ms.TotalAllocations) / ms.Duration.Seconds()
}

func (ms *MemoryStats) ThroughputMBps() float64 {
	if ms.Duration == 0 {
		return 0
	}
	return (float64(ms.TotalAllocBytes) / (1024 * 1024)) / ms.Duration.Seconds()
}

func (ms *MemoryStats) FragmentationRatio() float64 {
	if ms.TotalAllocBytes == 0 {
		return 0
	}
	return float64(ms.TotalAllocBytes-ms.TotalFreedBytes) / float64(ms.TotalAllocBytes)
}

type RuntimeMemoryStats struct {
	HeapAlloc          uint64
	HeapSys            uint64
	HeapIdle           uint64
	HeapInuse          uint64
	HeapReleased       uint64
	HeapObjects        uint64
	StackInuse         uint64
	StackSys           uint64
	MSpanInuse         uint64
	MSpanSys           uint64
	MCacheInuse        uint64
	MCacheSys          uint64
	BuckHashSys        uint64
	GCSys              uint64
	OtherSys           uint64
	GCCPUFraction      float64
	NumGC              uint32
	NumForcedGC        uint32
	GCCPUFractionTotal float64
	NumGoroutines      int
}

func GetRuntimeMemoryStats() RuntimeMemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	gcCPUFraction := 0.0
	if m.NumGC > 0 {
		gcCPUFraction = m.GCCPUFraction / float64(m.NumGC)
	}

	return RuntimeMemoryStats{
		HeapAlloc:          m.HeapAlloc,
		HeapSys:            m.HeapSys,
		HeapIdle:           m.HeapIdle,
		HeapInuse:          m.HeapInuse,
		HeapReleased:       m.HeapReleased,
		HeapObjects:        m.HeapObjects,
		StackInuse:         m.StackInuse,
		StackSys:           m.StackSys,
		MSpanInuse:         m.MSpanInuse,
		MSpanSys:           m.MSpanSys,
		MCacheInuse:        m.MCacheInuse,
		MCacheSys:          m.MCacheSys,
		BuckHashSys:        m.BuckHashSys,
		GCSys:              m.GCSys,
		OtherSys:           m.OtherSys,
		GCCPUFraction:      m.GCCPUFraction,
		NumGC:              m.NumGC,
		NumForcedGC:        m.NumForcedGC,
		GCCPUFractionTotal: gcCPUFraction,
		NumGoroutines:      runtime.NumGoroutine(),
	}
}

type MemoryAnalyzer struct {
	profiler *MemoryProfiler
}

func NewMemoryAnalyzer() *MemoryAnalyzer {
	return &MemoryAnalyzer{
		profiler: GetProfiler(),
	}
}

func (ma *MemoryAnalyzer) AnalyzeUsage() MemoryAnalysis {
	profilerStats := ma.profiler.GetStats()
	runtimeStats := GetRuntimeMemoryStats()

	analysis := MemoryAnalysis{
		ProfilerStats:   profilerStats,
		RuntimeStats:    runtimeStats,
		HeapUtilization: float64(runtimeStats.HeapInuse) / float64(runtimeStats.HeapSys) * 100,
		MemoryPressure:  ma.calculateMemoryPressure(&profilerStats, runtimeStats),
		Recommendations: ma.generateRecommendations(&profilerStats, runtimeStats),
	}

	return analysis
}

type MemoryAnalysis struct {
	ProfilerStats   MemoryStats
	RuntimeStats    RuntimeMemoryStats
	HeapUtilization float64
	MemoryPressure  MemoryPressure
	Recommendations []string
}

type MemoryPressure string

const (
	PressureLow      MemoryPressure = "low"
	PressureMedium   MemoryPressure = "medium"
	PressureHigh     MemoryPressure = "high"
	PressureCritical MemoryPressure = "critical"
)

func (ma *MemoryAnalyzer) calculateMemoryPressure(profilerStats *MemoryStats, runtimeStats RuntimeMemoryStats) MemoryPressure {
	heapUtilization := float64(runtimeStats.HeapInuse) / float64(runtimeStats.HeapSys)
	goroutineCount := float64(runtimeStats.NumGoroutines)
	gcFraction := runtimeStats.GCCPUFraction

	if heapUtilization > 0.9 || goroutineCount > 10000 || gcFraction > 0.5 {
		return PressureCritical
	}
	if heapUtilization > 0.7 || goroutineCount > 5000 || gcFraction > 0.3 {
		return PressureHigh
	}
	if heapUtilization > 0.5 || goroutineCount > 2000 || gcFraction > 0.2 {
		return PressureMedium
	}
	return PressureLow
}

func (ma *MemoryAnalyzer) generateRecommendations(profilerStats *MemoryStats, runtimeStats RuntimeMemoryStats) []string {
	var recs []string

	if profilerStats.FragmentationRatio() > 0.3 {
		recs = append(recs, "Consider implementing arena compaction to reduce fragmentation")
	}

	if float64(runtimeStats.HeapInuse)/float64(runtimeStats.HeapSys) > 0.8 {
		recs = append(recs, "High heap utilization - consider reducing allocation size or increasing heap size")
	}

	if runtimeStats.NumGoroutines > 5000 {
		recs = append(recs, "High goroutine count - check for goroutine leaks")
	}

	if runtimeStats.GCCPUFraction > 0.3 {
		recs = append(recs, "High GC CPU overhead - consider reducing allocation frequency")
	}

	if profilerStats.AllocationRate() > 1000000 {
		recs = append(recs, "Very high allocation rate - consider using object pooling")
	}

	if len(recs) == 0 {
		recs = append(recs, "Memory usage appears optimal")
	}

	return recs
}

func TrackAllocation(size int64) {
	if profiler := GetProfiler(); profiler != nil {
		profiler.RecordAllocation(size)
	}
}

func TrackFree(size int64) {
	if profiler := GetProfiler(); profiler != nil {
		profiler.RecordFree(size)
	}
}
