package memory

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Profiler tracks memory allocation and free events.
type Profiler struct {
	startTime   time.Time
	allocations int64
	allocBytes  int64
	frees       int64
	freedBytes  int64
	peakUsage   int64
	lastGCTime  time.Time
	gcCount     uint32
}

var globalProfiler atomic.Pointer[Profiler]

func init() {
	globalProfiler.Store(&Profiler{
		startTime: time.Now(),
	})
}

// GetProfiler returns the global memory profiler.
func GetProfiler() *Profiler {
	return globalProfiler.Load()
}

// RecordAllocation records a memory allocation of the given size.
func (mp *Profiler) RecordAllocation(size int64) {
	atomic.AddInt64(&mp.allocations, 1)
	atomic.AddInt64(&mp.allocBytes, size)

	current := atomic.LoadInt64(&mp.allocBytes) - atomic.LoadInt64(&mp.freedBytes)
	if current > mp.peakUsage {
		atomic.StoreInt64(&mp.peakUsage, current)
	}
}

// RecordFree records a memory free of the given size.
func (mp *Profiler) RecordFree(size int64) {
	atomic.AddInt64(&mp.frees, 1)
	atomic.AddInt64(&mp.freedBytes, size)
}

// GetStats returns the current memory statistics.
func (mp *Profiler) GetStats() Stats {
	return Stats{
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

// Stats contains memory usage statistics.
type Stats struct {
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

// AllocationRate returns the number of allocations per second.
func (ms *Stats) AllocationRate() float64 {
	if ms.Duration == 0 {
		return 0
	}
	return float64(ms.TotalAllocations) / ms.Duration.Seconds()
}

// ThroughputMBps returns the memory throughput in MB per second.
func (ms *Stats) ThroughputMBps() float64 {
	if ms.Duration == 0 {
		return 0
	}
	return (float64(ms.TotalAllocBytes) / (1024 * 1024)) / ms.Duration.Seconds()
}

// FragmentationRatio returns the ratio of allocated memory that has been freed.
func (ms *Stats) FragmentationRatio() float64 {
	if ms.TotalAllocBytes == 0 {
		return 0
	}
	return float64(ms.TotalAllocBytes-ms.TotalFreedBytes) / float64(ms.TotalAllocBytes)
}

// RuntimeStats contains memory statistics from the Go runtime.
type RuntimeStats struct {
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

// GetRuntimeStats returns current Go runtime memory statistics.
func GetRuntimeStats() RuntimeStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	gcCPUFraction := 0.0
	if m.NumGC > 0 {
		gcCPUFraction = m.GCCPUFraction / float64(m.NumGC)
	}

	return RuntimeStats{
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

// Analyzer analyzes memory usage and provides recommendations.
type Analyzer struct {
	profiler *Profiler
}

// NewAnalyzer creates a new memory analyzer.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		profiler: GetProfiler(),
	}
}

// AnalyzeUsage analyzes the current memory usage.
func (ma *Analyzer) AnalyzeUsage() Analysis {
	profilerStats := ma.profiler.GetStats()
	runtimeStats := GetRuntimeStats()

	analysis := Analysis{
		ProfilerStats:   profilerStats,
		RuntimeStats:    runtimeStats,
		HeapUtilization: float64(runtimeStats.HeapInuse) / float64(runtimeStats.HeapSys) * 100,
		Pressure:        ma.calculatePressure(runtimeStats),
		Recommendations: ma.generateRecommendations(&profilerStats, runtimeStats),
	}

	return analysis
}

// Analysis contains the results of a memory usage analysis.
type Analysis struct {
	ProfilerStats   Stats
	RuntimeStats    RuntimeStats
	HeapUtilization float64
	Pressure        Pressure
	Recommendations []string
}

// Pressure represents the level of memory pressure.
type Pressure string

const (
	// PressureLow indicates low memory pressure.
	PressureLow Pressure = "low"
	// PressureMedium indicates medium memory pressure.
	PressureMedium Pressure = "medium"
	// PressureHigh indicates high memory pressure.
	PressureHigh Pressure = "high"
	// PressureCritical indicates critical memory pressure.
	PressureCritical Pressure = "critical"
)

func (ma *Analyzer) calculatePressure(runtimeStats RuntimeStats) Pressure {
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

func (ma *Analyzer) generateRecommendations(profilerStats *Stats, runtimeStats RuntimeStats) []string {
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

// TrackAllocation records a memory allocation in the global profiler.
func TrackAllocation(size int64) {
	if profiler := GetProfiler(); profiler != nil {
		profiler.RecordAllocation(size)
	}
}

// TrackFree records a memory free in the global profiler.
func TrackFree(size int64) {
	if profiler := GetProfiler(); profiler != nil {
		profiler.RecordFree(size)
	}
}
