package profiling

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type MemoryLeakDetector struct {
	logger         zerolog.Logger
	mu             sync.RWMutex
	baseline       *MemSnapshot
	snapshots      []*MemSnapshot
	thresholdBytes int64
	checkInterval  time.Duration
	alertCallback  func(*LeakReport)
	running        bool
	stopChan       chan struct{}
}

type MemSnapshot struct {
	Timestamp     time.Time
	HeapAlloc     uint64
	HeapObjects   uint64
	StackInUse    uint64
	MSpanInUse    uint64
	MCacheInUse   uint64
	GCCPUFraction float64
	NumGoroutine  int
}

type LeakReport struct {
	Timestamp      time.Time
	Snapshots      []*MemSnapshot
	GrowthBytes    int64
	GrowthObjects  int64
	GrowthPercent  float64
	TopAllocations []string
	GoroutineDiff  int
}

func NewMemoryLeakDetector(logger zerolog.Logger, thresholdMB int, interval time.Duration) *MemoryLeakDetector {
	return &MemoryLeakDetector{
		logger:         logger.With().Str("component", "memory_leak_detector").Logger(),
		thresholdBytes: int64(thresholdMB) * 1024 * 1024,
		checkInterval:  interval,
		snapshots:      make([]*MemSnapshot, 0),
		stopChan:       make(chan struct{}),
	}
}

func (mld *MemoryLeakDetector) SetAlertCallback(callback func(*LeakReport)) {
	mld.alertCallback = callback
}

func (mld *MemoryLeakDetector) CaptureBaseline() {
	mld.mu.Lock()
	defer mld.mu.Unlock()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	mld.baseline = mld.captureSnapshot()
	mld.snapshots = append(mld.snapshots, mld.baseline)

	mld.logger.Info().
		Uint64("heap_alloc", mld.baseline.HeapAlloc).
		Uint64("heap_objects", mld.baseline.HeapObjects).
		Msg("Baseline memory snapshot captured")
}

func (mld *MemoryLeakDetector) captureSnapshot() *MemSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return &MemSnapshot{
		Timestamp:     time.Now(),
		HeapAlloc:     mem.HeapAlloc,
		HeapObjects:   mem.HeapObjects,
		StackInUse:    mem.StackInuse,
		MSpanInUse:    mem.MSpanInuse,
		MCacheInUse:   mem.MCacheInuse,
		GCCPUFraction: mem.GCCPUFraction,
		NumGoroutine:  runtime.NumGoroutine(),
	}
}

func (mld *MemoryLeakDetector) Start() {
	mld.mu.Lock()
	if mld.running {
		mld.mu.Unlock()
		return
	}
	mld.running = true
	mld.mu.Unlock()

	if mld.baseline == nil {
		mld.CaptureBaseline()
	}

	go mld.monitorLoop()

	mld.logger.Info().Msg("Memory leak detector started")
}

func (mld *MemoryLeakDetector) Stop() {
	mld.mu.Lock()
	defer mld.mu.Unlock()

	if !mld.running {
		return
	}

	close(mld.stopChan)
	mld.running = false

	mld.logger.Info().Msg("Memory leak detector stopped")
}

func (mld *MemoryLeakDetector) monitorLoop() {
	ticker := time.NewTicker(mld.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mld.stopChan:
			return
		case <-ticker.C:
			mld.checkForLeaks()
		}
	}
}

func (mld *MemoryLeakDetector) checkForLeaks() {
	mld.mu.Lock()
	snapshot := mld.captureSnapshot()
	mld.snapshots = append(mld.snapshots, snapshot)
	mld.mu.Unlock()

	mld.mu.RLock()
	baseline := mld.baseline
	mld.mu.RUnlock()

	if baseline == nil {
		return
	}

	growthBytes := int64(snapshot.HeapAlloc) - int64(baseline.HeapAlloc) // #nosec G115
	growthObjects := int64(snapshot.HeapObjects) - int64(baseline.HeapObjects) // #nosec G115

	mld.logger.Debug().
		Int64("growth_bytes", growthBytes).
		Int64("growth_objects", growthObjects).
		Uint64("current_heap", snapshot.HeapAlloc).
		Msg("Memory check")

	if growthBytes > mld.thresholdBytes {
		report := mld.generateLeakReport(baseline, snapshot, growthBytes, growthObjects)

		mld.logger.Warn().
			Int64("growth_bytes", growthBytes).
			Int64("growth_objects", growthObjects).
			Msg("Memory leak detected!")

		if mld.alertCallback != nil {
			mld.alertCallback(report)
		}
	}
}

func (mld *MemoryLeakDetector) generateLeakReport(baseline, snapshot *MemSnapshot, growthBytes, growthObjects int64) *LeakReport {
	var topAllocs []string

	_ = pprof.Lookup("heap").WriteTo(io.Discard, 0)

	growthPercent := 0.0
	if baseline.HeapAlloc > 0 {
		growthPercent = float64(growthBytes) / float64(baseline.HeapAlloc) * 100
	}

	goroutineDiff := snapshot.NumGoroutine - baseline.NumGoroutine

	return &LeakReport{
		Timestamp:      time.Now(),
		Snapshots:      []*MemSnapshot{baseline, snapshot},
		GrowthBytes:    growthBytes,
		GrowthObjects:  growthObjects,
		GrowthPercent:  growthPercent,
		TopAllocations: topAllocs,
		GoroutineDiff:  goroutineDiff,
	}
}

func (mld *MemoryLeakDetector) GetCurrentStats() (heapAlloc, heapObjects uint64, goroutines int) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem.HeapAlloc, mem.HeapObjects, runtime.NumGoroutine()
}

func (mld *MemoryLeakDetector) WriteHeapProfile(w io.Writer) error {
	return pprof.WriteHeapProfile(w)
}

func (mld *MemoryLeakDetector) WriteGoroutineProfile(w io.Writer) error {
	return pprof.Lookup("goroutine").WriteTo(w, 2)
}

func (mld *MemoryLeakDetector) WriteThreadCreateProfile(w io.Writer) error {
	return pprof.Lookup("threadcreate").WriteTo(w, 2)
}

func (mld *MemoryLeakDetector) WriteBlockProfile(w io.Writer) error {
	runtime.SetBlockProfileRate(100)
	return pprof.Lookup("block").WriteTo(w, 2)
}

func (mld *MemoryLeakDetector) WriteMutexProfile(w io.Writer) error {
	runtime.SetMutexProfileFraction(100)
	return pprof.Lookup("mutex").WriteTo(w, 2)
}

func (mld *MemoryLeakDetector) GetSnapshots() []*MemSnapshot {
	mld.mu.RLock()
	defer mld.mu.RUnlock()

	result := make([]*MemSnapshot, len(mld.snapshots))
	copy(result, mld.snapshots)
	return result
}

type LeakDetectorConfig struct {
	ThresholdMB      int
	CheckInterval    time.Duration
	EnableCPUPprof   bool
	EnableMemPprof   bool
	EnableGoroutine  bool
	EnableBlock      bool
	EnableMutex      bool
	ProfileOutputDir string
}

func DefaultLeakDetectorConfig() LeakDetectorConfig {
	return LeakDetectorConfig{
		ThresholdMB:      10,
		CheckInterval:    30 * time.Second,
		EnableMemPprof:   true,
		EnableGoroutine:  true,
		ProfileOutputDir: "./profiles",
	}
}

type LeakDetectorManager struct {
	logger   zerolog.Logger
	detector *MemoryLeakDetector
	config   LeakDetectorConfig
}

func NewLeakDetectorManager(logger zerolog.Logger, config LeakDetectorConfig) *LeakDetectorManager {
	return &LeakDetectorManager{
		logger:   logger,
		config:   config,
		detector: NewMemoryLeakDetector(logger, config.ThresholdMB, config.CheckInterval),
	}
}

func (ldm *LeakDetectorManager) Start() {
	_ = os.MkdirAll(filepath.Clean(ldm.config.ProfileOutputDir), 0700) // #nosec G301

	ldm.detector.SetAlertCallback(func(report *LeakReport) {
		ldm.logger.Warn().
			Int64("growth_bytes", report.GrowthBytes).
			Float64("growth_percent", report.GrowthPercent).
			Int("goroutine_diff", report.GoroutineDiff).
			Msg("LEAK ALERT: Memory growth exceeds threshold")
	})

	ldm.detector.Start()
}

func (ldm *LeakDetectorManager) Stop() {
	ldm.detector.Stop()
}

func (ldm *LeakDetectorManager) CaptureProfiles() error {
	timestamp := time.Now().Format("20060102_150405")
	dir := ldm.config.ProfileOutputDir

	if ldm.config.EnableMemPprof {
		heapFile, err := os.Create(fmt.Sprintf("%s/heap_%s.prof", dir, timestamp))
		if err != nil {
			return err
		}
		defer heapFile.Close()
		if err := ldm.detector.WriteHeapProfile(heapFile); err != nil {
			return err
		}
	}

	if ldm.config.EnableGoroutine {
		goroutineFile, err := os.Create(fmt.Sprintf("%s/goroutine_%s.prof", dir, timestamp))
		if err != nil {
			return err
		}
		defer goroutineFile.Close()
		if err := ldm.detector.WriteGoroutineProfile(goroutineFile); err != nil {
			return err
		}
	}

	if ldm.config.EnableBlock {
		blockFile, err := os.Create(fmt.Sprintf("%s/block_%s.prof", dir, timestamp))
		if err != nil {
			return err
		}
		defer blockFile.Close()
		if err := ldm.detector.WriteBlockProfile(blockFile); err != nil {
			return err
		}
	}

	if ldm.config.EnableMutex {
		mutexFile, err := os.Create(fmt.Sprintf("%s/mutex_%s.prof", dir, timestamp))
		if err != nil {
			return err
		}
		defer mutexFile.Close()
		if err := ldm.detector.WriteMutexProfile(mutexFile); err != nil {
			return err
		}
	}

	return nil
}

func (ldm *LeakDetectorManager) GetStats() (heapAlloc, heapObjects uint64, goroutines int) {
	return ldm.detector.GetCurrentStats()
}
