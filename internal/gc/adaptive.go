package gc

import (
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// AdaptiveGCConfig configures the adaptive GC controller
type AdaptiveGCConfig struct {
	Enabled        bool          // Enable adaptive GC
	MinGOGC        int           // Minimum GOGC value (default: 50)
	MaxGOGC        int           // Maximum GOGC value (default: 200)
	AdjustInterval time.Duration // How often to adjust GOGC (default: 1s)
}

// DefaultAdaptiveGCConfig returns sensible defaults
func DefaultAdaptiveGCConfig() AdaptiveGCConfig {
	return AdaptiveGCConfig{
		Enabled:        false, // Opt-in
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 1 * time.Second,
	}
}

// gcStats holds statistics for GC decision making
type gcStats struct {
	allocationRate int64   // Bytes allocated per second
	memoryPressure float64 // Memory pressure ratio (0-1)
}

// AdaptiveGCController dynamically adjusts GOGC based on allocation rate and memory pressure
type AdaptiveGCController struct {
	config AdaptiveGCConfig

	// State
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Previous stats for rate calculation
	lastAlloc uint64
	lastTime  time.Time
	lastGOGC  int
	mu        sync.Mutex
}

// NewAdaptiveGCController creates a new adaptive GC controller
func NewAdaptiveGCController(config AdaptiveGCConfig) *AdaptiveGCController {
	// Validate and fix config
	if config.MinGOGC <= 0 {
		config.MinGOGC = 50
	}
	if config.MaxGOGC <= 0 {
		config.MaxGOGC = 200
	}
	if config.MinGOGC > config.MaxGOGC {
		config.MinGOGC, config.MaxGOGC = config.MaxGOGC, config.MinGOGC
	}
	if config.AdjustInterval <= 0 {
		config.AdjustInterval = 1 * time.Second
	}

	return &AdaptiveGCController{
		config:   config,
		stopCh:   make(chan struct{}),
		lastTime: time.Now(),
		lastGOGC: 100, // Default GOGC
	}
}

// Start begins the adaptive GC control loop
func (c *AdaptiveGCController) Start() {
	if !c.config.Enabled {
		return
	}

	if c.running.Swap(true) {
		return // Already running
	}

	c.wg.Add(1)
	go c.run()
}

// Stop halts the adaptive GC controller
func (c *AdaptiveGCController) Stop() {
	if !c.running.Swap(false) {
		return // Not running
	}

	close(c.stopCh)
	c.wg.Wait()
}

// run is the main control loop
func (c *AdaptiveGCController) run() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.AdjustInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.adjust()
		}
	}
}

// adjust performs one adjustment cycle
func (c *AdaptiveGCController) adjust() {
	stats := c.collectStats()
	newGOGC := c.calculateGOGC(stats)

	c.mu.Lock()
	if newGOGC != c.lastGOGC {
		debug.SetGCPercent(newGOGC)
		c.lastGOGC = newGOGC
		metrics.AdaptiveGCAdjustmentsTotal.Inc()
	}
	c.mu.Unlock()

	// Update metrics
	metrics.AdaptiveGCCurrentGOGC.Set(float64(newGOGC))
	metrics.AdaptiveGCAllocationRate.Set(float64(stats.allocationRate))
	metrics.AdaptiveGCMemoryPressure.Set(stats.memoryPressure)
}

// collectStats gathers current GC and memory statistics
func (c *AdaptiveGCController) collectStats() *gcStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(c.lastTime).Seconds()

	// Calculate allocation rate
	var allocationRate int64
	if elapsed > 0 && m.TotalAlloc > c.lastAlloc {
		allocationRate = int64(float64(m.TotalAlloc-c.lastAlloc) / elapsed)
	}

	// Calculate memory pressure (heap in use / heap available)
	var memoryPressure float64
	if m.Sys > 0 {
		memoryPressure = float64(m.HeapInuse) / float64(m.Sys)
	}

	// Clamp memory pressure to [0, 1]
	if memoryPressure < 0 {
		memoryPressure = 0
	}
	if memoryPressure > 1 {
		memoryPressure = 1
	}

	// Update state
	c.lastAlloc = m.TotalAlloc
	c.lastTime = now

	return &gcStats{
		allocationRate: allocationRate,
		memoryPressure: memoryPressure,
	}
}

// calculateGOGC determines the optimal GOGC value based on current stats
func (c *AdaptiveGCController) calculateGOGC(stats *gcStats) int {
	// Sanitize inputs
	allocationRate := stats.allocationRate
	if allocationRate < 0 {
		allocationRate = 0
	}

	memoryPressure := stats.memoryPressure
	if memoryPressure < 0 {
		memoryPressure = 0
	}
	if memoryPressure > 1 {
		memoryPressure = 1
	}

	// Base GOGC calculation:
	// - High allocation rate + low pressure → increase GOGC (less frequent GC)
	// - Low allocation rate or high pressure → decrease GOGC (more frequent GC)

	// Normalize allocation rate to [0, 1] range
	// Assume 100 MB/s is "high" allocation
	const highAllocationRate = 100 * 1024 * 1024
	normalizedAlloc := float64(allocationRate) / float64(highAllocationRate)
	if normalizedAlloc > 1 {
		normalizedAlloc = 1
	}

	// Calculate target GOGC
	// Formula: GOGC = baseGOGC + (alloc_factor * range) - (pressure_factor * range)
	baseGOGC := float64(c.config.MinGOGC+c.config.MaxGOGC) / 2
	gogcRange := float64(c.config.MaxGOGC - c.config.MinGOGC)

	// Allocation factor: higher allocation → higher GOGC
	allocFactor := normalizedAlloc * 0.5

	// Pressure factor: higher pressure → lower GOGC
	pressureFactor := memoryPressure * 0.7

	targetGOGC := baseGOGC + (allocFactor * gogcRange) - (pressureFactor * gogcRange)

	// Round and clamp
	gogc := int(math.Round(targetGOGC))
	if gogc < c.config.MinGOGC {
		gogc = c.config.MinGOGC
	}
	if gogc > c.config.MaxGOGC {
		gogc = c.config.MaxGOGC
	}

	return gogc
}
