package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CompactionConfig holds configuration for background compaction processes.
type CompactionConfig struct {
	// Enable compaction background process
	Enabled bool

	// How often to check for compaction needs
	CheckInterval time.Duration

	// Size-based triggers
	TargetBatchSize     int
	MinBatchesToCompact int

	// Minimum threshold to trigger compaction
	MinFragmentationRatio float64
	MinDeletedNodes       int

	// Compaction behavior
	MaxCompactionTime time.Duration
	ParallelWorkers   int

	// Resource limits
	MaxMemoryUsageMB int
	MaxCPUUsage      float64

	// Strategy settings
	TriggerThresholds  map[string]int64
	CompactionStrategy string

	RateLimitBytesPerSec int64
}

// DefaultCompactionConfig returns a default compaction configuration.
func DefaultCompactionConfig() *CompactionConfig {
	return &CompactionConfig{
		Enabled:               true,
		CheckInterval:         5 * time.Minute,
		MinFragmentationRatio: 0.3,
		MinDeletedNodes:       1000,
		MaxCompactionTime:     30 * time.Minute,
		ParallelWorkers:       2,
		MaxMemoryUsageMB:      512,
		MaxCPUUsage:           0.5,
		TriggerThresholds: map[string]int64{
			"deleted_nodes": 1000,
			"fragmentation": 30,
		},
		CompactionStrategy:   "incremental",
		RateLimitBytesPerSec: 1024 * 1024 * 100,
	}
}

// CompactionWorker handles background compaction of vector indices.
type CompactionWorker struct {
	config  *CompactionConfig
	store   *VectorStore
	ctx     context.Context
	cancel  context.CancelFunc
	running atomic.Bool

	// Worker synchronization
	wg sync.WaitGroup
	mu sync.RWMutex

	// Metrics
	lastCompaction    time.Time
	totalCompactions  atomic.Int64
	failedCompactions atomic.Int64
}

// NewCompactionWorker creates a new compaction worker.
func NewCompactionWorker(store *VectorStore, config *CompactionConfig) *CompactionWorker {
	if config == nil {
		config = DefaultCompactionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &CompactionWorker{
		config: config,
		store:  store,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the compaction worker background process.
func (w *CompactionWorker) Start() {
	if !w.config.Enabled {
		return
	}

	if !w.running.CompareAndSwap(false, true) {
		return // Already running
	}

	w.wg.Add(1)
	go w.compactionLoop()
}

// Stop stops the compaction worker.
func (w *CompactionWorker) Stop() {
	if !w.running.CompareAndSwap(true, false) {
		return // Not running
	}

	w.cancel()
	w.wg.Wait()
}

// IsRunning returns true if the compaction worker is active.
func (w *CompactionWorker) IsRunning() bool {
	return w.running.Load()
}

// Trigger manually triggers a compaction for the specified dataset.
func (w *CompactionWorker) Trigger(dataset string) {
	if !w.running.Load() {
		return
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.compactDataset(dataset)
	}()
}

// compactionLoop runs the main compaction checking loop.
func (w *CompactionWorker) compactionLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.checkAndCompact()
		}
	}
}

// checkAndCompact checks all datasets for compaction needs.
func (w *CompactionWorker) checkAndCompact() {
	w.store.IterateDatasets(func(name string, ds *Dataset) {
		if ds.fragmentationTracker == nil {
			return
		}

		// 1. Check for fragmented batches
		fragmented := ds.fragmentationTracker.GetFragmentedBatches(w.config.MinFragmentationRatio)

		// 2. Trigger if criteria met
		if len(fragmented) >= w.config.MinBatchesToCompact {
			w.Trigger(name)
		}
	})
}

func (w *CompactionWorker) compactDataset(name string) {
	ds, ok := w.store.getDataset(name)
	if !ok {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			w.failedCompactions.Add(1)
			w.store.logger.Error().Any("panic", r).Str("dataset", name).Msg("Compaction panic")
		}
	}()

	w.mu.Lock()
	w.lastCompaction = time.Now()
	w.mu.Unlock()

	// Implement Compaction with Move-to-Front strategy
	// 1. Identify fragmented batches and hot batches
	fragmentedIdxs := ds.fragmentationTracker.GetFragmentedBatches(w.config.MinFragmentationRatio)
	hotIdxs := ds.fragmentationTracker.GetHotBatches(100) // Example threshold

	if len(fragmentedIdxs) == 0 && len(hotIdxs) == 0 {
		return
	}

	w.store.logger.Info().
		Str("dataset", name).
		Int("fragmented", len(fragmentedIdxs)).
		Int("hot", len(hotIdxs)).
		Msg("Starting fragmentation-aware compaction")

	// 2. Perform actual compaction (Atomic swap of records)
	// This is a complex operation that needs to be careful with RowLocations.
	// For now, we delegate to a method on Dataset that handles the heavy lifting.
	if err := ds.Compact(fragmentedIdxs, hotIdxs); err != nil {
		w.failedCompactions.Add(1)
		w.store.logger.Error().Err(err).Str("dataset", name).Msg("Compaction failed")
		return
	}

	w.totalCompactions.Add(1)
}

// GetStats returns compaction worker statistics.
func (w *CompactionWorker) GetStats() (total, failed int64, lastTime time.Time, isRunning bool) {
	return w.totalCompactions.Load(),
		w.failedCompactions.Load(),
		w.lastCompaction,
		w.running.Load()
}

func (cw *CompactionWorker) TriggerCompaction() {
	// Signal worker to start compaction cycle
}
