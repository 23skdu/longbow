package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CompactionConfig holds configuration for background compaction processes.
type CompactionConfig struct {
	// Enabled controls whether the background compaction process is active.
	Enabled bool

	// CheckInterval is how often to check for compaction needs.
	CheckInterval time.Duration

	// TargetBatchSize is the desired size of a batch after compaction.
	TargetBatchSize int
	// MinBatchesToCompact is the minimum number of fragmented batches required to trigger compaction.
	MinBatchesToCompact int

	// MinFragmentationRatio is the ratio of deleted nodes to trigger compaction.
	MinFragmentationRatio float64
	// MinDeletedNodes is the minimum number of deleted nodes to trigger compaction.
	MinDeletedNodes int

	// MaxCompactionTime is the maximum allowed duration for a single compaction run.
	MaxCompactionTime time.Duration
	// ParallelWorkers is the number of goroutines to use for compaction.
	ParallelWorkers int

	// MaxMemoryUsageMB is the memory limit for the compaction process.
	MaxMemoryUsageMB int
	// MaxCPUUsage is the CPU limit for the compaction process (0.0 to 1.0).
	MaxCPUUsage float64

	// TriggerThresholds holds named thresholds for triggering compaction.
	TriggerThresholds map[string]int64
	// CompactionStrategy defines the compaction algorithm to use.
	CompactionStrategy string

	// RateLimitBytesPerSec limits the I/O rate of compaction.
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
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.totalCompactions.Load(),
		w.failedCompactions.Load(),
		w.lastCompaction,
		w.running.Load()
}

// TriggerCompaction signals the worker to start a compaction cycle.
func (w *CompactionWorker) TriggerCompaction() {
	// Signal worker to start compaction cycle
}
