package store

import (
	"sync"
	"time"
)

// BatchNeighborUpdate represents a deferred update to a node's neighbor list.
type BatchNeighborUpdate struct {
	NodeID    uint32
	Layer     int
	Neighbors []uint32
}

// BatchNeighborUpdater accumulates neighbor updates and applies them in batches.
// This reduces the overhead of lock acquisition and release in hot paths.
type BatchNeighborUpdater struct {
	mu           sync.Mutex
	pending      []BatchNeighborUpdate
	maxBatchSize int
	onFlush      func(updates []BatchNeighborUpdate) error

	lastFlush     time.Time
	flushInterval time.Duration
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// BatchNeighborUpdaterConfig configures the batch updater.
type BatchNeighborUpdaterConfig struct {
	MaxBatchSize  int
	FlushInterval time.Duration
	OnFlush       func(updates []BatchNeighborUpdate) error
}

// NewBatchNeighborUpdater creates a new batch updater.
func NewBatchNeighborUpdater(cfg BatchNeighborUpdaterConfig) *BatchNeighborUpdater {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}

	bu := &BatchNeighborUpdater{
		pending:       make([]BatchNeighborUpdate, 0, cfg.MaxBatchSize),
		maxBatchSize:  cfg.MaxBatchSize,
		onFlush:       cfg.OnFlush,
		flushInterval: cfg.FlushInterval,
		lastFlush:     time.Now(),
		stopChan:      make(chan struct{}),
	}

	// Start background flush worker
	bu.wg.Add(1)
	go bu.runFlushWorker()

	return bu
}

// Add adds a neighbor update to the batch.
func (bu *BatchNeighborUpdater) Add(update BatchNeighborUpdate) {
	bu.mu.Lock()
	bu.pending = append(bu.pending, update)

	shouldFlush := len(bu.pending) >= bu.maxBatchSize
	bu.mu.Unlock()

	if shouldFlush {
		bu.Flush()
	}
}

// Flush immediately flushes all pending updates.
func (bu *BatchNeighborUpdater) Flush() error {
	bu.mu.Lock()
	if len(bu.pending) == 0 {
		bu.mu.Unlock()
		return nil
	}

	updates := bu.pending
	bu.pending = make([]BatchNeighborUpdate, 0, bu.maxBatchSize)
	bu.lastFlush = time.Now()
	bu.mu.Unlock()

	if bu.onFlush != nil {
		return bu.onFlush(updates)
	}
	return nil
}

// Stop stops the background worker and flushes any pending updates.
func (bu *BatchNeighborUpdater) Stop() error {
	close(bu.stopChan)
	bu.wg.Wait()
	return bu.Flush()
}

func (bu *BatchNeighborUpdater) runFlushWorker() {
	defer bu.wg.Done()

	ticker := time.NewTicker(bu.flushInterval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-bu.stopChan:
			return
		case <-ticker.C:
			bu.mu.Lock()
			elapsed := time.Since(bu.lastFlush)
			shouldFlush := len(bu.pending) > 0 && elapsed >= bu.flushInterval
			bu.mu.Unlock()

			if shouldFlush {
				bu.Flush()
			}
		}
	}
}

// PendingCount returns the number of pending updates.
func (bu *BatchNeighborUpdater) PendingCount() int {
	bu.mu.Lock()
	defer bu.mu.Unlock()
	return len(bu.pending)
}
