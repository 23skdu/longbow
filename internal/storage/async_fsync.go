package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncFsyncConfig configures asynchronous fsync behavior for WAL
type AsyncFsyncConfig struct {
	DirtyThreshold   int64
	MaxPendingFsyncs int
	FsyncTimeout     time.Duration
	Enabled          bool
	FallbackToSync   bool
}

// DefaultAsyncFsyncConfig returns sensible defaults
func DefaultAsyncFsyncConfig() AsyncFsyncConfig {
	return AsyncFsyncConfig{
		DirtyThreshold:   1024 * 1024, // 1MB
		MaxPendingFsyncs: 4,
		FsyncTimeout:     5 * time.Second,
		Enabled:          true,
		FallbackToSync:   false,
	}
}

// Validate checks if the configuration is valid
func (c AsyncFsyncConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.DirtyThreshold <= 0 {
		return fmt.Errorf("DirtyThreshold must be positive, got %d", c.DirtyThreshold)
	}
	if c.MaxPendingFsyncs <= 0 {
		return fmt.Errorf("MaxPendingFsyncs must be positive, got %d", c.MaxPendingFsyncs)
	}
	if c.FsyncTimeout <= 0 {
		return fmt.Errorf("FsyncTimeout must be positive, got %v", c.FsyncTimeout)
	}
	return nil
}

// IsAsyncEnabled returns whether async fsync is enabled
func (c AsyncFsyncConfig) IsAsyncEnabled() bool {
	return c.Enabled
}

// ShouldFsync returns true if dirty bytes exceed threshold
func (c AsyncFsyncConfig) ShouldFsync(dirtyBytes int64) bool {
	return dirtyBytes >= c.DirtyThreshold
}

// AsyncFsyncerStats holds statistics for the async fsyncer
type AsyncFsyncerStats struct {
	TotalRequests   int64
	CompletedFsyncs int64
	FailedFsyncs    int64
	SyncFallbacks   int64
	QueueFullDrops  int64
}

// AsyncFsyncer manages asynchronous fsync operations
type AsyncFsyncer struct {
	config AsyncFsyncConfig
	file   *os.File

	// Dirty byte tracking
	dirtyBytes atomic.Int64

	// Fsync request channel
	requestCh chan struct{}

	// Lifecycle
	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	waitCh  chan struct{} // signaled after each fsync completes

	// Statistics
	totalRequests   atomic.Int64
	completedFsyncs atomic.Int64
	failedFsyncs    atomic.Int64
	syncFallbacks   atomic.Int64
	queueFullDrops  atomic.Int64
}

// NewAsyncFsyncer creates a new async fsyncer
func NewAsyncFsyncer(cfg AsyncFsyncConfig) *AsyncFsyncer {
	return &AsyncFsyncer{
		config:    cfg,
		requestCh: make(chan struct{}, cfg.MaxPendingFsyncs),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		waitCh:    make(chan struct{}),
	}
}

// Config returns the configuration
func (a *AsyncFsyncer) Config() AsyncFsyncConfig {
	return a.config
}

// Start begins the async fsync goroutine
func (a *AsyncFsyncer) Start(f *os.File) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return nil // Already running
	}

	a.file = f
	a.running = true
	a.stopCh = make(chan struct{})
	a.doneCh = make(chan struct{})

	go a.fsyncLoop()
	return nil
}

// Stop gracefully shuts down the fsyncer
func (a *AsyncFsyncer) Stop() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	a.mu.Unlock()

	close(a.stopCh)
	<-a.doneCh
	return nil
}

// IsRunning returns whether the fsyncer is active
func (a *AsyncFsyncer) IsRunning() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

// AddDirtyBytes adds to the dirty byte counter
func (a *AsyncFsyncer) AddDirtyBytes(n int64) {
	a.dirtyBytes.Add(n)
}

// DirtyBytes returns current dirty byte count
func (a *AsyncFsyncer) DirtyBytes() int64 {
	return a.dirtyBytes.Load()
}

// RequestFsyncIfNeeded requests fsync if dirty threshold exceeded
func (a *AsyncFsyncer) RequestFsyncIfNeeded() bool {
	if !a.config.ShouldFsync(a.dirtyBytes.Load()) {
		return false
	}

	a.totalRequests.Add(1)

	// Non-blocking send to request channel
	select {
	case a.requestCh <- struct{}{}:
		return true
	default:
		// Queue full
		a.queueFullDrops.Add(1)
		return false
	}
}

// WaitForPendingFsyncs waits for all pending fsyncs to complete
func (a *AsyncFsyncer) WaitForPendingFsyncs() {
	// Drain the request channel and wait for completion
	for {
		select {
		case <-a.waitCh:
			// Check if more pending
			if len(a.requestCh) == 0 && a.dirtyBytes.Load() == 0 {
				return
			}
		case <-time.After(100 * time.Millisecond):
			// Timeout check
			if len(a.requestCh) == 0 && a.dirtyBytes.Load() == 0 {
				return
			}
		}
	}
}

// ForceFsync performs a synchronous fsync (fallback)
func (a *AsyncFsyncer) ForceFsync() error {
	a.syncFallbacks.Add(1)
	a.mu.Lock()
	f := a.file
	a.mu.Unlock()

	if f == nil {
		return nil
	}

	err := f.Sync()
	if err == nil {
		a.dirtyBytes.Store(0)
		a.completedFsyncs.Add(1)
	} else {
		a.failedFsyncs.Add(1)
	}
	return err
}

// Stats returns current statistics
func (a *AsyncFsyncer) Stats() AsyncFsyncerStats {
	return AsyncFsyncerStats{
		TotalRequests:   a.totalRequests.Load(),
		CompletedFsyncs: a.completedFsyncs.Load(),
		FailedFsyncs:    a.failedFsyncs.Load(),
		SyncFallbacks:   a.syncFallbacks.Load(),
		QueueFullDrops:  a.queueFullDrops.Load(),
	}
}

// fsyncLoop is the background goroutine processing fsync requests
func (a *AsyncFsyncer) fsyncLoop() {
	defer close(a.doneCh)

	for {
		select {
		case <-a.requestCh:
			a.doFsync()
			// Signal waiters
			select {
			case a.waitCh <- struct{}{}:
			default:
			}
		case <-a.stopCh:
			// Drain remaining requests
			for {
				select {
				case <-a.requestCh:
					a.doFsync()
				default:
					return
				}
			}
		}
	}
}

// doFsync performs the actual fsync operation
func (a *AsyncFsyncer) doFsync() {
	a.mu.Lock()
	f := a.file
	a.mu.Unlock()

	if f == nil {
		return
	}

	err := f.Sync()
	if err == nil {
		a.dirtyBytes.Store(0)
		a.completedFsyncs.Add(1)
	} else {
		a.failedFsyncs.Add(1)
	}
}
