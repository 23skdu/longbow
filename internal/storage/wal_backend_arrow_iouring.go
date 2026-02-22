//go:build linux

package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/iouring"
)

// NewIOUringBackend creates a new WALBackend using our custom io_uring library
func NewIOUringBackend(path string) (WALBackend, error) {
	return newArrowIOUringBackend(path)
}

// ArrowIOUringBackend implements WALBackend using custom io_uring library
// with async completion handling
type ArrowIOUringBackend struct {
	path   string
	file   *os.File
	ring   *iouring.Ring
	offset int64
	mu     sync.Mutex

	// Async completion handling
	pendingOps  int64 // Atomic counter for pending operations
	stopPoller  chan struct{}
	pollerDone  chan struct{}
	completions chan completion
}

// completion represents a completed io_uring operation
type completion struct {
	userData uint64
	res      int32
}

// newArrowIOUringBackend creates a new io_uring-based WAL backend
func newArrowIOUringBackend(path string) (*ArrowIOUringBackend, error) {
	// Create/Open file
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get current file size for offset tracking
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Create io_uring ring (256 entries)
	ring, err := iouring.NewRing(256, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create io_uring ring: %w", err)
	}

	backend := &ArrowIOUringBackend{
		path:        path,
		file:        file,
		ring:        ring,
		offset:      stat.Size(),
		stopPoller:  make(chan struct{}),
		pollerDone:  make(chan struct{}),
		completions: make(chan completion, 256),
	}

	// Start async completion poller
	go backend.completionPoller()

	return backend, nil
}

// Write implements WALBackend.Write with async completion
func (b *ArrowIOUringBackend) Write(p []byte) (int, error) {
	start := time.Now()

	// Get current offset atomically
	offset := atomic.AddInt64(&b.offset, int64(len(p))) - int64(len(p))

	// Submit write operation (non-blocking)
	userData := uint64(time.Now().UnixNano())
	err := b.ring.SubmitWrite(int(b.file.Fd()), p, uint64(offset), userData)
	if err != nil {
		atomic.AddInt64(&b.offset, -int64(len(p))) // Rollback offset
		iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(time.Since(start).Seconds())
		iouring.IoUringErrors.WithLabelValues("submit_failed").Inc()
		return 0, err
	}

	// Increment pending operations
	atomic.AddInt64(&b.pendingOps, 1)

	// Flush to kernel (non-blocking)
	_, err = b.ring.Flush()
	if err != nil {
		atomic.AddInt64(&b.pendingOps, -1)
		atomic.AddInt64(&b.offset, -int64(len(p)))
		iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(time.Since(start).Seconds())
		return 0, err
	}

	duration := time.Since(start).Seconds()
	iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(duration)
	iouring.IoUringOpsSubmitted.WithLabelValues("write").Inc()
	iouring.IoUringBytesWritten.Add(float64(len(p)))

	// Return bytes that will be written (async)
	return len(p), nil
}

// WriteSync performs a synchronous write (waits for completion)
func (b *ArrowIOUringBackend) WriteSync(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()

	// Submit write operation
	err := b.ring.SubmitWrite(int(b.file.Fd()), p, uint64(b.offset), 0)
	if err != nil {
		iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(time.Since(start).Seconds())
		iouring.IoUringErrors.WithLabelValues("submit_failed").Inc()
		return 0, err
	}

	// Flush and wait for completion
	_, err = b.ring.FlushAndWait(1, 0)
	if err != nil {
		iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(time.Since(start).Seconds())
		return 0, err
	}

	// Get completion
	cqe := b.ring.Peek()
	if cqe == nil {
		return 0, fmt.Errorf("no completion available")
	}

	n := int(cqe.Res)
	if n < 0 {
		b.ring.Advance(1)
		iouring.IoUringOpsCompleted.WithLabelValues("write", "error").Inc()
		return 0, fmt.Errorf("write failed: %d", n)
	}

	// Update offset and metrics
	b.offset += int64(n)
	b.ring.Advance(1)

	duration := time.Since(start).Seconds()
	iouring.IoUringSubmitLatency.WithLabelValues("write").Observe(duration)
	iouring.IoUringOpsCompleted.WithLabelValues("write", "success").Inc()

	return n, nil
}

// Sync implements WALBackend.Sync
func (b *ArrowIOUringBackend) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Wait for all pending operations to complete
	b.drainPendingOps()

	start := time.Now()

	// Submit fsync
	err := b.ring.SubmitFsync(int(b.file.Fd()), false, 0)
	if err != nil {
		iouring.IoUringSubmitLatency.WithLabelValues("fsync").Observe(time.Since(start).Seconds())
		return err
	}

	// Wait for completion
	_, err = b.ring.FlushAndWait(1, 0)
	if err != nil {
		iouring.IoUringSubmitLatency.WithLabelValues("fsync").Observe(time.Since(start).Seconds())
		return err
	}

	// Get completion
	cqe := b.ring.Peek()
	if cqe == nil {
		return fmt.Errorf("no completion for fsync")
	}

	if cqe.Res < 0 {
		b.ring.Advance(1)
		iouring.IoUringOpsCompleted.WithLabelValues("fsync", "error").Inc()
		return fmt.Errorf("fsync failed: %d", cqe.Res)
	}

	b.ring.Advance(1)

	duration := time.Since(start).Seconds()
	iouring.IoUringSubmitLatency.WithLabelValues("fsync").Observe(duration)
	iouring.IoUringOpsCompleted.WithLabelValues("fsync", "success").Inc()

	return nil
}

// completionPoller runs in a goroutine and processes async completions
func (b *ArrowIOUringBackend) completionPoller() {
	defer close(b.pollerDone)

	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopPoller:
			// Drain remaining completions before stopping
			b.drainCompletions()
			return

		case <-ticker.C:
			b.processCompletions()
		}
	}
}

// processCompletions processes available completions without blocking
func (b *ArrowIOUringBackend) processCompletions() {
	for {
		cqe := b.ring.Peek()
		if cqe == nil {
			break
		}

		// Send completion to channel (non-blocking)
		select {
		case b.completions <- completion{userData: cqe.UserData, res: cqe.Res}:
		default:
			// Channel full, drop completion (will be handled by drain)
		}

		// Mark as consumed
		b.ring.Advance(1)

		// Decrement pending operations
		atomic.AddInt64(&b.pendingOps, -1)

		// Update metrics
		if cqe.Res < 0 {
			iouring.IoUringOpsCompleted.WithLabelValues("write", "error").Inc()
		} else {
			iouring.IoUringOpsCompleted.WithLabelValues("write", "success").Inc()
		}
	}
}

// drainCompletions processes all remaining completions
func (b *ArrowIOUringBackend) drainCompletions() {
	// Process any remaining completions
	for b.ring.CqReady() > 0 {
		cqe := b.ring.Peek()
		if cqe == nil {
			break
		}

		atomic.AddInt64(&b.pendingOps, -1)
		b.ring.Advance(1)
	}
}

// drainPendingOps waits for all pending operations to complete
func (b *ArrowIOUringBackend) drainPendingOps() {
	// Poll until all pending operations complete
	for atomic.LoadInt64(&b.pendingOps) > 0 {
		b.processCompletions()
		time.Sleep(time.Microsecond)
	}
}

// Close implements WALBackend.Close
func (b *ArrowIOUringBackend) Close() error {
	// Stop the poller
	close(b.stopPoller)
	<-b.pollerDone

	b.mu.Lock()
	defer b.mu.Unlock()

	var errs []error

	// Close ring
	if b.ring != nil {
		if err := b.ring.Close(); err != nil {
			errs = append(errs, err)
		}
		b.ring = nil
	}

	// Close file
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			errs = append(errs, err)
		}
		b.file = nil
	}

	// Close completions channel
	close(b.completions)

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}

// Name implements WALBackend.Name
func (b *ArrowIOUringBackend) Name() string {
	return b.path
}

// File implements WALBackend.File
func (b *ArrowIOUringBackend) File() *os.File {
	return b.file
}

// Stats returns current statistics
func (b *ArrowIOUringBackend) Stats() Stats {
	return Stats{
		PendingOps: atomic.LoadInt64(&b.pendingOps),
		CQReady:    b.ring.CqReady(),
	}
}

// Stats provides backend statistics
type Stats struct {
	PendingOps int64
	CQReady    uint32
}

// Ensure ArrowIOUringBackend implements WALBackend
var _ WALBackend = (*ArrowIOUringBackend)(nil)
