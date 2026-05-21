package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// BufferedWAL is an asynchronous WAL implementation that buffers writes in memory.
// It trades a small window of durability (flushInterval) for high throughput.
// It uses lock-free double-buffering to allow writes to continue while flushing.
type BufferedWAL struct {
	mu           sync.Mutex
	buf          *PatchableBuffer
	backend      WALBackend
	maxBatchSize int
	flushDelay   time.Duration
	stopCh       chan struct{}
	doneCh       chan struct{}

	ErrCh   chan error
	onError func(error)

	// Synchronization
	currentSeq uint64
	flushedSeq atomic.Uint64
	syncCond   *sync.Cond

	// Group commit: pending sync waiters
	pendingSyncs []syncWaiter

	isFlushing bool
	flushCh    chan struct{}
	fatalErr   atomic.Value

	// Lock-free buffer pool for double buffering: pre-allocated buffers
	bufferPool    chan *PatchableBuffer
	bufferPoolLen int
}

// syncWaiter represents a goroutine waiting for a Sync() to complete.
type syncWaiter struct {
	targetSeq uint64
	done      chan struct{}
	err       error
}

// NewBufferedWAL creates a new buffered WAL.
func NewBufferedWAL(backend WALBackend, maxBatchSize int, flushDelay time.Duration) *BufferedWAL {
	poolLen := 4 // Keep up to 4 buffers in the pool
	pool := make(chan *PatchableBuffer, poolLen)
	for i := 0; i < poolLen; i++ {
		pool <- newBuffer(maxBatchSize * 2)
	}

	w := &BufferedWAL{
		buf:           newBuffer(maxBatchSize * 2),
		backend:       backend,
		maxBatchSize:  maxBatchSize,
		flushDelay:    flushDelay,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		flushCh:       make(chan struct{}, 1),
		ErrCh:         make(chan error, 1),
		pendingSyncs:  make([]syncWaiter, 0, 64),
		bufferPool:    pool,
		bufferPoolLen: poolLen,
	}
	w.syncCond = sync.NewCond(&w.mu)

	go w.runFlushLoop()
	return w
}

// newBuffer creates a fresh PatchableBuffer with the given capacity.
func newBuffer(capacity int) *PatchableBuffer {
	b := GetBuffer(capacity)
	b.Reset()
	return b
}

// acquireBuffer gets a buffer from the pool or allocates a new one.
func (w *BufferedWAL) acquireBuffer() *PatchableBuffer {
	select {
	case buf := <-w.bufferPool:
		buf.Reset()
		return buf
	default:
		return GetBuffer(w.maxBatchSize * 2)
	}
}

// releaseBuffer returns a buffer to the pool if there's room.
func (w *BufferedWAL) releaseBuffer(buf *PatchableBuffer) {
	select {
	case w.bufferPool <- buf:
	default:
		PutBuffer(buf)
	}
}

// SetOnError sets a callback function to be invoked when a flush error occurs.
func (w *BufferedWAL) SetOnError(fn func(error)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onError = fn
}

// Write writes a record to the in-memory buffer.
func (w *BufferedWAL) Write(name string, seq uint64, ts int64, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if errVal := w.fatalErr.Load(); errVal != nil {
		return errVal.(error)
	}

	const headerSize = 32
	nameBytes := []byte(name)
	// #nosec G115
	nameLen := uint32(len(nameBytes))

	headerOffset := w.buf.Len()
	w.buf.Grow(headerSize)
	if _, err := w.buf.Write(make([]byte, headerSize)); err != nil {
		return fmt.Errorf("failed to reserve header space: %w", err)
	}

	if _, err := w.buf.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write name: %w", err)
	}

	recStartOffset := w.buf.Len()
	writer := ipc.NewWriter(w.buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recEndOffset := w.buf.Len()
	// #nosec G115
	recLen := uint64(recEndOffset - recStartOffset)

	fullPayload := w.buf.Bytes()[headerOffset+headerSize : recEndOffset]
	crc := crc32.NewIEEE()
	_, _ = crc.Write(fullPayload)
	checksum := crc.Sum32()

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint64(header[4:12], seq)
	// #nosec G115
	binary.LittleEndian.PutUint64(header[12:20], uint64(ts))
	binary.LittleEndian.PutUint32(header[20:24], nameLen)
	binary.LittleEndian.PutUint64(header[24:32], recLen)

	if _, err := w.buf.WriteAt(header, int64(headerOffset)); err != nil {
		return fmt.Errorf("failed to patch header: %w", err)
	}

	if seq > w.currentSeq {
		w.currentSeq = seq
	}

	if w.buf.Len() >= w.maxBatchSize {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// Sync forces a flush to disk and waits for the *current* writes to be persisted.
// Uses group-commit batching: concurrent Sync callers register as waiters and
// a single flush drains all pending work at once, reducing total IOPS.
func (w *BufferedWAL) Sync() error {
	// Fast path: already flushed
	if w.flushedSeq.Load() >= w.currentSeq {
		return nil
	}

	w.mu.Lock()
	targetSeq := w.currentSeq

	if w.flushedSeq.Load() >= targetSeq {
		w.mu.Unlock()
		return nil
	}

	// Check for fatal error before registering
	if errVal := w.fatalErr.Load(); errVal != nil {
		w.mu.Unlock()
		return errVal.(error)
	}

	// Register as a group-commit waiter
	waiter := syncWaiter{
		targetSeq: targetSeq,
		done:      make(chan struct{}),
	}
	w.pendingSyncs = append(w.pendingSyncs, waiter)

	// If no flush is in progress, trigger one now
	if !w.isFlushing {
		w.tryFlushLocked()
	}

	w.mu.Unlock()

	// Wait for the group commit to complete
	<-waiter.done
	if waiter.err != nil {
		return waiter.err
	}
	return nil
}

// Close flushes ensuring all data is written and closes the background loop.
func (w *BufferedWAL) Close() error {
	close(w.stopCh)
	<-w.doneCh

	w.mu.Lock()
	if w.buf.Len() > 0 {
		wb := w.swapBufferLocked()
		w.mu.Unlock()
		if err := w.flushBufferToBackend(wb); err != nil {
			_ = w.backend.Close()
			return err
		}
	} else {
		w.mu.Unlock()
	}

	return w.backend.Close()
}

// runFlushLoop manages periodic flushing.
func (w *BufferedWAL) runFlushLoop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.flushDelay)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.tryFlush()
		case <-w.flushCh:
			w.tryFlush()
		}
	}
}

// tryFlushLocked attempts to flush if needed.
// Must be called with w.mu held. The lock may be released and re-acquired during flush.
func (w *BufferedWAL) tryFlushLocked() {
	for {
		if w.isFlushing || w.buf.Len() == 0 {
			break
		}

		wb := w.swapBufferLocked()
		if wb == nil {
			break
		}

		w.isFlushing = true
		batch := w.pendingSyncs
		w.pendingSyncs = make([]syncWaiter, 0, 64)

		onError := w.onError
		w.mu.Unlock()

		err := w.flushBufferToBackend(wb)

		w.mu.Lock()
		w.isFlushing = false

		var toSignal []syncWaiter
		if err != nil {
			metrics.WALFlushErrors.Inc()
			for i := range batch {
				batch[i].err = err
				toSignal = append(toSignal, batch[i])
			}
			for i := range w.pendingSyncs {
				w.pendingSyncs[i].err = err
				toSignal = append(toSignal, w.pendingSyncs[i])
			}
			w.pendingSyncs = nil
			if w.fatalErr.Load() == nil {
				w.fatalErr.Store(err)
			}
			select {
			case w.ErrCh <- err:
			default:
			}
			if onError != nil {
				onError(err)
			}
		} else {
			w.flushedSeq.Store(wb.maxSeq)
			for i := range batch {
				toSignal = append(toSignal, batch[i])
			}
			var stillPending []syncWaiter
			for _, wt := range w.pendingSyncs {
				if wt.targetSeq <= wb.maxSeq {
					toSignal = append(toSignal, wt)
				} else {
					stillPending = append(stillPending, wt)
				}
			}
			w.pendingSyncs = stillPending
		}

		w.syncCond.Broadcast()
		w.mu.Unlock()

		// Signal all satisfied/failed waiters outside the lock
		for i := range toSignal {
			close(toSignal[i].done)
		}

		w.mu.Lock()
		// If more waiters arrived during the flush and there's data, loop
		continue
	}
}

// tryFlush is called from the flush loop (background ticker/flushCh).
func (w *BufferedWAL) tryFlush() {
	w.mu.Lock()
	w.tryFlushLocked()
	w.mu.Unlock()
}

type writeBatch struct {
	data   []byte
	maxSeq uint64
	buf    *PatchableBuffer
}

// swapBufferLocked replaces the current buffer with a fresh one from the pool
// and returns the old one wrapped. Must be called with w.mu held.
// This is a lock-free-friendly operation since the pool provides pre-allocated buffers.
func (w *BufferedWAL) swapBufferLocked() *writeBatch {
	if w.buf.Len() == 0 {
		return nil
	}

	oldBuf := w.buf
	currentMax := w.currentSeq

	// Acquire a fresh buffer from the lock-free pool
	w.buf = w.acquireBuffer()

	return &writeBatch{
		data:   oldBuf.Bytes(),
		maxSeq: currentMax,
		buf:    oldBuf,
	}
}

// flushBufferToBackend writes the batch to disk and updates flushedSeq.
func (w *BufferedWAL) flushBufferToBackend(wb *writeBatch) error {
	if wb == nil || len(wb.data) == 0 {
		return nil
	}

	if _, err := w.backend.Write(wb.data); err != nil {
		return fmt.Errorf("wal flush write: %w", err)
	}

	if err := w.backend.Sync(); err != nil {
		return fmt.Errorf("wal backend sync: %w", err)
	}

	// Return buffer to pool for reuse instead of discarding
	if wb.buf != nil {
		w.releaseBuffer(wb.buf)
	}

	return nil
}
