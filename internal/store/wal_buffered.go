package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// BufferedWAL is an asynchronous WAL implementation that buffers writes in memory.
// It trades a small window of durability (flushInterval) for high throughput.
// It uses double-buffering to allow writes to continue while flushing.
type BufferedWAL struct {
	mu           sync.Mutex
	buf          *PatchableBuffer
	backend      WALBackend // Abstraction for file I/O
	maxBatchSize int
	flushDelay   time.Duration
	stopCh       chan struct{}
	doneCh       chan struct{}

	// Synchronization
	currentSeq uint64        // Max sequence currently in buffer
	flushedSeq atomic.Uint64 // Max sequence flushed to backend
	syncCond   *sync.Cond    // Broadcasts when flushedSeq updates
	isFlushing bool          // True if a flush is in progress
	flushCh    chan struct{} // Signal to force flush
}

// NewBufferedWAL creates a new buffered WAL.
func NewBufferedWAL(backend WALBackend, maxBatchSize int, flushDelay time.Duration) *BufferedWAL {
	w := &BufferedWAL{
		buf:          NewPatchableBuffer(maxBatchSize * 2), // Pre-allocate
		backend:      backend,
		maxBatchSize: maxBatchSize,
		flushDelay:   flushDelay,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		flushCh:      make(chan struct{}, 1),
	}
	w.syncCond = sync.NewCond(&w.mu)

	go w.runFlushLoop()
	return w
}

// Write writes a record to the in-memory buffer.
// It buffers the write in memory, avoiding double serialization.
func (w *BufferedWAL) Write(name string, seq uint64, ts int64, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Header: Checksum(4) | Seq(8) | Timestamp(8) | NameLen(4) | RecLen(8) = 32 bytes
	const headerSize = 32

	nameBytes := []byte(name)
	nameLen := uint32(len(nameBytes))

	// 1. Reserve Header Space (Write placeholder)
	headerOffset := w.buf.Len()
	w.buf.Grow(headerSize)
	// Append zeroed header bytes to move cursor
	_, _ = w.buf.Write(make([]byte, headerSize))

	// 2. Write Name
	_, _ = w.buf.Write(nameBytes)

	// 3. Write RecordBatch directly to buffer
	recStartOffset := w.buf.Len()
	writer := ipc.NewWriter(w.buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recEndOffset := w.buf.Len()
	recLen := uint64(recEndOffset - recStartOffset)

	// 4. Calculate Checksum (Name + RecordBytes)
	// We can access the written bytes directly from the buffer slice
	fullPayload := w.buf.Bytes()[headerOffset+headerSize : recEndOffset]
	// Verify payload length matches
	// expectedPayloadLen := int(nameLen) + int(recLen)
	// if len(fullPayload) != expectedPayloadLen { panic("buffer mismatch") }

	crc := crc32.NewIEEE()
	_, _ = crc.Write(fullPayload)
	checksum := crc.Sum32()

	// 5. Patch Header
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint64(header[4:12], seq)
	binary.LittleEndian.PutUint64(header[12:20], uint64(ts))
	binary.LittleEndian.PutUint32(header[20:24], nameLen)
	binary.LittleEndian.PutUint64(header[24:32], recLen)

	if _, err := w.buf.WriteAt(header, int64(headerOffset)); err != nil {
		return fmt.Errorf("failed to patch header: %w", err)
	}

	// Update current sequence
	if seq > w.currentSeq {
		w.currentSeq = seq
	}

	// 5. Trigger flush if buffer exceeds size
	if w.buf.Len() >= w.maxBatchSize {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// Sync forces a flush to disk and waits for the *current* writes to be persisted.
func (w *BufferedWAL) Sync() error {
	w.mu.Lock()
	targetSeq := w.currentSeq

	// Optimization: if already flushed
	if w.flushedSeq.Load() >= targetSeq {
		w.mu.Unlock()
		return nil
	}

	// If not flushing, we can trigger a flush on the current buffer *immediately*
	if !w.isFlushing && w.buf.Len() > 0 {
		wb := w.swapBufferLocked()
		if wb != nil {
			w.isFlushing = true
			w.mu.Unlock()

			err := w.flushBufferToBackend(wb)

			w.mu.Lock()
			w.isFlushing = false
			if err != nil {
				w.mu.Unlock()
				return err
			}
		}
	}

	// Wait until flushedSeq >= targetSeq
	for w.flushedSeq.Load() < targetSeq {
		// If we are waiting, and buffer has data, we should signal the flush loop again
		// because the previous flush might have finished but we missed the content we needed.
		// However, we don't want to spam the channel.
		// The issue is that the flush loop might be idle now (waiting for ticker).

		// We can try to notify the flush loop to wake up.
		w.mu.Unlock() // Release lock to allow flush loop to proceed if it was contending? No, chan send is safe.

		// Trigger flush if not empty
		select {
		case w.flushCh <- struct{}{}:
		default:
		}

		w.mu.Lock()

		// Check again before waiting
		if w.flushedSeq.Load() >= targetSeq {
			break
		}

		w.syncCond.Wait()
	}
	w.mu.Unlock()

	return nil
}

// Close flushes ensuring all data is written and closes the background loop.
func (w *BufferedWAL) Close() error {
	close(w.stopCh)
	<-w.doneCh // Wait for loop to exit

	// Final flush
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

// tryFlush attempts to flush if needed.
func (w *BufferedWAL) tryFlush() {
	w.mu.Lock()
	if w.isFlushing || w.buf.Len() == 0 {
		w.mu.Unlock()
		return
	}

	wb := w.swapBufferLocked()
	w.isFlushing = true
	w.mu.Unlock()

	_ = w.flushBufferToBackend(wb)
	// TODO: Log error?

	w.mu.Lock()
	w.isFlushing = false
	w.mu.Unlock()
}

type writeBatch struct {
	data   []byte
	maxSeq uint64
}

// swapBufferLocked replaces the current buffer with a new one and returns the old one wrapped.
// Must be called with w.mu held.
func (w *BufferedWAL) swapBufferLocked() *writeBatch {
	if w.buf.Len() == 0 {
		return nil
	}

	oldBuf := w.buf
	currentMax := w.currentSeq

	// Allocate new buffer
	// Optimization: we could pool these buffers to avoid allocs
	// But allocating 64KB chunks is relatively cheap in Go GC compared to the I/O.
	w.buf = NewPatchableBuffer(w.maxBatchSize * 2)

	return &writeBatch{
		data:   oldBuf.Bytes(),
		maxSeq: currentMax,
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

	// Update flushed sequence
	w.flushedSeq.Store(wb.maxSeq)

	// Broadcast to waiters
	w.syncCond.Broadcast()

	return nil
}
