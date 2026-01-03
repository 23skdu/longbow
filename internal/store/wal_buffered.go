package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// BufferedWAL is an asynchronous WAL implementation that buffers writes in memory.
// It trades a small window of durability (flushInterval) for high throughput.
type BufferedWAL struct {
	mu           sync.Mutex
	buf          *bytes.Buffer
	backend      WALBackend // Abstraction for file I/O
	maxBatchSize int
	flushDelay   time.Duration
	stopCh       chan struct{}
	doneCh       chan struct{}
	flushCh      chan struct{} // Signal to force flush
}

// NewBufferedWAL creates a new buffered WAL.
func NewBufferedWAL(backend WALBackend, maxBatchSize int, flushDelay time.Duration) *BufferedWAL {
	w := &BufferedWAL{
		buf:          bytes.NewBuffer(make([]byte, 0, maxBatchSize*2)), // Pre-allocate
		backend:      backend,
		maxBatchSize: maxBatchSize,
		flushDelay:   flushDelay,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		flushCh:      make(chan struct{}, 1),
	}

	go w.runFlushLoop()
	return w
}

// Write writes a record to the in-memory buffer.
// It returns nil immediately unless serialization fails.
func (w *BufferedWAL) Write(name string, seq uint64, ts int64, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Serialize RecordBatch to a temporary buffer (or directly to w.buf?)
	// We need the length for the header, so we might need a separate buffer or
	// reserve space in w.buf, write, measure, then fill header.
	// To minimize allocs, let's write to a reuseable staging buffer or directly to w.buf.
	// Since IPC writer needs a writer, let's use a temp buffer for the record itself
	// to calculate CRC and Length correctly before committing to the main buffer.
	// OPTIMIZATION: In the future we can serialize directly and backfill header,
	// but for now let's be safe with a scratch buffer.

	var recBuf bytes.Buffer
	writer := ipc.NewWriter(&recBuf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recBytes := recBuf.Bytes()

	nameBytes := []byte(name)
	nameLen := uint32(len(nameBytes))
	recLen := uint64(len(recBytes))

	// 2. Calculate Checksum
	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	// 3. Serialize Header
	// Header: Checksum(4) | Seq(8) | Timestamp(8) | NameLen(4) | RecLen(8) = 32 bytes
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint64(header[4:12], seq)
	binary.LittleEndian.PutUint64(header[12:20], uint64(ts))
	binary.LittleEndian.PutUint32(header[20:24], nameLen)
	binary.LittleEndian.PutUint64(header[24:32], recLen)

	// 4. Append to logical buffer
	// Check capacity? bytes.Buffer grows automatically.
	if _, err := w.buf.Write(header); err != nil {
		return err
	}
	if _, err := w.buf.Write(nameBytes); err != nil {
		return err
	}
	if _, err := w.buf.Write(recBytes); err != nil {
		return err
	}

	// 5. Trigger flush if buffer exceeds size
	if w.buf.Len() >= w.maxBatchSize {
		// Signal flush securely
		select {
		case w.flushCh <- struct{}{}:
		default:
			// Already signaled
		}
	}

	return nil
}

// Sync forces a flush to disk and waits for it to complete.
// Note: This implementation waits for *a* flush, effectively ensuring durability.
func (w *BufferedWAL) Sync() error {
	// Simple barrier: Trigger flush and wait?
	// The problem is waiting for the *specific* flush of currently buffered data.
	// For simplicity in MVP:
	// 1. Lock
	// 2. Flush synchronously (calling flushLocked)
	// 3. Backend Sync
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.flushLocked()
}

// Close flushes ensuring all data is written and closes the background loop.
func (w *BufferedWAL) Close() error {
	// Stop loop
	close(w.stopCh)
	<-w.doneCh

	// Final flush
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil {
		w.backend.Close() // Ignore error on backend close if flush failed?
		return err
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

// tryFlush builds a lock around flushLocked to be called from the loop.
func (w *BufferedWAL) tryFlush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.flushLocked()
	// TODO: Log error?
}

// flushLocked writes the buffer to the backend and resets it.
// Caller must hold w.mu.
func (w *BufferedWAL) flushLocked() error {
	if w.buf.Len() == 0 {
		return nil
	}

	// Write entire buffer to backend
	if _, err := w.backend.Write(w.buf.Bytes()); err != nil {
		return fmt.Errorf("wal flush write: %w", err)
	}

	// Sync backend (group commit)
	if err := w.backend.Sync(); err != nil {
		return fmt.Errorf("wal backend sync: %w", err)
	}

	// Reset buffer
	w.buf.Reset()
	return nil
}
