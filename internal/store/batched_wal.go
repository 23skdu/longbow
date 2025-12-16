package store

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// WALEntry represents a single entry to be written to the WAL
type WALEntry struct {
	Record arrow.Record
	Name   string
}

// WALBatcherConfig configures the batched WAL writer
type WALBatcherConfig struct {
	FlushInterval time.Duration // Time between flushes (e.g., 10ms)
	MaxBatchSize  int           // Max entries before forced flush (e.g., 100)
}

// DefaultWALBatcherConfig returns sensible defaults
func DefaultWALBatcherConfig() WALBatcherConfig {
	return WALBatcherConfig{
		FlushInterval: 10 * time.Millisecond,
		MaxBatchSize:  100,
	}
}

// WALBatcher batches WAL writes for improved performance
// Instead of fsync on every write, it groups writes and flushes periodically
type WALBatcher struct {
	dataPath string
	config   WALBatcherConfig
	mem      memory.Allocator

	// Buffered channel for incoming writes
	entries chan WALEntry

	// Internal state
	mu        sync.Mutex
	walFile   *os.File
	batch     []WALEntry
	backBatch []WALEntry // double-buffer: swap on flush to avoid allocation
	running   bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	flushErr  error
}

// NewWALBatcher creates a new batched WAL writer
func NewWALBatcher(dataPath string, config WALBatcherConfig) *WALBatcher {
	return &WALBatcher{
		dataPath:  dataPath,
		config:    config,
		mem:       memory.NewGoAllocator(),
		entries:   make(chan WALEntry, config.MaxBatchSize*10),
		batch:     make([]WALEntry, 0, config.MaxBatchSize),
		backBatch: make([]WALEntry, 0, config.MaxBatchSize),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start initializes the WAL file and starts the background flush goroutine
func (w *WALBatcher) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return nil
	}

	// Ensure data directory exists
	if err := os.MkdirAll(w.dataPath, 0o755); err != nil {
		return err
	}

	// Open WAL file
	walPath := filepath.Join(w.dataPath, walFileName)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.walFile = f
	w.running = true

	// Start background flusher
	go w.flushLoop()

	return nil
}

// Write queues a record for batched WAL writing (non-blocking)
func (w *WALBatcher) Write(rec arrow.Record, name string) error {
	if rec == nil {
		return nil
	}

	// Retain the record since we're passing it to another goroutine
	rec.Retain()

	select {
	case w.entries <- WALEntry{Record: rec, Name: name}:
		return nil
	case <-w.stopCh:
		rec.Release()
		return NewUnavailableError("write", "batcher stopped")
	}
}

// flushLoop runs in background, batching and flushing writes
func (w *WALBatcher) flushLoop() {
	defer close(w.doneCh)

	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case entry := <-w.entries:
			w.mu.Lock()
			w.batch = append(w.batch, entry)
			shoudFlush := len(w.batch) >= w.config.MaxBatchSize
			w.mu.Unlock()

			if shoudFlush {
				w.flush()
			}

		case <-ticker.C:
			w.flush()

		case <-w.stopCh:
			// Drain remaining entries
			w.drainAndFlush()
			return
		}
	}
}

// flush writes all batched entries to disk
func (w *WALBatcher) flush() {
	w.mu.Lock()
	if len(w.batch) == 0 {
		w.mu.Unlock()
		return
	}

	// Swap batch to avoid holding lock during I/O
	batch := w.batch
	w.batch = w.backBatch[:0]
	w.backBatch = batch // double-buffer swap: reuse slices to avoid allocation
	w.mu.Unlock()

	// Write all entries
	for _, entry := range batch {
		if err := w.writeEntry(entry); err != nil {
			w.mu.Lock()
			w.flushErr = err
			w.mu.Unlock()
			metrics.WalWritesTotal.WithLabelValues("error").Inc()
		} else {
			metrics.WalWritesTotal.WithLabelValues("ok").Inc()
		}
		// Release retained record
		entry.Record.Release()
	}

	// Sync once per batch instead of per-write
	if w.walFile != nil {
		if err := w.walFile.Sync(); err != nil {
			w.mu.Lock()
			w.flushErr = err
			w.mu.Unlock()
		}
	}
}

// drainAndFlush drains channel and flushes remaining entries on stop
func (w *WALBatcher) drainAndFlush() {
	// Drain channel
	for {
		select {
		case entry := <-w.entries:
			w.mu.Lock()
			w.batch = append(w.batch, entry)
			w.mu.Unlock()
		default:
			// Channel empty
			w.flush()
			return
		}
	}
}

// writeEntry serializes and writes a single entry to WAL
func (w *WALBatcher) writeEntry(entry WALEntry) error {
	rec := entry.Record
	name := entry.Name

	if w.walFile == nil {
		return nil
	}

	// Format: [NameLen: uint32][Name: bytes][RecordLen: uint64][RecordBytes: bytes]
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(w.mem))
	if err := writer.Write(rec); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recBytes := buf.Bytes()

	nameBytes := []byte(name)
	nameLen := uint32(len(nameBytes))
	recLen := uint64(len(recBytes))

	// Write header + data (no sync here, batched at flush level)
	if err := binary.Write(w.walFile, binary.LittleEndian, nameLen); err != nil {
		return err
	}
	if _, err := w.walFile.Write(nameBytes); err != nil {
		return err
	}
	if err := binary.Write(w.walFile, binary.LittleEndian, recLen); err != nil {
		return err
	}
	if _, err := w.walFile.Write(recBytes); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the batcher, flushing pending writes
func (w *WALBatcher) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	// Signal stop and wait for flush loop to complete
	close(w.stopCh)
	<-w.doneCh

	// Close file
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.walFile != nil {
		if err := w.walFile.Sync(); err != nil {
			return err
		}
		if err := w.walFile.Close(); err != nil {
			return err
		}
		w.walFile = nil
	}

	return w.flushErr
}

// FlushError returns the last flush error if any
func (w *WALBatcher) FlushError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushErr
}
