package store

import (
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
	Record arrow.RecordBatch
	Name   string
}

// WALBatcherConfig configures the batched WAL writer
type WALBatcherConfig struct {
	FlushInterval time.Duration     // Time between flushes (e.g., 10ms)
	MaxBatchSize  int               // Max entries before forced flush (e.g., 100)
	Adaptive      AdaptiveWALConfig // Adaptive batching configuration
	AsyncFsync AsyncFsyncConfig // Async fsync configuration
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
	mu           sync.Mutex
	walFile      *os.File
	batch        []WALEntry
	backBatch    []WALEntry // double-buffer: swap on flush to avoid allocation
	running      bool
	bufPool      *walBufferPool // pooled buffers for IPC serialization
	stopCh       chan struct{}
	doneCh       chan struct{}
	flushErr     error
	rateTracker  *WriteRateTracker           // Adaptive: tracks write rate
	intervalCalc *AdaptiveIntervalCalculator // Adaptive: calculates intervals
	asyncFsyncer *AsyncFsyncer // Async: background fsync handler
}

// NewWALBatcher creates a new batched WAL writer
func NewWALBatcher(dataPath string, config *WALBatcherConfig) *WALBatcher {
	w := &WALBatcher{
		dataPath:  dataPath,
		config:   *config,
		mem:       memory.NewGoAllocator(),
		entries:   make(chan WALEntry, config.MaxBatchSize*10),
		batch:     make([]WALEntry, 0, config.MaxBatchSize),
		backBatch: make([]WALEntry, 0, config.MaxBatchSize),
		bufPool:   newWALBufferPool(),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	if config.Adaptive.Enabled {
		w.rateTracker = NewWriteRateTracker(1 * time.Second)
		w.intervalCalc = NewAdaptiveIntervalCalculator(config.Adaptive)
	}
	// Initialize async fsyncer if enabled
	if config.AsyncFsync.Enabled {
		w.asyncFsyncer = NewAsyncFsyncer(config.AsyncFsync)
	}
	return w
}

// Start initializes the WAL file and starts the background flush goroutine
func (w *WALBatcher) Start() error {
	walLockStart1 := time.Now()
	w.mu.Lock()
	metrics.WALLockWaitDuration.WithLabelValues("append").Observe(time.Since(walLockStart1).Seconds())
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

	// Start async fsyncer if enabled
	if w.asyncFsyncer != nil {
		if err := w.asyncFsyncer.Start(f); err != nil {
			return err
		}
	}

	// Start background flusher
	go w.flushLoop()

	return nil
}

// Write queues a record for batched WAL writing (non-blocking)
func (w *WALBatcher) Write(rec arrow.RecordBatch, name string) error {
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
			walLockStart2 := time.Now()
			w.mu.Lock()
			metrics.WALLockWaitDuration.WithLabelValues("flush_check").Observe(time.Since(walLockStart2).Seconds())
// Track pending entries (backpressure indicator)
metrics.WalPendingEntries.Set(float64(len(w.entries)))
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
	walLockStart3 := time.Now()
	w.mu.Lock()
	metrics.WALLockWaitDuration.WithLabelValues("flush").Observe(time.Since(walLockStart3).Seconds())
	if len(w.batch) == 0 {
		w.mu.Unlock()
		return
	}

	// Swap batch to avoid holding lock during I/O
	batch := w.batch
	w.batch = w.backBatch[:0]
	w.backBatch = batch // double-buffer swap: reuse slices to avoid allocation
	w.mu.Unlock()
// Record batch size (measures batching efficiency)
metrics.WalBatchSize.Observe(float64(len(batch)))

	// Track bytes written for async fsync threshold
	var bytesWritten int

	// Write all entries
	for _, entry := range batch {
		n, err := w.writeEntryBytes(entry)
		if err != nil {
			walLockStart4 := time.Now()
			w.mu.Lock()
			metrics.WALLockWaitDuration.WithLabelValues("sync_check").Observe(time.Since(walLockStart4).Seconds())
			w.flushErr = err
			w.mu.Unlock()
			metrics.WalWritesTotal.WithLabelValues("error").Inc()
		} else {
			bytesWritten += n
			metrics.WalWritesTotal.WithLabelValues("ok").Inc()
		}
		// Release retained record
		entry.Record.Release()
	}

	// Sync once per batch instead of per-write (async or blocking)
	if w.walFile != nil {
		if w.asyncFsyncer != nil && w.asyncFsyncer.IsRunning() {
			// Async path: track dirty bytes and request fsync if threshold exceeded
			w.asyncFsyncer.AddDirtyBytes(int64(bytesWritten))
			w.asyncFsyncer.RequestFsyncIfNeeded()
		} else {
			// Sync fallback: blocking fsync
			fsyncStart := time.Now()
			err := w.walFile.Sync()
			fsyncDuration := time.Since(fsyncStart).Seconds()
			if err != nil {
				metrics.WalFsyncDurationSeconds.WithLabelValues("error").Observe(fsyncDuration)
				walLockStart5 := time.Now()
				w.mu.Lock()
				metrics.WALLockWaitDuration.WithLabelValues("sync_wait").Observe(time.Since(walLockStart5).Seconds())
				w.flushErr = err
				w.mu.Unlock()
			} else {
				metrics.WalFsyncDurationSeconds.WithLabelValues("success").Observe(fsyncDuration)
			}
		}
	}
}

// drainAndFlush drains channel and flushes remaining entries on stop
func (w *WALBatcher) drainAndFlush() {
	// Drain channel
	for {
		select {
		case entry := <-w.entries:
			walLockStart6 := time.Now()
			w.mu.Lock()
			metrics.WALLockWaitDuration.WithLabelValues("sync_complete").Observe(time.Since(walLockStart6).Seconds())
			w.batch = append(w.batch, entry)
			w.mu.Unlock()
		default:
			// Channel empty
			w.flush()
			return
		}
	}
}

// encodeWALEntryHeader encodes nameLen (uint32) and recLen (uint64) into a 12-byte slice
// using manual binary encoding to avoid reflection overhead from binary.Write
func encodeWALEntryHeader(nameLen uint32, recLen uint64) []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], nameLen)
	binary.LittleEndian.PutUint64(buf[4:12], recLen)
	return buf
}

// writeEntry serializes and writes a single entry to WAL
// writeEntryBytes writes a WAL entry and returns bytes written
func (w *WALBatcher) writeEntryBytes(entry WALEntry) (int, error) {
rec := entry.Record
name := entry.Name

if w.walFile == nil {
return 0, nil
}

// Format: [NameLen: uint32][Name: bytes][RecordLen: uint64][RecordBytes: bytes]
buf := w.bufPool.Get()
defer w.bufPool.Put(buf)
writer := ipc.NewWriter(buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(w.mem))
if err := writer.Write(rec); err != nil {
return 0, err
}
if err := writer.Close(); err != nil {
return 0, err
}
recBytes := buf.Bytes()

nameBytes := []byte(name)
nameLen := uint32(len(nameBytes))
recLen := uint64(len(recBytes))

// Write header + data (no sync here, batched at flush level)
// Use manual encoding to avoid reflection overhead from binary.Write
headerBuf := encodeWALEntryHeader(nameLen, recLen)
if _, err := w.walFile.Write(headerBuf[0:4]); err != nil { // nameLen
return 0, err
}
if _, err := w.walFile.Write(nameBytes); err != nil {
return 0, err
}
if _, err := w.walFile.Write(headerBuf[4:12]); err != nil { // recLen
return 0, err
}
if _, err := w.walFile.Write(recBytes); err != nil {
return 0, err
}

// Total bytes: 4 (nameLen) + len(name) + 8 (recLen) + len(recBytes)
totalBytes := 4 + len(nameBytes) + 8 + len(recBytes)
return totalBytes, nil
}

// Stop gracefully shuts down the batcher, flushing pending writes
func (w *WALBatcher) Stop() error {
	walLockStart7 := time.Now()
	w.mu.Lock()
	metrics.WALLockWaitDuration.WithLabelValues("close").Observe(time.Since(walLockStart7).Seconds())
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	// Signal stop and wait for flush loop to complete
	close(w.stopCh)
	<-w.doneCh

	// Stop async fsyncer if running (drains pending fsyncs)
	if w.asyncFsyncer != nil {
		_ = w.asyncFsyncer.Stop()
	}

	// Close file
	walLockStart8 := time.Now()
	w.mu.Lock()
	metrics.WALLockWaitDuration.WithLabelValues("replay").Observe(time.Since(walLockStart8).Seconds())
	defer w.mu.Unlock()
	if w.walFile != nil {
		// Final sync to ensure all data is persisted
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
	walLockStart9 := time.Now()
	w.mu.Lock()
	metrics.WALLockWaitDuration.WithLabelValues("reset").Observe(time.Since(walLockStart9).Seconds())
	defer w.mu.Unlock()
	return w.flushErr
}
// IsAsyncFsyncEnabled returns true if async fsync is configured and running
func (w *WALBatcher) IsAsyncFsyncEnabled() bool {
return w.asyncFsyncer != nil && w.asyncFsyncer.IsRunning()
}

// AsyncFsyncStats returns stats from the async fsyncer, or nil if not enabled
func (w *WALBatcher) AsyncFsyncStats() *AsyncFsyncerStats {
if w.asyncFsyncer == nil {
return nil
}
stats := w.asyncFsyncer.Stats()
return &stats
}


// IsAdaptiveEnabled returns whether adaptive batching is enabled
func (w *WALBatcher) IsAdaptiveEnabled() bool {
	return w.config.Adaptive.Enabled
}

// GetCurrentInterval returns the current flush interval
func (w *WALBatcher) GetCurrentInterval() time.Duration {
	if !w.config.Adaptive.Enabled {
		return w.config.FlushInterval
	}
	if w.rateTracker == nil || w.intervalCalc == nil {
		return w.config.FlushInterval
	}
	rate := w.rateTracker.GetRate()
	return w.intervalCalc.CalculateInterval(rate)
}
