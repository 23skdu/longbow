package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
	Record    arrow.RecordBatch
	Name      string
	Seq       uint64
	Timestamp int64
}

// WALBatcherConfig configures the batched WAL writer
type WALBatcherConfig struct {
	FlushInterval time.Duration     // Time between flushes (e.g., 10ms)
	MaxBatchSize  int               // Max entries before forced flush (e.g., 100)
	Adaptive      AdaptiveWALConfig // Adaptive batching configuration
	AsyncFsync    AsyncFsyncConfig  // Async fsync configuration
	UseIOUring    bool              // Use io_uring backend if available
	UseDirectIO   bool              // Use Direct I/O if available
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
	backend      WALBackend
	batch        []WALEntry
	backBatch    []WALEntry // double-buffer: swap on flush to avoid allocation
	running      bool
	bufPool      *BytePool // pooled buffers for IPC serialization
	stopCh       chan struct{}
	doneCh       chan struct{}
	flushErr     error
	rateTracker  *WriteRateTracker           // Adaptive: tracks write rate
	intervalCalc *AdaptiveIntervalCalculator // Adaptive: calculates intervals
	asyncFsyncer *AsyncFsyncer               // Async: background fsync handler
}

// NewWALBatcher creates a new batched WAL writer
func NewWALBatcher(dataPath string, config *WALBatcherConfig) *WALBatcher {
	w := &WALBatcher{
		dataPath:  dataPath,
		config:    *config,
		mem:       memory.NewGoAllocator(),
		entries:   make(chan WALEntry, config.MaxBatchSize*100), // Increased capacity (10k by default) to handle bursts
		batch:     make([]WALEntry, 0, config.MaxBatchSize),
		backBatch: make([]WALEntry, 0, config.MaxBatchSize),
		bufPool:   NewBytePool(),
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

	// Open WAL backend
	walPath := filepath.Join(w.dataPath, walFileName)
	// We pass true for preferAsync and directIO if configured.
	backend, err := NewWALBackend(walPath, w.config.UseIOUring, w.config.UseDirectIO)
	if err != nil {
		return err
	}
	w.backend = backend
	w.running = true

	// Start async fsyncer if enabled
	if w.asyncFsyncer != nil {
		if f := w.backend.File(); f != nil {
			if err := w.asyncFsyncer.Start(f); err != nil {
				w.backend.Close()
				return err
			}
		}
	}

	// Start background flusher
	go w.flushLoop()

	return nil
}

// Write queues a record for batched WAL writing (non-blocking)
func (w *WALBatcher) Write(rec arrow.RecordBatch, name string, seq uint64, ts int64) error {
	if rec == nil {
		return nil
	}

	// Validate record integrity before queuing for WAL
	if err := validateRecordBatch(rec); err != nil {
		metrics.ValidationFailuresTotal.WithLabelValues("WAL_Write", "invalid_batch").Inc()
		return err
	}

	// Retain the record since we're passing it to another goroutine
	rec.Retain()

	// Track write for adaptive interval calculation
	if w.rateTracker != nil {
		w.rateTracker.RecordWrite()
	}

	select {
	case w.entries <- WALEntry{Record: rec, Name: name, Seq: seq, Timestamp: ts}:
		return nil
	case <-w.stopCh:
		rec.Release()
		return NewUnavailableError("write", "batcher stopped")
	default:
		// Queue full - non-blocking fail to prevent deadlock
		rec.Release()
		return NewResourceExhaustedError("wal", "write queue full")
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

	// Swap batch to avoid holding lock during serialization/IO
	batch := w.batch
	w.batch = w.backBatch[:0]
	w.backBatch = batch // double-buffer swap
	w.mu.Unlock()

	metrics.WalBatchSize.Observe(float64(len(batch)))

	// Aggregate and serialize
	var multiBatchBuf bytes.Buffer // Using a simple buffer for aggregation. Could pool this.

	// We need a scratch buffer for each record serialization
	scratchBuf := w.bufPool.Get() // Get one scratch buffer for serialization
	defer w.bufPool.Put(scratchBuf)

	for _, entry := range batch {
		// Serialize entry into scratchBuf then append to multiBatchBuf
		// Or better: write directly to multiBatchBuf?
		// IPC Writer needs seekable/writer.
		// Let's use serializeEntry helper to append to multiBatchBuf.
		if err := w.serializeEntry(&multiBatchBuf, entry, scratchBuf); err != nil {
			w.handleFlushError(err)
			// release all
			for _, e := range batch {
				e.Record.Release()
			}
			return
		}
		// Release retained record immediately after serialization
		entry.Record.Release()
	}

	data := multiBatchBuf.Bytes()
	if len(data) == 0 {
		return
	}

	// Single Write call
	n, err := w.backend.Write(data)
	if err != nil {
		w.handleFlushError(err)
		return
	}
	metrics.WalWritesTotal.WithLabelValues("ok").Inc()
	metrics.WalBytesWritten.Add(float64(n))

	// Sync
	// Logic for async/sync remains similar, but simplified: call backend.Sync() if needed.
	// If AsyncFsyncer is managed externally/via interface, we use it.
	// For now, blocking sync as fallback or if configured.

	// TODO: Integrate AsyncFsyncer with WALBackend interface properly.
	// Falling back to blocking Sync for correctness in this refactor.
	if err := w.backend.Sync(); err != nil {
		w.handleFlushError(err)
	}
}

func (w *WALBatcher) handleFlushError(err error) {
	w.mu.Lock()
	w.flushErr = err
	w.mu.Unlock()
	metrics.WalWritesTotal.WithLabelValues("error").Inc()
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

// encodeWALEntryHeader encodes crc(uint32), seq(uint64), ts(int64), nameLen(uint32) and recLen(uint64) into a 32-byte slice
func encodeWALEntryHeader(crc uint32, seq uint64, ts int64, nameLen uint32, recLen uint64) []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint64(buf[4:12], seq)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts))
	binary.LittleEndian.PutUint32(buf[20:24], nameLen)
	binary.LittleEndian.PutUint64(buf[24:32], recLen)
	return buf
}

// writeEntry serializes and writes a single entry to WAL
// writeEntryBytes writes a WAL entry and returns bytes written
// serializeEntry appends serialized entry to buffer
func (w *WALBatcher) serializeEntry(out *bytes.Buffer, entry WALEntry, scratch *bytes.Buffer) error {
	scratch.Reset()
	writer := ipc.NewWriter(scratch, ipc.WithSchema(entry.Record.Schema()), ipc.WithAllocator(w.mem))
	if err := writer.Write(entry.Record); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recBytes := scratch.Bytes()

	nameBytes := []byte(entry.Name)
	nameLen := uint32(len(nameBytes))
	recLen := uint64(len(recBytes))

	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	header := encodeWALEntryHeader(checksum, entry.Seq, entry.Timestamp, nameLen, recLen)

	if _, err := out.Write(header); err != nil {
		return err
	}
	if _, err := out.Write(nameBytes); err != nil {
		return err
	}
	if _, err := out.Write(recBytes); err != nil {
		return err
	}

	return nil
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
	if w.backend != nil {
		// Final sync to ensure all data is persisted
		if err := w.backend.Sync(); err != nil {
			return err
		}
		if err := w.backend.Close(); err != nil {
			return err
		}
		w.backend = nil
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

// QueueDepth returns the current number of pending entries
func (w *WALBatcher) QueueDepth() int {
	return len(w.entries)
}

// QueueCapacity returns the maximum capacity of the queue
func (w *WALBatcher) QueueCapacity() int {
	return cap(w.entries)
}
