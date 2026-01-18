package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang/snappy"
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
	FlushInterval  time.Duration     // Time between flushes (e.g., 10ms)
	MaxBatchSize   int               // Max entries before forced flush (e.g., 100)
	Adaptive       AdaptiveWALConfig // Adaptive batching configuration
	AsyncFsync     AsyncFsyncConfig  // Async fsync configuration
	UseIOUring     bool              // Use io_uring backend if available
	UseDirectIO    bool              // Use Direct I/O if available
	WALCompression bool              // Enable Snappy block compression
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
	mu           sync.Mutex // Only for backend I/O, not for batching
	backend      WALBackend
	ringBuffer   *WALRingBuffer // Lock-free ring buffer for batching
	running      bool
	bufPool      *pool.BytePool // pooled buffers for IPC serialization
	stopCh       chan struct{}
	doneCh       chan struct{}
	flushCh      chan chan error // Channel for synchronous flush requests
	flushErr     error
	ErrCh        chan error                  // Channel for async flush errors
	rateTracker  *WriteRateTracker           // Adaptive: tracks write rate
	intervalCalc *AdaptiveIntervalCalculator // Adaptive: calculates intervals
	asyncFsyncer *AsyncFsyncer               // Async: background fsync handler
	flushBuf     bytes.Buffer                // Reused buffer for flush serialization
}

// compressBufPool is a global pool for compression buffers
var compressBufPool = sync.Pool{
	New: func() interface{} {
		// Start with 64KB, will grow as needed
		buf := make([]byte, 0, 64*1024)
		return &buf
	},
}

// NewWALBatcher creates a new batched WAL writer
func NewWALBatcher(dataPath string, config *WALBatcherConfig) *WALBatcher {
	// Use ring buffer capacity = MaxBatchSize * 2 for headroom
	ringCapacity := config.MaxBatchSize * 2
	w := &WALBatcher{
		dataPath:   dataPath,
		config:     *config,
		mem:        memory.NewGoAllocator(),
		entries:    make(chan WALEntry, config.MaxBatchSize*100), // Increased capacity (10k by default) to handle bursts
		ringBuffer: NewWALRingBuffer(ringCapacity),
		bufPool:    pool.NewBytePool(),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		flushCh:    make(chan chan error),
		ErrCh:      make(chan error, 1),
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
				_ = w.backend.Close()
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
		return core.NewUnavailableError("write", "batcher stopped")
	default:
		// Queue full - non-blocking fail to prevent deadlock
		rec.Release()
		return core.NewResourceExhaustedError("wal", "write queue full")
	}
}

// flushLoop runs in background, batching and flushing writes
func (w *WALBatcher) flushLoop() {
	defer close(w.doneCh)

	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	// Local buffer to accumulate entries before locking
	// We use this to drain the channel and minimize lock acquisitions.
	localBatch := make([]WALEntry, 0, w.config.MaxBatchSize)

	for {
		select {
		case entry := <-w.entries:
			// 1. Drain channel eagerly up to MaxBatchSize
			localBatch = localBatch[:0]
			localBatch = append(localBatch, entry)

			// Try to drain more if available
		DrainLoop:
			for len(localBatch) < w.config.MaxBatchSize {
				select {
				case e := <-w.entries:
					localBatch = append(localBatch, e)
				default:
					break DrainLoop
				}
			}

			// 2. Push to ring buffer (lock-free)
			// Track pending entries (backpressure indicator)
			metrics.WalPendingEntries.Set(float64(len(w.entries)))

			// Push entries to ring buffer
			pushCount := 0
			for _, e := range localBatch {
				if w.ringBuffer.Push(e) {
					pushCount++
					metrics.WalRingBufferPushesTotal.Inc()
				} else {
					// Buffer full - flush immediately
					metrics.WalRingBufferFullTotal.Inc()
					break
				}
			}

			// Update ring buffer utilization metric
			utilization := float64(w.ringBuffer.Len()) / float64(w.ringBuffer.Cap())
			metrics.WalRingBufferUtilization.Set(utilization)

			// Flush if buffer is getting full or we hit capacity
			shouldFlush := w.ringBuffer.Len() >= w.config.MaxBatchSize || pushCount < len(localBatch)
			if shouldFlush {
				w.flush()
			}

		case <-ticker.C:
			w.flush()

		case ch := <-w.flushCh:
			// Synchronous flush request
			w.drainChannelNonBlocking()
			w.flush()
			ch <- w.flushErr

		case <-w.stopCh:
			// Drain remaining entries
			w.drainAndFlush()
			return
		}
	}
}

// flush writes all batched entries to disk
func (w *WALBatcher) flush() {
	// Drain entries from ring buffer
	batch := make([]WALEntry, 0, w.config.MaxBatchSize)
	count := w.ringBuffer.Drain(&batch)

	if count == 0 {
		return
	}

	metrics.WalRingBufferDrainsTotal.Inc()
	metrics.WalBatchSize.Observe(float64(count))

	// Update utilization after drain
	utilization := float64(w.ringBuffer.Len()) / float64(w.ringBuffer.Cap())
	metrics.WalRingBufferUtilization.Set(utilization)

	// Prepare output buffer
	w.flushBuf.Reset()

	// If compression enabled, we compress the *entire payload* of the batch.
	// But simply concatenating serialized entries is easiest for replay compatibility.
	// We will serialize all entries into a buffer, then compress that buffer,
	// then write a SINGLE header for the compressed block.

	var payload []byte

	if w.config.WALCompression {
		// 1. Serialize all entries to a temporary buffer
		// Reuse a buffer from the pool for the raw batch
		rawBatch := w.bufPool.Get()
		defer w.bufPool.Put(rawBatch)

		scratchBuf := w.bufPool.Get()
		defer w.bufPool.Put(scratchBuf)

		for _, entry := range batch {
			if err := w.serializeEntry(rawBatch, entry, scratchBuf); err != nil {
				w.handleFlushError(err)
				for _, e := range batch {
					e.Record.Release()
				}
				return
			}
			entry.Record.Release()
		}

		// 2. Compress the raw batch
		// MaxEncodedLen ensures we have enough space
		src := rawBatch.Bytes()
		maxLen := snappy.MaxEncodedLen(len(src))

		// Get a buffer from the pool
		compressBufPtr := compressBufPool.Get().(*[]byte)
		compressBuf := *compressBufPtr

		// Ensure buffer is large enough
		if cap(compressBuf) < maxLen {
			compressBuf = make([]byte, 0, maxLen)
		}

		// Compress into the buffer
		dest := snappy.Encode(compressBuf[:0], src)
		payload = dest

		// Return buffer to pool (store the potentially grown buffer)
		*compressBufPtr = dest
		defer compressBufPool.Put(compressBufPtr)

		// 3. Construct Compressed Block Header
		// Checksum = 0xFFFFFFFF (Sentinel)
		// Seq = maxSeq in batch (to update flushedSeq correctly during replay if needed, though replay usually uses entry seqs)
		// We use the LAST entry's sequence for the block header.
		lastSeq := batch[len(batch)-1].Seq

		// Header fields:
		// Checksum: Sentinel
		// Seq: LastSeq
		// Ts: 0 (unused)
		// NameLen: 1 (Compression Type: 1=Snappy)
		// RecLen: len(payload)

		var header [32]byte
		encodeWALEntryHeader(header[:], 0xFFFFFFFF, lastSeq, 0, 1, uint64(len(payload)))

		w.flushBuf.Write(header[:])
		w.flushBuf.Write([]byte{1}) // Name (Type=1)
		w.flushBuf.Write(payload)   // Record (Compressed Data)

	} else {
		// Uncompressed: Serialize each entry directly to flushBuf
		scratchBuf := w.bufPool.Get()
		defer w.bufPool.Put(scratchBuf)

		for _, entry := range batch {
			if err := w.serializeEntry(&w.flushBuf, entry, scratchBuf); err != nil {
				w.handleFlushError(err)
				for _, e := range batch {
					e.Record.Release()
				}
				return
			}
			entry.Record.Release()
		}
	}

	data := w.flushBuf.Bytes()
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
	// Use AsyncFsyncer if enabled, otherwise block
	if w.asyncFsyncer != nil {
		w.asyncFsyncer.AddDirtyBytes(int64(n))
		w.asyncFsyncer.RequestFsyncIfNeeded()
	} else {
		// Fallback to blocking Sync
		if err := w.backend.Sync(); err != nil {
			w.handleFlushError(err)
		}
	}
}

func (w *WALBatcher) handleFlushError(err error) {
	w.mu.Lock()
	w.flushErr = err
	w.mu.Unlock()
	metrics.WalWritesTotal.WithLabelValues("error").Inc()

	// Try to report error
	select {
	case w.ErrCh <- err:
	default:
	}
}

// drainAndFlush drains channel and flushes remaining entries on stop
func (w *WALBatcher) drainAndFlush() {
	// Drain channel
	for {
		select {
		case entry := <-w.entries:
			// Push to ring buffer
			w.ringBuffer.Push(entry)
		default:
			// Channel empty
			w.flush()
			return
		}
	}
}

// encodeWALEntryHeader encodes header into provided buffer
func encodeWALEntryHeader(buf []byte, crc uint32, seq uint64, ts int64, nameLen uint32, recLen uint64) {
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint64(buf[4:12], seq)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts))
	binary.LittleEndian.PutUint32(buf[20:24], nameLen)
	binary.LittleEndian.PutUint64(buf[24:32], recLen)
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

	// Zero-allocation string length
	nameLen := uint32(len(entry.Name))
	recLen := uint64(len(recBytes))

	// Calculate CRC
	crc := crc32.NewIEEE()
	// crc.Write([]byte(string)) does allocation?
	// Go optimized string->byte conversion for Read/Write?
	// standard library handles this optimized in recent versions?
	// Manually unsafe slice? For now, we prefer safety.
	// io.WriteString is not on hash.Hash.
	// But we can cast safely if needed.
	// Let's allocation here is small (name is short).
	// Ideally we accept that or use unsafe if critical.
	_, _ = crc.Write([]byte(entry.Name))
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	// Reserve header space (32 bytes)
	// We can write header directly to out?
	var header [32]byte
	encodeWALEntryHeader(header[:], checksum, entry.Seq, entry.Timestamp, nameLen, recLen)

	if _, err := out.Write(header[:]); err != nil {
		return err
	}
	if _, err := out.WriteString(entry.Name); err != nil { // Use WriteString
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

// QueueDepth returns the current number of pending entries and the capacity.
func (w *WALBatcher) QueueStatus() (pending, batchSize int) {
	return len(w.entries), cap(w.entries)
}

// Flush synchronously flushes all pending writes to disk
func (w *WALBatcher) Flush() error {
	if !w.running {
		return nil
	}
	ch := make(chan error, 1)
	select {
	case w.flushCh <- ch:
		return <-ch
	case <-w.doneCh:
		return fmt.Errorf("batcher stopped")
	}
}

// drainChannelNonBlocking drains pending items from channel into batch
func (w *WALBatcher) drainChannelNonBlocking() {
	for {
		select {
		case entry := <-w.entries:
			w.ringBuffer.Push(entry)
		default:
			return
		}
	}
}
