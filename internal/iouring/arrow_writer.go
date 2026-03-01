//go:build linux

package iouring

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowWriter provides zero-copy Arrow IPC serialization with io_uring
type ArrowWriter struct {
	ring       *Ring
	schema     *arrow.Schema
	bufferPool *BufferPool
	mem        memory.Allocator

	// Reusable IPC writer
	ipcWriter *ipc.Writer
	writeBuf  bytes.Buffer

	// Completion tracking
	completions map[uint64]*WriteRequest
	mu          sync.Mutex
	nextID      uint64
}

// WriteRequest tracks an async write operation
type WriteRequest struct {
	ID     uint64
	Buffer []byte // Aligned buffer from pool
	Offset int64  // File offset
	Result int32  // Completion result
	Done   chan struct{}
}

// NewArrowWriter creates a new Arrow IPC writer with io_uring
func NewArrowWriter(ring *Ring, schema *arrow.Schema, pool *BufferPool) (*ArrowWriter, error) {
	if ring == nil {
		return nil, fmt.Errorf("ring cannot be nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}
	if pool == nil {
		return nil, fmt.Errorf("buffer pool cannot be nil")
	}

	w := &ArrowWriter{
		ring:        ring,
		schema:      schema,
		bufferPool:  pool,
		mem:         memory.NewGoAllocator(),
		completions: make(map[uint64]*WriteRequest),
	}

	// Setup IPC writer
	w.ipcWriter = ipc.NewWriter(&w.writeBuf,
		ipc.WithSchema(schema),
		ipc.WithAllocator(w.mem),
	)

	return w, nil
}

// WriteRecordBatch serializes and writes a RecordBatch asynchronously
// Returns a WriteRequest that can be used to wait for completion
func (w *ArrowWriter) WriteRecordBatch(rec arrow.RecordBatch, offset int64) (*WriteRequest, error) {
	// Get an aligned buffer from the pool
	buf := w.bufferPool.Get()
	if buf == nil {
		return nil, fmt.Errorf("buffer pool exhausted")
	}

	// Serialize RecordBatch to buffer
	w.writeBuf.Reset()
	if err := w.ipcWriter.Write(rec); err != nil {
		w.bufferPool.Put(buf)
		return nil, fmt.Errorf("IPC serialization failed: %w", err)
	}

	data := w.writeBuf.Bytes()
	if len(data) > len(buf) {
		w.bufferPool.Put(buf)
		return nil, fmt.Errorf("record too large for buffer: %d > %d", len(data), len(buf))
	}

	// Copy to aligned buffer
	copy(buf, data)

	// Create write request
	w.mu.Lock()
	id := w.nextID
	w.nextID++
	req := &WriteRequest{
		ID:     id,
		Buffer: buf[:len(data)],
		Offset: offset,
		Done:   make(chan struct{}),
	}
	w.completions[id] = req
	w.mu.Unlock()

	// Submit write operation
	if err := w.ring.SubmitWrite(0, req.Buffer, uint64(offset), id); err != nil {
		w.mu.Lock()
		delete(w.completions, id)
		w.mu.Unlock()
		w.bufferPool.Put(buf)
		return nil, fmt.Errorf("submit failed: %w", err)
	}

	return req, nil
}

// WriteV performs vectored write of multiple records
func (w *ArrowWriter) WriteV(records []arrow.RecordBatch, offset int64) (*WriteRequest, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to write")
	}

	// Calculate total size needed
	totalSize := 0
	sizes := make([]int, len(records))

	for i, rec := range records {
		w.writeBuf.Reset()
		if err := w.ipcWriter.Write(rec); err != nil {
			return nil, fmt.Errorf("IPC serialization failed for record %d: %w", i, err)
		}
		sizes[i] = w.writeBuf.Len()
		totalSize += sizes[i]
	}

	// Get aligned buffer
	buf := w.bufferPool.Get()
	if buf == nil {
		return nil, fmt.Errorf("buffer pool exhausted")
	}

	if totalSize > len(buf) {
		w.bufferPool.Put(buf)
		return nil, fmt.Errorf("total size too large for buffer: %d > %d", totalSize, len(buf))
	}

	// Serialize all records into single buffer
	off := 0
	iovs := make([]IOVec, len(records))
	for i, rec := range records {
		w.writeBuf.Reset()
		if err := w.ipcWriter.Write(rec); err != nil {
			w.bufferPool.Put(buf)
			return nil, fmt.Errorf("IPC serialization failed: %w", err)
		}
		copy(buf[off:], w.writeBuf.Bytes())
		iovs[i] = IOVec{
			Base: unsafe.Pointer(&buf[off]),
			Len:  uint64(sizes[i]),
		}
		off += sizes[i]
	}

	// Create request
	w.mu.Lock()
	id := w.nextID
	w.nextID++
	req := &WriteRequest{
		ID:     id,
		Buffer: buf[:totalSize],
		Offset: offset,
		Done:   make(chan struct{}),
	}
	w.completions[id] = req
	w.mu.Unlock()

	// Submit vectored write
	if err := w.ring.SubmitVectored(0, iovs, uint64(offset), id); err != nil {
		w.mu.Lock()
		delete(w.completions, id)
		w.mu.Unlock()
		w.bufferPool.Put(buf)
		return nil, fmt.Errorf("vectored submit failed: %w", err)
	}

	return req, nil
}

// ProcessCompletions processes available completions
// Call this periodically or in a dedicated goroutine
func (w *ArrowWriter) ProcessCompletions() int {
	processed := 0

	for {
		cqe := w.ring.Peek()
		if cqe == nil {
			break
		}

		id := cqe.UserData

		w.mu.Lock()
		req, ok := w.completions[id]
		if ok {
			req.Result = cqe.Res
			delete(w.completions, id)
		}
		w.mu.Unlock()

		if ok {
			// Return buffer to pool
			w.bufferPool.Put(req.Buffer)
			close(req.Done)
		}

		w.ring.Advance(1)
		processed++
	}

	return processed
}

// Wait waits for a specific write request to complete
func (w *WriteRequest) Wait() int32 {
	<-w.Done
	return w.Result
}

// Close releases resources
func (w *ArrowWriter) Close() error {
	if w.ipcWriter != nil {
		if err := w.ipcWriter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// ArrowReader provides zero-copy Arrow IPC deserialization with io_uring
type ArrowReader struct {
	ring       *Ring
	schema     *arrow.Schema
	bufferPool *BufferPool
	mem        memory.Allocator
}

// NewArrowReader creates a new Arrow IPC reader with io_uring
func NewArrowReader(ring *Ring, schema *arrow.Schema, pool *BufferPool) (*ArrowReader, error) {
	return &ArrowReader{
		ring:       ring,
		schema:     schema,
		bufferPool: pool,
		mem:        memory.NewGoAllocator(),
	}, nil
}

// ReadRecordBatch reads a RecordBatch from the specified offset
func (r *ArrowReader) ReadRecordBatch(offset int64, size int) (arrow.RecordBatch, error) {
	// Get aligned buffer
	buf := r.bufferPool.Get()
	if buf == nil {
		return nil, fmt.Errorf("buffer pool exhausted")
	}

	if size > len(buf) {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("read size too large for buffer: %d > %d", size, len(buf))
	}

	// Submit read operation
	if err := r.ring.SubmitRead(0, buf[:size], uint64(offset), 0); err != nil {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("submit failed: %w", err)
	}

	// Wait for completion
	cqe, err := r.ring.Wait()
	if err != nil {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("wait failed: %w", err)
	}

	if cqe.Res < 0 {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("read failed: %d", cqe.Res)
	}

	// Deserialize IPC data
	reader, err := ipc.NewReader(bytes.NewReader(buf[:cqe.Res]),
		ipc.WithSchema(r.schema),
		ipc.WithAllocator(r.mem),
	)
	if err != nil {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("IPC reader creation failed: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("no record in IPC data")
	}

	rec := reader.RecordBatch()

	// Return buffer to pool
	r.bufferPool.Put(buf)

	return rec, nil
}

// Ensure ArrowWriter implements necessary interfaces
var _ io.Closer = (*ArrowWriter)(nil)
