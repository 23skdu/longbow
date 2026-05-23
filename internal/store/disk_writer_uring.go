//go:build linux

package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/iouring"
)

// WriteRequest tracks an asynchronous write operation.
type WriteRequest struct {
	ID     uint64
	Buffer []byte
	Offset int64
	Result int32
	Done   chan error
}

// DiskWriterUring utilizes io_uring for high-performance, asynchronous snapshots.
type DiskWriterUring struct {
	f          *os.File
	ring       *iouring.Ring
	bufferPool *iouring.BufferPool

	mu          sync.RWMutex
	active      bool
	nextID      uint64
	pending     map[uint64]*WriteRequest
	pendingWait sync.WaitGroup

	closeOnce sync.Once
	stopChan  chan struct{}
}

// NewDiskWriterUring creates a new DiskWriterUring instance with io_uring support.
func NewDiskWriterUring(path string, bufferSize int, maxBuffers int) (*DiskWriterUring, error) {
	// 1. Open file with Direct I/O for bypass kernel page cache
	// Note: O_DIRECT is handled via iouring if possible, or we can use it here.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// 2. Initialize io_uring with 1024 depth
	ring, err := iouring.NewRing(1024, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to init io_uring: %w", err)
	}

	// 3. Initialize BufferPool for O_DIRECT alignment
	pool, err := iouring.NewBufferPool(bufferSize, maxBuffers)
	if err != nil {
		ring.Close()
		f.Close()
		return nil, fmt.Errorf("failed to init buffer pool: %w", err)
	}

	d := &DiskWriterUring{
		f:          f,
		ring:       ring,
		bufferPool: pool,
		active:     true,
		pending:    make(map[uint64]*WriteRequest),
		stopChan:   make(chan struct{}),
	}

	// 4. Start background completion handler
	go d.completionLoop()

	return d, nil
}

// SubmitWrite handles an asynchronous write using io_uring SQE.
// It returns a channel that will receive the error (or nil) when the write completes.
func (d *DiskWriterUring) SubmitWrite(data []byte, offset int64) (chan error, error) {
	d.mu.RLock()
	active := d.active
	d.mu.RUnlock()

	if !active {
		return nil, fmt.Errorf("disk writer inactive")
	}

	// 1. Get aligned buffer from pool
	buf := d.bufferPool.GetWait() // Block if pool is empty to throttle ingestion
	if len(data) > len(buf) {
		d.bufferPool.Put(buf)
		return nil, fmt.Errorf("data too large for buffer pool (%d > %d)", len(data), len(buf))
	}

	// 2. Copy data to aligned buffer
	copy(buf, data)

	// 3. Create request
	id := atomic.AddUint64(&d.nextID, 1)
	req := &WriteRequest{
		ID:     id,
		Buffer: buf[:len(data)],
		Offset: offset,
		Done:   make(chan error, 1),
	}

	d.mu.Lock()
	d.pending[id] = req
	d.pendingWait.Add(1)
	d.mu.Unlock()

	// 4. Submit write operation (non-blocking)
	err := d.ring.SubmitWrite(int(d.f.Fd()), req.Buffer, uint64(offset), id)
	if err != nil {
		d.mu.Lock()
		delete(d.pending, id)
		d.pendingWait.Done()
		d.mu.Unlock()
		d.bufferPool.Put(buf)
		return nil, fmt.Errorf("iouring submit error: %w", err)
	}

	// 5. Trigger submission to kernel
	_, err = d.ring.Flush()
	if err != nil {
		// Even if flush fails, the SQE is in the ring.
		// But we should probably report it.
		return nil, fmt.Errorf("iouring flush error: %w", err)
	}

	return req.Done, nil
}

// Flush waits for all pending writes to complete.
func (d *DiskWriterUring) Flush() {
	d.pendingWait.Wait()
}

func (d *DiskWriterUring) completionLoop() {
	for {
		select {
		case <-d.stopChan:
			return
		default:
			cqe, err := d.ring.Wait()
			if err != nil {
				// Handle ring error (e.g. closed)
				return
			}

			if cqe == nil {
				continue
			}

			id := cqe.UserData
			res := cqe.Res

			d.mu.Lock()
			req, ok := d.pending[id]
			if ok {
				delete(d.pending, id)
			}
			d.mu.Unlock()

			if ok {
				var finalErr error
				if res < 0 {
					finalErr = fmt.Errorf("async write failed: %d", res)
				} else if int(res) < len(req.Buffer) {
					finalErr = fmt.Errorf("short write: %d < %d", res, len(req.Buffer))
				}

				req.Done <- finalErr
				close(req.Done)

				d.bufferPool.Put(req.Buffer)
				d.pendingWait.Done()
			}

			d.ring.Advance(1)
		}
	}
}

// Close closes the underlying file and deactivates the writer.
func (d *DiskWriterUring) Close() error {
	var err error
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.active = false
		d.mu.Unlock()

		close(d.stopChan)

		// Wait for pending writes
		d.Flush()

		if d.ring != nil {
			err = d.ring.Close()
		}

		if d.bufferPool != nil {
			_ = d.bufferPool.Close()
		}

		closeErr := d.f.Close()
		if err == nil {
			err = closeErr
		}
	})
	return err
}
