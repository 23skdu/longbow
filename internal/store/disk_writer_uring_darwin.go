//go:build darwin && !linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// DiskWriterUring is the macOS implementation using Direct I/O (F_NOCACHE)
// and a high-performance worker pool to simulate non-blocking behavior
// while avoiding page cache overhead.
type DiskWriterUring struct {
	f      *os.File
	mu     sync.RWMutex
	active bool

	// Worker pool for async I/O
	writeQueue chan *darwinWriteReq
	wg         sync.WaitGroup
	stopChan   chan struct{}
}

type darwinWriteReq struct {
	data   []byte
	offset int64
	done   chan error
}

// NewDiskWriterUring creates a new DiskWriterUring instance for macOS.
func NewDiskWriterUring(path string, bufferSize int, maxBuffers int) (*DiskWriterUring, error) {
	path = filepath.Clean(path)
	// Open with O_SYNC to ensure metadata is flushed, and we'll use F_NOCACHE for data.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0600) // #nosec G304 G302
	if err != nil {
		return nil, err
	}

	// Set F_NOCACHE for Direct I/O behavior
	_, fcntlErr := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1)
	if fcntlErr != nil {
		// Fallback if F_NOCACHE fails, but log it
		fmt.Printf("Warning: failed to set F_NOCACHE on %s: %v\n", path, fcntlErr)
	}

	d := &DiskWriterUring{
		f:          f,
		active:     true,
		writeQueue: make(chan *darwinWriteReq, maxBuffers),
		stopChan:   make(chan struct{}),
	}

	// Start a pool of workers (matching hardware parallelism or fixed count)
	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		d.wg.Add(1)
		go d.ioWorker()
	}

	return d, nil
}

func (d *DiskWriterUring) ioWorker() {
	defer d.wg.Done()
	for {
		select {
		case req := <-d.writeQueue:
			_, err := d.f.WriteAt(req.data, req.offset)
			req.done <- err
			close(req.done)
		case <-d.stopChan:
			return
		}
	}
}

// SubmitWrite submits a write request to the background worker pool.
func (d *DiskWriterUring) SubmitWrite(data []byte, offset int64) (chan error, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.active {
		return nil, fmt.Errorf("disk writer inactive")
	}

	// Copy data to ensure it remains valid during async write (since the caller might reuse the buffer)
	// In a real io_uring implementation, we'd use a BufferPool to avoid this copy.
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	done := make(chan error, 1)
	req := &darwinWriteReq{
		data:   dataCopy,
		offset: offset,
		done:   done,
	}

	select {
	case d.writeQueue <- req:
		return done, nil
	default:
		return nil, fmt.Errorf("disk writer queue full")
	}
}

// Flush is a no-op as we use O_SYNC and F_NOCACHE.
func (d *DiskWriterUring) Flush() {
	// Wait for queue to empty if needed, but SubmitWrite already returns a chan.
	_ = d.f.Sync()
}

// Close closes the underlying file and deactivates the writer.
func (d *DiskWriterUring) Close() error {
	d.mu.Lock()
	if !d.active {
		d.mu.Unlock()
		return nil
	}
	d.active = false
	d.mu.Unlock()

	close(d.stopChan)
	d.wg.Wait()

	return d.f.Close()
}
