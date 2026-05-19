//go:build linux && iouring

package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/iouring"
	"github.com/23skdu/longbow/internal/metrics"
)

// UringStorageBackend implements StorageBackend using our custom high-performance io_uring library.
type UringStorageBackend struct {
	f          *os.File
	ring       *iouring.Ring
	bufferPool *iouring.BufferPool
	path       string
	
	mu          sync.RWMutex
	active      bool
	nextID      uint64
	pendingRead  map[uint64]*storageRequest
	pendingWrite map[uint64]*storageRequest
	
	stopChan chan struct{}
}

type storageRequest struct {
	done chan int
	err  chan error
}

func NewUringStorageBackend(path string) (StorageBackend, error) {
	// 1. Open file with standard flags. 
	// Note: We don't use O_DIRECT here yet, but we will if requested via BufferPool alignment.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// 2. Initialize io_uring with 1024 depth
	ring, err := iouring.NewRing(1024, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to init io_uring: %w", err)
	}

	// 3. Optional: Initialize BufferPool for O_DIRECT alignment if needed
	// For now, we use a 1MB pool for large vectored I/O
	pool, _ := iouring.NewBufferPool(1024*1024, 128)

	b := &UringStorageBackend{
		f:            f,
		ring:         ring,
		bufferPool:   pool,
		path:         path,
		active:       true,
		pendingRead:  make(map[uint64]*storageRequest),
		pendingWrite: make(map[uint64]*storageRequest),
		stopChan:     make(chan struct{}),
	}

	go b.completionLoop()

	return b, nil
}

func (b *UringStorageBackend) ReadAt(p []byte, off int64) (int, error) {
	return b.submitOp(p, off, true)
}

func (b *UringStorageBackend) WriteAt(p []byte, off int64) (int, error) {
	return b.submitOp(p, off, false)
}

func (b *UringStorageBackend) submitOp(p []byte, off int64, isRead bool) (int, error) {
	b.mu.RLock()
	active := b.active
	b.mu.RUnlock()

	if !active {
		return 0, fmt.Errorf("backend inactive")
	}

	id := atomic.AddUint64(&b.nextID, 1)
	req := &storageRequest{
		done: make(chan int, 1),
		err:  make(chan error, 1),
	}

	b.mu.Lock()
	if isRead {
		b.pendingRead[id] = req
	} else {
		b.pendingWrite[id] = req
	}
	b.mu.Unlock()

	var err error
	if isRead {
		err = b.ring.SubmitRead(int(b.f.Fd()), p, uint64(off), id)
	} else {
		err = b.ring.SubmitWrite(int(b.f.Fd()), p, uint64(off), id)
	}

	if err != nil {
		b.mu.Lock()
		if isRead {
			delete(b.pendingRead, id)
		} else {
			delete(b.pendingWrite, id)
		}
		b.mu.Unlock()
		return 0, err
	}

	if _, err := b.ring.Flush(); err != nil {
		// Ignore flush error if submission succeeded
	}

	select {
	case n := <-req.done:
		if isRead {
			metrics.IOReadBytesTotal.WithLabelValues("disk_store").Add(float64(n))
			metrics.IOReadOpsTotal.WithLabelValues("disk_store").Inc()
		} else {
			metrics.IOWriteBytesTotal.WithLabelValues("disk_store").Add(float64(n))
			metrics.IOWriteOpsTotal.WithLabelValues("disk_store").Inc()
		}
		return n, nil
	case err := <-req.err:
		return 0, err
	case <-b.stopChan:
		return 0, fmt.Errorf("backend closed")
	}
}

func (b *UringStorageBackend) Readv(iovs [][]byte, off int64) (int, error) {
	// Simplified vectored I/O using multiple SQEs
	total := 0
	currOff := off
	for _, buf := range iovs {
		n, err := b.ReadAt(buf, currOff)
		if n > 0 {
			total += n
			currOff += int64(n)
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *UringStorageBackend) Writev(iovs [][]byte, off int64) (int, error) {
	total := 0
	currOff := off
	for _, buf := range iovs {
		n, err := b.WriteAt(buf, currOff)
		if n > 0 {
			total += n
			currOff += int64(n)
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *UringStorageBackend) Sync() error {
	// Standard Sync for WAL consistency
	return b.f.Sync()
}

func (b *UringStorageBackend) completionLoop() {
	for {
		select {
		case <-b.stopChan:
			return
		default:
			cqe, err := b.ring.Wait()
			if err != nil {
				return
			}
			if cqe == nil {
				continue
			}

			id := cqe.UserData
			res := cqe.Res

			b.mu.Lock()
			req, ok := b.pendingRead[id]
			if ok {
				delete(b.pendingRead, id)
			} else {
				req, ok = b.pendingWrite[id]
				if ok {
					delete(b.pendingWrite, id)
				}
			}
			b.mu.Unlock()

			if ok {
				if res < 0 {
					req.err <- fmt.Errorf("uring op failed: %d", res)
				} else {
					req.done <- int(res)
				}
				close(req.done)
				close(req.err)
			}

			b.ring.Advance(1)
		}
	}
}

func (b *UringStorageBackend) Close() error {
	b.mu.Lock()
	if !b.active {
		b.mu.Unlock()
		return nil
	}
	b.active = false
	b.mu.Unlock()

	close(b.stopChan)
	if b.ring != nil {
		_ = b.ring.Close()
	}
	if b.bufferPool != nil {
		_ = b.bufferPool.Close()
	}
	return b.f.Close()
}

func (b *UringStorageBackend) Size() (int64, error) {
	fi, err := b.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (b *UringStorageBackend) Name() string {
	return b.path
}
