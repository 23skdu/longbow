//go:build linux

package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/iouring"
)

// ReadRequest tracks an asynchronous read operation.
type ReadRequest struct {
	ID   uint64
	Done chan int
	Err  chan error
}

// UringReader utilizes io_uring for high-performance, concurrent non-blocking reads.
type UringReader struct {
	f    *os.File
	ring *iouring.Ring

	mu      sync.RWMutex
	active  bool
	nextID  uint64
	pending map[uint64]*ReadRequest

	stopChan chan struct{}
}

func NewUringReader(path string) (*UringReader, error) {
	f, err := os.Open(path) // #nosec G304 - path from direct caller
	if err != nil {
		return nil, err
	}

	// 1024 depth is reasonable for high-throughput ingestion
	ring, err := iouring.NewRing(1024, 0)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to init io_uring: %w", err)
	}

	r := &UringReader{
		f:        f,
		ring:     ring,
		active:   true,
		pending:  make(map[uint64]*ReadRequest),
		stopChan: make(chan struct{}),
	}

	go r.completionLoop()

	return r, nil
}

// ReadAt reads data into buf from the specified offset using io_uring.
// This implementation is thread-safe and allows multiple concurrent reads.
func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	r.mu.RLock()
	active := r.active
	r.mu.RUnlock()

	if !active {
		return 0, fmt.Errorf("reader inactive")
	}

	id := atomic.AddUint64(&r.nextID, 1)
	req := &ReadRequest{
		ID:   id,
		Done: make(chan int, 1),
		Err:  make(chan error, 1),
	}

	r.mu.Lock()
	r.pending[id] = req
	r.mu.Unlock()

	// Submit read operation
	if offset < 0 {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return 0, fmt.Errorf("negative offset: %d", offset)
	}
	err := r.ring.SubmitRead(int(r.f.Fd()), buf, uint64(offset), id)
	if err != nil {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return 0, fmt.Errorf("iouring submit error: %w", err)
	}

	// Flush to ensure kernel sees the request
	if _, err := r.ring.Flush(); err != nil {
		// Ignore error if some entries were submitted
	}

	// Wait for THIS request to complete
	select {
	case n := <-req.Done:
		return n, nil
	case err := <-req.Err:
		return 0, err
	case <-r.stopChan:
		return 0, fmt.Errorf("reader closed")
	}
}

func (r *UringReader) completionLoop() {
	for {
		select {
		case <-r.stopChan:
			return
		default:
			cqe, err := r.ring.Wait()
			if err != nil {
				return
			}
			if cqe == nil {
				continue
			}

			id := cqe.UserData
			res := cqe.Res

			r.mu.Lock()
			req, ok := r.pending[id]
			if ok {
				delete(r.pending, id)
			}
			r.mu.Unlock()

			if ok {
				if res < 0 {
					req.Err <- fmt.Errorf("async read failed: %d", res)
				} else {
					req.Done <- int(res)
				}
				close(req.Done)
				close(req.Err)
			}

			r.ring.Advance(1)
		}
	}
}

func (r *UringReader) Close() error {
	r.mu.Lock()
	if !r.active {
		r.mu.Unlock()
		return nil
	}
	r.active = false
	r.mu.Unlock()

	close(r.stopChan)

	if r.ring != nil {
		_ = r.ring.Close()
	}
	return r.f.Close()
}
