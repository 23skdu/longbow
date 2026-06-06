//go:build linux

package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/iouring"
)

type ReadResult struct {
	N   int
	Err error
}

// ReadRequest tracks an asynchronous read operation.
type ReadRequest struct {
	ID     uint64
	Result chan ReadResult
}

// UringReader utilizes io_uring for high-performance, concurrent non-blocking reads.
type UringReader struct {
	f    *os.File
	ring *iouring.Ring

	mu       sync.RWMutex
	submitMu sync.Mutex // serializes SQ submissions (Ring.Submit is single-producer only)
	active   bool
	nextID   uint64
	pending  map[uint64]*ReadRequest

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
// It retries on short reads to satisfy the io.ReaderAt contract.
func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	r.mu.RLock()
	active := r.active
	r.mu.RUnlock()

	if !active {
		return 0, fmt.Errorf("reader inactive")
	}

	if offset < 0 {
		return 0, fmt.Errorf("negative offset: %d", offset)
	}

	total := 0
	remaining := buf
	curOffset := offset

	for len(remaining) > 0 {
		id := atomic.AddUint64(&r.nextID, 1)
		req := &ReadRequest{
			ID:     id,
			Result: make(chan ReadResult, 1),
		}

		r.mu.Lock()
		r.pending[id] = req
		r.mu.Unlock()

		// SubmitRead + Flush must be serialized: Ring.Submit is single-producer only.
		// submitMu is separate from mu to avoid blocking the completionLoop.
		r.submitMu.Lock()
		err := r.ring.SubmitRead(int(r.f.Fd()), remaining, uint64(curOffset), id)
		if err != nil {
			r.submitMu.Unlock()
			r.mu.Lock()
			delete(r.pending, id)
			r.mu.Unlock()
			return total, fmt.Errorf("iouring submit error: %w", err)
		}
		// Flush to ensure kernel sees the request before releasing the submit lock.
		_, _ = r.ring.Flush()
		r.submitMu.Unlock()

		// Wait for THIS request to complete
		var n int
		select {
		case res := <-req.Result:
			if res.Err != nil {
				return total, res.Err
			}
			n = res.N
		case <-r.stopChan:
			return total, fmt.Errorf("reader closed")
		}

		if n == 0 {
			// EOF reached before filling buffer
			return total, fmt.Errorf("unexpected EOF at offset %d (read 0 of %d bytes)", curOffset, len(remaining))
		}

		total += n
		curOffset += int64(n)
		remaining = remaining[n:]
	}

	return total, nil
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
				var result ReadResult
				if res < 0 {
					result.Err = fmt.Errorf("async read failed: %d", res)
				} else {
					result.N = int(res)
				}
				req.Result <- result
				close(req.Result)
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
