//go:build linux

package store

import (
	"fmt"
	"os"
	"sync"
	"github.com/23skdu/longbow/internal/iouring"
)

// UringReader utilizes io_uring for high-performance, non-blocking reads.
type UringReader struct {
	f      *os.File
	ring   *iouring.Ring
	mu     sync.RWMutex
	active bool
}

func NewUringReader(path string) (*UringReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// 1024 depth is reasonable for high-throughput ingestion
	ring, err := iouring.NewRing(1024, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to init io_uring: %w", err)
	}

	return &UringReader{
		f:      f,
		ring:   ring,
		active: true,
	}, nil
}

// ReadAt reads data into buf from the specified offset using io_uring.
func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.active {
		return 0, fmt.Errorf("reader inactive")
	}

	// Submit read operation
	err := r.ring.SubmitRead(int(r.f.Fd()), buf, uint64(offset), 0)
	if err != nil {
		return 0, fmt.Errorf("iouring submit error: %w", err)
	}

	// Wait for completion
	_, err = r.ring.Wait()
	if err != nil {
		return 0, fmt.Errorf("iouring wait error: %w", err)
	}

	cqe := r.ring.Peek()
	if cqe == nil {
		return 0, fmt.Errorf("no completion available")
	}
	defer r.ring.Advance(1)

	if cqe.Res < 0 {
		return 0, fmt.Errorf("async read failed: %d", cqe.Res)
	}

	return int(cqe.Res), nil
}

func (r *UringReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active {
		return nil
	}
	r.active = false
	if r.ring != nil {
		r.ring.Close()
	}
	return r.f.Close()
}
