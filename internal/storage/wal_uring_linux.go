//go:build linux && iouring

package storage

import (
	"fmt"
	"os"
	"sync"

	"github.com/iceber/iouring-go"
)

// URingWAL implements a WAL backend using io_uring for async I/O.
// This is a research prototype.
type URingWAL struct {
	path string
	file *os.File
	ring *iouring.IOURing
	mu   sync.Mutex
}

// NewURingWAL creates a new io_uring supported WAL
func NewURingWAL(path string) (*URingWAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Initialize io_uring with queue depth of 128
	ring, err := iouring.New(128)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("init iouring: %w", err)
	}

	return &URingWAL{
		path: path,
		file: f,
		ring: ring,
	}, nil
}

// WriteAsync submits a write request to the submission queue
func (u *URingWAL) WriteAsync(data []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Submit Write request using helper
	request := iouring.Write(int(u.file.Fd()), data)
	_, err := u.ring.SubmitRequest(request, nil)
	if err != nil {
		return err
	}

	return nil
}

func (u *URingWAL) Sync() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Submit Fsync request using helper
	// Note: iouring.Fsync returns a PrepRequest
	request := iouring.Fsync(int(u.file.Fd()))
	_, err := u.ring.SubmitRequest(request, nil)
	return err
}

func (u *URingWAL) Close() error {
	if u.ring != nil {
		_ = u.ring.Close()
	}
	if u.file != nil {
		return u.file.Close()
	}
	return nil
}
