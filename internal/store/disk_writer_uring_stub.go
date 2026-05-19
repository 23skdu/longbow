//go:build !linux && !darwin

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DiskWriterUring is a fallback for non-Linux platforms.
type DiskWriterUring struct {
	f      *os.File
	mu     sync.RWMutex
	active bool
}

// NewDiskWriterUring creates a new DiskWriterUring instance (fallback stub).
func NewDiskWriterUring(path string, bufferSize int, maxBuffers int) (*DiskWriterUring, error) {
	path = filepath.Clean(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600) // #nosec G304 G302
	if err != nil {
		return nil, err
	}
	return &DiskWriterUring{
		f:      f,
		active: true,
	}, nil
}

// SubmitWrite submits a write request to the underlying file (asynchronous simulation).
func (d *DiskWriterUring) SubmitWrite(data []byte, offset int64) (chan error, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.active {
		return nil, fmt.Errorf("disk writer inactive")
	}

	// Create completion channel
	done := make(chan error, 1)

	// Copy data to ensure it remains valid during async write
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	// Simulate async I/O with a goroutine
	go func() {
		_, err := d.f.WriteAt(dataCopy, offset)
		done <- err
		close(done)
	}()
	
	return done, nil
}

// Flush is a no-op for the stub implementation.
func (d *DiskWriterUring) Flush() {
	// Sync to disk to ensure all writes are persisted
	_ = d.f.Sync()
}

// Close closes the underlying file and deactivates the writer (stub implementation).
func (d *DiskWriterUring) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active = false
	return d.f.Close()
}
