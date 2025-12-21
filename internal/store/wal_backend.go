package store

import (
	"os"
	"sync"
)

// WALBackend defines the interface for low-level WAL I/O.
// Implementations must be thread-safe or serialization must be handled by caller.
// WALBatcher currently handles serialization (single writer in flush loop).
type WALBackend interface {
	// Write writes the data to the WAL.
	// Implementations should handle offset tracking if necessary (e.g. for io_uring pwrite).
	Write(p []byte) (n int, err error)

	// Sync explicitly flushes data to disk.
	Sync() error

	// Close closes the underlying resource.
	Close() error

	// Name returns the file name or identifier.
	Name() string
}

// FSBackend is a standard os.File backed implementation.
type FSBackend struct {
	f    *os.File
	path string
	mu   sync.Mutex
}

// NewWALBackend creates a new WALBackend.
// It attempts to use io_uring if preferAsync is true and the platform supports it.
// Otherwise, it falls back to FSBackend.
func NewWALBackend(path string, preferAsync bool) (WALBackend, error) {
	if preferAsync {
		// NewUringBackend depends on the platform specific file (linux or stub)
		backend, err := NewUringBackend(path)
		if err == nil {
			return backend, nil
		}
		// Fallback to FSBackend
	}
	return NewFSBackend(path)
}

func NewFSBackend(path string) (*FSBackend, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FSBackend{
		f:    f,
		path: path,
	}, nil
}

func (b *FSBackend) Write(p []byte) (int, error) {
	// os.File.Write is thread-safe for append if O_APPEND is used,
	// but we lock to be explicit if we move away from O_APPEND logic later.
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f.Write(p)
}

func (b *FSBackend) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f.Sync()
}

func (b *FSBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f.Close()
}

func (b *FSBackend) Name() string {
	return b.path
}
