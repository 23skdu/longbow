package storage

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

	// File returns the underlying *os.File if available.
	File() *os.File
}

// FSBackend is a standard os.File backed implementation.
type FSBackend struct {
	f    *os.File
	path string
	mu   sync.Mutex
}

// NewWALBackend creates a new WALBackend.
// It attempts to use io_uring if preferAsync is true and the platform supports it.
// If directIO is true, it attempts to use Direct I/O (O_DIRECT/F_NOCACHE).
func NewWALBackend(path string, preferAsync, directIO bool) (WALBackend, error) {
	if preferAsync {
		// NewUringBackend depends on the platform specific file (linux or stub)
		// For now, UringBackend handles its own opening. We might want to pass directIO hint there too eventually.
		backend, err := NewUringBackend(path)
		if err == nil {
			return backend, nil
		}
		// Fallback to FSBackend
	}
	if directIO {
		return NewFSBackendWithDirectIO(path)
	}
	return NewFSBackend(path)
}

func NewFSBackend(path string) (*FSBackend, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FSBackend{
		f:    f,
		path: path,
	}, nil
}

func NewFSBackendWithDirectIO(path string) (*FSBackend, error) {
	f, err := OpenFileDirect(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
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

func (b *FSBackend) File() *os.File {
	return b.f
}
