package storage

import (
	"io"
	"os"
	"sync"
	"time"
	"path/filepath"

	"github.com/23skdu/longbow/internal/metrics"
)

// StorageBackend defines the interface for high-performance vector/graph I/O.
// It supports positional reads/writes and vectored I/O.
type StorageBackend interface {
	io.ReaderAt
	io.WriterAt

	// Readv reads into multiple buffers at the given offset.
	Readv(iovs [][]byte, off int64) (int, error)
	// Writev writes from multiple buffers at the given offset.
	Writev(iovs [][]byte, off int64) (int, error)

	Sync() error
	Close() error
	Size() (int64, error)
	Name() string
}

// NewStorageBackend creates a StorageBackend based on the platform and configuration.
func NewStorageBackend(path string, preferUring, directIO bool) (StorageBackend, error) {
	if preferUring {
		backend, err := NewUringStorageBackend(path)
		if err == nil {
			return backend, nil
		}
	}
	return NewFSStorageBackend(path, directIO)
}

// FSStorageBackend is a standard os.File implementation.
type FSStorageBackend struct {
	f    *os.File
	path string
	mu   sync.RWMutex
}

func NewFSStorageBackend(path string, directIO bool) (*FSStorageBackend, error) {
	flags := os.O_RDWR | os.O_CREATE
	var f *os.File
	var err error

	f, err = os.OpenFile(filepath.Clean(path), flags, 0600) // #nosec G304

	if err != nil {
		return nil, err
	}

	return &FSStorageBackend{
		f:    f,
		path: path,
	}, nil
}

func (b *FSStorageBackend) ReadAt(p []byte, off int64) (int, error) {
	n, err := b.f.ReadAt(p, off)
	if err == nil || err == io.EOF {
		metrics.IOReadBytesTotal.WithLabelValues("disk_store").Add(float64(n))
		metrics.IOReadOpsTotal.WithLabelValues("disk_store").Inc()
	}
	return n, err
}

func (b *FSStorageBackend) WriteAt(p []byte, off int64) (int, error) {
	n, err := b.f.WriteAt(p, off)
	if err == nil {
		metrics.IOWriteBytesTotal.WithLabelValues("disk_store").Add(float64(n))
		metrics.IOWriteOpsTotal.WithLabelValues("disk_store").Inc()
	}
	return n, err
}

func (b *FSStorageBackend) Readv(iovs [][]byte, off int64) (int, error) {
	// Fallback to sequential Pread if Readv is not natively available or platform is not Linux.
	// Note: On Darwin/Unix, we could use unix.Readv but it doesn't take an offset (it uses current pos).
	// We'd need to use preadv which is available on Linux but not easily on Darwin.
	// For now, we simulate for compatibility.
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

func (b *FSStorageBackend) Writev(iovs [][]byte, off int64) (int, error) {
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

func (b *FSStorageBackend) Sync() error {
	start := time.Now()
	err := b.f.Sync()
	metrics.IOFsyncDurationSeconds.WithLabelValues("disk_store").Observe(time.Since(start).Seconds())
	return err
}

func (b *FSStorageBackend) Close() error {
	return b.f.Close()
}

func (b *FSStorageBackend) Size() (int64, error) {
	fi, err := b.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (b *FSStorageBackend) Name() string {
	return b.path
}
