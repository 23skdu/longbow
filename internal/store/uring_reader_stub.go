//go:build !linux

package store

import (
	"os"
	"path/filepath"
)

// UringReader is a stub implementation of the io_uring reader for non-Linux platforms.
type UringReader struct {
	f *os.File
}

// NewUringReader creates a new UringReader instance (fallback implementation).
func NewUringReader(path string) (*UringReader, error) {
	f, err := os.Open(filepath.Clean(path)) // #nosec G304
	if err != nil {
		return nil, err
	}
	return &UringReader{f: f}, nil
}

// ReadAt performs a positional read using standard os.File.ReadAt.
func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	return r.f.ReadAt(buf, offset)
}

// Close releases the file handle.
func (r *UringReader) Close() error {
	return r.f.Close()
}
