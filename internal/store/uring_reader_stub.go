//go:build !linux

package store

import (
	"fmt"
	"os"
)

// UringReader is a stub implementation of the io_uring reader for non-Linux platforms.
type UringReader struct {
	f *os.File
}

// NewUringReader creates a new UringReader instance (unsupported on this platform).
func NewUringReader(path string) (*UringReader, error) {
	return nil, fmt.Errorf("io_uring is only supported on Linux")
}

// ReadAt performs a positional read (unsupported on this platform).
func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, fmt.Errorf("io_uring not supported")
}

// Close releases any resources held by the reader.
func (r *UringReader) Close() error {
	return nil
}
