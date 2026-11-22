//go:build !linux || !iouring

package storage

import "fmt"

// NewUringStorageBackend constructs a new UringStorageBackend.
func NewUringStorageBackend(path string) (StorageBackend, error) {
	return nil, fmt.Errorf("io_uring storage backend not supported on this platform")
}
