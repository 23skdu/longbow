//go:build !linux || !iouring

package storage

import "fmt"

func NewUringStorageBackend(path string) (StorageBackend, error) {
	return nil, fmt.Errorf("io_uring storage backend not supported on this platform")
}
