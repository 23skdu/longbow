//go:build !linux

package storage

import "errors"

// NewIOUringBackend returns an error on non-Linux systems.
func NewIOUringBackend(path string) (WALBackend, error) {
	return nil, errors.New("custom io_uring is only supported on Linux")
}
