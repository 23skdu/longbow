//go:build !linux

package store

import "errors"

// NewUringBackend returns an error on non-Linux systems.
func NewUringBackend(path string) (WALBackend, error) {
	return nil, errors.New("io_uring is only supported on Linux")
}
