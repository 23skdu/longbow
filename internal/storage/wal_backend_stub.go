//go:build !linux || (linux && !iouring)

package storage

import "errors"

// NewUringBackend returns an error on non-Linux systems.
func NewUringBackend(path string) (WALBackend, error) {
	return nil, errors.New("io_uring is only supported on Linux")
}
