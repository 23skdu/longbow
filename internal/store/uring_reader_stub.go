//go:build !linux

package store

import (
	"fmt"
	"os"
)

type UringReader struct {
	f *os.File
}

func NewUringReader(path string) (*UringReader, error) {
	return nil, fmt.Errorf("io_uring is only supported on Linux")
}

func (r *UringReader) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, fmt.Errorf("io_uring not supported")
}

func (r *UringReader) Close() error {
	return nil
}
