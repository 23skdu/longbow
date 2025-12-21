//go:build linux

package store

import (
	"fmt"
)

// NewWAL creates the best available WAL implementation for the platform.
// On Linux, it attempts to use UringWAL.
func NewWAL(dir string, v *VectorStore) WAL {
	wal, err := NewUringWAL(dir, v)
	if err != nil {
		fmt.Printf("Failed to create UringWAL: %v. Falling back to StdWAL.\n", err)
		return NewStdWAL(dir, v)
	}
	return wal
}
