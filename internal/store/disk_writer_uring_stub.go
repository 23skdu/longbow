//go:build !linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DiskWriterUring is a fallback for non-Linux platforms.
type DiskWriterUring struct {
	f      *os.File
	mu     sync.RWMutex
	active bool
}

func NewDiskWriterUring(path string) (*DiskWriterUring, error) {
	path = filepath.Clean(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600) // #nosec G304 G302
	if err != nil {
		return nil, err
	}
	return &DiskWriterUring{
		f:      f,
		active: true,
	}, nil
}

func (d *DiskWriterUring) SubmitWrite(data []byte, offset int64) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.active {
		return fmt.Errorf("disk writer inactive")
	}

	// Fallback to synchronous WriteAt
	_, err := d.f.WriteAt(data, offset)
	return err
}

func (d *DiskWriterUring) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active = false
	return d.f.Close()
}
