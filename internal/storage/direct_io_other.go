//go:build !darwin && !linux

package storage

import "os"

// OpenFileDirect falls back to standard OpenFile on unsupported platforms.
func OpenFileDirect(path string, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flags, perm)
}

// AdviseDontNeed is a no-op on unsupported platforms.
func AdviseDontNeed(f *os.File) error {
	return nil
}
