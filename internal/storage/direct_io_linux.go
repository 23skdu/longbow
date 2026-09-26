//go:build linux

package storage

import (
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// OpenFileDirect opens a file with Direct I/O enabled (O_DIRECT on Linux).
func OpenFileDirect(path string, flags int, perm os.FileMode) (*os.File, error) {
	// Add O_DIRECT to flags
	// Note: O_DIRECT requires aligned memory buffers for reads/writes.
	// Go's runtime allocator usually aligns to 8 bytes, but O_DIRECT often needs 512 or 4096.
	// If the application doesn't align buffers, writes will fail with EINVAL.
	// For WAL, we must ensure our buffers are aligned.
	return os.OpenFile(filepath.Clean(path), flags|syscall.O_DIRECT, perm)
}

// AdviseDontNeed advises the kernel that the file data is not needed in cache.
func AdviseDontNeed(f *os.File) error {
	// FADV_DONTNEED attempts to free cache pages associated with the file.
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED) // #nosec G115
}

// AdviseWillNeed advises the kernel to read ahead the specified range into page cache.
func AdviseWillNeed(f *os.File, off int64, length int64) error {
	return unix.Fadvise(int(f.Fd()), off, length, unix.FADV_WILLNEED) // #nosec G115
}
