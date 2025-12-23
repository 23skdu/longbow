//go:build linux

package store

import (
	"os"
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
	return os.OpenFile(path, flags|syscall.O_DIRECT, perm)
}

// AdviseDontNeed advises the kernel that the file data is not needed in cache.
func AdviseDontNeed(f *os.File) error {
	// FADV_DONTNEED attempts to free cache pages associated with the file.
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
