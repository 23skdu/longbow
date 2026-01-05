//go:build darwin

package store

import (
	"fmt"
	"os"
	"syscall"
)

// OpenFileDirect opens a file with Direct I/O enabled (F_NOCACHE on macOS).
func OpenFileDirect(path string, flags int, perm os.FileMode) (*os.File, error) {
	// Open standard file first
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, err
	}

	// Enable F_NOCACHE
	// F_NOCACHE turns off data caching in the kernel.
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_NOCACHE, 1)
	if errno != 0 {
		_ = f.Close()
		return nil, fmt.Errorf("failed to enable F_NOCACHE: %w", errno)
	}

	return f, nil
}

// AdviseDontNeed advises the kernel that the file data is not needed in cache.
// On macOS, F_NOCACHE already handles this for read/write, so this is a no-op
// or can strictly ensure pages are dropped if we didn't use F_NOCACHE.
func AdviseDontNeed(f *os.File) error {
	// macOS doesn't have POSIX_FADV_DONTNEED for file descriptors in the same way Linux does.
	// F_NOCACHE is the primary mechanism.
	return nil
}
