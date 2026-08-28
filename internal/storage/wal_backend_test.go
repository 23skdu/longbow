package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestFSBackend_WriteSyncClose(t *testing.T) {
	tmpFile := t.TempDir() + "/fs_backend_test.wal"
	backend, err := NewFSBackend(tmpFile)
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile) }()

	data := []byte("hello world")
	n, err := backend.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	err = backend.Sync()
	assert.NoError(t, err)

	err = backend.Close()
	assert.NoError(t, err)

	// Verify content
	content, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestFSBackend_DirectIO(t *testing.T) {
	// Skip on CI where DirectIO might not be supported or behaves differently on tmpfs
	if os.Getenv("CI") != "" {
		t.Skip("Skipping DirectIO test in CI")
	}

	tmpFile := t.TempDir() + "/fs_direct_backend_test.wal"
	backend, err := NewFSBackendWithDirectIO(tmpFile)
	if err != nil {
		t.Logf("DirectIO not supported: %v", err)
		return
	}
	defer func() {
		_ = backend.Close()
		_ = os.Remove(tmpFile)
	}()

	// DirectIO requires aligned buffer and size (typically 4096 bytes on Linux).
	alignedBuf, err := unix.Mmap(-1, 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err == nil {
		defer func() { _ = unix.Munmap(alignedBuf) }()
		copy(alignedBuf, "hello direct io")
		n, err := backend.Write(alignedBuf)
		if err != nil {
			t.Logf("DirectIO write not supported on filesystem: %v", err)
		} else {
			assert.Equal(t, 4096, n)
		}
	}
}
