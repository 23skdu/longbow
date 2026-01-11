package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSBackend_WriteSyncClose(t *testing.T) {
	tmpFile := t.TempDir() + "/fs_backend_test.wal"
	backend, err := NewFSBackend(tmpFile)
	require.NoError(t, err)
	defer os.Remove(tmpFile)

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
		os.Remove(tmpFile)
	}()

	// DirectIO often requires aligned buffer and size.
	// Our Write implementation relies on os.File.Write which handles non-aligned writes
	// (though perhaps inefficiently or by buffering if kernel supports).
	// Let's test basic write.
	data := []byte("hello direct io")
	n, err := backend.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
}
