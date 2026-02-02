package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSStorageBackend(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.bin")

	backend, err := NewFSStorageBackend(path, false)
	require.NoError(t, err)
	defer backend.Close()

	// Test WriteAt
	data := []byte("hello world")
	n, err := backend.WriteAt(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Test ReadAt
	buf := make([]byte, 5)
	n, err = backend.ReadAt(buf, 6)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "world", string(buf))

	// Test Writev (fallback)
	iovs := [][]byte{
		[]byte("more"),
		[]byte("data"),
	}
	n, err = backend.Writev(iovs, int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, 8, n)

	// Test Readv (fallback)
	riovs := [][]byte{
		make([]byte, 4),
		make([]byte, 4),
	}
	n, err = backend.Readv(riovs, int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, "more", string(riovs[0]))
	assert.Equal(t, "data", string(riovs[1]))
}

func TestDirectIOBackend(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "direct.bin")

	backend, err := NewFSStorageBackend(path, true)
	if err != nil {
		t.Skip("Direct I/O might require specific alignment or platform support:", err)
	}
	defer backend.Close()

	// On many systems, Direct I/O requires page-aligned buffers.
	// Our FSStorageBackend currently uses standard buffers, which might fail on some OSes (like Linux O_DIRECT).
	// On Mac (F_NOCACHE), it usually works with any alignment.

	data := make([]byte, 4096)
	copy(data, "aligned data")
	_, err = backend.WriteAt(data, 0)
	require.NoError(t, err)

	buf := make([]byte, 4096)
	_, err = backend.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "aligned data", string(buf[:12]))
}
