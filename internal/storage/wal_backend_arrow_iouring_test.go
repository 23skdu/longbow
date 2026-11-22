//go:build linux

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowIOUringBackendCreation runs the test arrow io uring backend creation operation.
func TestArrowIOUringBackendCreation(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	assert.NotNil(t, backend)

	// Close backend
	err = backend.Close()
	assert.NoError(t, err)
}

// TestArrowIOUringBackendWrite runs the test arrow io uring backend write operation.
func TestArrowIOUringBackendWrite(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Write data
	data := []byte("hello io_uring wal backend")
	n, err := backend.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Sync
	err = backend.Sync()
	require.NoError(t, err)

	// Verify file content
	content, err := os.ReadFile(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

// TestArrowIOUringBackendMultipleWrites runs the test arrow io uring backend multiple writes operation.
func TestArrowIOUringBackendMultipleWrites(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Write multiple times
	writes := [][]byte{
		[]byte("first write"),
		[]byte("second write"),
		[]byte("third write"),
	}

	for _, data := range writes {
		n, err := backend.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
	}

	// Sync
	err = backend.Sync()
	require.NoError(t, err)

	// Verify file content
	content, err := os.ReadFile(tmpfile.Name())
	require.NoError(t, err)

	expected := []byte("first writesecond writethird write")
	assert.Equal(t, expected, content)
}

// TestArrowIOUringBackendLargeWrite runs the test arrow io uring backend large write operation.
func TestArrowIOUringBackendLargeWrite(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Write large data (8KB)
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := backend.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Sync
	err = backend.Sync()
	require.NoError(t, err)

	// Verify file content
	content, err := os.ReadFile(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

// TestArrowIOUringBackendName runs the test arrow io uring backend name operation.
func TestArrowIOUringBackendName(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Check name
	assert.Equal(t, tmpfile.Name(), backend.Name())
}

// TestArrowIOUringBackendFile runs the test arrow io uring backend file operation.
func TestArrowIOUringBackendFile(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Check file
	assert.NotNil(t, backend.File())
}

// TestArrowIOUringBackendConcurrency runs the test arrow io uring backend concurrency operation.
func TestArrowIOUringBackendConcurrency(t *testing.T) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-wal-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(t, err)
	defer backend.Close()

	// Write concurrently (backend has mutex, so this should be safe)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			data := []byte("concurrent write ")
			_, err := backend.Write(data)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Sync
	err = backend.Sync()
	require.NoError(t, err)
}

// Benchmarks

// BenchmarkArrowIOUringBackendWrite runs the benchmark arrow io uring backend write operation.
func BenchmarkArrowIOUringBackendWrite(b *testing.B) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-bench-")
	require.NoError(b, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(b, err)
	defer backend.Close()

	// Prepare data
	data := make([]byte, 4096)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		_, err := backend.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	_ = backend.Sync()
}

// BenchmarkArrowIOUringBackendWriteParallel runs the benchmark arrow io uring backend write parallel operation.
func BenchmarkArrowIOUringBackendWriteParallel(b *testing.B) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-bench-")
	require.NoError(b, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(b, err)
	defer backend.Close()

	// Prepare data
	data := make([]byte, 4096)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := backend.Write(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	_ = backend.Sync()
}

// BenchmarkArrowIOUringBackendSync runs the benchmark arrow io uring backend sync operation.
func BenchmarkArrowIOUringBackendSync(b *testing.B) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-bench-")
	require.NoError(b, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create backend
	backend, err := NewIOUringBackend(tmpfile.Name())
	require.NoError(b, err)
	defer backend.Close()

	// Write initial data
	data := make([]byte, 4096)
	_, _ = backend.Write(data)

	b.ResetTimer()

	for b.Loop() {
		err := backend.Sync()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Comparison with FSBackend

// BenchmarkFSBackendWrite runs the benchmark fs backend write operation.
func BenchmarkFSBackendWrite(b *testing.B) {
	// Create temp file
	tmpfile, err := os.CreateTemp("", "fs-bench-")
	require.NoError(b, err)
	defer os.Remove(tmpfile.Name())
	_ = tmpfile.Close()

	// Create standard FS backend
	backend, err := NewFSBackend(tmpfile.Name())
	require.NoError(b, err)
	defer backend.Close()

	// Prepare data
	data := make([]byte, 4096)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		_, err := backend.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	_ = backend.Sync()
}
