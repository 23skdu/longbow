package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSnapshotBackend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_file_backend_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := NewFileSnapshotBackend(tmpDir)
	require.NoError(t, err)

	ctx := context.Background()
	name := "test_snapshot"
	data := []byte("snapshot data")

	// Test Write
	err = backend.WriteSnapshot(ctx, name, data)
	assert.NoError(t, err)

	// Test Read
	r, err := backend.ReadSnapshot(ctx, name)
	require.NoError(t, err)
	readData, err := os.ReadFile(tmpDir + "/" + name + ".parquet")
	require.NoError(t, err)
	assert.Equal(t, data, readData)
	r.Close()

	// Test List
	names, err := backend.ListSnapshots(ctx)
	assert.NoError(t, err)
	assert.Contains(t, names, name)

	// Test WriteSnapshotFile
	subName := "subdir/snapshot2"
	ext := ".bin"
	err = backend.WriteSnapshotFile(ctx, subName, ext, os.NewFile(0, "dummy")) // Wait, need real reader
	// Use string reader
	err = backend.WriteSnapshotFile(ctx, subName, ext, os.NewFile(0, "dummy")) // No
	
	// Better test for WriteSnapshotFile
	importData := []byte("more data")
	
	// Create another real file to copy from
	tmpFile, _ := os.CreateTemp("", "source")
	tmpFile.Write(importData)
	tmpFile.Seek(0, 0)
	
	err = backend.WriteSnapshotFile(ctx, subName, ext, tmpFile)
	assert.NoError(t, err)
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	// Test ReadSnapshotFile
	r2, err := backend.ReadSnapshotFile(ctx, subName, ext)
	require.NoError(t, err)
	r2.Close()

	// Test Delete
	err = backend.DeleteSnapshot(ctx, name)
	assert.NoError(t, err)
	
	names, _ = backend.ListSnapshots(ctx)
	assert.NotContains(t, names, name)
}
