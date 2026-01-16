package store

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockSnapshotBackend for testing
type MockSnapshotBackend struct {
	mock.Mock
}

func (m *MockSnapshotBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	args := m.Called(ctx, name, data)
	return args.Error(0)
}

func (m *MockSnapshotBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockSnapshotBackend) ListSnapshots(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockSnapshotBackend) DeleteSnapshot(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockSnapshotBackend) WriteSnapshotAsync(name string, data []byte) {
	m.Called(name, data)
}

func (m *MockSnapshotBackend) Bucket() string                    { return "test-bucket" }
func (m *MockSnapshotBackend) Prefix() string                    { return "test-prefix" }
func (m *MockSnapshotBackend) GetHTTPTransport() *http.Transport { return nil }
func (m *MockSnapshotBackend) GetHTTPClient() *http.Client       { return nil }

// New interface method we plan to add
func (m *MockSnapshotBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	args := m.Called(ctx, name, ext, r)
	return args.Error(0)
}

func (m *MockSnapshotBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	args := m.Called(ctx, name, ext)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func TestDiskVectorStore_Advise(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors.bin")
	dvs, err := NewDiskVectorStore(path, 128)
	assert.NoError(t, err)
	defer func() { _ = dvs.Close() }()

	vec := make([]float32, 128)
	_, err = dvs.Append(vec)
	assert.NoError(t, err)

	// Test Valid Advice (MADV_RANDOM)
	// Note: We can't easily verify the syscall happened without deeper mocking,
	// but we can ensure it doesn't crash or return error on valid input.
	err = dvs.Advise(syscall.MADV_RANDOM)
	assert.NoError(t, err)

	// Test Another Advice (MADV_SEQUENTIAL)
	err = dvs.Advise(syscall.MADV_SEQUENTIAL)
	assert.NoError(t, err)
}

func TestDiskVectorStore_SnapshotTo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source_vectors.bin")
	dvs, err := NewDiskVectorStore(path, 4)
	assert.NoError(t, err)
	defer func() { _ = dvs.Close() }()

	// Append some data
	vec := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = dvs.Append(vec)
	assert.NoError(t, err)

	mockBackend := new(MockSnapshotBackend)
	// Expect WriteSnapshotFile to be called
	mockBackend.On("WriteSnapshotFile", mock.Anything, "test_dataset", ".bin", mock.Anything).Return(nil)

	err = dvs.SnapshotTo(context.Background(), mockBackend, "test_dataset")
	assert.NoError(t, err)

	mockBackend.AssertExpectations(t)
}

func TestDiskVectorStore_RestoreFrom(t *testing.T) {
	// Create a dummy file content representing the vector store
	// 1 vector of dim 4 (16 bytes)
	dummyData := make([]byte, 16)
	// Provide Reader
	mockReader := io.NopCloser(bytes.NewReader(dummyData))

	mockBackend := new(MockSnapshotBackend)
	mockBackend.On("ReadSnapshotFile", mock.Anything, "test_dataset_restore.bin", ".bin").Return(mockReader, nil)

	destPath := filepath.Join(t.TempDir(), "restored_vectors.bin")

	// Static Restore function? Or method on instance?
	// Usually Restore creates a new instances or populates a path.
	// Let's assume a helper function RestoreDiskVectorStore(ctx, backend, name, destPath)
	err := RestoreDiskVectorStore(context.Background(), mockBackend, "test_dataset_restore.bin", destPath)
	assert.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(destPath)
	assert.NoError(t, err)

	mockBackend.AssertExpectations(t)
}
