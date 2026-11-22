package storage

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// FileSnapshotBackend represents a file snapshot backend value.
type FileSnapshotBackend struct {
	baseDir string
}

func sanitizeSnapshotName(name string) string {
	// Prevent directory traversal
	return filepath.Clean(strings.ReplaceAll(name, "..", ""))
}

// NewFileSnapshotBackend constructs a new FileSnapshotBackend.
func NewFileSnapshotBackend(baseDir string) (SnapshotBackend, error) {
	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0700); err != nil { // #nosec G301
		return nil, err
	}
	return &FileSnapshotBackend{baseDir: baseDir}, nil
}

// WriteSnapshot runs the write snapshot operation.
func (b *FileSnapshotBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	path := filepath.Join(b.baseDir, sanitizeSnapshotName(name)+".parquet")
	return os.WriteFile(path, data, 0600) // #nosec G306
}

// ReadSnapshot runs the read snapshot operation.
func (b *FileSnapshotBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	path := filepath.Join(b.baseDir, sanitizeSnapshotName(name)+".parquet")
	f, err := os.Open(filepath.Clean(path)) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &NotFoundError{Name: name}
		}
		return nil, err
	}
	return f, nil
}

// ListSnapshots runs the list snapshots operation.
func (b *FileSnapshotBackend) ListSnapshots(ctx context.Context) ([]string, error) {
	files, err := os.ReadDir(b.baseDir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".parquet") {
			names = append(names, strings.TrimSuffix(f.Name(), ".parquet"))
		}
	}
	return names, nil
}

// DeleteSnapshot runs the delete snapshot operation.
func (b *FileSnapshotBackend) DeleteSnapshot(ctx context.Context, name string) error {
	path := filepath.Join(b.baseDir, sanitizeSnapshotName(name)+".parquet")
	return os.Remove(filepath.Clean(path))
}

// WriteSnapshotAsync runs the write snapshot async operation.
func (b *FileSnapshotBackend) WriteSnapshotAsync(name string, data []byte) {
	_ = b.WriteSnapshot(context.Background(), name, data)
}

// WriteSnapshotFile runs the write snapshot file operation.
func (b *FileSnapshotBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	path := filepath.Join(b.baseDir, sanitizeSnapshotName(name)+ext)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil { // #nosec G301
		return err
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600) // #nosec G304 G302
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

// ReadSnapshotFile runs the read snapshot file operation.
func (b *FileSnapshotBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	path := filepath.Join(b.baseDir, sanitizeSnapshotName(name)+ext)
	f, err := os.Open(filepath.Clean(path)) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &NotFoundError{Name: name}
		}
		return nil, err
	}
	return f, nil
}

// Bucket runs the bucket operation.
func (b *FileSnapshotBackend) Bucket() string { return "" }

// Prefix runs the prefix operation.
func (b *FileSnapshotBackend) Prefix() string { return b.baseDir }

// GetHTTPTransport returns the http transport.
func (b *FileSnapshotBackend) GetHTTPTransport() *http.Transport { return nil }

// GetHTTPClient returns the http client.
func (b *FileSnapshotBackend) GetHTTPClient() *http.Client { return nil }
