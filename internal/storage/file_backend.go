package storage

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type FileSnapshotBackend struct {
	baseDir string
}

func NewFileSnapshotBackend(baseDir string) (SnapshotBackend, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &FileSnapshotBackend{baseDir: baseDir}, nil
}

func (b *FileSnapshotBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	path := filepath.Join(b.baseDir, name+".parquet")
	return os.WriteFile(path, data, 0644)
}

func (b *FileSnapshotBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	path := filepath.Join(b.baseDir, name+".parquet")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &NotFoundError{Name: name}
		}
		return nil, err
	}
	return f, nil
}

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

func (b *FileSnapshotBackend) DeleteSnapshot(ctx context.Context, name string) error {
	path := filepath.Join(b.baseDir, name+".parquet")
	return os.Remove(path)
}

func (b *FileSnapshotBackend) WriteSnapshotAsync(name string, data []byte) {
	_ = b.WriteSnapshot(context.Background(), name, data)
}

func (b *FileSnapshotBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	path := filepath.Join(b.baseDir, name+ext)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (b *FileSnapshotBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	path := filepath.Join(b.baseDir, name+ext)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &NotFoundError{Name: name}
		}
		return nil, err
	}
	return f, nil
}

func (b *FileSnapshotBackend) Bucket() string                    { return "" }
func (b *FileSnapshotBackend) Prefix() string                    { return b.baseDir }
func (b *FileSnapshotBackend) GetHTTPTransport() *http.Transport { return nil }
func (b *FileSnapshotBackend) GetHTTPClient() *http.Client       { return nil }
