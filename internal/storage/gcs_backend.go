package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSBackendConfig holds configuration for the GCS backend
type GCSBackendConfig struct {
	Bucket          string // Bucket name
	Prefix          string // Optional key prefix for all snapshots
	CredentialsFile string // Path to Google service account key file
	ProjectID       string // Google Cloud Project ID
}

// GCSBackend implements SnapshotBackend for Google Cloud Storage
type GCSBackend struct {
	client *storage.Client
	bucket string
	prefix string
}

// NewGCSBackend creates a new GCS backend from configuration
func NewGCSBackend(ctx context.Context, cfg *GCSBackendConfig) (SnapshotBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("GCS bucket is required")
	}

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSBackend{
		client: client,
		bucket: cfg.Bucket,
		prefix: strings.TrimSuffix(cfg.Prefix, "/"),
	}, nil
}

func (b *GCSBackend) Bucket() string { return b.bucket }
func (b *GCSBackend) Prefix() string { return b.prefix }

func (b *GCSBackend) GetHTTPTransport() *http.Transport { return nil }
func (b *GCSBackend) GetHTTPClient() *http.Client       { return nil }

func (b *GCSBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	return b.WriteSnapshotFile(ctx, name, ".parquet", bytes.NewReader(data))
}

func (b *GCSBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	key := buildGCSKeyWithExt(b.prefix, name, ext)
	obj := b.client.Bucket(b.bucket).Object(key)
	w := obj.NewWriter(ctx)

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return fmt.Errorf("GCS write failed: %w", err)
	}

	return w.Close()
}

func (b *GCSBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.ReadSnapshotFile(ctx, name, ".parquet")
}

func (b *GCSBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	key := buildGCSKeyWithExt(b.prefix, name, ext)
	r, err := b.client.Bucket(b.bucket).Object(key).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, &NotFoundError{Name: name}
		}
		return nil, fmt.Errorf("GCS read failed: %w", err)
	}
	return r, nil
}

func (b *GCSBackend) ListSnapshots(ctx context.Context) ([]string, error) {
	prefix := path.Join(b.prefix, "snapshots") + "/"
	if b.prefix == "" {
		prefix = "snapshots/"
	}

	var collections []string
	it := b.client.Bucket(b.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("GCS list failed: %w", err)
		}

		base := path.Base(attrs.Name)
		if strings.HasSuffix(base, ".parquet") {
			name := strings.TrimSuffix(base, ".parquet")
			collections = append(collections, name)
		}
	}
	return collections, nil
}

func (b *GCSBackend) DeleteSnapshot(ctx context.Context, name string) error {
	key := buildGCSKeyWithExt(b.prefix, name, ".parquet")
	err := b.client.Bucket(b.bucket).Object(key).Delete(ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		return fmt.Errorf("GCS delete failed: %w", err)
	}
	return nil
}

func (b *GCSBackend) WriteSnapshotAsync(name string, data []byte) {
	_ = b.WriteSnapshot(context.Background(), name, data)
}

func buildGCSKeyWithExt(prefix, name, ext string) string {
	key := path.Join("snapshots", name+ext)
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/")
		key = path.Join(prefix, key)
	}
	return key
}
