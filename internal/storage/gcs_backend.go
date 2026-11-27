package storage

import (
	"bytes"
	"context"
	"errors"
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

	// Remote encryption at rest settings.
	EncryptionMode    string // "none", "client", "cmek", "kms"
	CMEKKeyID         string // KMS key resource name for CMEK
	KMSKeyID          string // alias for CMEKKeyID
	RequireEncryption bool   // fail closed when no approved mode is configured
}

// GCSBackend implements SnapshotBackend for Google Cloud Storage
type GCSBackend struct {
	client *storage.Client
	bucket string
	prefix string

	encryptionMode    string
	cmekKeyID         string
	lastObjectVersion string
}

// NewGCSBackend creates a new GCS backend from configuration
func NewGCSBackend(ctx context.Context, cfg *GCSBackendConfig) (SnapshotBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("GCS bucket is required")
	}
	if err := cfg.ValidateRemoteEncryption(); err != nil {
		return nil, err
	}

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile)) //nolint:staticcheck
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSBackend{
		client:         client,
		bucket:         cfg.Bucket,
		prefix:         strings.TrimSuffix(cfg.Prefix, "/"),
		encryptionMode: strings.ToLower(strings.TrimSpace(cfg.EncryptionMode)),
		cmekKeyID:      gcsCMEKKeyID(cfg),
	}, nil
}

// Bucket runs the bucket operation.
func (b *GCSBackend) Bucket() string { return b.bucket }

// Prefix runs the prefix operation.
func (b *GCSBackend) Prefix() string { return b.prefix }

// GetHTTPTransport returns the http transport.
func (b *GCSBackend) GetHTTPTransport() *http.Transport { return nil }

// GetHTTPClient returns the http client.
func (b *GCSBackend) GetHTTPClient() *http.Client { return nil }

// WriteSnapshot runs the write snapshot operation.
func (b *GCSBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	return b.WriteSnapshotFile(ctx, name, ".parquet", bytes.NewReader(data))
}

// ValidateRemoteEncryption validates provider-managed GCS encryption settings.
func (c *GCSBackendConfig) ValidateRemoteEncryption() error {
	mode := strings.ToLower(strings.TrimSpace(c.EncryptionMode))
	if mode == "" {
		mode = "none"
	}
	switch mode {
	case "none", "plaintext":
		if c.RequireEncryption {
			return errors.New("GCS remote encryption is required but no approved mode is configured")
		}
	case "client":
	case "cmek", "kms":
		if strings.TrimSpace(c.CMEKKeyID) == "" && strings.TrimSpace(c.KMSKeyID) == "" {
			return errors.New("GCS CMEK requires a KMS key ID")
		}
	default:
		return fmt.Errorf("unsupported GCS encryption mode: %q", c.EncryptionMode)
	}
	return nil
}

func gcsCMEKKeyID(cfg *GCSBackendConfig) string {
	if key := strings.TrimSpace(cfg.CMEKKeyID); key != "" {
		return key
	}
	return strings.TrimSpace(cfg.KMSKeyID)
}

// ObjectVersion returns the version ID of the most recently written object, if available.
func (b *GCSBackend) ObjectVersion() string { return b.lastObjectVersion }

// ObjectVersion returns the version ID of the most recently written object, if available.
func (b *S3Backend) ObjectVersion() string { return b.lastObjectVersion }

// WriteSnapshotFile runs the write snapshot file operation.
func (b *GCSBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	key := buildGCSKeyWithExt(b.prefix, name, ext)
	obj := b.client.Bucket(b.bucket).Object(key)
	w := obj.NewWriter(ctx)
	if b.cmekKeyID != "" {
		w.KMSKeyName = b.cmekKeyID
	}

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return fmt.Errorf("GCS write failed: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("GCS write failed: %w", err)
	}
	if attrs, err := obj.Attrs(ctx); err == nil {
		b.lastObjectVersion = fmt.Sprintf("%d", attrs.Generation) // generation is not a version ID but a stable object revision
	}
	return nil
}

// ReadSnapshot runs the read snapshot operation.
func (b *GCSBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.ReadSnapshotFile(ctx, name, ".parquet")
}

// ReadSnapshotFile runs the read snapshot file operation.
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

// ListSnapshots runs the list snapshots operation.
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

// DeleteSnapshot runs the delete snapshot operation.
func (b *GCSBackend) DeleteSnapshot(ctx context.Context, name string) error {
	key := buildGCSKeyWithExt(b.prefix, name, ".parquet")
	err := b.client.Bucket(b.bucket).Object(key).Delete(ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		return fmt.Errorf("GCS delete failed: %w", err)
	}
	return nil
}

// WriteSnapshotAsync runs the write snapshot async operation.
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
