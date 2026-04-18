package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/23skdu/longbow/internal/metrics"
	"google.golang.org/api/option"
)

// GCSRemoteStorage is a GCP Cloud Storage implementation of RemoteStorage.
type GCSRemoteStorage struct {
	client *storage.Client
	bucket *storage.BucketHandle
}

var _ RemoteStorage = (*GCSRemoteStorage)(nil)

// NewGCSRemoteStorage constructs a GCS-backed RemoteStorage.
func NewGCSRemoteStorage(ctx context.Context, bucketName string, opts ...option.ClientOption) (*GCSRemoteStorage, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &GCSRemoteStorage{
		client: client,
		bucket: client.Bucket(bucketName),
	}, nil
}

// Close closes the underlying GCP storage client connections.
func (g *GCSRemoteStorage) Close() error {
	return g.client.Close()
}

// Put uploads an object to Google Cloud Storage.
// GCS allows streaming directly via the ObjectHandle Writer.
func (g *GCSRemoteStorage) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	op := "put"
	status := "success"

	obj := g.bucket.Object(key)
	writer := obj.NewWriter(ctx)

	// We copy bytes from io.Reader to the GCS streaming writer.
	uploaded, err := io.Copy(writer, r)
	if err != nil {
		_ = writer.Close() // Best effort
		status = "error"
		metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()
		return err
	}

	err = writer.Close()
	if err != nil {
		status = "error"
	} else {
		metrics.RemoteStorageUploadBytes.WithLabelValues("gcs").Add(float64(uploaded))
	}

	metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
	metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()
	return err
}

// Get downloads an object from Google Cloud Storage.
func (g *GCSRemoteStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	op := "get"
	status := "success"

	obj := g.bucket.Object(key)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		status = "error"
		metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()
		return nil, err
	}

	// We intentionally defer the metrics observation to when the ReadCloser is actually wrapped,
	// but the `Get` latency itself is just time-to-first-byte essentially.
	metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
	metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()

	return &metricsReader{
		r:        reader,
		provider: "gcs",
	}, nil
}

// Delete removes an object from Google Cloud Storage.
func (g *GCSRemoteStorage) Delete(ctx context.Context, key string) error {
	start := time.Now()
	op := "delete"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()
	}()

	obj := g.bucket.Object(key)
	err := obj.Delete(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// Some interfaces consider deleting a non-existent object as a success, some as error.
			// Let's pass the error up for standard behavior, but it could be ignored depending on caller.
		}
		status = "error"
		return err
	}
	return nil
}

// Exists checks if an object exists by retrieving its attributes.
func (g *GCSRemoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	op := "exists"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("gcs", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("gcs", op, status).Inc()
	}()

	obj := g.bucket.Object(key)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		status = "error"
		return false, err
	}
	return true, nil
}
