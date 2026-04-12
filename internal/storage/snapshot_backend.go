package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// SnapshotBackend defines the interface for snapshot storage backends
type SnapshotBackend interface {
	// WriteSnapshot writes snapshot data for a collection
	WriteSnapshot(ctx context.Context, name string, data []byte) error
	// ReadSnapshot returns a reader for snapshot data
	ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error)
	// ListSnapshots returns all collection names with snapshots
	ListSnapshots(ctx context.Context) ([]string, error)
	// DeleteSnapshot removes a snapshot
	DeleteSnapshot(ctx context.Context, name string) error
	// WriteSnapshotAsync performs a non-blocking upload
	WriteSnapshotAsync(name string, data []byte)

	// WriteSnapshotFile writes a snapshot file with a custom extension
	WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error
	// ReadSnapshotFile reads a snapshot file with a custom extension
	ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error)

	// Expose properties for testing/coordination (optional/nullable)
	Bucket() string
	Prefix() string
	GetHTTPTransport() *http.Transport
	GetHTTPClient() *http.Client
}

// NotFoundError indicates a snapshot was not found
type NotFoundError struct {
	Name string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("snapshot not found: %s", e.Name)
}

// IsNotFoundError checks if an error is a NotFoundError
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var nfe *NotFoundError
	if errors.As(err, &nfe) {
		return true
	}
	return false
}
