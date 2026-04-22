package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// MockRemoteStorage is an in-memory implementation of RemoteStorage for testing.
type MockRemoteStorage struct {
	mu       sync.RWMutex
	data     map[string][]byte
	provider string // "s3" or "gcs"
}

func NewMockRemoteStorage(provider string) *MockRemoteStorage {
	return &MockRemoteStorage{
		data:     make(map[string][]byte),
		provider: provider,
	}
}

func (m *MockRemoteStorage) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	op := "put"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues(m.provider, op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues(m.provider, op, status).Inc()
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	buf := new(bytes.Buffer)
	n, err := io.Copy(buf, r)
	if err != nil {
		status = "error"
		return err
	}
	m.data[key] = buf.Bytes()
	metrics.RemoteStorageUploadBytes.WithLabelValues(m.provider).Add(float64(n))
	return nil
}

func (m *MockRemoteStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	op := "get"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues(m.provider, op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues(m.provider, op, status).Inc()
	}()

	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		status = "error"
		return nil, fmt.Errorf("key not found: %s", key)
	}
	
	metrics.RemoteStorageDownloadBytes.WithLabelValues(m.provider).Add(float64(len(data)))
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockRemoteStorage) Delete(ctx context.Context, key string) error {
	start := time.Now()
	op := "delete"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues(m.provider, op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues(m.provider, op, status).Inc()
	}()

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MockRemoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	op := "exists"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues(m.provider, op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues(m.provider, op, status).Inc()
	}()

	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok, nil
}
