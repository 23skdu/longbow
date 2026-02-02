package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// MockRemoteStorage is an in-memory implementation of RemoteStorage for testing.
type MockRemoteStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMockRemoteStorage() *MockRemoteStorage {
	return &MockRemoteStorage{
		data: make(map[string][]byte),
	}
}

func (m *MockRemoteStorage) Put(ctx context.Context, key string, r io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, r); err != nil {
		return err
	}
	m.data[key] = buf.Bytes()
	return nil
}

func (m *MockRemoteStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockRemoteStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MockRemoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok, nil
}
