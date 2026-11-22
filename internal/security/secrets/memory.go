package secrets

import (
	"context"
	"sync"
)

// MemoryStore is a process-local Store intended for tests and single-process
// development. Keys are lost on restart; use file or an external provider for
// anything durable.
type MemoryStore struct {
	mu   sync.RWMutex
	keys map[string]StoredKey
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore constructs a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{keys: make(map[string]StoredKey)}
}

// LoadAll runs the load all operation.
func (s *MemoryStore) LoadAll(ctx context.Context) ([]StoredKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StoredKey, 0, len(s.keys))
	for _, key := range s.keys {
		out = append(out, key)
	}
	return out, nil
}

// Put runs the put operation.
func (s *MemoryStore) Put(ctx context.Context, key StoredKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateKeyID(key.KeyID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[key.KeyID] = key
	return nil
}

// Delete runs the delete operation.
func (s *MemoryStore) Delete(ctx context.Context, keyID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, keyID)
	return nil
}
