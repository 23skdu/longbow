package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileStore persists one JSON document per KeyID in a directory. It is the
// default provider: durable, dependency-free, and easy to mount from a
// Kubernetes Secret or a restricted host directory.
type FileStore struct {
	dir string
}

var _ Store = (*FileStore)(nil)

func NewFileStore(dir string) (*FileStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("secrets: file store path must not be empty")
	}
	clean := filepath.Clean(dir)
	if err := os.MkdirAll(clean, 0700); err != nil {
		return nil, fmt.Errorf("secrets: create store dir: %w", err)
	}
	return &FileStore{dir: clean}, nil
}

func (s *FileStore) path(keyID string) (string, error) {
	if err := ValidateKeyID(keyID); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, keyID+".json"), nil
}

func (s *FileStore) LoadAll(ctx context.Context) ([]StoredKey, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("secrets: list store dir: %w", err)
	}
	var out []StoredKey
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		// The directory is operator-controlled and names come from ReadDir.
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name())) // #nosec G304 -- trusted store directory and ReadDir entry
		if err != nil {
			return nil, fmt.Errorf("secrets: read %s: %w", entry.Name(), err)
		}
		var key StoredKey
		if err := json.Unmarshal(data, &key); err != nil {
			return nil, fmt.Errorf("secrets: decode %s: %w", entry.Name(), err)
		}
		if err := ValidateKeyID(key.KeyID); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

func (s *FileStore) Put(ctx context.Context, key StoredKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key.KeyID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(key)
	if err != nil {
		return err
	}
	// Write to a temporary file in the same directory, fsync it, then rename so
	// a crash cannot leave a half-written credential record behind.
	tmp, err := os.CreateTemp(s.dir, ".tmp-"+key.KeyID+"-")
	if err != nil {
		return fmt.Errorf("secrets: create temp file for %s: %w", key.KeyID, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secrets: chmod temp file for %s: %w", key.KeyID, err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secrets: write temp file for %s: %w", key.KeyID, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secrets: sync temp file for %s: %w", key.KeyID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("secrets: close temp file for %s: %w", key.KeyID, err)
	}
	if err := os.Rename(tmpName, path); err != nil { // #nosec G304 -- validated KeyID confined to store dir
		return fmt.Errorf("secrets: write %s: %w", key.KeyID, err)
	}
	if dir, err := os.Open(s.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return ctx.Err()
}

func (s *FileStore) Delete(ctx context.Context, keyID string) error {
	path, err := s.path(keyID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("secrets: delete %s: %w", keyID, err)
	}
	return ctx.Err()
}
