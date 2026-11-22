package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHTTPTimeout = 15 * time.Second

// VaultConfig configures HashiCorp Vault KV v2 secret storage.
type VaultConfig struct {
	Addr   string
	Token  string
	Mount  string
	Prefix string
	Client *http.Client
}

// VaultStore stores API keys under <mount>/data/<prefix>/<KeyID> and lists them
// through the KV v2 metadata endpoint.
type VaultStore struct {
	cfg    VaultConfig
	client *http.Client
}

var _ Store = (*VaultStore)(nil)

// NewVaultStore constructs a new VaultStore.
func NewVaultStore(cfg VaultConfig) (*VaultStore, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, fmt.Errorf("secrets: vault addr must not be empty")
	}
	if strings.TrimSpace(cfg.Mount) == "" {
		cfg.Mount = DefaultVaultMount
	}
	if strings.TrimSpace(cfg.Prefix) == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &VaultStore{cfg: cfg, client: cfg.Client}, nil
}

func (s *VaultStore) dataPath(keyID string) (string, error) {
	if err := ValidateKeyID(keyID); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/data/%s/%s", s.cfg.Mount, s.cfg.Prefix, url.PathEscape(keyID)), nil
}

func (s *VaultStore) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method,
		strings.TrimRight(s.cfg.Addr, "/")+"/v1/"+strings.TrimLeft(path, "/"), reader)
	if err != nil {
		return err
	}
	if s.cfg.Token != "" {
		req.Header.Set("X-Vault-Token", s.cfg.Token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("secrets: vault request: %w", err)
	}
	defer resp.Body.Close() // #nosec G104
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{Status: resp.StatusCode, Body: string(payload)}
	}
	if out != nil && len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("secrets: decode vault response: %w", err)
		}
	}
	return nil
}

// LoadAll runs the load all operation.
func (s *VaultStore) LoadAll(ctx context.Context) ([]StoredKey, error) {
	var listed struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	listPath := fmt.Sprintf("%s/metadata/%s", s.cfg.Mount, s.cfg.Prefix)
	if err := s.do(ctx, "LIST", listPath, nil, &listed); err != nil {
		return nil, err
	}
	var out []StoredKey
	for _, name := range listed.Data.Keys {
		name = strings.TrimSuffix(name, "/")
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		path, err := s.dataPath(name)
		if err != nil {
			return nil, err
		}
		var got struct {
			Data struct {
				Data StoredKey `json:"data"`
			} `json:"data"`
		}
		if err := s.do(ctx, http.MethodGet, path, nil, &got); err != nil {
			return nil, err
		}
		out = append(out, got.Data.Data)
	}
	return out, nil
}

// Put runs the put operation.
func (s *VaultStore) Put(ctx context.Context, key StoredKey) error {
	path, err := s.dataPath(key.KeyID)
	if err != nil {
		return err
	}
	return s.do(ctx, http.MethodPut, path, map[string]any{"data": key}, nil)
}

// Delete runs the delete operation.
func (s *VaultStore) Delete(ctx context.Context, keyID string) error {
	path, err := s.dataPath(keyID)
	if err != nil {
		return err
	}
	err = s.do(ctx, http.MethodDelete, path, nil, nil)
	if err == nil {
		return nil
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) && statusErr.Status == http.StatusNotFound {
		return fmt.Errorf("%w: vault key %q", ErrNotFound, keyID)
	}
	return err
}
