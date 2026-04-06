package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Role string

const (
	RoleAdmin     Role = "admin"
	RoleReadWrite Role = "read-write"
	RoleReadOnly  Role = "read-only"
	RoleIngest    Role = "ingest-only"
)

type Permission string

const (
	PermRead   Permission = "read"
	PermWrite  Permission = "write"
	PermDelete Permission = "delete"
	PermIngest Permission = "ingest"
	PermAdmin  Permission = "admin"
	PermBackup Permission = "backup"
)

var rolePermissions = map[Role][]Permission{
	RoleAdmin:     {PermRead, PermWrite, PermDelete, PermIngest, PermAdmin, PermBackup},
	RoleReadWrite: {PermRead, PermWrite, PermDelete, PermIngest},
	RoleReadOnly:  {PermRead},
	RoleIngest:    {PermRead, PermIngest},
}

func (r Role) HasPermission(perm Permission) bool {
	perms, ok := rolePermissions[r]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p == perm {
			return true
		}
	}
	return false
}

type APIKey struct {
	KeyID       string
	KeyHash     string
	Name        string
	Role        Role
	Namespace   string
	Datasets    []string
	Permissions []Permission
	CreatedAt   time.Time
	ExpiresAt   *time.Time
	LastUsed    time.Time
	Enabled     bool
}

type RBACManager struct {
	mu      sync.RWMutex
	apiKeys map[string]*APIKey
	roles   map[string]map[Role]bool
}

func NewRBACManager() *RBACManager {
	return &RBACManager{
		apiKeys: make(map[string]*APIKey),
		roles:   make(map[string]map[Role]bool),
	}
}

func (r *RBACManager) CreateAPIKey(name string, role Role, namespace string, datasets []string) (*APIKey, error) {
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	keyStr := hex.EncodeToString(keyBytes)
	keyHash := sha256Hash(keyStr)

	apiKey := &APIKey{
		KeyID:       generateKeyID(),
		KeyHash:     keyHash,
		Name:        name,
		Role:        role,
		Namespace:   namespace,
		Datasets:    datasets,
		Permissions: rolePermissions[role],
		CreatedAt:   time.Now(),
		Enabled:     true,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.apiKeys[keyStr] = apiKey

	return apiKey, nil
}

func (r *RBACManager) GetAPIKey(keyStr string) (*APIKey, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ak, ok := r.apiKeys[keyStr]
	return ak, ok
}

func (r *RBACManager) ValidateAPIKey(keyStr string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	apiKey, ok := r.apiKeys[keyStr]
	if !ok {
		return false, errors.New("invalid API key")
	}

	if !apiKey.Enabled {
		return false, errors.New("API key disabled")
	}

	if apiKey.ExpiresAt != nil && time.Now().After(*apiKey.ExpiresAt) {
		return false, errors.New("API key expired")
	}

	return true, nil
}

func (r *RBACManager) CheckPermission(keyStr string, perm Permission, namespace, dataset string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	apiKey, ok := r.apiKeys[keyStr]
	if !ok {
		return false, errors.New("invalid API key")
	}

	if !apiKey.Enabled {
		return false, errors.New("API key disabled")
	}

	if !apiKey.Role.HasPermission(perm) {
		return false, errors.New("permission denied")
	}

	if apiKey.Namespace != "" && apiKey.Namespace != namespace {
		return false, errors.New("namespace mismatch")
	}

	if len(apiKey.Datasets) > 0 {
		found := false
		for _, ds := range apiKey.Datasets {
			if ds == dataset {
				found = true
				break
			}
		}
		if !found {
			return false, errors.New("dataset not authorized")
		}
	}

	return true, nil
}

func (r *RBACManager) RevokeAPIKey(keyStr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.apiKeys[keyStr]; !ok {
		return errors.New("API key not found")
	}

	r.apiKeys[keyStr].Enabled = false
	return nil
}

func (r *RBACManager) DeleteAPIKey(keyStr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.apiKeys[keyStr]; !ok {
		return errors.New("API key not found")
	}

	delete(r.apiKeys, keyStr)
	return nil
}

func (r *RBACManager) ListAPIKeys() []*APIKey {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]*APIKey, 0, len(r.apiKeys))
	for _, ak := range r.apiKeys {
		keys = append(keys, ak)
	}
	return keys
}

func (r *RBACManager) UpdateAPIKey(keyStr string, role Role, namespace string, datasets []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	apiKey, ok := r.apiKeys[keyStr]
	if !ok {
		return errors.New("API key not found")
	}

	apiKey.Role = role
	apiKey.Namespace = namespace
	apiKey.Datasets = datasets
	apiKey.Permissions = rolePermissions[role]

	return nil
}

func generateKeyID() string {
	return fmt.Sprintf("key_%s", time.Now().Format("20060102150405"))
}

func sha256Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
