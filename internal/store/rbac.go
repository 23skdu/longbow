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

// Role defines the access level for a user or service.
type Role string

const (
	// RoleAdmin has full access to all system operations.
	RoleAdmin Role = "admin"
	// RoleReadWrite has access to read and modify data.
	RoleReadWrite Role = "read-write"
	// RoleReadOnly has access to read data only.
	RoleReadOnly Role = "read-only"
	// RoleIngest has access to read data and ingest new vectors.
	RoleIngest Role = "ingest-only"
)

// Permission defines a specific action that can be performed in the system.
type Permission string

const (
	// PermRead allows reading vector data and metadata.
	PermRead Permission = "read"
	// PermWrite allows updating existing vectors and metadata.
	PermWrite Permission = "write"
	// PermDelete allows deleting vectors and datasets.
	PermDelete Permission = "delete"
	// PermIngest allows adding new vectors to datasets.
	PermIngest Permission = "ingest"
	// PermAdmin allows performing administrative tasks like backup and sync.
	PermAdmin Permission = "admin"
	// PermBackup allows performing dataset backups.
	PermBackup Permission = "backup"
)

var rolePermissions = map[Role][]Permission{
	RoleAdmin:     {PermRead, PermWrite, PermDelete, PermIngest, PermAdmin, PermBackup},
	RoleReadWrite: {PermRead, PermWrite, PermDelete, PermIngest},
	RoleReadOnly:  {PermRead},
	RoleIngest:    {PermRead, PermIngest},
}

// HasPermission checks if a role has the specified permission.
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

// APIKey represents a security credential for accessing the store with associated permissions.
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

// RBACManager handles Role-Based Access Control for the vector store.
type RBACManager struct {
	mu      sync.RWMutex
	apiKeys map[string]*APIKey
	roles   map[string]map[Role]bool
}

// NewRBACManager creates a new instance of RBACManager.
func NewRBACManager() *RBACManager {
	return &RBACManager{
		apiKeys: make(map[string]*APIKey),
		roles:   make(map[string]map[Role]bool),
	}
}

// CreateAPIKey generates and registers a new API key with the specified role and scope.
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

// GetAPIKey retrieves an API key by its raw string value.
func (r *RBACManager) GetAPIKey(keyStr string) (*APIKey, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ak, ok := r.apiKeys[keyStr]
	return ak, ok
}

// ValidateAPIKey checks if an API key exists, is enabled, and has not expired.
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

// CheckPermission verifies if the provided API key has permission for a specific action in a namespace/dataset.
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

// RevokeAPIKey disables an existing API key.
func (r *RBACManager) RevokeAPIKey(keyStr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.apiKeys[keyStr]; !ok {
		return errors.New("API key not found")
	}

	r.apiKeys[keyStr].Enabled = false
	return nil
}

// DeleteAPIKey removes an API key from the registry.
func (r *RBACManager) DeleteAPIKey(keyStr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.apiKeys[keyStr]; !ok {
		return errors.New("API key not found")
	}

	delete(r.apiKeys, keyStr)
	return nil
}

// ListAPIKeys returns a list of all registered API keys.
func (r *RBACManager) ListAPIKeys() []*APIKey {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]*APIKey, 0, len(r.apiKeys))
	for _, ak := range r.apiKeys {
		keys = append(keys, ak)
	}
	return keys
}

// UpdateAPIKey updates the properties of an existing API key.
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
