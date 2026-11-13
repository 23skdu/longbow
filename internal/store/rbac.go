package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/security"
)

// Role defines the access level for a user or service.
type Role string

const (
	// RoleAdmin has full access to all system operations.
	RoleAdmin Role = "admin"
	// RoleTenantAdmin has administrative access scoped to a single tenant/namespace.
	RoleTenantAdmin Role = "tenant-admin"
	// RoleWriter / RoleReadWrite has access to read and modify data.
	RoleWriter    Role = "writer"
	RoleReadWrite Role = "read-write"
	// RoleReader / RoleReadOnly has access to read data only.
	RoleReader   Role = "reader"
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
	// PermManageKeys allows creating, rotating, and revoking API keys.
	PermManageKeys Permission = "manage-keys"
	// PermManageTenant allows managing tenant namespaces, RLS policies, and quotas.
	PermManageTenant Permission = "manage-tenant"
)

var rolePermissions = map[Role][]Permission{
	RoleAdmin:       {PermRead, PermWrite, PermDelete, PermIngest, PermAdmin, PermBackup, PermManageKeys, PermManageTenant},
	RoleTenantAdmin: {PermRead, PermWrite, PermDelete, PermIngest, PermBackup, PermManageKeys, PermManageTenant},
	RoleWriter:      {PermRead, PermWrite, PermDelete, PermIngest},
	RoleReadWrite:   {PermRead, PermWrite, PermDelete, PermIngest},
	RoleReader:      {PermRead},
	RoleReadOnly:    {PermRead},
	RoleIngest:      {PermRead, PermIngest},
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
	KeyID          string            `json:"key_id"`
	KeyHash        string            `json:"key_hash"`
	Name           string            `json:"name"`
	Role           Role              `json:"role"`
	TenantID       string            `json:"tenant_id,omitempty"`
	Namespace      string            `json:"namespace,omitempty"`
	Datasets       []string          `json:"datasets,omitempty"`
	Permissions    []Permission      `json:"permissions"`
	RowFilters     map[string]string `json:"row_filters,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	ExpiresAt      *time.Time        `json:"expires_at,omitempty"`
	GraceExpiresAt *time.Time        `json:"grace_expires_at,omitempty"`
	RevokedAt      *time.Time        `json:"revoked_at,omitempty"`
	RotatedFrom    string            `json:"rotated_from,omitempty"`
	LastUsed       time.Time         `json:"last_used"`
	Enabled        bool              `json:"enabled"`
}

// APIKeyOptions configures API key creation.
type APIKeyOptions struct {
	Name       string
	Role       Role
	TenantID   string
	Namespace  string
	Datasets   []string
	RowFilters map[string]string
	TTL        time.Duration
}

// RBACManager handles Role-Based Access Control for the vector store.
type RBACManager struct {
	mu        sync.RWMutex
	apiKeys   map[string]*APIKey
	keyHashes map[string]*APIKey
	roles     map[string]map[Role]bool
	audit     *security.AuditLogger
	rls       *security.RLSEvaluator
}

// NewRBACManager creates a new instance of RBACManager.
func NewRBACManager() *RBACManager {
	return &RBACManager{
		apiKeys:   make(map[string]*APIKey),
		keyHashes: make(map[string]*APIKey),
		roles:     make(map[string]map[Role]bool),
		audit:     security.NewAuditLogger(),
		rls:       security.NewRLSEvaluator(),
	}
}

// SetAuditLogger attaches an external AuditLogger.
func (r *RBACManager) SetAuditLogger(a *security.AuditLogger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.audit = a
}

// SetRLSEvaluator attaches a row-level security evaluator.
func (r *RBACManager) SetRLSEvaluator(e *security.RLSEvaluator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rls = e
}

// RLSEvaluator returns the active RLS evaluator.
func (r *RBACManager) RLSEvaluator() *security.RLSEvaluator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rls
}

// CreateAPIKey generates and registers a new API key with the specified role and scope.
func (r *RBACManager) CreateAPIKey(name string, role Role, namespace string, datasets []string) (*APIKey, error) {
	ak, _, err := r.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      name,
		Role:      role,
		TenantID:  namespace,
		Namespace: namespace,
		Datasets:  datasets,
	})
	return ak, err
}

// CreateAPIKeyWithOptions issues a new API key and returns both the key record and the raw secret string.
func (r *RBACManager) CreateAPIKeyWithOptions(opts APIKeyOptions) (*APIKey, string, error) {
	if _, ok := rolePermissions[opts.Role]; !ok {
		return nil, "", fmt.Errorf("invalid role: %s", opts.Role)
	}
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, "", fmt.Errorf("failed to generate key: %w", err)
	}

	keyStr := hex.EncodeToString(keyBytes)
	keyHash := sha256Hash(keyStr)

	var expiresAt *time.Time
	if opts.TTL > 0 {
		exp := time.Now().Add(opts.TTL)
		expiresAt = &exp
	}

	tenantID := opts.TenantID
	if tenantID == "" && opts.Namespace != "" && opts.Role != RoleAdmin {
		tenantID = opts.Namespace
	}

	apiKey := &APIKey{
		KeyID:       generateKeyID(),
		KeyHash:     keyHash,
		Name:        opts.Name,
		Role:        opts.Role,
		TenantID:    tenantID,
		Namespace:   opts.Namespace,
		Datasets:    append([]string(nil), opts.Datasets...),
		Permissions: append([]Permission(nil), rolePermissions[opts.Role]...),
		RowFilters:  copyStringMap(opts.RowFilters),
		CreatedAt:   time.Now(),
		ExpiresAt:   expiresAt,
		Enabled:     true,
	}

	r.mu.Lock()
	r.apiKeys[keyStr] = apiKey
	r.keyHashes[keyHash] = apiKey
	audit := r.audit
	r.mu.Unlock()

	if audit != nil {
		audit.RecordEvent(context.Background(), apiKey.KeyID, apiKey.TenantID, "create_api_key", apiKey.Namespace, "internal", true, "")
	}

	return apiKey, keyStr, nil
}

// RotateAPIKey issues a replacement key with the same role/tenant scope and places a grace period on the old key.
func (r *RBACManager) RotateAPIKey(oldRawKey string, gracePeriod time.Duration) (*APIKey, string, error) {
	r.mu.Lock()
	oldKey := r.lookupLocked(oldRawKey)
	if oldKey == nil {
		r.mu.Unlock()
		return nil, "", errors.New("API key not found")
	}
	opts := APIKeyOptions{
		Name:       oldKey.Name,
		Role:       oldKey.Role,
		TenantID:   oldKey.TenantID,
		Namespace:  oldKey.Namespace,
		Datasets:   oldKey.Datasets,
		RowFilters: oldKey.RowFilters,
	}
	if oldKey.ExpiresAt != nil {
		rem := time.Until(*oldKey.ExpiresAt)
		if rem > 0 {
			opts.TTL = rem
		}
	}
	oldID := oldKey.KeyID
	now := time.Now()
	if gracePeriod > 0 {
		grace := now.Add(gracePeriod)
		oldKey.GraceExpiresAt = &grace
	} else {
		oldKey.Enabled = false
		oldKey.RevokedAt = &now
	}
	r.mu.Unlock()

	newKey, newRawKey, err := r.CreateAPIKeyWithOptions(opts)
	if err != nil {
		return nil, "", err
	}
	r.mu.Lock()
	newKey.RotatedFrom = oldID
	audit := r.audit
	r.mu.Unlock()

	if audit != nil {
		audit.RecordEvent(context.Background(), newKey.KeyID, newKey.TenantID, "rotate_api_key", oldID, "internal", true, "")
	}
	return newKey, newRawKey, nil
}

// GetAPIKey retrieves an API key by its raw string value or SHA-256 hash.
func (r *RBACManager) GetAPIKey(keyStr string) (*APIKey, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ak := r.lookupLocked(keyStr)
	return ak, ak != nil
}

func (r *RBACManager) lookupLocked(keyStr string) *APIKey {
	if ak, ok := r.apiKeys[keyStr]; ok {
		return ak
	}
	if ak, ok := r.keyHashes[sha256Hash(keyStr)]; ok {
		return ak
	}
	if ak, ok := r.keyHashes[keyStr]; ok {
		return ak
	}
	return nil
}

// ValidateAPIKey checks if an API key exists, is enabled, and has not expired.
func (r *RBACManager) ValidateAPIKey(keyStr string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		return false, errors.New("invalid API key")
	}

	if !apiKey.Enabled {
		return false, errors.New("API key disabled")
	}

	now := time.Now()
	if apiKey.GraceExpiresAt != nil && now.After(*apiKey.GraceExpiresAt) {
		return false, errors.New("API key rotation grace period expired")
	}
	if apiKey.ExpiresAt != nil && now.After(*apiKey.ExpiresAt) {
		return false, errors.New("API key expired")
	}

	return true, nil
}

// AuthorizeToken implements security.AccessValidator, validating key, permission, and tenant/dataset scope.
func (r *RBACManager) AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*security.TenantContext, error) {
	perm := Permission(permission)
	r.mu.RLock()
	apiKey := r.lookupLocked(rawToken)
	audit := r.audit
	if apiKey == nil {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, "", namespace, "authorize:"+permission, dataset, "rpc", false, "invalid API key")
		}
		return nil, fmt.Errorf("%w: invalid API key", security.ErrUnauthorized)
	}

	if !apiKey.Enabled {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, dataset, "rpc", false, "API key disabled")
		}
		return nil, fmt.Errorf("%w: API key disabled", security.ErrUnauthorized)
	}

	now := time.Now()
	if apiKey.GraceExpiresAt != nil && now.After(*apiKey.GraceExpiresAt) {
		r.mu.RUnlock()
		return nil, fmt.Errorf("%w: API key rotation grace period expired", security.ErrUnauthorized)
	}
	if apiKey.ExpiresAt != nil && now.After(*apiKey.ExpiresAt) {
		r.mu.RUnlock()
		return nil, fmt.Errorf("%w: API key expired", security.ErrUnauthorized)
	}

	if perm != "" && !apiKey.Role.HasPermission(perm) {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, dataset, "rpc", false, "permission denied")
		}
		return nil, fmt.Errorf("%w: permission denied for role %s", security.ErrForbidden, apiKey.Role)
	}

	// Enforce tenant/namespace boundary (Admin bypasses cross-namespace restriction unless key specifies namespace)
	if apiKey.Namespace != "" && apiKey.Namespace != "*" && namespace != "" && apiKey.Namespace != namespace {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, namespace+"/"+dataset, "rpc", false, "namespace mismatch")
		}
		return nil, fmt.Errorf("%w: namespace mismatch (%s)", security.ErrTenantIsolation, namespace)
	}

	if len(apiKey.Datasets) > 0 && dataset != "" && !datasetAllowed(apiKey.Datasets, dataset) {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, dataset, "rpc", false, "dataset not authorized")
		}
		return nil, fmt.Errorf("%w: dataset not authorized", security.ErrForbidden)
	}

	perms := make([]string, len(apiKey.Permissions))
	for i, p := range apiKey.Permissions {
		perms[i] = string(p)
	}
	tc := &security.TenantContext{
		TenantID:    apiKey.TenantID,
		Namespace:   apiKey.Namespace,
		UserID:      apiKey.Name,
		KeyID:       apiKey.KeyID,
		Roles:       []string{string(apiKey.Role)},
		Permissions: perms,
		Datasets:    append([]string(nil), apiKey.Datasets...),
		RowFilters:  copyStringMap(apiKey.RowFilters),
		IsAdmin:     apiKey.Role == RoleAdmin && (apiKey.Namespace == "" || apiKey.Namespace == "*"),
	}
	r.mu.RUnlock()

	if audit != nil {
		audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, dataset, "rpc", true, "")
	}
	return tc, nil
}

// CheckPermission verifies if the provided API key has permission for a specific action in a namespace/dataset.
func (r *RBACManager) CheckPermission(keyStr string, perm Permission, namespace, dataset string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		return false, errors.New("invalid API key")
	}

	if !apiKey.Enabled {
		return false, errors.New("API key disabled")
	}

	now := time.Now()
	if apiKey.GraceExpiresAt != nil && now.After(*apiKey.GraceExpiresAt) {
		return false, errors.New("API key rotation grace period expired")
	}
	if apiKey.ExpiresAt != nil && now.After(*apiKey.ExpiresAt) {
		return false, errors.New("API key expired")
	}

	if !apiKey.Role.HasPermission(perm) {
		return false, errors.New("permission denied")
	}

	if apiKey.Namespace != "" && apiKey.Namespace != "*" && apiKey.Namespace != namespace {
		return false, errors.New("namespace mismatch")
	}

	if len(apiKey.Datasets) > 0 && !datasetAllowed(apiKey.Datasets, dataset) {
		return false, errors.New("dataset not authorized")
	}

	return true, nil
}

func datasetAllowed(allowed []string, dataset string) bool {
	for _, ds := range allowed {
		if ds == "*" || ds == dataset {
			return true
		}
	}
	return false
}

// RevokeAPIKey disables an existing API key by raw secret, KeyID, or hash.
func (r *RBACManager) RevokeAPIKey(keyStr string) error {
	r.mu.Lock()
	apiKey := r.findByRawHashOrIDLocked(keyStr)
	if apiKey == nil {
		r.mu.Unlock()
		return errors.New("API key not found")
	}

	now := time.Now()
	apiKey.Enabled = false
	apiKey.RevokedAt = &now
	audit := r.audit
	keyID := apiKey.KeyID
	tenantID := apiKey.TenantID
	r.mu.Unlock()

	if audit != nil {
		audit.RecordEvent(context.Background(), keyID, tenantID, "revoke_api_key", keyID, "internal", true, "")
	}
	return nil
}

// DeleteAPIKey removes an API key from the registry.
func (r *RBACManager) DeleteAPIKey(keyStr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		return errors.New("API key not found")
	}

	delete(r.apiKeys, keyStr)
	delete(r.keyHashes, apiKey.KeyHash)
	for raw, candidate := range r.apiKeys {
		if candidate == apiKey {
			delete(r.apiKeys, raw)
		}
	}
	return nil
}

// ListAPIKeys returns a list of all registered API keys (metadata without exposing raw secrets).
func (r *RBACManager) ListAPIKeys() []*APIKey {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{}, len(r.keyHashes))
	keys := make([]*APIKey, 0, len(r.apiKeys))
	for _, ak := range r.apiKeys {
		if _, dup := seen[ak.KeyID]; dup {
			continue
		}
		seen[ak.KeyID] = struct{}{}
		keys = append(keys, ak)
	}
	return keys
}

// UpdateAPIKey updates the properties of an existing API key.
func (r *RBACManager) UpdateAPIKey(keyStr string, role Role, namespace string, datasets []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		return errors.New("API key not found")
	}

	apiKey.Role = role
	apiKey.Namespace = namespace
	apiKey.Datasets = append([]string(nil), datasets...)
	apiKey.Permissions = append([]Permission(nil), rolePermissions[role]...)

	return nil
}

func (r *RBACManager) findByRawHashOrIDLocked(idOrSecret string) *APIKey {
	if ak := r.lookupLocked(idOrSecret); ak != nil {
		return ak
	}
	for _, ak := range r.apiKeys {
		if ak.KeyID == idOrSecret {
			return ak
		}
	}
	return nil
}

var keyIDCounter atomic.Uint64

func generateKeyID() string {
	seq := keyIDCounter.Add(1)
	return fmt.Sprintf("key_%s_%06d", time.Now().Format("20060102150405"), seq)
}

func sha256Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
