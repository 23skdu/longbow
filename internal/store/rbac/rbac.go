package rbac

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/security/secrets"
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

// NamespaceRole is a role granted to an API key inside one namespace.
type NamespaceRole string

const (
	NamespaceRoleAdmin  NamespaceRole = "admin"
	NamespaceRoleWriter NamespaceRole = "writer"
	NamespaceRoleReader NamespaceRole = "reader"
)

// NamespacePolicy is the ACL attached to a namespace. Namespaces are created
// by a server administrator; the policy records the namespace admin and its
// read/write users, plus an optional public permission set that is granted to
// every authenticated API key.
type NamespacePolicy struct {
	Namespace         string       `json:"namespace"`
	AdminKeyIDs       []string     `json:"admin_key_ids,omitempty"`
	WriterKeyIDs      []string     `json:"writer_key_ids,omitempty"`
	ReaderKeyIDs      []string     `json:"reader_key_ids,omitempty"`
	PublicPermissions []Permission `json:"public_permissions,omitempty"`
	// Ephemeral policies exist only in runtime memory. They are recreated from
	// startup configuration on each process start and are not written to the
	// secret store.
	Ephemeral bool `json:"ephemeral,omitempty"`
}

// ValidateNamespaceName enforces one namespace-name syntax for startup
// policies, runtime creation, and REST/Flight payloads. Names become the first
// path segment of a dataset path, so separators and whitespace are unsafe.
func ValidateNamespaceName(name string) error {
	if name != strings.TrimSpace(name) {
		return errors.New("namespace name must not contain leading or trailing whitespace")
	}
	if name == "" {
		return errors.New("namespace name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("namespace name must be at most 128 characters")
	}
	if strings.ContainsAny(name, "/\\\n\r\t\v\f ") || strings.Contains(name, "..") {
		return fmt.Errorf("invalid namespace name %q", name)
	}
	return nil
}

// Validate rejects unknown public permissions and invalid namespace names.
func (p NamespacePolicy) Validate() error {
	if err := ValidateNamespaceName(p.Namespace); err != nil {
		return fmt.Errorf("namespace policy: %w", err)
	}
	for _, perm := range p.PublicPermissions {
		switch perm {
		case PermRead, PermWrite, PermDelete, PermIngest:
		default:
			return fmt.Errorf("unsupported public namespace permission %q", perm)
		}
	}
	return nil
}

// Allows reports whether the policy grants perm to the general authenticated
// population. Write/delete/ingest grants imply read access.
func (p NamespacePolicy) Allows(perm Permission) bool {
	for _, granted := range p.PublicPermissions {
		if granted == perm || granted == "*" {
			return true
		}
		if (granted == PermWrite || granted == PermDelete || granted == PermIngest) && perm == PermRead {
			return true
		}
	}
	return false
}

func namespacePermissionsForRole(role NamespaceRole) []Permission {
	switch role {
	case NamespaceRoleAdmin:
		// Namespace admins manage data in one namespace only. Global key and
		// tenant administration are intentionally not granted here.
		return []Permission{PermRead, PermWrite, PermDelete, PermIngest}
	case NamespaceRoleWriter:
		return []Permission{PermRead, PermWrite, PermDelete, PermIngest}
	case NamespaceRoleReader:
		return []Permission{PermRead}
	default:
		return nil
	}
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func permissionIn(list []string, want Permission) bool {
	for _, p := range list {
		if p == string(want) || p == "*" {
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
	KeyID      string // optional stable identifier (used by bootstrap/admin keys)
	Name       string
	Role       Role
	TenantID   string
	Namespace  string
	Datasets   []string
	RowFilters map[string]string
	TTL        time.Duration
}

// RBACManager handles Role-Based Access Control for the vector store. Raw API
// secrets are never retained in memory: apiKeys is keyed by SHA-256 hash and
// the durable raw secret lives in the configured secrets.Store.
type RBACManager struct {
	mu       sync.RWMutex
	apiKeys  map[string]*APIKey // by key hash
	keyIDs   map[string]*APIKey
	policies map[string]*NamespacePolicy
	store    secrets.Store
	audit    *security.AuditLogger
	rls      *security.RLSEvaluator
}

// NewRBACManager creates an RBACManager backed by a process-local secret store.
// Use NewRBACManagerWithStore for durable provider-backed storage.
func NewRBACManager() *RBACManager {
	mgr, _ := NewRBACManagerWithStore(context.Background(), secrets.NewMemoryStore())
	return mgr
}

// NewRBACManagerWithStore creates an RBACManager and rehydrates all previously
// issued API keys and durable namespace ACLs from the supplied secret store.
func NewRBACManagerWithStore(ctx context.Context, store secrets.Store) (*RBACManager, error) {
	if store == nil {
		store = secrets.NewMemoryStore()
	}
	mgr := &RBACManager{
		apiKeys:  make(map[string]*APIKey),
		keyIDs:   make(map[string]*APIKey),
		policies: make(map[string]*NamespacePolicy),
		store:    store,
		audit:    security.NewAuditLogger(),
		rls:      security.NewRLSEvaluator(),
	}
	stored, err := store.LoadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load RBAC records: %w", err)
	}
	for _, item := range stored {
		switch item.Kind {
		case "", secrets.KindAPIKey:
			if err := mgr.loadStoredKey(item); err != nil {
				return nil, err
			}
		case secrets.KindNamespacePolicy:
			if err := mgr.loadStoredPolicy(item); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported RBAC record kind %q for %q", item.Kind, item.KeyID)
		}
	}
	return mgr, nil
}

func (r *RBACManager) loadStoredKey(item secrets.StoredKey) error {
	if len(item.Meta) == 0 {
		return fmt.Errorf("stored RBAC key %q has no metadata", item.KeyID)
	}
	var ak APIKey
	if err := json.Unmarshal(item.Meta, &ak); err != nil {
		return fmt.Errorf("decode stored RBAC key %q: %w", item.KeyID, err)
	}
	if ak.KeyID == "" {
		ak.KeyID = item.KeyID
	}
	if ak.KeyHash == "" {
		ak.KeyHash = sha256Hash(item.Raw)
	}
	if ak.KeyHash != sha256Hash(item.Raw) {
		return fmt.Errorf("stored RBAC key %q does not match its hash", item.KeyID)
	}
	if !ak.Enabled {
		return nil
	}
	r.apiKeys[ak.KeyHash] = &ak
	r.keyIDs[ak.KeyID] = &ak
	return nil
}

func (r *RBACManager) loadStoredPolicy(item secrets.StoredKey) error {
	if len(item.Meta) == 0 {
		return fmt.Errorf("stored namespace policy %q has no metadata", item.KeyID)
	}
	var policy NamespacePolicy
	if err := json.Unmarshal(item.Meta, &policy); err != nil {
		return fmt.Errorf("decode stored namespace policy %q: %w", item.KeyID, err)
	}
	// Ephemeral policies are never written by Longbow. Ignore stale records so
	// a provider copied between environments cannot resurrect runtime-only ACLs.
	if policy.Ephemeral {
		return nil
	}
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("stored namespace policy %q: %w", item.KeyID, err)
	}
	if want := secrets.NamespacePolicyKeyID(policy.Namespace); item.KeyID != want {
		return fmt.Errorf("stored namespace policy %q has unexpected record ID %q", policy.Namespace, item.KeyID)
	}
	r.policies[policy.Namespace] = &policy
	return nil
}

// HasGlobalAdmin reports whether at least one enabled server administrator key
// is available. Used at startup to decide whether a bootstrap key is required.
func (r *RBACManager) HasGlobalAdmin() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ak := range r.apiKeys {
		if ak.Enabled && ak.Role == RoleAdmin && (ak.Namespace == "" || ak.Namespace == "*") {
			return true
		}
	}
	return false
}

// SetNamespacePolicy registers or replaces a namespace ACL. Durable policies
// are written through to the secret store; ephemeral policies remain runtime
// only. Callers must enforce server-admin authorization before invoking it.
func (r *RBACManager) SetNamespacePolicy(policy NamespacePolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	copyPolicy := cloneNamespacePolicy(policy)
	if policy.Ephemeral {
		// An ephemeral ACL must not leave a durable record behind, even when it
		// replaces a previously durable policy for the same namespace.
		if err := r.deleteStoredNamespacePolicy(policy.Namespace); err != nil {
			return err
		}
	} else if err := r.persistNamespacePolicy(copyPolicy); err != nil {
		return err
	}
	r.mu.Lock()
	r.policies[policy.Namespace] = &copyPolicy
	r.mu.Unlock()
	return nil
}

func cloneAPIKey(key APIKey) APIKey {
	copyKey := key
	copyKey.Datasets = append([]string(nil), key.Datasets...)
	copyKey.Permissions = append([]Permission(nil), key.Permissions...)
	copyKey.RowFilters = copyStringMap(key.RowFilters)
	if key.ExpiresAt != nil {
		v := *key.ExpiresAt
		copyKey.ExpiresAt = &v
	}
	if key.GraceExpiresAt != nil {
		v := *key.GraceExpiresAt
		copyKey.GraceExpiresAt = &v
	}
	if key.RevokedAt != nil {
		v := *key.RevokedAt
		copyKey.RevokedAt = &v
	}
	return copyKey
}

func cloneNamespacePolicy(policy NamespacePolicy) NamespacePolicy {
	copyPolicy := policy
	copyPolicy.AdminKeyIDs = append([]string(nil), policy.AdminKeyIDs...)
	copyPolicy.WriterKeyIDs = append([]string(nil), policy.WriterKeyIDs...)
	copyPolicy.ReaderKeyIDs = append([]string(nil), policy.ReaderKeyIDs...)
	copyPolicy.PublicPermissions = append([]Permission(nil), policy.PublicPermissions...)
	return copyPolicy
}

func (r *RBACManager) persistNamespacePolicy(policy NamespacePolicy) error {
	meta, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("encode namespace policy %q: %w", policy.Namespace, err)
	}
	record := secrets.StoredKey{
		KeyID: secrets.NamespacePolicyKeyID(policy.Namespace),
		Kind:  secrets.KindNamespacePolicy,
		Meta:  meta,
	}
	if err := r.store.Put(context.Background(), record); err != nil {
		return fmt.Errorf("persist namespace policy %q: %w", policy.Namespace, err)
	}
	return nil
}

func (r *RBACManager) deleteStoredNamespacePolicy(namespace string) error {
	err := r.store.Delete(context.Background(), secrets.NamespacePolicyKeyID(namespace))
	if err != nil && !errors.Is(err, secrets.ErrNotFound) {
		return fmt.Errorf("delete namespace policy %q: %w", namespace, err)
	}
	return nil
}

// GetNamespacePolicy returns a copy of the ACL for a namespace, or nil.
func (r *RBACManager) GetNamespacePolicy(namespace string) *NamespacePolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	policy := r.policies[namespace]
	if policy == nil {
		return nil
	}
	copyPolicy := *policy
	copyPolicy.AdminKeyIDs = append([]string(nil), policy.AdminKeyIDs...)
	copyPolicy.WriterKeyIDs = append([]string(nil), policy.WriterKeyIDs...)
	copyPolicy.ReaderKeyIDs = append([]string(nil), policy.ReaderKeyIDs...)
	copyPolicy.PublicPermissions = append([]Permission(nil), policy.PublicPermissions...)
	return &copyPolicy
}

// ListNamespacePolicies returns copies of all namespace ACLs.
func (r *RBACManager) ListNamespacePolicies() []NamespacePolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NamespacePolicy, 0, len(r.policies))
	for _, policy := range r.policies {
		out = append(out, *r.copyPolicyLocked(policy))
	}
	return out
}

func (r *RBACManager) copyPolicyLocked(policy *NamespacePolicy) *NamespacePolicy {
	if policy == nil {
		return nil
	}
	copyPolicy := *policy
	copyPolicy.AdminKeyIDs = append([]string(nil), policy.AdminKeyIDs...)
	copyPolicy.WriterKeyIDs = append([]string(nil), policy.WriterKeyIDs...)
	copyPolicy.ReaderKeyIDs = append([]string(nil), policy.ReaderKeyIDs...)
	copyPolicy.PublicPermissions = append([]Permission(nil), policy.PublicPermissions...)
	return &copyPolicy
}

// DeleteNamespacePolicy removes the ACL for a namespace and its durable record.
// Ephemeral policies have no durable record to remove.
func (r *RBACManager) DeleteNamespacePolicy(namespace string) error {
	r.mu.RLock()
	policy := r.copyPolicyLocked(r.policies[namespace])
	r.mu.RUnlock()
	if policy == nil {
		return nil
	}
	// Remove the durable ACL before dropping the in-memory policy. If the
	// provider write fails, restart rehydration must still see the ACL.
	if !policy.Ephemeral {
		if err := r.deleteStoredNamespacePolicy(namespace); err != nil {
			return err
		}
	}
	r.mu.Lock()
	delete(r.policies, namespace)
	r.mu.Unlock()
	return nil
}

// replaceNamespaceKeyIDLocked rewrites ACL references after key rotation. It
// persists all durable changes before committing any in-memory policy, and
// rolls back records already written when a later write fails. The caller must
// hold r.mu.
func (r *RBACManager) replaceNamespaceKeyIDLocked(oldID, newID string) error {
	type policyUpdate struct {
		namespace string
		before    NamespacePolicy
		after     NamespacePolicy
	}
	var updates []policyUpdate
	for namespace, policy := range r.policies {
		after := cloneNamespacePolicy(*policy)
		changed := false
		for _, list := range [][]string{after.AdminKeyIDs, after.WriterKeyIDs, after.ReaderKeyIDs} {
			for i, id := range list {
				if id == oldID {
					list[i] = newID
					changed = true
				}
			}
		}
		if changed {
			updates = append(updates, policyUpdate{
				namespace: namespace,
				before:    cloneNamespacePolicy(*policy),
				after:     after,
			})
		}
	}

	var applied []policyUpdate
	for _, update := range updates {
		if update.after.Ephemeral {
			continue
		}
		if err := r.persistNamespacePolicy(update.after); err != nil {
			for _, done := range applied {
				_ = r.persistNamespacePolicy(done.before)
			}
			return fmt.Errorf("persist rotated namespace ACL %q: %w", update.namespace, err)
		}
		applied = append(applied, update)
	}
	for _, update := range updates {
		after := update.after
		r.policies[update.namespace] = &after
	}
	return nil
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

// RegisterAPIKeyWithSecret registers an operator-supplied raw secret with an
// explicit scope. It is used for bootstrap credentials configured before the
// server starts, so the caller already knows the secret and does not need it
// echoed back.
func (r *RBACManager) RegisterAPIKeyWithSecret(opts APIKeyOptions, raw string) (*APIKey, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 16 {
		return nil, fmt.Errorf("bootstrap API key must be at least 16 characters")
	}
	hash := sha256Hash(raw)
	r.mu.RLock()
	existing := r.apiKeys[hash]
	r.mu.RUnlock()
	if existing != nil {
		// Idempotent restart registration of the same configured bootstrap key.
		return existing, nil
	}
	if opts.KeyID == "" {
		opts.KeyID = "bootstrap-admin"
	}
	return r.createAPIKey(opts, raw)
}

// CreateAPIKeyWithOptions issues a new API key and returns both the key record and the raw secret string.
func (r *RBACManager) CreateAPIKeyWithOptions(opts APIKeyOptions) (*APIKey, string, error) {
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, "", fmt.Errorf("failed to generate key: %w", err)
	}
	raw := hex.EncodeToString(keyBytes)
	ak, err := r.createAPIKey(opts, raw)
	return ak, raw, err
}

func (r *RBACManager) createAPIKey(opts APIKeyOptions, keyStr string) (*APIKey, error) {
	if _, ok := rolePermissions[opts.Role]; !ok {
		return nil, fmt.Errorf("invalid role: %s", opts.Role)
	}
	keyID := opts.KeyID
	if keyID == "" {
		generated, err := generateKeyID()
		if err != nil {
			return nil, err
		}
		keyID = generated
	}
	if err := secrets.ValidateKeyID(keyID); err != nil {
		return nil, fmt.Errorf("invalid API key id: %w", err)
	}
	if secrets.IsNamespacePolicyKeyID(keyID) {
		return nil, fmt.Errorf("invalid API key id %q: reserved for namespace policies", keyID)
	}
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
		KeyID:       keyID,
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

	meta, err := json.Marshal(apiKey)
	if err != nil {
		return nil, fmt.Errorf("encode RBAC key metadata: %w", err)
	}
	if err := r.store.Put(context.Background(), secrets.StoredKey{
		KeyID: keyID,
		Kind:  secrets.KindAPIKey,
		Raw:   keyStr,
		Meta:  meta,
	}); err != nil {
		return nil, fmt.Errorf("persist RBAC API key: %w", err)
	}

	r.mu.Lock()
	if old := r.keyIDs[keyID]; old != nil && old.KeyHash != keyHash {
		delete(r.apiKeys, old.KeyHash)
	}
	r.apiKeys[keyHash] = apiKey
	r.keyIDs[keyID] = apiKey
	audit := r.audit
	r.mu.Unlock()

	if audit != nil {
		audit.RecordEvent(context.Background(), apiKey.KeyID, apiKey.TenantID, "create_api_key", apiKey.Namespace, "internal", true, "")
	}

	return apiKey, nil
}

// persistAPIKeyMeta writes an updated key record back to the secret store while
// preserving the provider-stored raw secret. Admin mutations are infrequent,
// so re-reading the provider is preferable to caching raw secrets in memory.
func (r *RBACManager) persistAPIKeyMeta(ak *APIKey) error {
	meta, err := json.Marshal(ak)
	if err != nil {
		return fmt.Errorf("encode RBAC key metadata: %w", err)
	}
	ctx := context.Background()
	stored, err := r.store.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("load RBAC secret store: %w", err)
	}
	for _, item := range stored {
		if item.KeyID != ak.KeyID {
			continue
		}
		item.Meta = meta
		if err := r.store.Put(ctx, item); err != nil {
			return fmt.Errorf("persist RBAC API key: %w", err)
		}
		return nil
	}
	return fmt.Errorf("RBAC API key %q not found in secret store", ak.KeyID)
}

// RotateAPIKey issues a replacement key with the same role/tenant scope and places a grace period on the old key.
func (r *RBACManager) RotateAPIKey(oldRawKey string, gracePeriod time.Duration) (*APIKey, string, error) {
	r.mu.RLock()
	oldKey := r.lookupLocked(oldRawKey)
	if oldKey == nil {
		r.mu.RUnlock()
		return nil, "", errors.New("API key not found")
	}
	opts := APIKeyOptions{
		Name:       oldKey.Name,
		Role:       oldKey.Role,
		TenantID:   oldKey.TenantID,
		Namespace:  oldKey.Namespace,
		Datasets:   append([]string(nil), oldKey.Datasets...),
		RowFilters: copyStringMap(oldKey.RowFilters),
	}
	if oldKey.ExpiresAt != nil {
		rem := time.Until(*oldKey.ExpiresAt)
		if rem > 0 {
			opts.TTL = rem
		}
	}
	oldID := oldKey.KeyID
	r.mu.RUnlock()

	// Issue the replacement first. A provider failure here leaves the working
	// credential untouched instead of revoking it without a successor.
	newKey, newRawKey, err := r.CreateAPIKeyWithOptions(opts)
	if err != nil {
		return nil, "", err
	}

	r.mu.Lock()
	currentOld := r.keyIDs[oldID]
	if currentOld == nil {
		r.rollbackRotatedKeyLocked(newKey)
		r.mu.Unlock()
		return nil, "", errors.New("old API key disappeared during rotation")
	}
	snapshot := cloneAPIKey(*currentOld)
	now := time.Now()
	if gracePeriod > 0 {
		grace := now.Add(gracePeriod)
		currentOld.GraceExpiresAt = &grace
	} else {
		currentOld.Enabled = false
		currentOld.RevokedAt = &now
	}
	if err := r.persistAPIKeyMeta(currentOld); err != nil {
		r.restoreRotatedKeyLocked(currentOld, snapshot)
		r.rollbackRotatedKeyLocked(newKey)
		r.mu.Unlock()
		return nil, "", err
	}
	newKey.RotatedFrom = oldID
	if err := r.persistAPIKeyMeta(newKey); err != nil {
		r.restoreRotatedKeyLocked(currentOld, snapshot)
		r.rollbackRotatedKeyLocked(newKey)
		r.mu.Unlock()
		return nil, "", err
	}
	if err := r.replaceNamespaceKeyIDLocked(oldID, newKey.KeyID); err != nil {
		r.restoreRotatedKeyLocked(currentOld, snapshot)
		r.rollbackRotatedKeyLocked(newKey)
		r.mu.Unlock()
		return nil, "", err
	}
	audit := r.audit
	r.mu.Unlock()

	if audit != nil {
		audit.RecordEvent(context.Background(), newKey.KeyID, newKey.TenantID, "rotate_api_key", oldID, "internal", true, "")
	}
	return newKey, newRawKey, nil
}

// restoreRotatedKeyLocked restores an old key after a failed rotation. The
// caller must hold r.mu.
func (r *RBACManager) restoreRotatedKeyLocked(key *APIKey, snapshot APIKey) {
	*key = snapshot
	_ = r.persistAPIKeyMeta(key)
}

// rollbackRotatedKeyLocked removes a replacement key that was issued before a
// later rotation step failed. The caller must hold r.mu.
func (r *RBACManager) rollbackRotatedKeyLocked(key *APIKey) {
	now := time.Now()
	key.Enabled = false
	key.RevokedAt = &now
	_ = r.persistAPIKeyMeta(key)
	delete(r.apiKeys, key.KeyHash)
	delete(r.keyIDs, key.KeyID)
	_ = r.store.Delete(context.Background(), key.KeyID)
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
		return ak // already a SHA-256 hash
	}
	if ak, ok := r.apiKeys[sha256Hash(keyStr)]; ok {
		return ak
	}
	return r.keyIDs[keyStr]
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

// namespacePermissionsLocked builds the per-namespace grants for a key. It is
// called with r.mu held. Grants are attached to every request context so the
// store can authorize a namespace even when the auth interceptor did not see
// one in the request metadata.
func (r *RBACManager) namespacePermissionsLocked(apiKey *APIKey) map[string][]string {
	out := make(map[string][]string)
	for name, policy := range r.policies {
		var granted []Permission
		switch {
		case containsString(policy.AdminKeyIDs, apiKey.KeyID):
			granted = namespacePermissionsForRole(NamespaceRoleAdmin)
		case containsString(policy.WriterKeyIDs, apiKey.KeyID):
			granted = namespacePermissionsForRole(NamespaceRoleWriter)
		case containsString(policy.ReaderKeyIDs, apiKey.KeyID):
			granted = namespacePermissionsForRole(NamespaceRoleReader)
		default:
			for _, perm := range []Permission{PermRead, PermWrite, PermDelete, PermIngest} {
				if policy.Allows(perm) {
					granted = append(granted, perm)
				}
			}
		}
		if len(granted) == 0 {
			continue
		}
		strs := make([]string, len(granted))
		for i, perm := range granted {
			strs[i] = string(perm)
		}
		out[name] = strs
	}
	return out
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

	nsPerms := r.namespacePermissionsLocked(apiKey)
	if perm != "" && !apiKey.Role.HasPermission(perm) && !permissionIn(nsPerms[namespace], perm) {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, dataset, "rpc", false, "permission denied")
		}
		return nil, fmt.Errorf("%w: permission denied for role %s", security.ErrForbidden, apiKey.Role)
	}

	// Namespace ACL grants override the key's default tenant boundary.
	if len(nsPerms[namespace]) == 0 &&
		apiKey.Namespace != "" && apiKey.Namespace != "*" && namespace != "" && apiKey.Namespace != namespace {
		r.mu.RUnlock()
		if audit != nil {
			audit.RecordEvent(ctx, apiKey.KeyID, apiKey.TenantID, "authorize:"+permission, namespace+"/"+dataset, "rpc", false, "namespace mismatch")
		}
		return nil, fmt.Errorf("%w: namespace mismatch (%s)", security.ErrTenantIsolation, namespace)
	}

	// Public/namespace ACL grants intentionally ignore dataset allow-lists so
	// any authenticated key can reach the namespace's datasets.
	if len(nsPerms[namespace]) == 0 &&
		len(apiKey.Datasets) > 0 && dataset != "" && !datasetAllowed(apiKey.Datasets, namespace, dataset) {
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
		TenantID:             apiKey.TenantID,
		Namespace:            apiKey.Namespace,
		UserID:               apiKey.Name,
		KeyID:                apiKey.KeyID,
		Roles:                []string{string(apiKey.Role)},
		Permissions:          perms,
		Datasets:             append([]string(nil), apiKey.Datasets...),
		RowFilters:           copyStringMap(apiKey.RowFilters),
		IsAdmin:              apiKey.Role == RoleAdmin && (apiKey.Namespace == "" || apiKey.Namespace == "*"),
		NamespacePermissions: nsPerms,
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

	nsPerms := r.namespacePermissionsLocked(apiKey)
	if !apiKey.Role.HasPermission(perm) && !permissionIn(nsPerms[namespace], perm) {
		return false, errors.New("permission denied")
	}

	if len(nsPerms[namespace]) == 0 && apiKey.Namespace != "" && apiKey.Namespace != "*" && apiKey.Namespace != namespace {
		return false, errors.New("namespace mismatch")
	}

	if len(nsPerms[namespace]) == 0 && len(apiKey.Datasets) > 0 && !datasetAllowed(apiKey.Datasets, namespace, dataset) {
		return false, errors.New("dataset not authorized")
	}

	return true, nil
}

func datasetAllowed(allowed []string, namespace, dataset string) bool {
	full := dataset
	if namespace != "" && !strings.HasPrefix(dataset, namespace+"/") {
		full = namespace + "/" + dataset
	}
	short := dataset
	if namespace != "" {
		short = strings.TrimPrefix(dataset, namespace+"/")
	}
	for _, ds := range allowed {
		if ds == "*" || ds == dataset || ds == full || ds == short {
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
	if err := r.persistAPIKeyMeta(apiKey); err != nil {
		r.mu.Unlock()
		return err
	}
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
	r.mu.RLock()
	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		r.mu.RUnlock()
		return errors.New("API key not found")
	}
	keyHash := apiKey.KeyHash
	keyID := apiKey.KeyID
	r.mu.RUnlock()

	if err := r.store.Delete(context.Background(), keyID); err != nil {
		return fmt.Errorf("delete persisted RBAC API key: %w", err)
	}
	r.mu.Lock()
	delete(r.apiKeys, keyHash)
	delete(r.keyIDs, keyID)
	r.mu.Unlock()
	return nil
}

// ListAPIKeys returns a list of all registered API keys (metadata without exposing raw secrets).
func (r *RBACManager) ListAPIKeys() []*APIKey {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{}, len(r.apiKeys))
	keys := make([]*APIKey, 0, len(r.apiKeys))
	for _, ak := range r.apiKeys {
		if _, dup := seen[ak.KeyID]; dup {
			continue
		}
		seen[ak.KeyID] = struct{}{}
		copyKey := cloneAPIKey(*ak)
		keys = append(keys, &copyKey)
	}
	return keys
}

// UpdateAPIKey updates the properties of an existing API key.
func (r *RBACManager) UpdateAPIKey(keyStr string, role Role, namespace string, datasets []string) error {
	perms, ok := rolePermissions[role]
	if !ok {
		return fmt.Errorf("invalid role: %s", role)
	}
	r.mu.Lock()
	apiKey := r.lookupLocked(keyStr)
	if apiKey == nil {
		r.mu.Unlock()
		return errors.New("API key not found")
	}

	apiKey.Role = role
	apiKey.Namespace = namespace
	apiKey.Datasets = append([]string(nil), datasets...)
	apiKey.Permissions = append([]Permission(nil), perms...)
	if apiKey.TenantID == "" && namespace != "" && role != RoleAdmin {
		apiKey.TenantID = namespace
	}
	if err := r.persistAPIKeyMeta(apiKey); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
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

func generateKeyID() (string, error) {
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", fmt.Errorf("generate API key id: %w", err)
	}
	return "key_" + hex.EncodeToString(idBytes), nil
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
