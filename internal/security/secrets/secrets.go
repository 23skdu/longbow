// Package secrets provides durable storage for Longbow RBAC records.
//
// The store is keyed by a stable, operator-visible KeyID and holds either an
// API credential or a namespace ACL policy. Implementations cover local
// file/memory storage and the external providers operators already use:
// HashiCorp Vault KV v2, Google Secret Manager, and AWS Secrets Manager.
package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	// ErrNotFound is returned when a key does not exist in the store.
	ErrNotFound = errors.New("secrets: key not found")
	// ErrInvalidKeyID is returned for identifiers that cannot be used as a
	// secret name (path traversal and provider-invalid characters).
	ErrInvalidKeyID = errors.New("secrets: invalid key id")
)

const (
	// DefaultPrefix namespaces Longbow secrets inside a shared provider.
	DefaultPrefix = "longbow/rbac"
	// DefaultVaultMount is the conventional Vault KV v2 mount.
	DefaultVaultMount = "secret"

	// KindAPIKey identifies a durable RBAC API credential. An empty Kind is
	// treated as an API key for compatibility with pre-existing records.
	KindAPIKey = "api_key"
	// KindNamespacePolicy identifies a durable namespace ACL.
	KindNamespacePolicy = "namespace_policy"

	namespacePolicyKeyPrefix = "nspolicy-"
)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// ValidateKeyID rejects identifiers that could escape a secret path or that
// external providers would refuse.
func ValidateKeyID(id string) error {
	if !keyIDPattern.MatchString(id) || strings.Contains(id, "..") {
		return fmt.Errorf("%w: %q", ErrInvalidKeyID, id)
	}
	return nil
}

// NamespacePolicyKeyID returns the deterministic provider-safe record ID for a
// namespace ACL. The namespace itself is kept in the record metadata; hashing
// it avoids exposing arbitrary namespace names in provider secret IDs.
func NamespacePolicyKeyID(namespace string) string {
	sum := sha256.Sum256([]byte(namespace))
	return namespacePolicyKeyPrefix + hex.EncodeToString(sum[:])[:16]
}

// IsNamespacePolicyKeyID reports whether id is reserved for namespace ACLs.
func IsNamespacePolicyKeyID(id string) bool {
	return strings.HasPrefix(id, namespacePolicyKeyPrefix)
}

// StoredKey is one durable RBAC record. Raw is the API secret returned to the
// caller at issuance time; Meta carries either API-key metadata or a namespace
// ACL policy. Kind distinguishes the two record types, with an empty Kind
// interpreted as KindAPIKey for backward compatibility.
type StoredKey struct {
	KeyID string          `json:"key_id"`
	Kind  string          `json:"kind,omitempty"`
	Raw   string          `json:"raw"`
	Meta  json.RawMessage `json:"meta,omitempty"`
}

// Store is the pluggable RBAC record store.
//
// Put is write-through on issue, rotation, policy changes, and metadata
// updates; Delete is write-through on removal; LoadAll rehydrates state on
// startup.
type Store interface {
	LoadAll(ctx context.Context) ([]StoredKey, error)
	Put(ctx context.Context, key StoredKey) error
	Delete(ctx context.Context, keyID string) error
}

// Config selects and configures a Store. Field names follow the LONGBOW_
// envconfig convention used by cmd/longbow.
type Config struct {
	Provider string `envconfig:"SECRETS_PROVIDER" default:"file"`
	Path     string `envconfig:"SECRETS_PATH" default:"data/rbac_secrets"`
	Prefix   string `envconfig:"SECRETS_PREFIX" default:"longbow/rbac"`

	// Vault KV v2.
	VaultAddr  string `envconfig:"SECRETS_VAULT_ADDR" default:""`
	VaultToken string `envconfig:"SECRETS_VAULT_TOKEN" default:""`
	VaultMount string `envconfig:"SECRETS_VAULT_MOUNT" default:"secret"`

	// Google Secret Manager.
	GCPProjectID string `envconfig:"SECRETS_GCP_PROJECT_ID" default:""`
	GCPToken     string `envconfig:"SECRETS_GCP_TOKEN" default:""`

	// AWS Secrets Manager.
	AWSRegion   string `envconfig:"SECRETS_AWS_REGION" default:""`
	AWSEndpoint string `envconfig:"SECRETS_AWS_ENDPOINT" default:""`
}

// New builds a Store from cfg. An empty provider means "file" so operators get
// durable local storage by default instead of silently losing keys on restart.
func New(ctx context.Context, cfg Config) (Store, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	switch provider {
	case "", "file":
		return NewFileStore(cfg.Path)
	case "memory":
		return NewMemoryStore(), nil
	case "vault":
		return NewVaultStore(VaultConfig{
			Addr:   cfg.VaultAddr,
			Token:  cfg.VaultToken,
			Mount:  cfg.VaultMount,
			Prefix: prefixOrDefault(cfg.Prefix),
		})
	case "gcp":
		return NewGCPStore(ctx, GCPConfig{
			ProjectID: cfg.GCPProjectID,
			Token:     cfg.GCPToken,
			Prefix:    prefixOrDefault(cfg.Prefix),
		})
	case "aws":
		return NewAWSStore(ctx, AWSConfig{
			Region:   cfg.AWSRegion,
			Endpoint: cfg.AWSEndpoint,
			Prefix:   prefixOrDefault(cfg.Prefix),
		})
	default:
		return nil, fmt.Errorf("secrets: unknown provider %q (want file, memory, vault, gcp, or aws)", cfg.Provider)
	}
}

func prefixOrDefault(prefix string) string {
	if strings.TrimSpace(prefix) == "" {
		return DefaultPrefix
	}
	return strings.Trim(prefix, "/")
}
