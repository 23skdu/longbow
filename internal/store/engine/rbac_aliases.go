package engine

import (
	"context"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/23skdu/longbow/internal/store/rbac"
)

// Role represents a role value.
type Role = rbac.Role

// Permission represents a permission value.
type Permission = rbac.Permission

// APIKey represents a api key value.
type APIKey = rbac.APIKey

// APIKeyOptions represents a api key options value.
type APIKeyOptions = rbac.APIKeyOptions

// RBACManager represents a rbac manager value.
type RBACManager = rbac.RBACManager

// NamespacePolicy represents a namespace policy value.
type NamespacePolicy = rbac.NamespacePolicy

// NamespaceRole represents a namespace role value.
type NamespaceRole = rbac.NamespaceRole

const (
	// RoleAdmin is a role admin constant.
	RoleAdmin = rbac.RoleAdmin
	// RoleTenantAdmin is a role tenant admin constant.
	RoleTenantAdmin = rbac.RoleTenantAdmin
	// RoleWriter is a role writer constant.
	RoleWriter = rbac.RoleWriter
	// RoleReadWrite is a role read write constant.
	RoleReadWrite = rbac.RoleReadWrite
	// RoleReader is a role reader constant.
	RoleReader = rbac.RoleReader
	// RoleReadOnly is a role read only constant.
	RoleReadOnly = rbac.RoleReadOnly
	// RoleIngest is a role ingest constant.
	RoleIngest = rbac.RoleIngest

	// PermRead is a perm read constant.
	PermRead = rbac.PermRead
	// PermWrite is a perm write constant.
	PermWrite = rbac.PermWrite
	// PermDelete is a perm delete constant.
	PermDelete = rbac.PermDelete
	// PermIngest is a perm ingest constant.
	PermIngest = rbac.PermIngest
	// PermAdmin is a perm admin constant.
	PermAdmin = rbac.PermAdmin
	// PermBackup is a perm backup constant.
	PermBackup = rbac.PermBackup
	// PermManageKeys is a perm manage keys constant.
	PermManageKeys = rbac.PermManageKeys
	// PermManageTenant is a perm manage tenant constant.
	PermManageTenant = rbac.PermManageTenant

	// NamespaceRoleAdmin is a namespace role admin constant.
	NamespaceRoleAdmin = rbac.NamespaceRoleAdmin
	// NamespaceRoleWriter is a namespace role writer constant.
	NamespaceRoleWriter = rbac.NamespaceRoleWriter
	// NamespaceRoleReader is a namespace role reader constant.
	NamespaceRoleReader = rbac.NamespaceRoleReader
)

// NewRBACManager creates a new RBAC manager.
func NewRBACManager() *RBACManager {
	return rbac.NewRBACManager()
}

// NewRBACManagerWithStore constructs a new RBACManagerWithStore.
func NewRBACManagerWithStore(ctx context.Context, store secrets.Store) (*RBACManager, error) {
	return rbac.NewRBACManagerWithStore(ctx, store)
}
