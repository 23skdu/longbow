package store

import (
	"context"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/23skdu/longbow/internal/store/rbac"
)

type Role = rbac.Role
type Permission = rbac.Permission
type APIKey = rbac.APIKey
type APIKeyOptions = rbac.APIKeyOptions
type RBACManager = rbac.RBACManager
type NamespacePolicy = rbac.NamespacePolicy
type NamespaceRole = rbac.NamespaceRole

const (
	RoleAdmin       = rbac.RoleAdmin
	RoleTenantAdmin = rbac.RoleTenantAdmin
	RoleWriter      = rbac.RoleWriter
	RoleReadWrite   = rbac.RoleReadWrite
	RoleReader      = rbac.RoleReader
	RoleReadOnly    = rbac.RoleReadOnly
	RoleIngest      = rbac.RoleIngest

	PermRead         = rbac.PermRead
	PermWrite        = rbac.PermWrite
	PermDelete       = rbac.PermDelete
	PermIngest       = rbac.PermIngest
	PermAdmin        = rbac.PermAdmin
	PermBackup       = rbac.PermBackup
	PermManageKeys   = rbac.PermManageKeys
	PermManageTenant = rbac.PermManageTenant

	NamespaceRoleAdmin  = rbac.NamespaceRoleAdmin
	NamespaceRoleWriter = rbac.NamespaceRoleWriter
	NamespaceRoleReader = rbac.NamespaceRoleReader
)

// NewRBACManager creates a new RBAC manager.
func NewRBACManager() *RBACManager {
	return rbac.NewRBACManager()
}

func NewRBACManagerWithStore(ctx context.Context, store secrets.Store) (*RBACManager, error) {
	return rbac.NewRBACManagerWithStore(ctx, store)
}
