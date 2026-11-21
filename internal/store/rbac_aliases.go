package store

import "github.com/23skdu/longbow/internal/store/rbac"

type Role = rbac.Role
type Permission = rbac.Permission
type APIKey = rbac.APIKey
type APIKeyOptions = rbac.APIKeyOptions
type RBACManager = rbac.RBACManager

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
)

// NewRBACManager creates a new RBAC manager.
func NewRBACManager() *RBACManager {
	return rbac.NewRBACManager()
}
