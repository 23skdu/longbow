package rbac

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRBACManager_Lifecycle(t *testing.T) {
	manager := NewRBACManager()

	// 1. Create API Key
	apiKey, rawKey, err := manager.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "test-key",
		Role:      RoleReadWrite,
		TenantID:  "default",
		Namespace: "default",
		Datasets:  []string{"dataset1"},
	})
	require.NoError(t, err)
	require.NotNil(t, apiKey)
	require.True(t, apiKey.Enabled)
	require.NotEmpty(t, rawKey)

	// 2. Validate
	ok, err := manager.ValidateAPIKey(rawKey)
	require.NoError(t, err)
	require.True(t, ok)

	// 3. Check Permissions
	// Admin perms on ReadWrite key should fail
	ok, err = manager.CheckPermission(rawKey, PermAdmin, "default", "dataset1")
	require.Error(t, err)
	require.False(t, ok)

	// Read perms should pass
	ok, err = manager.CheckPermission(rawKey, PermRead, "default", "dataset1")
	require.NoError(t, err)
	require.True(t, ok)

	// Wrong namespace should fail
	ok, err = manager.CheckPermission(rawKey, PermRead, "other", "dataset1")
	require.Error(t, err)
	require.False(t, ok)

	// Unauthorized dataset should fail
	ok, err = manager.CheckPermission(rawKey, PermRead, "default", "dataset2")
	require.Error(t, err)
	require.False(t, ok)

	// 4. Update Key
	err = manager.UpdateAPIKey(rawKey, RoleAdmin, "all", []string{"dataset2"})
	require.NoError(t, err)

	// Now Admin perms should pass
	ok, err = manager.CheckPermission(rawKey, PermAdmin, "all", "dataset2")
	require.NoError(t, err)
	require.True(t, ok)

	// 5. Revoke
	err = manager.RevokeAPIKey(rawKey)
	require.NoError(t, err)

	ok, err = manager.ValidateAPIKey(rawKey)
	require.Error(t, err)
	require.False(t, ok)

	// 6. Delete
	err = manager.DeleteAPIKey(rawKey)
	require.NoError(t, err)

	_, ok = manager.GetAPIKey(rawKey)
	require.False(t, ok)
}

func TestRBACManager_Expiration(t *testing.T) {
	manager := NewRBACManager()
	apiKey, rawKey, _ := manager.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "expiring-key",
		Role:      RoleReadOnly,
		TenantID:  "ns",
		Namespace: "ns",
	})

	// Set expiration in the past
	past := time.Now().Add(-1 * time.Hour)
	apiKey.ExpiresAt = &past

	ok, err := manager.ValidateAPIKey(rawKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")
	require.False(t, ok)
}

func TestRole_HasPermission(t *testing.T) {
	require.True(t, RoleAdmin.HasPermission(PermAdmin))
	require.True(t, RoleAdmin.HasPermission(PermRead))
	require.False(t, RoleReadOnly.HasPermission(PermWrite))
	require.True(t, RoleIngest.HasPermission(PermIngest))
	require.True(t, RoleIngest.HasPermission(PermRead))
}

func TestRBACManager_RegisterAPIKeyWithSecret(t *testing.T) {
	mgr := NewRBACManager()
	const secret = "bootstrap-admin-secret-0123456789"

	ak, err := mgr.RegisterAPIKeyWithSecret(APIKeyOptions{Name: "bootstrap-admin", Role: RoleAdmin}, secret)
	require.NoError(t, err)
	require.Equal(t, "bootstrap-admin", ak.Name)
	require.Equal(t, RoleAdmin, ak.Role)

	tc, err := mgr.AuthorizeToken(context.Background(), secret, string(PermManageKeys), "", "")
	require.NoError(t, err)
	require.Equal(t, ak.KeyID, tc.KeyID)
	require.True(t, tc.IsAdmin)

	_, err = mgr.RegisterAPIKeyWithSecret(APIKeyOptions{Name: "too-short", Role: RoleAdmin}, "short")
	require.Error(t, err)
}

// FuzzRBACAuthorize exercises key creation and authorization across arbitrary role/permission inputs.
func FuzzRBACAuthorize(f *testing.F) {
	f.Add("default", "ds1", string(RoleAdmin), string(PermRead))
	f.Add("ns1", "ds2", string(RoleReader), string(PermWrite))
	f.Add("", "", string(RoleWriter), string(PermIngest))

	f.Fuzz(func(t *testing.T, ns, ds, roleStr, permStr string) {
		mgr := NewRBACManager()
		_, rawKey, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
			Name:      "fuzz-key",
			Role:      Role(roleStr),
			TenantID:  ns,
			Namespace: ns,
			Datasets:  []string{ds},
		})
		if err != nil || rawKey == "" {
			return
		}
		_, _ = mgr.ValidateAPIKey(rawKey)
		_, _ = mgr.CheckPermission(rawKey, Permission(permStr), ns, ds)
	})
}
