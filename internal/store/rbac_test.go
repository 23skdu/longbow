package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRBACManager_Lifecycle(t *testing.T) {
	manager := NewRBACManager()

	// 1. Create API Key
	apiKey, err := manager.CreateAPIKey("test-key", RoleReadWrite, "default", []string{"dataset1"})
	require.NoError(t, err)
	require.NotNil(t, apiKey)
	require.True(t, apiKey.Enabled)

	// We need to find the raw key string to use it in other methods
	var rawKey string
	for k, ak := range manager.apiKeys {
		if ak.KeyID == apiKey.KeyID {
			rawKey = k
			break
		}
	}
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
	apiKey, _ := manager.CreateAPIKey("expiring-key", RoleReadOnly, "ns", nil)
	
	var rawKey string
	for k, ak := range manager.apiKeys {
		if ak.KeyID == apiKey.KeyID {
			rawKey = k
			break
		}
	}

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
