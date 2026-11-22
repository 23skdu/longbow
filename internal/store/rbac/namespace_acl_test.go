package rbac

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/stretchr/testify/require"
)

func TestNamespacePolicyPublicRead(t *testing.T) {
	ctx := context.Background()
	mgr := NewRBACManager()

	_, otherRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "other-agent", Role: RoleReader, Namespace: "other-private",
	})
	require.NoError(t, err)
	other, ok := mgr.GetAPIKey(otherRaw)
	require.True(t, ok)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:         "public-read",
		ReaderKeyIDs:      []string{other.KeyID},
		PublicPermissions: []Permission{PermRead},
	}))

	tc, err := mgr.AuthorizeToken(ctx, otherRaw, string(PermRead), "public-read", "notes")
	require.NoError(t, err)
	require.True(t, tc.CanAccessDataset("public-read", "notes"))
	require.True(t, tc.HasNamespacePermission("public-read", string(PermRead)))
	require.False(t, tc.HasNamespacePermission("public-read", string(PermWrite)))

	_, err = mgr.AuthorizeToken(ctx, otherRaw, string(PermWrite), "public-read", "notes")
	require.ErrorIs(t, err, security.ErrForbidden)

	_, err = mgr.AuthorizeToken(ctx, otherRaw, string(PermRead), "someone-elses-private", "notes")
	require.ErrorIs(t, err, security.ErrTenantIsolation)
}

func TestNamespacePolicyPublicWrite(t *testing.T) {
	ctx := context.Background()
	mgr := NewRBACManager()
	_, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "reader-key", Role: RoleReader, Namespace: "somewhere",
	})
	require.NoError(t, err)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:         "public-rw",
		PublicPermissions: []Permission{PermWrite, PermDelete},
	}))

	// A reader-role key can write and delete in a namespace that explicitly
	// publishes those permissions, even though its global role cannot.
	for _, perm := range []Permission{PermRead, PermWrite, PermDelete} {
		tc, err := mgr.AuthorizeToken(ctx, raw, string(perm), "public-rw", "notes")
		require.NoError(t, err, perm)
		require.True(t, tc.HasNamespacePermission("public-rw", string(perm)))
	}
}

func TestNamespacePolicyAdminAndUsers(t *testing.T) {
	ctx := context.Background()
	mgr := NewRBACManager()

	admin, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "team-admin", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)
	writer, writerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "team-writer", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)
	reader, readerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "team-reader", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:    "team",
		AdminKeyIDs:  []string{admin.KeyID},
		WriterKeyIDs: []string{writer.KeyID},
		ReaderKeyIDs: []string{reader.KeyID},
	}))

	tc, err := mgr.AuthorizeToken(ctx, adminRaw, string(PermManageTenant), "team", "")
	require.NoError(t, err)
	require.True(t, tc.HasNamespacePermission("team", string(PermManageTenant)))
	_, err = mgr.AuthorizeToken(ctx, adminRaw, string(PermRead), "unrelated", "notes")
	require.ErrorIs(t, err, security.ErrTenantIsolation)

	_, err = mgr.AuthorizeToken(ctx, writerRaw, string(PermDelete), "team", "notes")
	require.NoError(t, err)
	_, err = mgr.AuthorizeToken(ctx, readerRaw, string(PermWrite), "team", "notes")
	require.ErrorIs(t, err, security.ErrForbidden)
	_, err = mgr.AuthorizeToken(ctx, readerRaw, string(PermRead), "team", "notes")
	require.NoError(t, err)
}

func TestNamespacePolicyRotationFollowsKeyID(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	writer, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "writer", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:    "team",
		WriterKeyIDs: []string{writer.KeyID},
	}))

	rotated, newRaw, err := mgr.RotateAPIKey(raw, time.Minute)
	require.NoError(t, err)
	policy := mgr.GetNamespacePolicy("team")
	require.Contains(t, policy.WriterKeyIDs, rotated.KeyID)
	require.NotContains(t, policy.WriterKeyIDs, writer.KeyID)

	_, err = mgr.AuthorizeToken(ctx, newRaw, string(PermWrite), "team", "notes")
	require.NoError(t, err)
}

func TestRBACManagerPersistsKeysInSecretStore(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	_, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "admin", Role: RoleAdmin})
	require.NoError(t, err)
	_, workerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "worker", Role: RoleWriter, Namespace: "team",
	})
	require.NoError(t, err)
	require.NoError(t, mgr.RevokeAPIKey(workerRaw))

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	require.True(t, reloaded.HasGlobalAdmin())

	tc, err := reloaded.AuthorizeToken(ctx, adminRaw, string(PermManageKeys), "", "")
	require.NoError(t, err)
	require.True(t, tc.IsAdmin)

	_, err = reloaded.ValidateAPIKey(workerRaw)
	require.Error(t, err, "revoked keys must not be rehydrated")

	require.NoError(t, reloaded.DeleteAPIKey(adminRaw))
	third, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	require.False(t, third.HasGlobalAdmin())
}
