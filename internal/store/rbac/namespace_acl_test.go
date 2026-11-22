package rbac

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/stretchr/testify/require"
)

// TestNamespacePolicyPublicRead runs the test namespace policy public read operation.
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

// TestNamespacePolicyPublicWrite runs the test namespace policy public write operation.
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

// TestNamespacePolicyAdminAndUsers runs the test namespace policy admin and users operation.
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

	tc, err := mgr.AuthorizeToken(ctx, adminRaw, string(PermWrite), "team", "notes")
	require.NoError(t, err)
	require.True(t, tc.HasNamespacePermission("team", string(PermWrite)))
	_, err = mgr.AuthorizeToken(ctx, adminRaw, string(PermManageTenant), "team", "")
	require.ErrorIs(t, err, security.ErrForbidden)
	_, err = mgr.AuthorizeToken(ctx, adminRaw, string(PermRead), "unrelated", "notes")
	require.ErrorIs(t, err, security.ErrTenantIsolation)

	_, err = mgr.AuthorizeToken(ctx, writerRaw, string(PermDelete), "team", "notes")
	require.NoError(t, err)
	_, err = mgr.AuthorizeToken(ctx, readerRaw, string(PermWrite), "team", "notes")
	require.ErrorIs(t, err, security.ErrForbidden)
	_, err = mgr.AuthorizeToken(ctx, readerRaw, string(PermRead), "team", "notes")
	require.NoError(t, err)
}

// TestNamespacePolicyRotationFollowsKeyID runs the test namespace policy rotation follows key id operation.
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

// TestNamespacePolicyDurableAndEphemeralPersistence runs the test namespace policy durable and ephemeral persistence operation.
func TestNamespacePolicyDurableAndEphemeralPersistence(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	_, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "bootstrap", Role: RoleAdmin})
	require.NoError(t, err)
	writer, writerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "team-writer", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:    "durable-team",
		WriterKeyIDs: []string{writer.KeyID},
	}))
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:    "temporary-team",
		WriterKeyIDs: []string{writer.KeyID},
		Ephemeral:    true,
	}))

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	require.True(t, reloaded.HasGlobalAdmin())

	require.NotNil(t, reloaded.GetNamespacePolicy("durable-team"))
	require.Nil(t, reloaded.GetNamespacePolicy("temporary-team"), "ephemeral ACLs must not be rehydrated")

	tc, err := reloaded.AuthorizeToken(ctx, adminRaw, string(PermManageKeys), "", "")
	require.NoError(t, err)
	require.True(t, tc.IsAdmin)

	_, err = reloaded.AuthorizeToken(ctx, writerRaw, string(PermWrite), "durable-team", "notes")
	require.NoError(t, err)
	_, err = reloaded.AuthorizeToken(ctx, writerRaw, string(PermWrite), "temporary-team", "notes")
	require.Error(t, err, "ephemeral grant must disappear with the process")
}

// TestEphemeralPolicyReplacesDurableRecord runs the test ephemeral policy replaces durable record operation.
func TestEphemeralPolicyReplacesDurableRecord(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:         "switching-team",
		PublicPermissions: []Permission{PermRead},
	}))
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:         "switching-team",
		PublicPermissions: []Permission{PermRead},
		Ephemeral:         true,
	}))

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	require.Nil(t, reloaded.GetNamespacePolicy("switching-team"))
}

// TestNamespacePolicyDeleteRemovesDurableRecord runs the test namespace policy delete removes durable record operation.
func TestNamespacePolicyDeleteRemovesDurableRecord(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:         "public-team",
		PublicPermissions: []Permission{PermRead},
	}))
	require.NoError(t, mgr.DeleteNamespacePolicy("public-team"))

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	require.Nil(t, reloaded.GetNamespacePolicy("public-team"))
}

// TestNamespacePolicyRotationPersistsUpdatedKeyID runs the test namespace policy rotation persists updated key id operation.
func TestNamespacePolicyRotationPersistsUpdatedKeyID(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	writer, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "writer", Role: RoleReader, Namespace: "home",
	})
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:    "rotating-team",
		WriterKeyIDs: []string{writer.KeyID},
	}))

	rotated, newRaw, err := mgr.RotateAPIKey(raw, time.Minute)
	require.NoError(t, err)

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	policy := reloaded.GetNamespacePolicy("rotating-team")
	require.NotNil(t, policy)
	require.Contains(t, policy.WriterKeyIDs, rotated.KeyID)
	require.NotContains(t, policy.WriterKeyIDs, writer.KeyID)
	_, err = reloaded.AuthorizeToken(ctx, newRaw, string(PermWrite), "rotating-team", "notes")
	require.NoError(t, err)
}

// TestLegacyAPIKeyRecordStillLoads runs the test legacy api key record still loads operation.
func TestLegacyAPIKeyRecordStillLoads(t *testing.T) {
	ctx := context.Background()
	store := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	_, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "legacy", Role: RoleReader})
	require.NoError(t, err)

	records, err := store.LoadAll(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	legacy := records[0]
	legacy.Kind = ""
	require.NoError(t, store.Put(ctx, legacy))

	reloaded, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)
	_, err = reloaded.ValidateAPIKey(raw)
	require.NoError(t, err)
}

// TestAPIKeyCannotUseNamespacePolicyKeyID runs the test api key cannot use namespace policy key id operation.
func TestAPIKeyCannotUseNamespacePolicyKeyID(t *testing.T) {
	mgr := NewRBACManager()
	reserved := secrets.NamespacePolicyKeyID("team-a")
	_, err := mgr.RegisterAPIKeyWithSecret(APIKeyOptions{
		KeyID: reserved,
		Name:  "should-fail",
		Role:  RoleAdmin,
	}, "long-enough-bootstrap-secret")
	require.ErrorContains(t, err, "reserved for namespace policies")
}

// TestRBACManagerPersistsKeysInSecretStore runs the test rbac manager persists keys in secret store operation.
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
