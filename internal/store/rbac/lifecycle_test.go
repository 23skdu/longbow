package rbac

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/stretchr/testify/require"
)

type failingDeleteStore struct {
	secrets.Store
	err error
}

func (s *failingDeleteStore) Delete(context.Context, string) error { return s.err }

type failingPutStore struct {
	secrets.Store
	calls    int
	failCall int
	err      error
}

func (s *failingPutStore) Put(ctx context.Context, key secrets.StoredKey) error {
	s.calls++
	if s.calls == s.failCall {
		return s.err
	}
	return s.Store.Put(ctx, key)
}

func TestDeleteNamespacePolicyKeepsPolicyWhenProviderFails(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("provider unavailable")
	mgr, err := NewRBACManagerWithStore(ctx, &failingDeleteStore{
		Store: secrets.NewMemoryStore(), err: boom,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{Namespace: "keep-me"}))

	err = mgr.DeleteNamespacePolicy("keep-me")
	require.ErrorIs(t, err, boom)
	require.NotNil(t, mgr.GetNamespacePolicy("keep-me"))
}

func TestDeleteEphemeralNamespacePolicyDoesNotTouchProvider(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewRBACManagerWithStore(ctx, secrets.NewMemoryStore())
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{Namespace: "temporary", Ephemeral: true}))

	mgr.store = &failingDeleteStore{Store: secrets.NewMemoryStore(), err: errors.New("provider unavailable")}
	require.NoError(t, mgr.DeleteNamespacePolicy("temporary"))
	require.Nil(t, mgr.GetNamespacePolicy("temporary"))
}

func TestRotateAPIKeyDoesNotRevokeOldKeyWhenIssuanceFails(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("provider unavailable")
	store := &failingPutStore{Store: secrets.NewMemoryStore(), err: boom}
	mgr, err := NewRBACManagerWithStore(ctx, store)
	require.NoError(t, err)

	old, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "old", Role: RoleAdmin})
	require.NoError(t, err)
	store.calls = 0
	store.failCall = 1

	_, _, err = mgr.RotateAPIKey(raw, time.Hour)
	require.ErrorIs(t, err, boom)

	got, ok := mgr.GetAPIKey(raw)
	require.True(t, ok)
	require.Equal(t, old.KeyID, got.KeyID)
	require.True(t, got.Enabled)
	require.Nil(t, got.GraceExpiresAt)
}

func TestListAPIKeysReturnsCopies(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewRBACManagerWithStore(ctx, secrets.NewMemoryStore())
	require.NoError(t, err)
	_, _, err = mgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "original", Role: RoleReader})
	require.NoError(t, err)

	listed := mgr.ListAPIKeys()
	require.Len(t, listed, 1)
	listed[0].Name = "mutated"
	listed[0].Permissions[0] = Permission("tampered")

	got := mgr.ListAPIKeys()
	require.Equal(t, "original", got[0].Name)
	require.NotEqual(t, "tampered", string(got[0].Permissions[0]))
}
