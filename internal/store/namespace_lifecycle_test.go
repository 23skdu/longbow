package store

import (
	"context"
	"errors"
	"testing"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/stretchr/testify/require"
)

type failingDeleteSecretStore struct {
	secrets.Store
	err error
}

func (s *failingDeleteSecretStore) Delete(context.Context, string) error { return s.err }

func TestDeleteNamespaceKeepsNamespaceWhenACLProviderFails(t *testing.T) {
	vs, _, secretStore, _, _ := newNamespacePayloadStore(t)
	require.NoError(t, vs.EnsureNamespace("keep-namespace"))

	boom := errors.New("provider unavailable")
	failingStore := &failingDeleteSecretStore{Store: secretStore, err: boom}
	mgr, err := NewRBACManagerWithStore(context.Background(), failingStore)
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{Namespace: "keep-namespace"}))
	vs.EnableRBAC(mgr)

	err = vs.DeleteNamespace("keep-namespace")
	require.ErrorIs(t, err, boom)
	require.True(t, vs.NamespaceExists("keep-namespace"))
	require.NotNil(t, mgr.GetNamespacePolicy("keep-namespace"))
}
