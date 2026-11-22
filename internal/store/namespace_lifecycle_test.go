package store

import (
	"context"
	"errors"
	"testing"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

type failingDeleteSecretStore struct {
	secrets.Store
	err error
}

// Delete runs the delete operation.
func (s *failingDeleteSecretStore) Delete(context.Context, string) error { return s.err }

// TestPruneNamespacesKeepsDurableDataAndRuntimeNamespace runs the test prune namespaces keeps durable data and runtime namespace operation.
func TestPruneNamespacesKeepsDurableDataAndRuntimeNamespace(t *testing.T) {
	vs, _, _, _, _ := newNamespacePayloadStore(t)
	for _, name := range []string{"durable", "deleted", "ephemeral"} {
		require.NoError(t, vs.EnsureNamespace(name))
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for _, name := range []string{"default/data", "durable/data", "deleted/data", "ephemeral/data"} {
		_, ok := vs.getOrCreateDataset(name, func() *Dataset { return NewDataset(name, schema) })
		require.True(t, ok, name)
	}

	pruned, err := vs.PruneNamespaces(
		map[string]struct{}{"default": {}, "durable": {}, "ephemeral": {}},
		map[string]struct{}{"default": {}, "durable": {}},
	)
	require.NoError(t, err)
	require.Equal(t, 2, pruned)

	_, ok := vs.getDataset("default/data")
	require.True(t, ok)
	_, ok = vs.getDataset("durable/data")
	require.True(t, ok)
	_, ok = vs.getDataset("deleted/data")
	require.False(t, ok)
	_, ok = vs.getDataset("ephemeral/data")
	require.False(t, ok)
	require.False(t, vs.NamespaceExists("deleted"))
	require.True(t, vs.NamespaceExists("ephemeral"))
}

// TestDeleteNamespaceKeepsNamespaceWhenACLProviderFails runs the test delete namespace keeps namespace when acl provider fails operation.
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
