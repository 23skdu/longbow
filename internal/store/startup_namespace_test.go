package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/23skdu/longbow/internal/store/rbac"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestProvisionNamespaceWithPolicy runs the test provision namespace with policy operation.
func TestProvisionNamespaceWithPolicy(t *testing.T) {
	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, 0)
	defer vs.Close() //nolint:errcheck

	mgr, err := rbac.NewRBACManagerWithStore(context.Background(), secrets.NewMemoryStore())
	require.NoError(t, err)
	vs.EnableRBAC(mgr)

	policy := NamespacePolicy{
		Namespace:         "  startup-team  ",
		PublicPermissions: []Permission{PermRead},
		Ephemeral:         true,
	}
	require.NoError(t, vs.ProvisionNamespaceWithPolicy(policy))
	require.True(t, vs.NamespaceExists("startup-team"))
	stored := mgr.GetNamespacePolicy("startup-team")
	require.NotNil(t, stored)
	require.True(t, stored.Ephemeral)

	// Startup provisioning is idempotent so restart/reconciliation is safe.
	require.NoError(t, vs.ProvisionNamespaceWithPolicy(policy))
}

// TestEnsureNamespaceIsIdempotent runs the test ensure namespace is idempotent operation.
func TestEnsureNamespaceIsIdempotent(t *testing.T) {
	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, 0)
	defer vs.Close() //nolint:errcheck

	require.NoError(t, vs.EnsureNamespace("existing"))
	require.NoError(t, vs.EnsureNamespace("existing"))
	require.True(t, vs.NamespaceExists("existing"))
	require.Error(t, vs.EnsureNamespace(" "))
}
