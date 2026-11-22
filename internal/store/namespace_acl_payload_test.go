package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func newNamespacePayloadStore(t *testing.T) (*VectorStore, *RBACManager, secrets.Store, *APIKey, string) {
	t.Helper()

	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, 0)
	t.Cleanup(func() { _ = vs.Close() })

	secretStore := secrets.NewMemoryStore()
	mgr, err := NewRBACManagerWithStore(context.Background(), secretStore)
	require.NoError(t, err)
	vs.EnableRBAC(mgr)

	admin, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "namespace-admin",
		Role: RoleAdmin,
	})
	require.NoError(t, err)
	return vs, mgr, secretStore, admin, adminRaw
}

func requireStoredNamespacePolicy(t *testing.T, store secrets.Store, namespace string, want bool) {
	t.Helper()

	stored, err := store.LoadAll(context.Background())
	require.NoError(t, err)
	found := false
	for _, item := range stored {
		if item.KeyID == secrets.NamespacePolicyKeyID(namespace) {
			found = true
			require.Equal(t, secrets.KindNamespacePolicy, item.Kind)
		}
	}
	require.Equal(t, want, found, "durable record for namespace %q: found=%v", namespace, found)
}

// TestRESTCreateNamespaceEphemeralPayload runs the test rest create namespace ephemeral payload operation.
func TestRESTCreateNamespaceEphemeralPayload(t *testing.T) {
	vs, mgr, secretStore, admin, adminRaw := newNamespacePayloadStore(t)

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	code, body := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/namespaces", adminRaw, map[string]any{
		"name":               "rest-ephemeral",
		"reader_key_ids":     []string{admin.KeyID},
		"public_permissions": []string{"read"},
		"ephemeral":          true,
	})
	require.Equal(t, http.StatusCreated, code, body)
	require.True(t, vs.NamespaceExists("rest-ephemeral"))

	policy := mgr.GetNamespacePolicy("rest-ephemeral")
	require.NotNil(t, policy)
	require.True(t, policy.Ephemeral)
	require.Contains(t, policy.AdminKeyIDs, admin.KeyID)
	requireStoredNamespacePolicy(t, secretStore, "rest-ephemeral", false)
}

// TestFlightCreateNamespaceDurablePayload runs the test flight create namespace durable payload operation.
func TestFlightCreateNamespaceDurablePayload(t *testing.T) {
	vs, mgr, secretStore, admin, adminRaw := newNamespacePayloadStore(t)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+adminRaw))
	tc, err := mgr.AuthorizeToken(ctx, adminRaw, string(PermManageTenant), "", "")
	require.NoError(t, err)
	ctx = security.ContextWithTenant(ctx, tc)
	stream := &captureActionStream{ctx: ctx}
	body, err := json.Marshal(map[string]any{
		"name":               "flight-durable",
		"writer_key_ids":     []string{admin.KeyID},
		"public_permissions": []string{"write"},
	})
	require.NoError(t, err)
	require.NoError(t, vs.DoAction(&flight.Action{Type: "CreateNamespace", Body: body}, stream))
	require.Len(t, stream.results, 1)
	require.Contains(t, string(stream.results[0].Body), "namespace created")
	require.True(t, vs.NamespaceExists("flight-durable"))

	policy := mgr.GetNamespacePolicy("flight-durable")
	require.NotNil(t, policy)
	require.False(t, policy.Ephemeral)
	require.Contains(t, policy.AdminKeyIDs, admin.KeyID)
	requireStoredNamespacePolicy(t, secretStore, "flight-durable", true)
}
