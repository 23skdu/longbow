package store

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func doJSONReqWithHeader(t *testing.T, client *http.Client, method, url, apiKey, namespace string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(method, url, &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if namespace != "" {
		req.Header.Set("X-Tenant-ID", namespace)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestNamespaceAdminCannotUseGlobalAdminRoutes runs the test namespace admin cannot use global admin routes operation.
func TestNamespaceAdminCannotUseGlobalAdminRoutes(t *testing.T) {
	vs, mgr, _, _, adminRaw := newNamespacePayloadStore(t)

	reader, readerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "namespace-admin-reader-role",
		Role: RoleReader,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.SetNamespacePolicy(NamespacePolicy{
		Namespace:   "admin-ns",
		AdminKeyIDs: []string{reader.KeyID},
	}))

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{
		"/api/v1/rbac/keys",
		"/api/v1/rbac/rls",
		"/api/v1/admin/usage",
		"/api/v1/namespaces",
	} {
		code, _ := doJSONReqWithHeader(t, srv.Client(), http.MethodPost, srv.URL+path, readerRaw, "admin-ns", map[string]any{"name": "escalated"})
		require.Contains(t, []int{http.StatusForbidden, http.StatusMethodNotAllowed}, code, path)
	}

	code, _ := doJSONReqWithHeader(t, srv.Client(), http.MethodGet, srv.URL+"/api/v1/rbac/keys", readerRaw, "admin-ns", nil)
	require.Equal(t, http.StatusForbidden, code)

	scopedAdmin, scopedAdminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "namespace-scoped-admin-role",
		Role:      RoleAdmin,
		Namespace: "admin-ns",
	})
	require.NoError(t, err)
	require.NotNil(t, scopedAdmin)
	code, _ = doJSONReqWithHeader(t, srv.Client(), http.MethodGet, srv.URL+"/api/v1/rbac/keys", scopedAdminRaw, "admin-ns", nil)
	require.Equal(t, http.StatusForbidden, code)

	code, _ = doJSONReqWithHeader(t, srv.Client(), http.MethodGet, srv.URL+"/api/v1/rbac/keys", adminRaw, "", nil)
	require.Equal(t, http.StatusOK, code)
}

// TestNamespaceNameValidationAtRuntime runs the test namespace name validation at runtime operation.
func TestNamespaceNameValidationAtRuntime(t *testing.T) {
	vs, _, _, _, _ := newNamespacePayloadStore(t)
	for _, name := range []string{"team-a", "team_a", "team.1"} {
		require.NoError(t, vs.CreateNamespace(name), name)
	}
	for _, name := range []string{"", " team", "team/a", `team\a`, "team..a", "team\na"} {
		require.Error(t, vs.CreateNamespace(name), name)
	}
}
