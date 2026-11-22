package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type startupRESTResponse struct {
	status  int
	payload map[string]any
	body    string
}

func startupRESTRequest(t *testing.T, si *serverInstance, method, path, apiKey, namespace string, body any) startupRESTResponse {
	t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, "http://"+si.metricsAddr+path, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	if namespace != "" {
		req.Header.Set("X-Tenant-ID", namespace)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	payload := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	return startupRESTResponse{status: resp.StatusCode, payload: payload, body: string(raw)}
}

func startupNamespaceNames(t *testing.T, si *serverInstance) []string {
	t.Helper()
	resp := startupRESTRequest(t, si, http.MethodGet, "/api/v1/namespaces", testAdminAPIKey, "", nil)
	require.Equal(t, http.StatusOK, resp.status, resp.body)
	raw, ok := resp.payload["namespaces"].([]any)
	require.True(t, ok, "namespaces response: %s", resp.body)
	names := make([]string, 0, len(raw))
	for _, item := range raw {
		name, ok := item.(string)
		require.True(t, ok, "namespace name is not a string: %v", item)
		names = append(names, name)
	}
	return names
}

func requireContains(items []string, want string) {
	for _, item := range items {
		if item == want {
			return
		}
	}
	panic(fmt.Sprintf("missing %q in %v", want, items))
}

func requireNotContains(items []string, want string) {
	for _, item := range items {
		if item == want {
			panic(fmt.Sprintf("unexpected %q in %v", want, items))
		}
	}
}

func TestStartupNamespacePoliciesSurviveRestart(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "longbow-startup-ns-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dataDir) }()

	policies := `[
		{"namespace":"startup_durable","public_permissions":["write"]},
		{"namespace":"startup_ephemeral","public_permissions":["read"],"ephemeral":true}
	]`
	first := startServerWithEnv(t, dataDir, []string{
		"LONGBOW_STARTUP_NAMESPACE_POLICIES=" + policies,
	})

	names := startupNamespaceNames(t, first)
	requireContains(names, "startup_durable")
	requireContains(names, "startup_ephemeral")

	keyResp := startupRESTRequest(t, first, http.MethodPost, "/api/v1/rbac/keys", testAdminAPIKey, "", map[string]any{
		"name":      "startup-agent",
		"role":      "reader",
		"tenant_id": "agent-home",
		"namespace": "agent-home",
	})
	require.Equal(t, http.StatusCreated, keyResp.status, keyResp.body)
	agentKey, ok := keyResp.payload["api_key"].(string)
	require.True(t, ok, "api_key missing from response: %s", keyResp.body)

	writeResp := startupRESTRequest(t, first, http.MethodPost, "/api/v1/datasets", agentKey, "startup_durable", map[string]any{
		"name": "startup_durable/probe",
	})
	require.Equal(t, http.StatusCreated, writeResp.status, writeResp.body)
	first.stopPreserveData()

	// No startup configuration on restart: only the durable ACL and its
	// namespace should return. The API key and dataset data are also durable.
	second := startServerWithEnv(t, dataDir, nil)
	defer second.stop()

	names = startupNamespaceNames(t, second)
	requireContains(names, "startup_durable")
	requireNotContains(names, "startup_ephemeral")

	writeResp = startupRESTRequest(t, second, http.MethodPost, "/api/v1/datasets", agentKey, "startup_durable", map[string]any{
		"name": "startup_durable/after_restart",
	})
	require.Equal(t, http.StatusCreated, writeResp.status, writeResp.body)

	ephemeralResp := startupRESTRequest(t, second, http.MethodPost, "/api/v1/datasets", agentKey, "startup_ephemeral", map[string]any{
		"name": "startup_ephemeral/denied",
	})
	require.NotEqual(t, http.StatusCreated, ephemeralResp.status, ephemeralResp.body)
}
