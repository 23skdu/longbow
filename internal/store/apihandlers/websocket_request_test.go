package apihandlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebsocketRequestHelpers(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet,
		"http://example.com/ws?access_token=query-token&namespace=query-ns&dataset=query-ds", nil)
	request.Header.Set("Authorization", "Bearer header-token")
	request.Header.Set("X-Tenant-ID", "header-ns")
	request.Header.Set("X-Dataset", "header-ds")

	token, namespace, dataset := websocketCredentials(request)
	require.Equal(t, "header-token", token)
	require.Equal(t, "header-ns", namespace)
	require.Equal(t, "header-ds", dataset)

	noOrigin := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
	require.True(t, websocketOriginAllowed(noOrigin))

	sameOrigin := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
	sameOrigin.Header.Set("Origin", "http://example.com")
	require.True(t, websocketOriginAllowed(sameOrigin))

	evilOrigin := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
	evilOrigin.Header.Set("Origin", "https://evil.example")
	require.False(t, websocketOriginAllowed(evilOrigin))
}

func FuzzWebsocketRequestHelpers(f *testing.F) {
	f.Add("tok", "http://example.com/ws", "example.com", "https://example.com", "ns", "ds")
	f.Add("", "http://example.com/ws", "example.com", "", "", "")
	f.Add("tok", "not a url", "", "https://evil.example", "ns", "ds")

	f.Fuzz(func(t *testing.T, token, rawURL, host, origin, namespace, dataset string) {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			t.Skip()
		}
		request := &http.Request{Header: make(http.Header), URL: parsed, Host: host}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		if namespace != "" {
			request.Header.Set("X-Tenant-ID", namespace)
		}
		if dataset != "" {
			request.Header.Set("X-Dataset", dataset)
		}

		gotToken, gotNamespace, gotDataset := websocketCredentials(request)
		if want := strings.TrimSpace(token); want != "" && gotToken != want {
			t.Fatalf("token = %q, want %q", gotToken, want)
		}
		if want := strings.TrimSpace(namespace); want != "" && gotNamespace != want {
			t.Fatalf("namespace = %q, want %q", gotNamespace, want)
		}
		if want := strings.TrimSpace(dataset); want != "" && gotDataset != want {
			t.Fatalf("dataset = %q, want %q", gotDataset, want)
		}
		if origin == "" && !websocketOriginAllowed(request) {
			t.Fatal("requests without an Origin header must be allowed")
		}
	})
}
