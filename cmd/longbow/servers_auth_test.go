package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type opsStubValidator struct {
	principals map[string]*security.TenantContext
}

func (s *opsStubValidator) AuthorizeToken(_ context.Context, rawToken, _ string, _, _ string) (*security.TenantContext, error) {
	tc, ok := s.principals[rawToken]
	if !ok {
		return nil, security.ErrUnauthorized
	}
	return tc, nil
}

func TestAdminHTTPWrapRequiresGlobalAdministrator(t *testing.T) {
	validator := &opsStubValidator{principals: map[string]*security.TenantContext{
		"global": {
			KeyID:       "global",
			IsAdmin:     true,
			Permissions: []string{"admin"},
		},
		"namespace": {
			KeyID:       "namespace",
			Namespace:   "team",
			Permissions: []string{"admin", "manage-tenant"},
		},
	}}

	handler := adminHTTPWrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), validator, nil, zerolog.Nop())

	for _, tc := range []struct {
		name   string
		token  string
		status int
	}{
		{name: "missing", status: http.StatusUnauthorized},
		{name: "namespace scoped", token: "namespace", status: http.StatusForbidden},
		{name: "global admin", token: "global", status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/diagnostics", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code)
		})
	}
}

func TestReadinessTracksStartupCompletion(t *testing.T) {
	previous := globalIsReady.Load()
	t.Cleanup(func() { globalIsReady.Store(previous) })

	globalIsReady.Store(false)
	rec := httptest.NewRecorder()
	readinessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	globalIsReady.Store(true)
	rec = httptest.NewRecorder()
	readinessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}
