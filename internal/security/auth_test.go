package security

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthManager_Basic(t *testing.T) {
	config := SSOConfig{
		Enabled:      true,
		Providers:    []OAuthProvider{ProviderGitHub, ProviderGoogle},
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		RedirectURL:  "http://localhost/callback",
		Scopes:       []string{"user:email"},
	}

	mgr, err := NewOAuthManager(config)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// Test GetAuthURL
	url, err := mgr.GetAuthURL(ProviderGitHub, "random-state")
	require.NoError(t, err)
	assert.Contains(t, url, "github.com")
	assert.Contains(t, url, "state=random-state")

	url, err = mgr.GetAuthURL("invalid", "state")
	require.Error(t, err)

	// Test Domain Allowed
	mgr.config.AllowedDomains = []string{"example.com"}
	assert.True(t, mgr.IsDomainAllowed("user@example.com"))
	assert.False(t, mgr.IsDomainAllowed("user@other.com"))
	assert.False(t, mgr.IsDomainAllowed("invalid-email"))

	mgr.config.AllowedDomains = nil
	assert.True(t, mgr.IsDomainAllowed("user@any.com"))
}

func TestSessionStore(t *testing.T) {
	store := NewSessionStore()
	require.NotNil(t, store)

	session := &Session{
		ID:        "session-1",
		UserID:    "user-1",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}

	store.Store(session)

	// Test Get
	s, err := store.Get("session-1")
	require.NoError(t, err)
	assert.Equal(t, "user-1", s.UserID)

	// Test Get Missing
	_, err = store.Get("missing")
	require.Error(t, err)

	// Test Get Expired
	expiredSession := &Session{
		ID:        "session-expired",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}
	store.Store(expiredSession)
	_, err = store.Get("session-expired")
	require.Error(t, err)

	// Test Delete
	store.Delete("session-1")
	_, err = store.Get("session-1")
	require.Error(t, err)
}

func TestTokenGenerator(t *testing.T) {
	gen, err := NewTokenGenerator("my-issuer", "my-audience")
	require.NoError(t, err)

	claims := Claims{
		Sub:   "user-123",
		Email: "user@example.com",
		Name:  "User One",
		Admin: true,
	}

	token, err := gen.GenerateToken(claims)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	// Validate Token
	validated, err := gen.ValidateToken(token)
	require.NoError(t, err)
	assert.Equal(t, "user-123", validated.Sub)
	assert.Equal(t, "user@example.com", validated.Email)
	assert.True(t, validated.Admin)

	// Test Expired Token
	expiredClaims := Claims{
		Exp: time.Now().Add(-1 * time.Hour).Unix(),
	}
	
	tokenBytes, _ := json.Marshal(expiredClaims)
	expiredToken := base64.RawURLEncoding.EncodeToString(tokenBytes)
	
	_, err = gen.ValidateToken(expiredToken)
	require.Error(t, err)
}

func TestAuthMiddleware(t *testing.T) {
	config := SSOConfig{
		Enabled:      true,
		Providers:    []OAuthProvider{ProviderGitHub},
		ClientID:     "id",
		ClientSecret: "secret",
		SessionLifetime: 1 * time.Hour,
	}
	mgr, _ := NewOAuthManager(config)

	session := &Session{
		ID:        "valid-session",
		UserID:    "user-1",
		ExpiresAt: time.Now().Add(1 * time.Hour),
		Data:      map[string]interface{}{"admin": true},
	}
	mgr.sessionStore.Store(session)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Test RequireAuth - Fail
	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	mgr.RequireAuth(handler).ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	// Test RequireAuth - Success (via Header)
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Session-ID", "valid-session")
	rr = httptest.NewRecorder()
	mgr.Middleware()(mgr.RequireAuth(handler)).ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	// Test RequireAdmin - Fail (non-admin)
	userSession := &Session{
		ID:        "user-session",
		UserID:    "user-2",
		ExpiresAt: time.Now().Add(1 * time.Hour),
		Data:      map[string]interface{}{"admin": false},
	}
	mgr.sessionStore.Store(userSession)
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Session-ID", "user-session")
	rr = httptest.NewRecorder()
	mgr.Middleware()(mgr.RequireAdmin(handler)).ServeHTTP(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code)

	// Test RequireAdmin - Success
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Session-ID", "valid-session")
	rr = httptest.NewRecorder()
	mgr.Middleware()(mgr.RequireAdmin(handler)).ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}
