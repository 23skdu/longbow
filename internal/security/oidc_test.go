package security

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

func testOIDCSetup(t *testing.T) (*OIDCManager, *rsa.PrivateKey, *httptest.Server) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pub := &priv.PublicKey
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: pub, KeyID: "kid1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(srv.Close)

	cfg := OIDCConfig{
		Enabled: true,
		Issuers: []OIDCIssuerConfig{{
			Provider:       OIDCProviderGeneric,
			IssuerURL:      "https://issuer.example",
			JWKSURL:        srv.URL,
			Audience:       "longbow",
			AllowedAlgs:    []string{"RS256"},
			TenantClaim:    "tenant_id",
			NamespaceClaim: "namespace",
			DatasetsClaim:  "datasets",
			RoleClaim:      "groups",
			AdminClaim:     "role",
			AdminValue:     "admin",
			ClockSkew:      time.Minute,
		}},
	}
	return NewOIDCManager(cfg), priv, srv
}

func signToken(t *testing.T, priv *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "kid1"
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOIDCValidateTokenSuccess(t *testing.T) {
	m, priv, _ := testOIDCSetup(t)
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "user-123",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"nbf":       now.Add(-time.Minute).Unix(),
		"jti":       "jti-1",
		"email":     "u@example.com",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"writer"},
	}
	raw := signToken(t, priv, claims)
	c, err := m.ValidateToken(context.Background(), raw)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if c.Subject != "user-123" {
		t.Fatalf("subject mismatch: %s", c.Subject)
	}
	tc := c.ToTenantContext()
	if tc.TenantID != "acme" || tc.Namespace != "acme" {
		t.Fatalf("tenant/namespace mismatch: %+v", tc)
	}
	if !tc.HasPermission("write") {
		t.Fatalf("missing write permission: %+v", tc.Permissions)
	}
}

func TestOIDCValidateTokenFailures(t *testing.T) {
	m, priv, _ := testOIDCSetup(t)
	now := time.Now()
	base := func() jwt.MapClaims {
		return jwt.MapClaims{
			"iss": "https://issuer.example",
			"aud": "longbow",
			"sub": "user-123",
			"exp": now.Add(time.Hour).Unix(),
			"iat": now.Unix(),
			"jti": "jti-x",
		}
	}

	cases := []struct {
		name   string
		mutate func(jwt.MapClaims)
	}{
		{"wrong issuer", func(c jwt.MapClaims) { c["iss"] = "https://evil.example" }},
		{"wrong audience", func(c jwt.MapClaims) { c["aud"] = "other" }},
		{"expired", func(c jwt.MapClaims) { c["exp"] = now.Add(-time.Hour).Unix() }},
		{"missing sub", func(c jwt.MapClaims) { delete(c, "sub") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := base()
			tc.mutate(cl)
			raw := signToken(t, priv, cl)
			if _, err := m.ValidateToken(context.Background(), raw); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestOIDCReplayProtection(t *testing.T) {
	m, priv, _ := testOIDCSetup(t)
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": "https://issuer.example",
		"aud": "longbow",
		"sub": "user-123",
		"exp": now.Add(time.Hour).Unix(),
		"iat": now.Unix(),
		"jti": "replay-jti",
	}
	raw := signToken(t, priv, claims)
	if _, err := m.ValidateToken(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ValidateToken(context.Background(), raw); err == nil {
		t.Fatal("expected replay error")
	}
}

func TestOIDCAlgorithmConfusionRejected(t *testing.T) {
	m, priv, _ := testOIDCSetup(t)
	// Sign with HS256 using RSA key bytes is not valid; instead craft token with alg none.
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"iss": "https://issuer.example",
		"aud": "longbow",
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "kid1"
	raw, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ValidateToken(context.Background(), raw); err == nil {
		t.Fatal("expected rejection of none alg")
	}
	_ = priv
}

func FuzzOIDCValidateToken(f *testing.F) {
	f.Add([]byte("not-a-token"))
	mgr := NewOIDCManager(OIDCConfig{Enabled: true})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = mgr.ValidateToken(context.Background(), string(data))
	})
}
