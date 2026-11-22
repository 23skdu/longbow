package security

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

// TestOIDCRedTeamSecurityVectors runs the test oidc red team security vectors operation.
func TestOIDCRedTeamSecurityVectors(t *testing.T) {
	m, priv, _ := testOIDCSetup(t)
	now := time.Now()

	// 1. Oversized token DoS vector
	huge := strings.Repeat("a", maxOIDCTokenBytes+1) + ".b.c"
	if _, err := m.ValidateToken(context.Background(), huge); err == nil {
		t.Fatal("expected oversized token to be rejected")
	}

	// 2. Tampered payload / signature mismatch
	valid := signToken(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "victim",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "rt-1",
		"tenant_id": "acme",
	})
	parts := strings.Split(valid, ".")
	tamperedPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://issuer.example","aud":"longbow","sub":"victim","role":"admin","exp":9999999999,"iat":1000,"jti":"rt-1"}`))
	tampered := parts[0] + "." + tamperedPayload + "." + parts[2]
	if _, err := m.ValidateToken(context.Background(), tampered); err == nil {
		t.Fatal("expected tampered JWT payload to fail signature verification")
	}

	// 3. Missing kid header
	tokNoKid := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "https://issuer.example",
		"aud": "longbow",
		"sub": "user-nokid",
		"exp": now.Add(time.Hour).Unix(),
		"iat": now.Unix(),
	})
	rawNoKid, err := tokNoKid.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ValidateToken(context.Background(), rawNoKid); err == nil {
		t.Fatal("expected missing kid to be rejected")
	}

	// 4. Unknown kid / key confusion
	tokBadKid := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "https://issuer.example",
		"aud": "longbow",
		"sub": "user-badkid",
		"exp": now.Add(time.Hour).Unix(),
		"iat": now.Unix(),
	})
	tokBadKid.Header["kid"] = "../../etc/passwd"
	rawBadKid, err := tokBadKid.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ValidateToken(context.Background(), rawBadKid); err == nil {
		t.Fatal("expected unknown kid to be rejected")
	}

	// 5. alg=none unsigned token rejection
	noneHeader := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	nonePayload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://issuer.example","aud":"longbow","sub":"admin","role":"admin"}`))
	if _, err := m.ValidateToken(context.Background(), noneHeader+"."+nonePayload+"."); err == nil {
		t.Fatal("expected alg=none token to be rejected")
	}

	// 6. SSRF / insecure plain HTTP issuer URL rejection in AddIssuer
	if err := m.AddIssuer(OIDCIssuerConfig{
		IssuerURL: "http://169.254.169.254/latest/meta-data",
		Audience:  "longbow",
	}); err == nil {
		t.Fatal("expected non-HTTPS metadata IP issuer URL to be rejected")
	}
	if err := m.AddIssuer(OIDCIssuerConfig{
		IssuerURL: "https://issuer.example",
		JWKSURL:   "http://10.0.0.1/jwks.json",
		Audience:  "longbow",
	}); err == nil {
		t.Fatal("expected non-HTTPS internal JWKS URL to be rejected")
	}
}

// BenchmarkOIDCValidateToken runs the benchmark oidc validate token operation.
func BenchmarkOIDCValidateToken(b *testing.B) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &priv.PublicKey, KeyID: "kid1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()

	mgr := NewOIDCManager(OIDCConfig{
		Enabled: true,
		Issuers: []OIDCIssuerConfig{{
			Provider:       OIDCProviderGeneric,
			IssuerURL:      "https://issuer.example",
			JWKSURL:        srv.URL,
			Audience:       "longbow",
			AllowedAlgs:    []string{"RS256"},
			TenantClaim:    "tenant_id",
			NamespaceClaim: "namespace",
			RoleClaim:      "groups",
			ClockSkew:      time.Minute,
		}},
	})
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "bench-user",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"tenant_id": "acme",
		"namespace": "acme",
		"groups":    []interface{}{"reader"},
	})
	tok.Header["kid"] = "kid1"
	raw, err := tok.SignedString(priv)
	if err != nil {
		b.Fatal(err)
	}

	// Warm JWKS cache
	if _, err := mgr.ValidateToken(context.Background(), raw); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := mgr.ValidateToken(context.Background(), raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOIDCAccessValidator runs the benchmark oidc access validator operation.
func BenchmarkOIDCAccessValidator(b *testing.B) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &priv.PublicKey, KeyID: "kid1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()

	mgr := NewOIDCManager(OIDCConfig{
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
			ClockSkew:      time.Minute,
		}},
	})
	validator := NewOIDCAccessValidator(mgr)
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "bench-user",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"reader"},
	})
	tok.Header["kid"] = "kid1"
	raw, err := tok.SignedString(priv)
	if err != nil {
		b.Fatal(err)
	}

	if _, err := validator.AuthorizeToken(context.Background(), raw, "read", "acme", "acme/docs"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := validator.AuthorizeToken(context.Background(), raw, "read", "acme", "acme/docs"); err != nil {
			b.Fatal(err)
		}
	}
}
