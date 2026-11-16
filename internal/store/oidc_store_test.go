package store

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/security"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func setupStoreOIDCTest(t *testing.T) (*security.OIDCManager, *rsa.PrivateKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
		{Key: &priv.PublicKey, KeyID: "kid1", Algorithm: "RS256", Use: "sig"},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(srv.Close)

	cfg := security.OIDCConfig{
		Enabled: true,
		Issuers: []security.OIDCIssuerConfig{{
			Provider:       security.OIDCProviderGeneric,
			IssuerURL:      "https://issuer.example",
			JWKSURL:        srv.URL,
			Audience:       "longbow",
			AllowedAlgs:    []string{"RS256"},
			TenantClaim:    "tenant_id",
			NamespaceClaim: "namespace",
			DatasetsClaim:  "datasets",
			RoleClaim:      "groups",
			RowFilterClaim: "row_filters",
			AdminClaim:     "role",
			AdminValue:     "admin",
			ClockSkew:      time.Minute,
		}},
	}
	return security.NewOIDCManager(cfg), priv
}

func signStoreTestJWT(t *testing.T, priv *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "kid1"
	s, err := tok.SignedString(priv)
	require.NoError(t, err)
	return s
}

func TestVectorStoreOIDCAuthorizeAndComposite(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	require.Nil(t, s.AccessValidator())

	oidcMgr, priv := setupStoreOIDCTest(t)
	s.EnableOIDC(oidcMgr)
	require.NotNil(t, s.OIDC())
	require.NotNil(t, s.AccessValidator())

	now := time.Now()
	jwtToken := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "aws-role-1",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "store-jti-1",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"writer"},
		"row_filters": map[string]interface{}{
			"env": "prod",
		},
	})

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+jwtToken))
	tc, err := s.AuthorizeDatasetAccess(ctx, PermWrite, "acme/docs")
	require.NoError(t, err)
	require.Equal(t, "acme", tc.TenantID)
	require.Equal(t, "prod", tc.RowFilters["env"])

	// Cross-tenant rejection (fresh jti since replay cache tracks jti)
	jwtToken2 := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "aws-role-1",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "store-jti-2",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"writer"},
	})
	ctx2 := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+jwtToken2))
	_, err = s.AuthorizeDatasetAccess(ctx2, PermRead, "other/docs")
	require.Error(t, err)

	// Enable RBAC alongside OIDC -> composite validator supports both API keys and OIDC JWTs
	rbacMgr := NewRBACManager()
	s.EnableRBAC(rbacMgr)
	_, apiKey, err := rbacMgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "svc-key",
		Role:      RoleReader,
		TenantID:  "beta",
		Namespace: "beta",
	})
	require.NoError(t, err)

	apiCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", apiKey))
	tcAPI, err := s.AuthorizeDatasetAccess(apiCtx, PermRead, "beta/items")
	require.NoError(t, err)
	require.Equal(t, "beta", tcAPI.TenantID)

	jwtToken3 := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "gcp-sa-1",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "store-jti-3",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"reader"},
	})
	oidcCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+jwtToken3))
	tcOIDC, err := s.AuthorizeDatasetAccess(oidcCtx, PermRead, "acme/docs")
	require.NoError(t, err)
	require.Equal(t, "acme", tcOIDC.TenantID)

	// RLS filtering via OIDC RowFilterClaim
	prodMeta, err := core.EncodeMetadata(map[string]any{"env": "prod", "tenant_id": "acme"})
	require.NoError(t, err)
	devMeta, err := core.EncodeMetadata(map[string]any{"env": "dev", "tenant_id": "acme"})
	require.NoError(t, err)

	rlsCtx := security.ContextWithTenant(context.Background(), tc)
	filtered := s.FilterResultsByRLS(rlsCtx, "acme/docs", []SearchResult{
		{ID: 1, Metadata: prodMeta},
		{ID: 2, Metadata: devMeta},
	})
	require.Len(t, filtered, 1)
	require.Equal(t, VectorID(1), filtered[0].ID)

	// HTTP middleware integration
	jwtToken4 := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "http-user",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "store-jti-4",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"reader"},
	})
	h := s.httpAuthWrap(PermRead, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/datasets", nil)
	req.Header.Set("Authorization", "Bearer "+jwtToken4)
	req.Header.Set("X-Tenant-ID", "acme")
	req.Header.Set("X-Dataset", "acme/docs")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	unauthReq := httptest.NewRequest(http.MethodGet, "/api/v1/datasets", nil)
	unauthRR := httptest.NewRecorder()
	h.ServeHTTP(unauthRR, unauthReq)
	require.Equal(t, http.StatusUnauthorized, unauthRR.Code)
}
