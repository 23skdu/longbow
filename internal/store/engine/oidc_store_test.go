package engine

import (
	"bytes"
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
	"github.com/apache/arrow-go/v18/arrow/flight"
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

// TestVectorStoreOIDCAuthorizeAndComposite runs the test vector store oidc authorize and composite operation.
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

// TestVectorStoreOIDCRESTAndActions runs the test vector store oidcrest and actions operation.
func TestVectorStoreOIDCRESTAndActions(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	oidcMgr, priv := setupStoreOIDCTest(t)
	s.EnableOIDC(oidcMgr)
	rbacMgr := NewRBACManager()
	s.EnableRBAC(rbacMgr)
	_, adminRaw, err := rbacMgr.CreateAPIKeyWithOptions(APIKeyOptions{Name: "admin", Role: RoleAdmin})
	require.NoError(t, err)

	// 1. Exchange OIDC token via REST -> short-lived session token
	now := time.Now()
	jwtTok := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "exchange-user",
		"exp":       now.Add(2 * time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "exchange-jti-1",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"writer"},
		"row_filters": map[string]interface{}{
			"region": "us-west",
		},
	})
	exBody, _ := json.Marshal(map[string]any{"token": jwtTok, "ttl_seconds": 7200})
	exReq := httptest.NewRequest(http.MethodPost, "/api/v1/oidc/token", bytes.NewReader(exBody))
	exRR := httptest.NewRecorder()
	s.oidcTokenExchangeHTTPHandler().ServeHTTP(exRR, exReq)
	require.Equal(t, http.StatusOK, exRR.Code)

	var exResp struct {
		KeyID     string    `json:"key_id"`
		Token     string    `json:"token"`
		Role      string    `json:"role"`
		TenantID  string    `json:"tenant_id"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	require.NoError(t, json.Unmarshal(exRR.Body.Bytes(), &exResp))
	require.NotEmpty(t, exResp.Token)
	require.Equal(t, "writer", exResp.Role)
	require.Equal(t, "acme", exResp.TenantID)
	// TTL capped at <= 1h
	require.True(t, time.Until(exResp.ExpiresAt) <= 61*time.Minute)

	// Exchanged token works for scoped dataset access and enforces boundaries
	sessCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", exResp.Token))
	tcSess, err := s.AuthorizeDatasetAccess(sessCtx, PermWrite, "acme/docs")
	require.NoError(t, err)
	require.Equal(t, "us-west", tcSess.RowFilters["region"])

	_, err = s.AuthorizeDatasetAccess(sessCtx, PermRead, "other/docs")
	require.Error(t, err)

	// 2. REST provider CRUD (/api/v1/oidc/providers)
	provHandler := s.httpAuthWrap(PermAdmin, s.oidcProvidersHTTPHandler())
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/oidc/providers", nil)
	listReq.Header.Set("X-API-Key", adminRaw)
	listRR := httptest.NewRecorder()
	provHandler.ServeHTTP(listRR, listReq)
	require.Equal(t, http.StatusOK, listRR.Code)

	addBody, _ := json.Marshal(security.AWSOIDCIssuerConfig("https://oidc.eks.us-west-2.amazonaws.com/id/TEST123", "sts.amazonaws.com"))
	addReq := httptest.NewRequest(http.MethodPost, "/api/v1/oidc/providers", bytes.NewReader(addBody))
	addReq.Header.Set("X-API-Key", adminRaw)
	addRR := httptest.NewRecorder()
	provHandler.ServeHTTP(addRR, addReq)
	require.Equal(t, http.StatusCreated, addRR.Code)
	require.Len(t, s.OIDC().ListIssuers(), 2)

	// Reject HS256 algorithm on provider registration
	badAlgBody, _ := json.Marshal(security.OIDCIssuerConfig{
		IssuerURL:   "https://bad.example",
		Audience:    "longbow",
		AllowedAlgs: []string{"HS256"},
	})
	badReq := httptest.NewRequest(http.MethodPost, "/api/v1/oidc/providers", bytes.NewReader(badAlgBody))
	badReq.Header.Set("X-API-Key", adminRaw)
	badRR := httptest.NewRecorder()
	provHandler.ServeHTTP(badRR, badReq)
	require.Equal(t, http.StatusBadRequest, badRR.Code)

	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/oidc/providers?issuer_url=https://oidc.eks.us-west-2.amazonaws.com/id/TEST123", nil)
	delReq.Header.Set("X-API-Key", adminRaw)
	delRR := httptest.NewRecorder()
	provHandler.ServeHTTP(delRR, delReq)
	require.Equal(t, http.StatusOK, delRR.Code)
	require.Len(t, s.OIDC().ListIssuers(), 1)

	// 3. Flight DoAction OIDC actions
	adminCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", adminRaw))
	stream := &captureActionStream{ctx: adminCtx}
	handled, err := s.handleRBACAction(&flight.Action{Type: "ListOIDCProviders"}, stream)
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, stream.results, 1)

	jwtTok2 := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "flight-exchange-user",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "exchange-jti-2",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"reader"},
	})
	exActionBody, _ := json.Marshal(map[string]any{"token": jwtTok2, "ttl_seconds": 300})
	exStream := &captureActionStream{ctx: context.Background()}
	handled, err = s.handleRBACAction(&flight.Action{Type: "ExchangeOIDCToken", Body: exActionBody}, exStream)
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, exStream.results, 1)
}

// TestOIDC_DynamicAccessValidatorAndNoDoubleReplayOnFlightExchange runs the test oidc_dynamic access validator and no double replay on flight exchange operation.
func TestOIDC_DynamicAccessValidatorAndNoDoubleReplayOnFlightExchange(t *testing.T) {
	oidcMgr, priv := setupStoreOIDCTest(t)

	// Start with OIDC-only (RBAC not enabled at startup), matching LONGBOW_OIDC_ENABLED=true, LONGBOW_RBAC_ENABLED=false.
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<30, 0)
	defer func() { _ = s.Close() }()
	s.EnableOIDC(oidcMgr)

	// Capture DynamicAccessValidator once at startup (as cmd/longbow does for gRPC interceptors).
	dynValidator := s.DynamicAccessValidator()

	now := time.Now()
	jwtTok := signStoreTestJWT(t, priv, jwt.MapClaims{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "ingest-svc",
		"exp":       now.Add(time.Hour).Unix(),
		"iat":       now.Unix(),
		"jti":       "dyn-jti-1",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/events"},
		"groups":    []interface{}{"ingest-only"},
	})

	// Simulate AuthStreamInterceptor validating the Bearer JWT first (which consumes jti)
	// and injecting TenantContext into ctx before DoAction("ExchangeOIDCToken") runs with the same token in body.
	mdCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+jwtTok))
	tc, err := dynValidator.AuthorizeToken(mdCtx, jwtTok, "", "", "")
	require.NoError(t, err)
	require.True(t, tc.HasPermission(string(PermIngest)))
	require.False(t, tc.HasPermission(string(PermWrite)))

	exBody, _ := json.Marshal(map[string]any{"token": jwtTok, "ttl_seconds": 300})
	exStream := &captureActionStream{ctx: security.ContextWithTenant(mdCtx, tc)}
	handled, err := s.handleRBACAction(&flight.Action{Type: "ExchangeOIDCToken", Body: exBody}, exStream)
	require.True(t, handled)
	require.NoError(t, err, "pre-authenticated ExchangeOIDCToken must not re-validate JWT and trip jti replay")
	require.Len(t, exStream.results, 1)

	var resp struct {
		Token string `json:"token"`
		Role  Role   `json:"role"`
	}
	require.NoError(t, json.Unmarshal(exStream.results[0].Body, &resp))
	require.NotEmpty(t, resp.Token)
	require.Equal(t, RoleIngest, resp.Role)

	// The SAME startup DynamicAccessValidator must now accept the minted session API key
	// even though RBAC was lazily initialized by ExchangeOIDCToken.
	sessTC, err := dynValidator.AuthorizeToken(context.Background(), resp.Token, string(PermIngest), "acme", "acme/events")
	require.NoError(t, err)
	require.Equal(t, "acme", sessTC.TenantID)
	// ingest-only must still be denied write/delete.
	_, err = dynValidator.AuthorizeToken(context.Background(), resp.Token, string(PermWrite), "acme", "acme/events")
	require.Error(t, err)
}
