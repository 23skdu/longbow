package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type stubValidator struct {
	tokens map[string]*security.TenantContext
}

func (s *stubValidator) AuthorizeToken(_ context.Context, rawToken, permission, namespace, dataset string) (*security.TenantContext, error) {
	tc, ok := s.tokens[rawToken]
	if !ok {
		return nil, security.ErrUnauthorized
	}
	if !tc.CanAccessDataset(namespace, dataset) {
		return nil, security.ErrTenantIsolation
	}
	if permission != "" && !tc.HasPermission(permission) {
		return nil, security.ErrForbidden
	}
	return tc, nil
}

func TestAuthUnaryAndHTTPMiddleware(t *testing.T) {
	audit := security.NewAuditLogger()
	validator := &stubValidator{
		tokens: map[string]*security.TenantContext{
			"writer-t1": {
				TenantID:    "tenant1",
				Namespace:   "tenant1",
				UserID:      "u-writer",
				KeyID:       "k1",
				Permissions: []string{"read", "ingest"},
			},
			"reader-t1": {
				TenantID:    "tenant1",
				Namespace:   "tenant1",
				UserID:      "u-reader",
				KeyID:       "k2",
				Permissions: []string{"read"},
			},
		},
	}

	unary := AuthUnaryInterceptor(validator, audit)
	handler := func(ctx context.Context, _ any) (any, error) {
		tc, ok := security.TenantFromContext(ctx)
		if !ok {
			return nil, errors.New("missing tenant context")
		}
		return tc.TenantID, nil
	}

	// Missing auth -> Unauthenticated
	_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/DoGet"}, handler)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// Reader calling DoPut -> PermissionDenied
	readerCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer reader-t1",
		"x-tenant-id", "tenant1",
	))
	_, err = unary(readerCtx, nil, &grpc.UnaryServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/DoPut"}, handler)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Cross-tenant attempt -> PermissionDenied
	crossCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer writer-t1",
		"x-tenant-id", "tenant2",
	))
	_, err = unary(crossCtx, nil, &grpc.UnaryServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/DoGet"}, handler)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Authorized call attaches TenantContext
	okCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-api-key", "writer-t1",
		"x-tenant-id", "tenant1",
	))
	res, err := unary(okCtx, nil, &grpc.UnaryServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/DoPut"}, handler)
	require.NoError(t, err)
	require.Equal(t, "tenant1", res)

	// HTTP middleware check
	protected := HTTPAuthMiddleware(validator, audit, "read")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tc, ok := security.TenantFromContext(r.Context())
		require.True(t, ok)
		_, _ = w.Write([]byte(tc.TenantID))
	}))

	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	rec1 := httptest.NewRecorder()
	protected.ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusUnauthorized, rec1.Code)

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	req2.Header.Set("Authorization", "Bearer reader-t1")
	req2.Header.Set("X-Tenant-ID", "tenant1")
	rec2 := httptest.NewRecorder()
	protected.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, "tenant1", rec2.Body.String())
}

func FuzzExtractBearerToken(f *testing.F) {
	f.Add("Bearer abc123", "tenant-a", "ds1")
	f.Add("rawtoken", "", "")
	f.Fuzz(func(t *testing.T, auth, ns, ds string) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"authorization", auth,
			"x-tenant-id", ns,
			"x-dataset", ds,
		))
		_, _, _ = ExtractBearerToken(ctx)
	})
}
