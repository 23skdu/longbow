package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/23skdu/longbow/internal/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ExtractBearerToken extracts an API key or bearer token from incoming gRPC metadata.
func ExtractBearerToken(ctx context.Context) (token, namespace, dataset string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", ""
	}
	for _, raw := range md.Get("authorization") {
		if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
			token = strings.TrimSpace(raw[7:])
			break
		}
		if raw != "" {
			token = strings.TrimSpace(raw)
			break
		}
	}
	if token == "" {
		if vals := md.Get("x-api-key"); len(vals) > 0 {
			token = strings.TrimSpace(vals[0])
		}
	}
	if vals := md.Get("x-tenant-id"); len(vals) > 0 {
		namespace = strings.TrimSpace(vals[0])
	} else if vals := md.Get("x-namespace"); len(vals) > 0 {
		namespace = strings.TrimSpace(vals[0])
	}
	if vals := md.Get("x-dataset"); len(vals) > 0 {
		dataset = strings.TrimSpace(vals[0])
	}
	return token, namespace, dataset
}

func permissionForFlightMethod(fullMethod string) string {
	switch {
	case strings.Contains(fullMethod, "DoPut"), strings.Contains(fullMethod, "DoExchange"):
		return "ingest"
	case strings.Contains(fullMethod, "DoAction"):
		return "" // Action handler performs fine-grained action/dataset check
	case strings.Contains(fullMethod, "DoGet"),
		strings.Contains(fullMethod, "GetFlightInfo"),
		strings.Contains(fullMethod, "GetSchema"),
		strings.Contains(fullMethod, "ListFlights"),
		strings.Contains(fullMethod, "ListActions"):
		return "read"
	default:
		return "read"
	}
}

func mapAuthErrToStatus(err error) error {
	if errors.Is(err, security.ErrForbidden) || errors.Is(err, security.ErrTenantIsolation) {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return status.Error(codes.Unauthenticated, err.Error())
}

// AuthUnaryInterceptor enforces RBAC and injects TenantContext into unary gRPC contexts.
// If validator is nil, authentication is disabled and calls pass through unchanged.
func AuthUnaryInterceptor(validator security.AccessValidator, audit *security.AuditLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if validator == nil {
			return handler(ctx, req)
		}
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") {
			return handler(ctx, req)
		}
		token, ns, ds := ExtractBearerToken(ctx)
		if token == "" {
			if audit != nil {
				audit.RecordEvent(ctx, "", ns, info.FullMethod, ds, "grpc", false, "missing credentials")
			}
			return nil, status.Error(codes.Unauthenticated, "missing authentication credentials")
		}
		tc, err := validator.AuthorizeToken(ctx, token, permissionForFlightMethod(info.FullMethod), ns, ds)
		if err != nil {
			return nil, mapAuthErrToStatus(err)
		}
		return handler(security.ContextWithTenant(ctx, tc), req)
	}
}

// AuthStreamInterceptor enforces RBAC and injects TenantContext into streaming Flight/gRPC calls.
func AuthStreamInterceptor(validator security.AccessValidator, audit *security.AuditLogger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if validator == nil {
			return handler(srv, ss)
		}
		ctx := ss.Context()
		token, ns, ds := ExtractBearerToken(ctx)
		if token == "" {
			if audit != nil {
				audit.RecordEvent(ctx, "", ns, info.FullMethod, ds, "grpc-stream", false, "missing credentials")
			}
			return status.Error(codes.Unauthenticated, "missing authentication credentials")
		}
		tc, err := validator.AuthorizeToken(ctx, token, permissionForFlightMethod(info.FullMethod), ns, ds)
		if err != nil {
			return mapAuthErrToStatus(err)
		}
		wrapped := &wrappedAuthStream{
			ServerStream: ss,
			ctx:          security.ContextWithTenant(ctx, tc),
		}
		return handler(srv, wrapped)
	}
}

type wrappedAuthStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedAuthStream) Context() context.Context {
	return w.ctx
}

// HTTPAuthMiddleware enforces RBAC Bearer / X-API-Key auth on HTTP handlers when validator is non-nil.
func HTTPAuthMiddleware(validator security.AccessValidator, audit *security.AuditLogger, requiredPerm string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if validator == nil {
				next.ServeHTTP(w, r)
				return
			}
			var token string
			if authz := r.Header.Get("Authorization"); authz != "" {
				if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
					token = strings.TrimSpace(authz[7:])
				} else {
					token = strings.TrimSpace(authz)
				}
			}
			if token == "" {
				token = strings.TrimSpace(r.Header.Get("X-API-Key"))
			}
			if token == "" {
				if audit != nil {
					audit.RecordEvent(r.Context(), "", "", r.Method+" "+r.URL.Path, r.URL.Path, r.RemoteAddr, false, "missing HTTP credentials")
				}
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			ns := r.Header.Get("X-Tenant-ID")
			ds := r.Header.Get("X-Dataset")
			tc, err := validator.AuthorizeToken(r.Context(), token, requiredPerm, ns, ds)
			if err != nil {
				code := http.StatusUnauthorized
				if errors.Is(err, security.ErrForbidden) || errors.Is(err, security.ErrTenantIsolation) {
					code = http.StatusForbidden
				}
				http.Error(w, err.Error(), code)
				return
			}
			next.ServeHTTP(w, r.WithContext(security.ContextWithTenant(r.Context(), tc)))
		})
	}
}
