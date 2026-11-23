package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/security"
)

const maxRESTBodyBytes = 32 << 20

// httpAuthWrap applies HTTP RBAC/OIDC authentication dynamically at request time whenever
// VectorStore RBAC or OIDC is enabled (including when enabled after route registration).
func (s *VectorStore) httpAuthWrap(requiredPerm Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validator := s.AccessValidator()
		if validator == nil {
			next.ServeHTTP(w, r)
			return
		}
		security.HTTPAuthMiddleware(validator, s.AuditLogger(), string(requiredPerm))(next).ServeHTTP(w, r)
	})
}

// httpGlobalAuthWrap authenticates and then verifies global authority. It is
// for administrative endpoints whose permissions must never be satisfied by a
// namespace-scoped ACL grant, even when the caller supplies X-Tenant-ID.
func (s *VectorStore) httpGlobalAuthWrap(requiredPerm Permission, next http.Handler) http.Handler {
	return s.httpAuthWrap(requiredPerm, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := s.requireHTTPGlobalPermission(r.Context(), requiredPerm, ""); err != nil {
			code := http.StatusForbidden
			if errors.Is(err, security.ErrUnauthorized) {
				code = http.StatusUnauthorized
			}
			writeRESTError(w, "auth", r.Method, code, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	}))
}

func (s *VectorStore) httpGlobalMethodAuthWrap(readPerm, writePerm Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		perm := readPerm
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			perm = writePerm
		}
		s.httpGlobalAuthWrap(perm, next).ServeHTTP(w, r)
	})
}

func (s *VectorStore) httpMethodAuthWrap(readPerm, writePerm Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		perm := readPerm
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			perm = writePerm
		}
		s.httpAuthWrap(perm, next).ServeHTTP(w, r)
	})
}

func (s *VectorStore) requireHTTPGlobalPermission(ctx context.Context, perm Permission, datasetPath string) error {
	if s.AccessValidator() == nil {
		return nil
	}
	if datasetPath != "" {
		_, err := s.AuthorizeDatasetAccess(ctx, perm, datasetPath)
		return err
	}
	tc, ok := security.TenantFromContext(ctx)
	if !ok {
		return security.ErrUnauthorized
	}
	if tc.IsAdmin {
		return nil
	}
	// A namespace-scoped principal must not be able to reach global
	// administration through REST handlers that echo X-Tenant-ID.
	if tc.Namespace != "" && tc.Namespace != "*" {
		return security.ErrForbidden
	}
	for _, p := range tc.Permissions {
		if p == string(perm) || p == string(PermAdmin) {
			return nil
		}
	}
	return security.ErrForbidden
}

func writeRESTJSON(w http.ResponseWriter, endpoint, method string, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
	metrics.RESTRequestsTotal.WithLabelValues(endpoint, method, strconv.Itoa(statusCode)).Inc()
}

func writeRESTError(w http.ResponseWriter, endpoint, method string, statusCode int, msg string) {
	writeRESTJSON(w, endpoint, method, statusCode, map[string]any{"error": msg})
}

func isValidResourceName(name string) bool {
	return name != "" && !strings.Contains(name, "..") && !strings.HasPrefix(name, "/")
}

// RESTHealthHTTPHandler returns Liveness and active runtime metadata for /api/v1/health.
func (s *VectorStore) RESTHealthHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "health", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeRESTJSON(w, "health", r.Method, http.StatusOK, map[string]any{
			"status":          "ok",
			"active_datasets": len(s.GetActiveDatasets()),
			"namespaces":      len(s.ListNamespaces()),
			"cloud_clusters":  len(s.GetCloudControlPlane().ListClusters("")),
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})
}
