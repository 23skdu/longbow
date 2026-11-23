package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/security"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// EnableRBAC configures RBAC, RLS, and audit logging on the VectorStore.
func (s *VectorStore) EnableRBAC(mgr *RBACManager) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.auditLogger == nil {
		s.auditLogger = security.NewAuditLogger()
	}
	if mgr != nil {
		mgr.SetAuditLogger(s.auditLogger)
	}
	s.rbacManager = mgr
}

// RBAC returns the active RBACManager (nil if RBAC is disabled).
func (s *VectorStore) RBAC() *RBACManager {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.rbacManager
}

// AuditLogger returns the store's AuditLogger, lazily creating one if needed.
func (s *VectorStore) AuditLogger() *security.AuditLogger {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.auditLogger == nil {
		s.auditLogger = security.NewAuditLogger()
	}
	return s.auditLogger
}

// AuthorizeDatasetAccess validates RBAC, OIDC, & tenant boundaries when authentication is enabled.
// When neither RBAC nor OIDC is configured, all requests are allowed.
func (s *VectorStore) AuthorizeDatasetAccess(ctx context.Context, perm Permission, datasetPath string) (*security.TenantContext, error) {
	validator := s.AccessValidator()
	audit := s.AuditLogger()

	if validator == nil {
		tc, _ := security.TenantFromContext(ctx)
		return tc, nil
	}

	ns, ds := ParseNamespacedPath(datasetPath)

	// If middleware already attached a verified TenantContext, verify permission + scope directly.
	if tc, ok := security.TenantFromContext(ctx); ok && tc != nil {
		if !tc.HasPermission(string(perm)) && !tc.HasNamespacePermission(ns, string(perm)) {
			if audit != nil {
				audit.RecordEvent(ctx, tc.KeyID, tc.TenantID, string(perm), datasetPath, "flight", false, "permission denied")
			}
			return nil, status.Errorf(codes.PermissionDenied, "permission %s denied", perm)
		}
		if !tc.CanAccessDataset(ns, ds) {
			if audit != nil {
				audit.RecordEvent(ctx, tc.KeyID, tc.TenantID, string(perm), datasetPath, "flight", false, "tenant isolation boundary")
			}
			return nil, status.Errorf(codes.PermissionDenied, "tenant isolation denied for %s", datasetPath)
		}
		return tc, nil
	}

	// Fall back to direct gRPC incoming metadata bearer token validation when interceptor is bypassed (e.g. unit tests calling DoAction directly).
	rawKey := extractTokenFromMetadata(ctx)
	if rawKey == "" {
		if audit != nil {
			audit.RecordEvent(ctx, "", ns, string(perm), datasetPath, "flight", false, "missing API key")
		}
		return nil, status.Error(codes.Unauthenticated, "missing authentication token")
	}

	tc, err := validator.AuthorizeToken(ctx, rawKey, string(perm), ns, ds)
	if err != nil {
		if errors.Is(err, security.ErrForbidden) || errors.Is(err, security.ErrTenantIsolation) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return tc, nil
}

// CanTenantListDataset checks whether the caller's TenantContext allows seeing the named dataset.
func (s *VectorStore) CanTenantListDataset(ctx context.Context, datasetPath string) bool {
	if s.AccessValidator() == nil {
		return true
	}
	tc, err := s.AuthorizeDatasetAccess(ctx, PermRead, datasetPath)
	return err == nil && (tc == nil || tc.CanAccessDataset(ParseNamespacedPath(datasetPath)))
}

// FilterResultsByRLS filters search results through row-level security policies when RBAC/OIDC/RLS is active.
func (s *VectorStore) FilterResultsByRLS(ctx context.Context, datasetPath string, results []SearchResult) []SearchResult {
	if len(results) == 0 {
		return results
	}
	validator := s.AccessValidator()
	if validator == nil {
		return results
	}
	s.configMu.RLock()
	mgr := s.rbacManager
	s.configMu.RUnlock()

	tc, ok := security.TenantFromContext(ctx)
	if !ok || tc == nil {
		rawKey := extractTokenFromMetadata(ctx)
		if rawKey != "" {
			if resolved, err := validator.AuthorizeToken(ctx, rawKey, string(PermRead), "", ""); err == nil {
				tc = resolved
			}
		}
	}
	if tc == nil {
		// RBAC is active but no verified principal is available. Never leak
		// unfiltered rows in that case.
		return nil
	}
	if tc.IsAdmin {
		return results
	}
	var rls *security.RLSEvaluator
	if mgr != nil {
		rls = mgr.RLSEvaluator()
	}
	ns, ds := ParseNamespacedPath(datasetPath)

	filtered := make([]SearchResult, 0, len(results))
	for _, res := range results {
		var meta map[string]any
		if len(res.Metadata) > 0 {
			meta, _ = core.DecodeMetadata(res.Metadata)
		}
		if rls.EvaluateMetadata(tc, ns, ds, meta) {
			filtered = append(filtered, res)
		}
	}
	return filtered
}

func extractTokenFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, raw := range md.Get("authorization") {
		if len(raw) > 7 && (raw[:7] == "Bearer " || raw[:7] == "bearer ") {
			return raw[7:]
		}
		if raw != "" {
			return raw
		}
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (s *VectorStore) handleRBACAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	ctx := stream.Context()
	switch action.Type {
	case "CreateAPIKey":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageKeys, ""); err != nil {
			return true, err
		}
		var req struct {
			Name       string            `json:"name"`
			Role       string            `json:"role"`
			TenantID   string            `json:"tenant_id"`
			Namespace  string            `json:"namespace"`
			Datasets   []string          `json:"datasets"`
			RowFilters map[string]string `json:"row_filters"`
			TTLSeconds int64             `json:"ttl_seconds"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		mgr := s.ensureRBACManager()
		ak, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
			Name:       req.Name,
			Role:       Role(req.Role),
			TenantID:   req.TenantID,
			Namespace:  req.Namespace,
			Datasets:   req.Datasets,
			RowFilters: req.RowFilters,
			TTL:        time.Duration(req.TTLSeconds) * time.Second,
		})
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{
			"key_id":     ak.KeyID,
			"api_key":    raw,
			"role":       ak.Role,
			"tenant_id":  ak.TenantID,
			"namespace":  ak.Namespace,
			"expires_at": ak.ExpiresAt,
		})
		return true, stream.Send(&flight.Result{Body: body})

	case "RotateAPIKey":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageKeys, ""); err != nil {
			return true, err
		}
		var req struct {
			APIKey             string `json:"api_key"`
			GracePeriodSeconds int64  `json:"grace_period_seconds"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		mgr := s.ensureRBACManager()
		ak, newRaw, err := mgr.RotateAPIKey(req.APIKey, time.Duration(req.GracePeriodSeconds)*time.Second)
		if err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{
			"key_id":       ak.KeyID,
			"api_key":      newRaw,
			"rotated_from": ak.RotatedFrom,
		})
		return true, stream.Send(&flight.Result{Body: body})

	case "RevokeAPIKey":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageKeys, ""); err != nil {
			return true, err
		}
		var req struct {
			APIKey string `json:"api_key"`
			KeyID  string `json:"key_id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		target := req.APIKey
		if target == "" {
			target = req.KeyID
		}
		mgr := s.ensureRBACManager()
		if err := mgr.RevokeAPIKey(target); err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"revoked"}`)})

	case "ListAPIKeys":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageKeys, ""); err != nil {
			return true, err
		}
		mgr := s.ensureRBACManager()
		keys := mgr.ListAPIKeys()
		body, _ := json.Marshal(map[string]any{"keys": keys})
		return true, stream.Send(&flight.Result{Body: body})

	case "SetRLSPolicy":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageTenant, ""); err != nil {
			return true, err
		}
		var pol security.RLSPolicy
		if err := json.Unmarshal(action.Body, &pol); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid RLS policy: %v", err)
		}
		mgr := s.ensureRBACManager()
		if err := mgr.RLSEvaluator().SetPolicy(pol); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"rls_policy_set"}`)})

	case "GetAuditLog":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermAdmin, ""); err != nil {
			return true, err
		}
		var req struct {
			Limit int `json:"limit"`
		}
		_ = json.Unmarshal(action.Body, &req)
		if req.Limit <= 0 {
			req.Limit = 100
		}
		entries := s.AuditLogger().RecentEntries(req.Limit)
		body, _ := json.Marshal(map[string]any{"entries": entries})
		return true, stream.Send(&flight.Result{Body: body})

	case "GetAuditDenialCounts":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermAdmin, ""); err != nil {
			return true, err
		}
		counts := s.AuditLogger().DenialCounts()
		total := 0
		for _, n := range counts {
			total += n
		}
		body, _ := json.Marshal(map[string]any{"counts": counts, "total": total})
		return true, stream.Send(&flight.Result{Body: body})

	case "ListOIDCProviders":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermAdmin, ""); err != nil {
			return true, err
		}
		issuers := s.OIDC().ListIssuers()
		body, _ := json.Marshal(map[string]any{"providers": issuers, "count": len(issuers)})
		return true, stream.Send(&flight.Result{Body: body})

	case "AddOIDCProvider":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermAdmin, ""); err != nil {
			return true, err
		}
		var cfg security.OIDCIssuerConfig
		if err := json.Unmarshal(action.Body, &cfg); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		mgr := s.ensureOIDCManager()
		if err := mgr.AddIssuer(cfg); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		s.AuditLogger().RecordEvent(ctx, "", "", "add_oidc_provider", cfg.IssuerURL, "flight", true, "")
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"oidc_provider_added"}`)})

	case "RemoveOIDCProvider":
		if _, err := s.AuthorizeDatasetAccess(ctx, PermAdmin, ""); err != nil {
			return true, err
		}
		var req struct {
			IssuerURL string `json:"issuer_url"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if req.IssuerURL == "" || !s.OIDC().RemoveIssuer(req.IssuerURL) {
			return true, status.Error(codes.NotFound, "oidc provider not found")
		}
		s.AuditLogger().RecordEvent(ctx, "", "", "remove_oidc_provider", req.IssuerURL, "flight", true, "")
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"oidc_provider_removed"}`)})

	case "ExchangeOIDCToken":
		var req struct {
			Token      string `json:"token"`
			TTLSeconds int64  `json:"ttl_seconds"`
		}
		if len(action.Body) > 0 {
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
			}
		}
		ttl := time.Duration(req.TTLSeconds) * time.Second
		var (
			ak  *APIKey
			raw string
			err error
		)
		// If gRPC AuthStreamInterceptor already verified the caller's OIDC JWT and
		// injected TenantContext into ctx, reuse it when no different body token is
		// supplied so we do not double-validate the JWT and trip jti replay protection.
		if tc, ok := security.TenantFromContext(ctx); ok && tc != nil && strings.HasPrefix(tc.KeyID, "oidc:") && (req.Token == "" || req.Token == extractTokenFromMetadata(ctx)) {
			ak, raw, err = s.issueSessionForTenantContext(ctx, tc, ttl)
		} else if req.Token != "" {
			ak, raw, err = s.ExchangeOIDCToken(ctx, req.Token, ttl)
		} else if tok := extractTokenFromMetadata(ctx); tok != "" {
			ak, raw, err = s.ExchangeOIDCToken(ctx, tok, ttl)
		} else {
			return true, status.Error(codes.Unauthenticated, "missing OIDC token")
		}
		if err != nil {
			if errors.Is(err, security.ErrUnauthorized) {
				return true, status.Error(codes.Unauthenticated, err.Error())
			}
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{
			"key_id":     ak.KeyID,
			"token":      raw,
			"role":       ak.Role,
			"tenant_id":  ak.TenantID,
			"namespace":  ak.Namespace,
			"expires_at": ak.ExpiresAt,
		})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}

func (s *VectorStore) ensureRBACManager() *RBACManager {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.rbacManager == nil {
		s.rbacManager = NewRBACManager()
	}
	if s.auditLogger == nil {
		s.auditLogger = security.NewAuditLogger()
	}
	s.rbacManager.SetAuditLogger(s.auditLogger)
	return s.rbacManager
}

// EnableOIDC configures OIDC federation on the VectorStore.
func (s *VectorStore) EnableOIDC(mgr *security.OIDCManager) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.auditLogger == nil {
		s.auditLogger = security.NewAuditLogger()
	}
	s.oidcManager = mgr
}

// OIDC returns the active OIDCManager (nil if disabled).
func (s *VectorStore) OIDC() *security.OIDCManager {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.oidcManager
}

// ensureOIDCValidator returns an OIDCAccessValidator if OIDC is enabled.
func (s *VectorStore) ensureOIDCValidator() *security.OIDCAccessValidator {
	mgr := s.OIDC()
	if mgr == nil {
		return nil
	}
	v := security.NewOIDCAccessValidator(mgr)
	v.SetAuditLogger(s.AuditLogger())
	return v
}

func (s *VectorStore) ensureOIDCManager() *security.OIDCManager {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.auditLogger == nil {
		s.auditLogger = security.NewAuditLogger()
	}
	if s.oidcManager == nil {
		s.oidcManager = security.NewOIDCManager(security.OIDCConfig{Enabled: true})
	}
	return s.oidcManager
}

// ExchangeOIDCToken validates a federated OIDC JWT and issues a short-lived
// Longbow session API key bound to the federated identity's tenant and scope.
func (s *VectorStore) ExchangeOIDCToken(ctx context.Context, rawToken string, ttl time.Duration) (*APIKey, string, error) {
	mgr := s.OIDC()
	if mgr == nil || !mgr.Enabled() {
		return nil, "", errors.New("oidc federation disabled")
	}
	claims, err := mgr.ValidateToken(ctx, rawToken)
	if err != nil {
		s.AuditLogger().RecordEvent(ctx, "", "", "oidc_token_exchange", "", "auth", false, err.Error())
		return nil, "", err
	}
	if claims.Expiry > 0 {
		remaining := time.Until(time.Unix(claims.Expiry, 0))
		if remaining <= 0 {
			return nil, "", errors.New("oidc token expired")
		}
		if ttl <= 0 || ttl > remaining {
			ttl = remaining
		}
	}
	return s.issueSessionForTenantContext(ctx, claims.ToTenantContext(), ttl)
}

func (s *VectorStore) issueSessionForTenantContext(ctx context.Context, tc *security.TenantContext, ttl time.Duration) (*APIKey, string, error) {
	if tc == nil {
		return nil, "", errors.New("missing tenant context")
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	const maxSessionTTL = time.Hour
	if ttl > maxSessionTTL {
		ttl = maxSessionTTL
	}
	role := RoleReader
	switch {
	case tc.IsAdmin:
		role = RoleAdmin
	case tc.HasPermission(string(PermManageTenant)) || tc.HasPermission(string(PermManageKeys)):
		role = RoleTenantAdmin
	case tc.HasPermission(string(PermWrite)):
		role = RoleWriter
	case tc.HasPermission(string(PermIngest)):
		role = RoleIngest
	}
	rbac := s.ensureRBACManager()
	ak, raw, err := rbac.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:       tc.KeyID,
		Role:       role,
		TenantID:   tc.TenantID,
		Namespace:  tc.Namespace,
		Datasets:   tc.Datasets,
		RowFilters: tc.RowFilters,
		TTL:        ttl,
	})
	if err != nil {
		return nil, "", err
	}
	s.AuditLogger().RecordEvent(ctx, tc.KeyID, tc.TenantID, "oidc_token_exchange", ak.KeyID, "auth", true, "")
	return ak, raw, nil
}

// AccessValidator returns the active AccessValidator combining RBAC and OIDC
// when enabled, or nil if authentication is disabled.
func (s *VectorStore) AccessValidator() security.AccessValidator {
	rbac := s.RBAC()
	oidc := s.ensureOIDCValidator()
	switch {
	case rbac != nil && oidc != nil:
		return &compositeAccessValidator{rbac: rbac, oidc: oidc}
	case rbac != nil:
		return rbac
	case oidc != nil:
		return oidc
	default:
		return nil
	}
}

// DynamicAccessValidator returns an AccessValidator that resolves the store's
// current RBAC and OIDC managers on every call. Use this when attaching gRPC
// auth interceptors at startup so dynamically added OIDC providers or lazily
// created session-key RBAC managers (from ExchangeOIDCToken) take effect
// immediately without restarting the server.
func (s *VectorStore) DynamicAccessValidator() security.AccessValidator {
	return &dynamicStoreAccessValidator{store: s}
}

type dynamicStoreAccessValidator struct {
	store *VectorStore
}

// AuthorizeToken runs the authorize token operation.
func (d *dynamicStoreAccessValidator) AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*security.TenantContext, error) {
	v := d.store.AccessValidator()
	if v == nil {
		return nil, security.ErrUnauthorized
	}
	return v.AuthorizeToken(ctx, rawToken, permission, namespace, dataset)
}

type compositeAccessValidator struct {
	rbac *RBACManager
	oidc *security.OIDCAccessValidator
}

// AuthorizeToken runs the authorize token operation.
func (c *compositeAccessValidator) AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*security.TenantContext, error) {
	if strings.Count(rawToken, ".") == 2 && c.oidc != nil {
		return c.oidc.AuthorizeToken(ctx, rawToken, permission, namespace, dataset)
	}
	return c.rbac.AuthorizeToken(ctx, rawToken, permission, namespace, dataset)
}
