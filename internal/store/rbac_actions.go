package store

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

// RBACManager returns the active RBACManager (nil if RBAC is disabled).
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
		if !tc.HasPermission(string(perm)) {
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
	if tc == nil || tc.IsAdmin {
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

func permissionAndDatasetForAction(action *flight.Action) (Permission, string) {
	var dsPayload struct {
		Dataset   string `json:"dataset"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	_ = json.Unmarshal(action.Body, &dsPayload)
	target := dsPayload.Dataset
	if target == "" {
		target = dsPayload.Name
	}
	if target == "" && dsPayload.Namespace != "" {
		target = dsPayload.Namespace + "/*"
	}

	switch action.Type {
	case "VectorSearch", "VectorSearchByID", "search", "dense", "sparse", "filtered", "hybrid",
		"HybridSearch", "GeoSearch", "TemporalSearch", "TemporalRangeSearch", "TemporalVersionHistory",
		"TemporalAggregation", "RangeSearch", "FederatedSearch", "ListFederatedCollections",
		"traverse-graph", "GetGraphStats", "calculate-pagerank", "detect-communities",
		"GraphRAGExpand", "ListNamespaces", "ListDatasetsInNamespace", "ExplainSearch", "DebugSearchPath",
		"SearchFacets", "SearchAnalytics", "AdvancedTextSearch", "QueryMaterializedView", "ListMaterializedViews", "GetProvenance",
		"Rerank", "RewriteQuery", "EvaluateReranker", "MultiModalSearch", "ValidateSchema", "GetSchemaRegistry", "GetTenantUsage",
		"ListStreamConnectors", "GetStreamStats", "GetCloudCluster", "ListCloudClusters", "GetCloudSLAReport",
		"GraphQLQuery":
		return PermRead, target
	case "IngestStreamEvent", "MultiModalIngest", "MultiModalIngestParquet", "RecordProvenance", "ReplicateClusterMutation", "RegisterFederatedCollection":
		return PermWrite, target
	case "delete", "Delete", "drop", "Drop", "ResetDataset", "delete-dataset", "delete-vector":
		return PermDelete, target
	case "ForceSnapshot", "TriggerBackup", "RestoreBackup", "ListBackups", "VerifyBackup", "Compact", "TieredOffload":
		return PermBackup, target
	case "CreateAPIKey", "RotateAPIKey", "RevokeAPIKey", "ListAPIKeys":
		return PermManageKeys, target
	case "SetRLSPolicy", "CreateNamespace", "DeleteNamespace", "SetSchemaPolicy", "RegisterSchema",
		"RefreshMaterializedView", "CreateMaterializedView", "SetTenantQuota", "ExportBillingReport",
		"CreateStreamConnector", "StopStreamConnector", "RegisterStreamSchema", "TuneBM25", "MigrateSchema",
		"CreateCloudCluster", "ScaleCloudCluster", "AddReplicaRegion", "PromoteClusterRegion", "DeleteCloudCluster":
		return PermManageTenant, target
	case "GetAuditLog", "ReplicateWAL", "cluster-status", "MeshIdentity", "MeshStatus", "DiscoveryStatus",
		"GetAutoScaleConfig", "SetAutoScaleConfig", "TriggerShardSplit", "TriggerShardMerge",
		"TriggerRebalance", "GetClusterTopology":
		return PermAdmin, ""
	default:
		return PermAdmin, target
	}
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

// OIDCManager returns the active OIDCManager (nil if disabled).
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

type compositeAccessValidator struct {
	rbac *RBACManager
	oidc *security.OIDCAccessValidator
}

func (c *compositeAccessValidator) AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*security.TenantContext, error) {
	if strings.Count(rawToken, ".") == 2 && c.oidc != nil {
		return c.oidc.AuthorizeToken(ctx, rawToken, permission, namespace, dataset)
	}
	return c.rbac.AuthorizeToken(ctx, rawToken, permission, namespace, dataset)
}
