package store

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/security"
)

func (s *VectorStore) adminUsageHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "admin_usage", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		reports := s.GetUsageMeter().GetAllUsage()
		var totalQueries, totalVectors, totalStorage, totalEgress, totalWarnings, totalRejections int64
		var totalCost float64
		for _, rep := range reports {
			totalQueries += rep.Queries
			totalVectors += rep.VectorsIngested
			totalStorage += rep.CurrentStorageBytes
			totalEgress += rep.EgressBytes
			totalWarnings += rep.SoftWarnings
			totalRejections += rep.HardRejections
			totalCost += rep.EstimatedCost
		}
		writeRESTJSON(w, "admin_usage", r.Method, http.StatusOK, map[string]any{
			"tenant_count": len(reports),
			"summary": map[string]any{
				"total_queries":         totalQueries,
				"total_vectors":         totalVectors,
				"total_storage_bytes":   totalStorage,
				"total_egress_bytes":    totalEgress,
				"soft_quota_warnings":   totalWarnings,
				"hard_quota_rejections": totalRejections,
				"estimated_cost_usd":    totalCost,
			},
			"tenants":      reports,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	})
}

func (s *VectorStore) rbacKeysHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mgr := s.ensureRBACManager()
		switch r.Method {
		case http.MethodGet:
			keys := mgr.ListAPIKeys()
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusOK, map[string]any{"keys": keys, "count": len(keys)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name       string            `json:"name"`
				Role       string            `json:"role"`
				TenantID   string            `json:"tenant_id"`
				Namespace  string            `json:"namespace"`
				Datasets   []string          `json:"datasets"`
				RowFilters map[string]string `json:"row_filters"`
				TTLSeconds int64             `json:"ttl_seconds"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "rbac_keys", r.Method, http.StatusBadRequest, err.Error())
				return
			}
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
				writeRESTError(w, "rbac_keys", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusCreated, map[string]any{
				"key_id":     ak.KeyID,
				"api_key":    raw,
				"role":       ak.Role,
				"tenant_id":  ak.TenantID,
				"namespace":  ak.Namespace,
				"expires_at": ak.ExpiresAt,
			})
		case http.MethodDelete:
			keyID := r.URL.Query().Get("key_id")
			if keyID == "" {
				keyID = r.URL.Query().Get("api_key")
			}
			if err := mgr.RevokeAPIKey(keyID); err != nil {
				writeRESTError(w, "rbac_keys", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusOK, map[string]any{"status": "revoked"})
		default:
			writeRESTError(w, "rbac_keys", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) rbacRLSHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var pol security.RLSPolicy
		if err := json.NewDecoder(r.Body).Decode(&pol); err != nil {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.ensureRBACManager().RLSEvaluator().SetPolicy(pol); err != nil {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "rbac_rls", r.Method, http.StatusOK, map[string]any{"status": "rls_policy_set"})
	})
}

func (s *VectorStore) rbacAuditHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "rbac_audit", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 100
		}
		entries := s.AuditLogger().RecentEntries(limit)
		writeRESTJSON(w, "rbac_audit", r.Method, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
	})
}

func (s *VectorStore) oidcProvidersHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			issuers := s.OIDC().ListIssuers()
			writeRESTJSON(w, "oidc_providers", r.Method, http.StatusOK, map[string]any{"providers": issuers, "count": len(issuers)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var cfg security.OIDCIssuerConfig
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				writeRESTError(w, "oidc_providers", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if err := s.ensureOIDCManager().AddIssuer(cfg); err != nil {
				writeRESTError(w, "oidc_providers", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			s.AuditLogger().RecordEvent(r.Context(), "", "", "add_oidc_provider", cfg.IssuerURL, r.RemoteAddr, true, "")
			writeRESTJSON(w, "oidc_providers", r.Method, http.StatusCreated, map[string]any{"status": "added", "issuer_url": cfg.IssuerURL})
		case http.MethodDelete:
			issuerURL := r.URL.Query().Get("issuer_url")
			if issuerURL == "" {
				writeRESTError(w, "oidc_providers", r.Method, http.StatusBadRequest, "issuer_url query parameter is required")
				return
			}
			if !s.OIDC().RemoveIssuer(issuerURL) {
				writeRESTError(w, "oidc_providers", r.Method, http.StatusNotFound, "provider not found")
				return
			}
			s.AuditLogger().RecordEvent(r.Context(), "", "", "remove_oidc_provider", issuerURL, r.RemoteAddr, true, "")
			writeRESTJSON(w, "oidc_providers", r.Method, http.StatusOK, map[string]any{"status": "removed", "issuer_url": issuerURL})
		default:
			writeRESTError(w, "oidc_providers", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) oidcTokenExchangeHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "oidc_token", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Token      string `json:"token"`
			TTLSeconds int64  `json:"ttl_seconds"`
		}
		if r.Body != nil && r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "oidc_token", r.Method, http.StatusBadRequest, err.Error())
				return
			}
		}
		rawToken := strings.TrimSpace(req.Token)
		if rawToken == "" {
			if authz := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(authz), "bearer ") {
				rawToken = strings.TrimSpace(authz[7:])
			}
		}
		if rawToken == "" {
			writeRESTError(w, "oidc_token", r.Method, http.StatusUnauthorized, "missing OIDC token")
			return
		}
		ak, raw, err := s.ExchangeOIDCToken(r.Context(), rawToken, time.Duration(req.TTLSeconds)*time.Second)
		if err != nil {
			writeRESTError(w, "oidc_token", r.Method, http.StatusUnauthorized, err.Error())
			return
		}
		writeRESTJSON(w, "oidc_token", r.Method, http.StatusOK, map[string]any{
			"key_id":     ak.KeyID,
			"token":      raw,
			"role":       ak.Role,
			"tenant_id":  ak.TenantID,
			"namespace":  ak.Namespace,
			"expires_at": ak.ExpiresAt,
		})
	})
}

func (s *VectorStore) backupsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			ds := r.URL.Query().Get("dataset")
			if s.backupManager == nil {
				writeRESTJSON(w, "backups", r.Method, http.StatusOK, map[string]any{"backups": []any{}, "count": 0})
				return
			}
			backups := s.backupManager.ListBackups(ds)
			writeRESTJSON(w, "backups", r.Method, http.StatusOK, map[string]any{"backups": backups, "count": len(backups)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Dataset     string `json:"dataset"`
				Destination string `json:"destination"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "backups", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			meta, err := s.CreateOnlineBackup(r.Context(), req.Dataset, req.Destination)
			if err != nil {
				writeRESTError(w, "backups", r.Method, http.StatusInternalServerError, err.Error())
				return
			}
			writeRESTJSON(w, "backups", r.Method, http.StatusCreated, meta)
		default:
			writeRESTError(w, "backups", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) backupsRestoreHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "backups_restore", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			BackupID      string `json:"backup_id"`
			Dataset       string `json:"dataset"`
			WALDir        string `json:"wal_dir"`
			PITRTimestamp int64  `json:"pitr_timestamp"`
			MaxSeq        uint64 `json:"max_seq"`
			VerifyOnly    bool   `json:"verify_only"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "backups_restore", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		var targetTime time.Time
		if req.PITRTimestamp > 0 {
			targetTime = time.Unix(0, req.PITRTimestamp)
		}
		rep, err := s.RestoreWithPITR(r.Context(), PITRConfig{
			BackupID:     req.BackupID,
			DatasetName:  req.Dataset,
			WALDir:       req.WALDir,
			TargetTime:   targetTime,
			TargetMaxSeq: req.MaxSeq,
			VerifyOnly:   req.VerifyOnly,
		})
		if err != nil {
			writeRESTError(w, "backups_restore", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "backups_restore", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) backupsVerifyHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "backups_verify", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			BackupID string `json:"backup_id"`
			Dataset  string `json:"dataset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "backups_verify", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		rep, err := s.RestoreWithPITR(r.Context(), PITRConfig{
			BackupID:    req.BackupID,
			DatasetName: req.Dataset,
			VerifyOnly:  true,
		})
		if err != nil {
			writeRESTError(w, "backups_verify", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "backups_verify", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) streamsConnectorsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			stats := s.GetStreamManager().ListStats()
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusOK, map[string]any{"connectors": stats, "count": len(stats)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				StreamConnectorConfig
				Start bool `json:"start"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			conn, err := s.GetStreamManager().CreateConnector(req.StreamConnectorConfig, req.Start)
			if err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusCreated, conn.Stats())
		case http.MethodDelete:
			name := r.URL.Query().Get("name")
			if name == "" {
				var req struct {
					Name string `json:"name"`
				}
				_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req)
				name = req.Name
			}
			if err := s.GetStreamManager().StopConnector(name); err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusOK, map[string]any{"status": "stopped", "name": name})
		default:
			writeRESTError(w, "stream_connectors", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) streamsIngestHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Connector string           `json:"connector"`
			Envelopes []StreamEnvelope `json:"envelopes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		conn, ok := s.GetStreamManager().GetConnector(req.Connector)
		if !ok {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusNotFound, fmt.Sprintf("connector %q not found", req.Connector))
			return
		}
		committed, deduped, err := conn.ProcessEnvelopes(r.Context(), req.Envelopes)
		if err != nil {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "stream_ingest", r.Method, http.StatusOK, map[string]any{
			"committed":    committed,
			"deduplicated": deduped,
			"stats":        conn.Stats(),
		})
	})
}

func (s *VectorStore) streamsSchemasHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "stream_schemas", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req RegisteredStreamSchema
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "stream_schemas", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		id := s.GetStreamManager().Registry().RegisterLocal(req.Subject, req)
		writeRESTJSON(w, "stream_schemas", r.Method, http.StatusCreated, map[string]any{"schema_id": id, "subject": req.Subject})
	})
}
