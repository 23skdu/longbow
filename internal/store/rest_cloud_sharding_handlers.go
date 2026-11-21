package store

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

func (s *VectorStore) rerankHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rerank", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Strategy string         `json:"strategy"`
			Query    string         `json:"query"`
			Results  []SearchResult `json:"results"`
			TopK     int            `json:"top_k"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rerank", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		tenant := resolveTenantIDFromCtx(r.Context(), "")
		res, cacheHit, err := s.GetRerankManager().Rerank(r.Context(), req.Strategy, req.Query, req.Results, tenant)
		if err != nil {
			writeRESTError(w, "rerank", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.TopK > 0 && req.TopK < len(res) {
			res = res[:req.TopK]
		}
		writeRESTJSON(w, "rerank", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res), "cache_hit": cacheHit})
	})
}

func (s *VectorStore) rerankEvaluateHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Strategy string          `json:"strategy"`
			Cases    []EvalQueryCase `json:"cases"`
			K        int             `json:"k"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		var rr Reranker = s.GetRerankManager().hybrid
		if req.Strategy == "heuristic" {
			rr = &HeuristicReranker{ModelName: "default"}
		}
		rep, err := EvaluateReranker(r.Context(), rr, req.Cases, req.K)
		if err != nil {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "rerank_evaluate", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) rewriteQueryHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rewrite_query", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Query string           `json:"query"`
			Mode  QueryRewriteMode `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rewrite_query", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		variants := s.GetRerankManager().Rewrite(req.Query, req.Mode)
		writeRESTJSON(w, "rewrite_query", r.Method, http.StatusOK, map[string]any{"queries": variants})
	})
}

func (s *VectorStore) shardingTopologyHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "sharding_topology", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeRESTJSON(w, "sharding_topology", r.Method, http.StatusOK, s.GetElasticController().TopologySnapshot())
	})
}

func (s *VectorStore) shardingAutoScaleHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeRESTJSON(w, "sharding_autoscale", r.Method, http.StatusOK, s.GetElasticController().Policy())
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var pol ElasticScalingPolicy
			if err := json.NewDecoder(r.Body).Decode(&pol); err != nil {
				writeRESTError(w, "sharding_autoscale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if err := s.GetElasticController().SetPolicy(pol); err != nil {
				writeRESTError(w, "sharding_autoscale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "sharding_autoscale", r.Method, http.StatusOK, map[string]any{"status": "policy_updated"})
		default:
			writeRESTError(w, "sharding_autoscale", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) shardingSplitHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_split", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset string `json:"dataset"`
			ShardID string `json:"shard_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_split", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		left, right, migrated, err := s.GetElasticController().SplitShard(req.Dataset, req.ShardID)
		if err != nil {
			writeRESTError(w, "sharding_split", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_split", r.Method, http.StatusOK, map[string]any{"left": left, "right": right, "migrated": migrated})
	})
}

func (s *VectorStore) shardingMergeHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset      string `json:"dataset"`
			LeftShardID  string `json:"left_shard_id"`
			RightShardID string `json:"right_shard_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		merged, moved, err := s.GetElasticController().MergeShards(req.Dataset, req.LeftShardID, req.RightShardID)
		if err != nil {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_merge", r.Method, http.StatusOK, map[string]any{"merged": merged, "moved": moved})
	})
}

func (s *VectorStore) shardingRebalanceHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		tasks, err := s.GetElasticController().RebalanceShards(req.Dataset)
		if err != nil {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_rebalance", r.Method, http.StatusOK, map[string]any{"tasks": tasks, "count": len(tasks)})
	})
}

func (s *VectorStore) viewsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			views := s.GetMaterializedViewManager().ListViews()
			writeRESTJSON(w, "views", r.Method, http.StatusOK, map[string]any{"views": views, "count": len(views)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var spec MaterializedViewSpec
			if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
				writeRESTError(w, "views", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			snap, err := s.GetMaterializedViewManager().CreateView(r.Context(), spec)
			if err != nil {
				writeRESTError(w, "views", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "views", r.Method, http.StatusCreated, snap)
		default:
			writeRESTError(w, "views", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) viewsQueryHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "views_query", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Name        string            `json:"name"`
			GroupFilter map[string]string `json:"group_filter,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "views_query", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		snap, err := s.GetMaterializedViewManager().QueryView(req.Name, req.GroupFilter)
		if err != nil {
			writeRESTError(w, "views_query", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "views_query", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) viewsRefreshHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "views_refresh", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "views_refresh", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		snap, err := s.GetMaterializedViewManager().RefreshView(r.Context(), req.Name)
		if err != nil {
			writeRESTError(w, "views_refresh", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "views_refresh", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) searchAnalyticsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "search_analytics", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		snap := s.GetTextSearchManager().Analytics(limit)
		writeRESTJSON(w, "search_analytics", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) cloudClustersCollectionHTTPHandler() http.Handler {
	ctrl := s.GetCloudControlPlane()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusForbidden, err.Error())
				return
			}
			tenantID := r.URL.Query().Get("tenant_id")
			clusters := ctrl.ListClusters(tenantID)
			writeRESTJSON(w, "cloud_clusters", r.Method, http.StatusOK, map[string]any{"clusters": clusters, "count": len(clusters)})
		case http.MethodPost:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusForbidden, err.Error())
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req CreateCloudClusterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.CreateCluster(req)
			if err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_clusters", r.Method, http.StatusCreated, cl)
		default:
			writeRESTError(w, "cloud_clusters", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) cloudClusterItemHTTPHandler() http.Handler {
	ctrl := s.GetCloudControlPlane()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/cloud/clusters/"), "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusBadRequest, "missing cluster id")
			return
		}
		clusterID := parts[0]
		sub := ""
		if len(parts) > 1 {
			sub = parts[1]
		}

		switch sub {
		case "":
			switch r.Method {
			case http.MethodGet:
				if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusForbidden, err.Error())
					return
				}
				cl, ok := ctrl.GetCluster(clusterID)
				if !ok {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, "cluster not found")
					return
				}
				writeRESTJSON(w, "cloud_cluster_item", r.Method, http.StatusOK, cl)
			case http.MethodDelete:
				if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusForbidden, err.Error())
					return
				}
				if err := ctrl.DeleteCluster(clusterID); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, err.Error())
					return
				}
				writeRESTJSON(w, "cloud_cluster_item", r.Method, http.StatusOK, map[string]any{"deleted": clusterID})
			default:
				writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			}

		case "scale":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				Nodes int       `json:"nodes"`
				Tier  CloudTier `json:"tier"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.ScaleCluster(clusterID, req.Nodes, req.Tier)
			if err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_scale", r.Method, http.StatusOK, cl)

		case "regions":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				Region   string `json:"region"`
				Endpoint string `json:"endpoint"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.AddReplicaRegion(clusterID, req.Region, req.Endpoint)
			if err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_regions", r.Method, http.StatusOK, cl)

		case "replicate":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var ev ReplicationEvent
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&ev); err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			n, err := ctrl.ReplicateMutation(r.Context(), clusterID, ev)
			if err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_replicate", r.Method, http.StatusOK, map[string]any{"replicated_regions": n})

		case "promote":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				TargetRegion string `json:"target_region"`
				Reason       string `json:"reason"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.PromoteRegion(r.Context(), clusterID, req.TargetRegion, req.Reason)
			if err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_promote", r.Method, http.StatusOK, cl)

		case "sla":
			if r.Method != http.MethodGet {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var p99 float64
			if v := r.URL.Query().Get("p99_ms"); v != "" {
				p99, _ = strconv.ParseFloat(v, 64)
			}
			rep, err := ctrl.EvaluateSLA(clusterID, p99)
			if err != nil {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_sla", r.Method, http.StatusOK, rep)

		default:
			writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, "unknown cloud sub-resource")
		}
	})
}

func (s *VectorStore) cloudInternalReplicateHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var ev ReplicationEvent
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&ev); err != nil {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.GetCloudControlPlane().ApplyIncomingReplication(r.Context(), ev); err != nil {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "cloud_internal_replicate", r.Method, http.StatusOK, map[string]any{"status": "applied", "lsn": ev.LSN})
	})
}
