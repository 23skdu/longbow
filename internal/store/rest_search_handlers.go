package store

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/23skdu/longbow/internal/core"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
)

func (s *VectorStore) handleDatasetSubAction(w http.ResponseWriter, r *http.Request, dsName, action string) {
	if !isValidResourceName(dsName) {
		writeRESTError(w, action[1:], r.Method, http.StatusBadRequest, "invalid dataset name")
		return
	}
	allowsGet := action == "/schema" || action == "/provenance"
	if r.Method != http.MethodPost && (!allowsGet || r.Method != http.MethodGet) {
		writeRESTError(w, action[1:], r.Method, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)

	if s.handleDatasetDataSubAction(w, r, dsName, action) {
		return
	}
	switch action {
	case "/search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Vector  []float32     `json:"vector"`
			K       int           `json:"k"`
			Filters []core.Filter `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := s.SearchDatasetVectors(r.Context(), dsName, req.Vector, req.K, req.Filters)
		if err != nil {
			writeRESTError(w, "search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/hybrid":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Vector     []float32     `json:"vector"`
			TextQuery  string        `json:"text_query"`
			K          int           `json:"k"`
			Alpha      float32       `json:"alpha"`
			RRFK       int           `json:"rrf_k"`
			GraphAlpha float32       `json:"graph_alpha"`
			GraphDepth int           `json:"graph_depth"`
			RawHybrid  bool          `json:"raw_hybrid,omitempty"`
			Filters    []core.Filter `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		if req.RRFK <= 0 {
			req.RRFK = 60
		}
		res, err := s.SearchHybridWithFilters(r.Context(), dsName, req.Vector, req.TextQuery, req.K, req.Alpha, req.RRFK, req.GraphAlpha, req.GraphDepth, req.RawHybrid, req.Filters)
		if err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "hybrid", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/search/by-id":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req core.VectorSearchByIDRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.SearchByID(r.Context(), &req)
		if err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "search_by_id", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/text-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req AdvancedTextQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		resp, err := s.GetTextSearchManager().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "text_search", r.Method, http.StatusOK, resp)

	case "/geo-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req types.GeoSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		if req.SearchType == "" {
			req.SearchType = "radius"
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := s.GeoSearch(r.Context(), &req)
		if err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "geo_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/temporal-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			SearchType string          `json:"search_type"`
			K          int             `json:"k"`
			Timestamp  int64           `json:"timestamp,omitempty"`
			StartTime  int64           `json:"start_time,omitempty"`
			EndTime    int64           `json:"end_time,omitempty"`
			WindowSize int             `json:"window_size,omitempty"`
			Duration   json.RawMessage `json:"duration,omitempty"`
			DurationNs int64           `json:"duration_nanos,omitempty"`
			Filters    []core.Filter   `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.SearchType == "" {
			req.SearchType = "as_of"
		}
		if req.K <= 0 {
			req.K = 10
		}
		dur := time.Duration(req.DurationNs)
		if len(req.Duration) > 0 && string(req.Duration) != "null" {
			var durStr string
			var durInt int64
			if uerr := json.Unmarshal(req.Duration, &durStr); uerr == nil {
				parsed, perr := time.ParseDuration(durStr)
				if perr != nil {
					writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, perr.Error())
					return
				}
				dur = parsed
			} else if uerr := json.Unmarshal(req.Duration, &durInt); uerr == nil {
				dur = time.Duration(durInt)
			} else {
				writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, "invalid duration format")
				return
			}
		}
		res, err := s.TemporalSearch(r.Context(), &types.TemporalSearchRequest{
			Dataset:    dsName,
			SearchType: req.SearchType,
			K:          req.K,
			Timestamp:  req.Timestamp,
			StartTime:  req.StartTime,
			EndTime:    req.EndTime,
			WindowSize: req.WindowSize,
			Duration:   dur,
			Filters:    req.Filters,
		})
		if err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "temporal_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/multimodal-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req MultiModalSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.GetMultiModalPipeline().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "multimodal_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/range-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req core.RangeSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.RangeSearch(r.Context(), dsName, req.Vector, req.Threshold, req.Filters, req.MaxResults)
		if err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "range_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res), "threshold": req.Threshold})

	case "/recommend":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			SeedIDs []string `json:"seed_ids"`
			K       int      `json:"k"`
			Alpha   float32  `json:"alpha"`
			MaxHops int      `json:"max_hops"`
			Decay   float32  `json:"decay"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		if req.Alpha == 0 {
			req.Alpha = 0.7
		}
		res, err := s.Recommend(r.Context(), &qry.RecommendRequest{
			Dataset: dsName,
			SeedIDs: req.SeedIDs,
			K:       req.K,
			Alpha:   req.Alpha,
			MaxHops: req.MaxHops,
			Decay:   req.Decay,
		})
		if err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "recommend", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/aggregations":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			GroupBy      []string          `json:"group_by"`
			Aggregations []AggSpec         `json:"aggregations"`
			FilterEquals map[string]string `json:"filter_equals,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		rows, sourceRows, err := s.runDatasetAggregation(dsName, req.GroupBy, req.Aggregations, req.FilterEquals)
		if err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "aggregations", r.Method, http.StatusOK, map[string]any{
			"dataset":     dsName,
			"source_rows": sourceRows,
			"rows":        rows,
		})

	case "/facets":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req AdvancedTextQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		resp, err := s.GetTextSearchManager().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "facets", r.Method, http.StatusOK, map[string]any{"facets": resp.Facets, "total_hits": resp.TotalHits})

	case "/explain":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req ExplainSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		exp, err := s.GetProvenanceManager().ExplainSearch(r.Context(), req)
		if err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "explain", r.Method, http.StatusOK, exp)

	}
}

func (s *VectorStore) federatedSearchHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "federated_search", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req core.FederatedSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "federated_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res, err := s.FederatedSearch(r.Context(), req.Collections, req.Tags, req.Vector, req.K, req.RRFK)
		if err != nil {
			writeRESTError(w, "federated_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "federated_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})
	})
}

func (s *VectorStore) federatedCollectionsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router := s.GetFederatedRouter()
		switch r.Method {
		case http.MethodGet:
			s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
			cols := router.ListCollections()
			visible := make([]*CollectionInfo, 0, len(cols))
			for _, c := range cols {
				if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, c.Name); err == nil {
					visible = append(visible, c)
				}
			}
			writeRESTJSON(w, "federated_collections", r.Method, http.StatusOK, map[string]any{"collections": visible, "count": len(visible)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Dataset     string            `json:"dataset"`
				Name        string            `json:"name"`
				Description string            `json:"description,omitempty"`
				Tags        []string          `json:"tags,omitempty"`
				Metadata    map[string]string `json:"metadata,omitempty"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "federated_collections", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			name := req.Dataset
			if name == "" {
				name = req.Name
			}
			if !isValidResourceName(name) {
				writeRESTError(w, "federated_collections", r.Method, http.StatusBadRequest, "invalid collection name")
				return
			}
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, name); err != nil {
				writeRESTError(w, "federated_collections", r.Method, http.StatusForbidden, err.Error())
				return
			}
			info := &CollectionInfo{Name: name, Description: req.Description, Tags: req.Tags, Metadata: req.Metadata}
			router.UpsertCollection(name, &storeDatasetSearcher{store: s, dataset: name}, info)
			for _, tag := range req.Tags {
				_ = router.RegisterRoutingRule(&RoutingRule{Tag: tag, Collection: name, Priority: 1})
			}
			writeRESTJSON(w, "federated_collections", r.Method, http.StatusOK, map[string]any{"status": "registered", "collection": name, "tags": req.Tags})
		default:
			writeRESTError(w, "federated_collections", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) runDatasetAggregation(dsName string, groupBy []string, aggs []AggSpec, filters map[string]string) ([]MaterializedViewRow, int64, error) {
	if len(aggs) == 0 {
		aggs = []AggSpec{{Alias: "count", Op: AggCount}}
	}
	if len(filters) == 0 {
		if snap, ok := s.GetMaterializedViewManager().TryPlanQuery(dsName, groupBy, aggs); ok && snap != nil {
			return snap.Rows, snap.SourceRows, nil
		}
	}
	ds, ok := s.getDataset(dsName)
	if !ok {
		return nil, 0, NewNotFoundError("dataset", dsName)
	}
	ds.dataMu.RLock()
	batches := ds.Records.Read()
	tombstones := ds.Tombstones
	ds.dataMu.RUnlock()

	rows, sourceRows := AggregateBatches(batches, tombstones, MaterializedViewSpec{
		SourceDataset: dsName,
		GroupBy:       groupBy,
		Aggregations:  aggs,
		FilterEquals:  filters,
	})
	return rows, sourceRows, nil
}
