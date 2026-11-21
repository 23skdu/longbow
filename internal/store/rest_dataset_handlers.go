package store

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

func (s *VectorStore) namespacesHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusForbidden, err.Error())
				return
			}
			names := s.ListNamespaces()
			sort.Strings(names)
			writeRESTJSON(w, "namespaces", r.Method, http.StatusOK, map[string]any{
				"namespaces": names,
				"count":      len(names),
			})
		case http.MethodPost:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusForbidden, err.Error())
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if err := s.CreateNamespace(req.Name); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "namespaces", r.Method, http.StatusCreated, map[string]any{"name": req.Name, "status": "created"})
		default:
			writeRESTError(w, "namespaces", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) namespaceItemHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/")
		name, _ = url.PathUnescape(strings.Trim(name, "/"))
		if !isValidResourceName(name) || strings.Contains(name, "/") {
			writeRESTError(w, "namespace_item", r.Method, http.StatusBadRequest, "invalid namespace name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if !s.NamespaceExists(name) {
				writeRESTError(w, "namespace_item", r.Method, http.StatusNotFound, "namespace not found")
				return
			}
			writeRESTJSON(w, "namespace_item", r.Method, http.StatusOK, map[string]any{
				"name":     name,
				"datasets": s.ListDatasetsInNamespace(name),
			})
		case http.MethodDelete:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if err := s.DeleteNamespace(name); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "namespace_item", r.Method, http.StatusOK, map[string]any{"name": name, "status": "deleted"})
		default:
			writeRESTError(w, "namespace_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) datasetsCollectionHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusForbidden, err.Error())
				return
			}
			list := s.listDatasetInfos(r.Context())
			writeRESTJSON(w, "datasets", r.Method, http.StatusOK, map[string]any{"datasets": list, "count": len(list)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name    string              `json:"name"`
				Vectors []StreamVectorEvent `json:"vectors,omitempty"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if !isValidResourceName(req.Name) {
				writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, "invalid dataset name")
				return
			}
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, req.Name); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusForbidden, err.Error())
				return
			}
			inserted := 0
			if len(req.Vectors) > 0 {
				var err error
				inserted, err = s.UpsertStreamEvents(r.Context(), req.Name, req.Vectors)
				if err != nil {
					writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, err.Error())
					return
				}
			} else {
				_, _ = s.getOrCreateDataset(req.Name, func() *Dataset {
					d := NewDataset(req.Name, nil)
					d.Logger = s.logger
					return d
				})
			}
			writeRESTJSON(w, "datasets", r.Method, http.StatusCreated, map[string]any{
				"name":     req.Name,
				"inserted": inserted,
				"status":   "created",
			})
		default:
			writeRESTError(w, "datasets", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) datasetItemHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawPath := strings.TrimPrefix(r.URL.Path, "/api/v1/datasets/")
		rawPath = strings.TrimSuffix(rawPath, "/")
		if rawPath == "" {
			writeRESTError(w, "dataset_item", r.Method, http.StatusBadRequest, "missing dataset path")
			return
		}

		// Handle /api/v1/datasets/{name}/vectors/{id}
		if idx := strings.Index(rawPath, "/vectors/"); idx != -1 {
			dsName, _ := url.PathUnescape(rawPath[:idx])
			vecID, _ := url.PathUnescape(rawPath[idx+len("/vectors/"):])
			s.handleDatasetVectorByID(w, r, dsName, vecID)
			return
		}

		for _, suffix := range []string{
			"/vectors",
			"/search/by-id",
			"/search",
			"/hybrid",
			"/text-search",
			"/geo-search",
			"/temporal-search",
			"/multimodal-search",
			"/multimodal-ingest",
			"/range-search",
			"/recommend",
			"/aggregations",
			"/bm25",
			"/facets",
			"/explain",
			"/provenance",
			"/schema/migrate",
			"/schema",
		} {
			if strings.HasSuffix(rawPath, suffix) {
				dsName, _ := url.PathUnescape(strings.TrimSuffix(rawPath, suffix))
				s.handleDatasetSubAction(w, r, dsName, suffix)
				return
			}
		}

		dsName, _ := url.PathUnescape(rawPath)
		if !isValidResourceName(dsName) {
			writeRESTError(w, "dataset_item", r.Method, http.StatusBadRequest, "invalid dataset name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			info, err := s.inspectDataset(dsName)
			if err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "dataset_item", r.Method, http.StatusOK, info)
		case http.MethodDelete:
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if err := s.DropDataset(r.Context(), dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "dataset_item", r.Method, http.StatusOK, map[string]any{"name": dsName, "status": "deleted"})
		default:
			writeRESTError(w, "dataset_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) handleDatasetVectorByID(w http.ResponseWriter, r *http.Request, dsName, vecID string) {
	if !isValidResourceName(dsName) || vecID == "" {
		writeRESTError(w, "vector_item", r.Method, http.StatusBadRequest, "invalid dataset or vector id")
		return
	}
	switch r.Method {
	case http.MethodGet:
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusForbidden, err.Error())
			return
		}
		vec, err := s.GetVectorByID(dsName, vecID)
		if err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "vector_item", r.Method, http.StatusOK, vec)
	case http.MethodDelete:
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusForbidden, err.Error())
			return
		}
		if err := s.DeleteVectorByID(r.Context(), dsName, vecID); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "vector_item", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "id": vecID, "status": "deleted"})
	default:
		writeRESTError(w, "vector_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleDatasetDataSubAction handles dataset mutation, schema, BM25, and provenance sub-actions.
func (s *VectorStore) handleDatasetDataSubAction(w http.ResponseWriter, r *http.Request, dsName, action string) bool {
	switch action {
	case "/vectors":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var req struct {
			Vectors []StreamVectorEvent `json:"vectors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		n, err := s.UpsertStreamEvents(r.Context(), dsName, req.Vectors)
		if err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "vectors_upsert", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "upserted": n})

	case "/multimodal-ingest":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var req struct {
			Items       []MultiModalItem `json:"items,omitempty"`
			ParquetPath string           `json:"parquet_path,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		var n int
		var err error
		if req.ParquetPath != "" {
			n, err = s.GetMultiModalPipeline().IngestParquetFile(r.Context(), dsName, req.ParquetPath)
		} else {
			n, err = s.GetMultiModalPipeline().IngestItems(r.Context(), dsName, req.Items)
		}
		if err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "multimodal_ingest", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "ingested": n})

	case "/bm25":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var req struct {
			K1 float64 `json:"k1"`
			B  float64 `json:"b"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		if err := s.GetTextSearchManager().SetBM25Params(dsName, BM25TuningParams{K1: req.K1, B: req.B}); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "bm25", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "status": "bm25_tuned", "k1": req.K1, "b": req.B})

	case "/provenance":
		if r.Method == http.MethodGet {
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "provenance", r.Method, http.StatusForbidden, err.Error())
				return true
			}
			vecID := r.URL.Query().Get("vector_id")
			if vecID != "" {
				prov, ok := s.GetProvenanceManager().Get(dsName, vecID)
				if !ok {
					writeRESTError(w, "provenance", r.Method, http.StatusNotFound, "provenance not found")
					return true
				}
				writeRESTJSON(w, "provenance", r.Method, http.StatusOK, prov)
				return true
			}
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			list := s.GetProvenanceManager().ListByDataset(dsName, limit)
			writeRESTJSON(w, "provenance", r.Method, http.StatusOK, map[string]any{"provenance": list, "count": len(list)})
			return true
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var prov VectorProvenance
		if err := json.NewDecoder(r.Body).Decode(&prov); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		prov.Dataset = dsName
		if err := s.GetProvenanceManager().Record(prov); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "provenance", r.Method, http.StatusOK, map[string]any{"status": "recorded", "dataset": dsName, "vector_id": prov.VectorID})

	case "/schema":
		if r.Method == http.MethodGet {
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "schema", r.Method, http.StatusForbidden, err.Error())
				return true
			}
			spec, ok := s.GetSchemaRegistry().LatestSchema(dsName)
			if !ok {
				writeRESTError(w, "schema", r.Method, http.StatusNotFound, "no registered schema")
				return true
			}
			writeRESTJSON(w, "schema", r.Method, http.StatusOK, spec)
			return true
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var spec DatasetSchemaSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		spec.Dataset = dsName
		ver, err := s.GetSchemaRegistry().RegisterSchema(spec)
		if err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "schema", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})

	case "/schema/migrate":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusForbidden, err.Error())
			return true
		}
		var req struct {
			Target        DatasetSchemaSpec `json:"target"`
			AddStringCols map[string]string `json:"add_string_cols,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		req.Target.Dataset = dsName
		ds, ok := s.getDataset(dsName)
		if !ok {
			ver, err := s.GetSchemaRegistry().RegisterSchema(req.Target)
			if err != nil {
				writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
				return true
			}
			writeRESTJSON(w, "schema_migrate", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})
			return true
		}
		ver, err := s.GetSchemaRegistry().MigrateDatasetSchema(ds, req.Target, req.AddStringCols)
		if err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
			return true
		}
		writeRESTJSON(w, "schema_migrate", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})
	default:
		return false
	}
	return true
}
