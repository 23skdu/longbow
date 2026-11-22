package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
)

// ExecuteGraphQL parses and runs a GraphQL query/mutation against VectorStore.
func (s *VectorStore) ExecuteGraphQL(ctx context.Context, query string, variables map[string]any, opName string) map[string]any {
	ops, err := parseGraphQL(query)
	if err != nil {
		metrics.GraphQLQueriesTotal.WithLabelValues("parse", "error").Inc()
		return map[string]any{"errors": []map[string]any{{"message": err.Error()}}}
	}

	selected := ops[0]
	if opName != "" {
		found := false
		for _, op := range ops {
			if op.name == opName {
				selected = op
				found = true
				break
			}
		}
		if !found {
			metrics.GraphQLQueriesTotal.WithLabelValues(selected.opType, "error").Inc()
			return map[string]any{"errors": []map[string]any{{"message": fmt.Sprintf("operation %q not found", opName)}}}
		}
	}

	mergedVars := make(map[string]any, len(selected.defaults)+len(variables))
	for k, v := range selected.defaults {
		mergedVars[k] = v
	}
	for k, v := range variables {
		mergedVars[k] = v
	}

	data := make(map[string]any, len(selected.sels))
	var gqlErrs []map[string]any

	for _, sel := range selected.sels {
		outKey := sel.name
		if sel.alias != "" {
			outKey = sel.alias
		}
		args := resolveGQLArgs(sel.args, mergedVars)
		var val any
		var execErr error
		if selected.opType == "mutation" {
			val, execErr = s.execGQLMutationField(ctx, sel.name, args)
		} else {
			val, execErr = s.execGQLQueryField(ctx, sel.name, args)
		}
		if execErr != nil {
			gqlErrs = append(gqlErrs, map[string]any{
				"message": execErr.Error(),
				"path":    []string{outKey},
			})
			data[outKey] = nil
			continue
		}
		data[outKey] = projectSelectionSet(val, sel.sels)
	}

	statusLabel := "success"
	if len(gqlErrs) > 0 {
		statusLabel = "error"
	}
	metrics.GraphQLQueriesTotal.WithLabelValues(selected.opType, statusLabel).Inc()
	resp := map[string]any{"data": data}
	if len(gqlErrs) > 0 {
		resp["errors"] = gqlErrs
	}
	return resp
}

func (s *VectorStore) execGQLQueryField(ctx context.Context, field string, args map[string]any) (any, error) {
	switch field {
	case "__schema":
		return graphqlSchemaIntrospection(), nil
	case "__type":
		typeName := argString(args, "name")
		schema := graphqlSchemaIntrospection()
		for _, t := range schema["types"].([]map[string]any) {
			if t["name"] == typeName {
				return t, nil
			}
		}
		return nil, nil
	case "health":
		return map[string]any{
			"status":         "ok",
			"activeDatasets": len(s.GetActiveDatasets()),
			"namespaces":     len(s.ListNamespaces()),
			"cloudClusters":  len(s.GetCloudControlPlane().ListClusters("")),
		}, nil
	case "namespaces":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		names := s.ListNamespaces()
		sort.Strings(names)
		return names, nil
	case "datasets":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.listDatasetInfos(ctx), nil
	case "dataset":
		name := argString(args, "name")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, name); err != nil {
			return nil, err
		}
		return s.inspectDataset(name)
	case "search":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		vec := argFloat64Slice(args["vector"])
		k := argInt(args, "k", 10)
		return s.SearchDatasetVectors(ctx, ds, vec, k, nil)
	case "searchById":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.SearchByID(ctx, &core.VectorSearchByIDRequest{
			Dataset: ds,
			ID:      argString(args, "id"),
			K:       argInt(args, "k", 10),
		})
	case "hybridSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		vec := argFloat64Slice(args["vector"])
		textQ := argString(args, "text")
		k := argInt(args, "k", 10)
		alpha := argFloat32(args, "alpha", 0.5)
		rrfK := argInt(args, "rrfK", 60)
		res, err := s.SearchHybridWithFilters(ctx, ds, vec, textQ, k, alpha, rrfK, argFloat32(args, "graphAlpha", 0), argInt(args, "graphDepth", 0), false, nil)
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "textSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.GetTextSearchManager().Search(ctx, AdvancedTextQueryRequest{
			Dataset:      ds,
			Query:        argString(args, "query"),
			K:            argInt(args, "k", 10),
			FuzzyMaxDist: argInt(args, "fuzzyMaxDist", 0),
			FacetFields:  argStringSlice(args["facetFields"]),
		})
	case "geoSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		searchType := argString(args, "searchType")
		if searchType == "" {
			searchType = "radius"
		}
		var box *core.GeoBoundingBox
		if argFloat32(args, "maxLat", 0) != 0 || argFloat32(args, "minLat", 0) != 0 {
			box = &core.GeoBoundingBox{
				MinLat: float64(argFloat32(args, "minLat", 0)),
				MaxLat: float64(argFloat32(args, "maxLat", 0)),
				MinLon: float64(argFloat32(args, "minLon", 0)),
				MaxLon: float64(argFloat32(args, "maxLon", 0)),
			}
		}
		return s.GeoSearch(ctx, &types.GeoSearchRequest{
			Dataset:     ds,
			Center:      core.GeoPoint{Lat: float64(argFloat32(args, "lat", 0)), Lon: float64(argFloat32(args, "lon", 0))},
			RadiusKm:    float64(argFloat32(args, "radiusKm", 10)),
			Box:         box,
			K:           argInt(args, "k", 10),
			SearchType:  searchType,
			QueryVector: argFloat64Slice(args["vector"]),
		})
	case "temporalSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		searchType := argString(args, "searchType")
		if searchType == "" {
			searchType = "as_of"
		}
		return s.TemporalSearch(ctx, &types.TemporalSearchRequest{
			Dataset:    ds,
			SearchType: searchType,
			K:          argInt(args, "k", 10),
			Timestamp:  int64(argInt(args, "timestamp", 0)),
			StartTime:  int64(argInt(args, "startTime", 0)),
			EndTime:    int64(argInt(args, "endTime", 0)),
			WindowSize: argInt(args, "windowSize", 0),
		})
	case "multimodalSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		res, err := s.GetMultiModalPipeline().Search(ctx, MultiModalSearchRequest{
			Dataset:   ds,
			Modality:  MediaModality(argString(args, "modality")),
			TextQuery: argString(args, "textQuery"),
			K:         argInt(args, "k", 10),
		})
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "rangeSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.RangeSearch(ctx, ds, argFloat64Slice(args["vector"]), argFloat32(args, "threshold", 0.5), nil, argInt(args, "maxResults", 0))
	case "federatedSearch":
		return s.FederatedSearch(ctx, argStringSlice(args["collections"]), argStringSlice(args["tags"]), argFloat64Slice(args["vector"]), argInt(args, "k", 10), argInt(args, "rrfK", 0))
	case "federatedCollections":
		router := s.GetFederatedRouter()
		s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
		cols := router.ListCollections()
		visible := make([]*CollectionInfo, 0, len(cols))
		for _, c := range cols {
			if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, c.Name); err == nil {
				visible = append(visible, c)
			}
		}
		return visible, nil
	case "recommend":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		res, err := s.Recommend(ctx, &qry.RecommendRequest{
			Dataset: ds,
			SeedIDs: argStringSlice(args["seedIds"]),
			K:       argInt(args, "k", 10),
			Alpha:   argFloat32(args, "alpha", 0.5),
			MaxHops: argInt(args, "maxHops", 2),
			Decay:   argFloat32(args, "decay", 0.5),
		})
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "aggregate":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		var groupBy []string
		if rawGB, ok := args["groupBy"].([]any); ok {
			for _, it := range rawGB {
				groupBy = append(groupBy, fmt.Sprint(it))
			}
		}
		rows, _, err := s.runDatasetAggregation(ds, groupBy, nil, nil)
		return rows, err
	case "usage":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		if t := argString(args, "tenant"); t != "" {
			return []TenantUsageReport{s.GetUsageMeter().GetTenantUsage(t)}, nil
		}
		return s.GetUsageMeter().GetAllUsage(), nil
	case "cloudClusters":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().ListClusters(argString(args, "tenant")), nil
	case "cloudSla":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().EvaluateSLA(argString(args, "clusterId"), float64(argFloat32(args, "observedP99Ms", 0)))
	case "explainSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.GetProvenanceManager().ExplainSearch(ctx, ExplainSearchRequest{
			Dataset:   ds,
			Vector:    argFloat64Slice(args["vector"]),
			TextQuery: argString(args, "textQuery"),
			K:         argInt(args, "k", 10),
			Alpha:     float64(argFloat32(args, "alpha", 0.5)),
		})
	case "provenance":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		if vecID := argString(args, "vectorId"); vecID != "" {
			prov, ok := s.GetProvenanceManager().Get(ds, vecID)
			if !ok {
				return []VectorProvenance{}, nil
			}
			return []VectorProvenance{prov}, nil
		}
		return s.GetProvenanceManager().ListByDataset(ds, argInt(args, "limit", 50)), nil
	case "searchAnalytics":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetTextSearchManager().Analytics(argInt(args, "limit", 20)), nil
	case "materializedViews":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		if name := argString(args, "name"); name != "" {
			snap, err := s.GetMaterializedViewManager().QueryView(name, nil)
			if err != nil {
				return nil, err
			}
			return []*MaterializedViewSnapshot{snap}, nil
		}
		return s.GetMaterializedViewManager().ListViews(), nil
	case "schemaRegistry":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		spec, ok := s.GetSchemaRegistry().LatestSchema(ds)
		if !ok {
			return nil, NewNotFoundError("schema", ds)
		}
		return spec, nil
	case "clusterTopology":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetElasticController().TopologySnapshot(), nil
	case "streamConnectors":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetStreamManager().ListStats(), nil
	case "backups":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		if s.backupManager == nil {
			return []any{}, nil
		}
		return s.backupManager.ListBackups(argString(args, "dataset")), nil
	case "rewriteQuery":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		variants := s.GetRerankManager().Rewrite(argString(args, "query"), QueryRewriteMode(argString(args, "mode")))
		return map[string]any{"queries": variants}, nil
	default:
		return nil, fmt.Errorf("unknown GraphQL query field %q", field)
	}
}

func (s *VectorStore) execGQLMutationField(ctx context.Context, field string, args map[string]any) (any, error) {
	switch field {
	case "createNamespace":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.CreateNamespaceWithPolicy(ctx, name, NamespacePolicy{}); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "created"}, nil
	case "deleteNamespace":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.DeleteNamespace(name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "deleted"}, nil
	case "upsertVectors":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		rawItems, ok := args["vectors"].([]any)
		if !ok {
			return nil, errors.New("vectors array is required")
		}
		events := make([]StreamVectorEvent, 0, len(rawItems))
		for _, raw := range rawItems {
			m, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			var meta map[string]any
			if rm, ok := m["metadata"].(map[string]any); ok {
				meta = rm
			}
			textVal, _ := m["text"].(string)
			events = append(events, StreamVectorEvent{
				Op:       "upsert",
				ID:       fmt.Sprint(m["id"]),
				Vector:   argFloat64Slice(m["vector"]),
				Text:     textVal,
				Metadata: meta,
			})
		}
		n, err := s.UpsertStreamEvents(ctx, ds, events)
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "upserted": n}, nil
	case "deleteDataset":
		name := argString(args, "name")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, name); err != nil {
			return nil, err
		}
		if err := s.DropDataset(ctx, name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "deleted"}, nil
	case "deleteVector":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		id := argString(args, "id")
		if err := s.DeleteVectorByID(ctx, ds, id); err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "id": id, "status": "deleted"}, nil
	case "createCloudCluster":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().CreateCluster(CreateCloudClusterRequest{
			ID:            argString(args, "id"),
			Name:          argString(args, "name"),
			TenantID:      argString(args, "tenantId"),
			Tier:          CloudTier(argString(args, "tier")),
			PrimaryRegion: argString(args, "primaryRegion"),
			Nodes:         argInt(args, "nodes", 1),
		})
	case "promoteCloudRegion":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().PromoteRegion(ctx, argString(args, "clusterId"), argString(args, "targetRegion"), argString(args, "reason"))
	case "recordProvenance":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		prov := VectorProvenance{
			Dataset:      ds,
			VectorID:     argString(args, "vectorId"),
			SourceURI:    argString(args, "sourceUri"),
			ChunkIndex:   argInt(args, "chunkIndex", 0),
			Model:        argString(args, "model"),
			ModelVersion: argString(args, "modelVersion"),
		}
		if err := s.GetProvenanceManager().Record(prov); err != nil {
			return nil, err
		}
		return map[string]any{"status": "recorded", "dataset": ds, "vectorId": prov.VectorID}, nil
	case "createMaterializedView":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetMaterializedViewManager().CreateView(ctx, MaterializedViewSpec{
			Name:          argString(args, "name"),
			SourceDataset: argString(args, "sourceDataset"),
			GroupBy:       argStringSlice(args["groupBy"]),
			Aggregations:  []AggSpec{{Alias: "count", Op: AggCount}},
			RefreshPolicy: ViewRefreshPolicy(argString(args, "refreshMode")),
		})
	case "refreshMaterializedView":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetMaterializedViewManager().RefreshView(ctx, argString(args, "name"))
	case "registerSchema":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageTenant, ds); err != nil {
			return nil, err
		}
		ver, err := s.GetSchemaRegistry().RegisterSchema(DatasetSchemaSpec{
			Dataset:       ds,
			Compatibility: SchemaCompatibilityMode(argString(args, "compatibility")),
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "version": ver}, nil
	case "triggerBackup":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		return s.CreateOnlineBackup(ctx, argString(args, "dataset"), argString(args, "destination"))
	case "restoreBackup":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		return s.RestoreWithPITR(ctx, PITRConfig{
			BackupID:    argString(args, "backupId"),
			DatasetName: argString(args, "dataset"),
			VerifyOnly:  argString(args, "verifyOnly") == "true",
		})
	case "createStreamConnector":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		conn, err := s.GetStreamManager().CreateConnector(StreamConnectorConfig{
			Name:       argString(args, "name"),
			Dataset:    argString(args, "dataset"),
			SourceType: StreamSourceType(argString(args, "sourceType")),
			Brokers:    argString(args, "brokers"),
			Topic:      argString(args, "topic"),
		}, argString(args, "start") == "true")
		if err != nil {
			return nil, err
		}
		return conn.Stats(), nil
	case "stopStreamConnector":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.GetStreamManager().StopConnector(name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "stopped"}, nil
	case "setTenantQuota":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		tenant := argString(args, "tenantId")
		if err := s.GetUsageMeter().SetQuota(tenant, TenantQuota{
			MaxQueriesPerWindow: int64(argInt(args, "maxQueries", 0)),
			MaxVectors:          int64(argInt(args, "maxVectors", 0)),
			MaxStorageBytes:     int64(argInt(args, "maxStorageBytes", 0)),
		}); err != nil {
			return nil, err
		}
		return map[string]any{"tenantId": tenant, "status": "quota_set"}, nil
	case "triggerShardSplit":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		left, right, migrated, err := s.GetElasticController().SplitShard(argString(args, "dataset"), argString(args, "shardId"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"left": left, "right": right, "migrated": migrated}, nil
	case "triggerShardMerge":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		merged, moved, err := s.GetElasticController().MergeShards(argString(args, "dataset"), argString(args, "leftShardId"), argString(args, "rightShardId"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"merged": merged, "moved": moved}, nil
	case "triggerRebalance":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		tasks, err := s.GetElasticController().RebalanceShards(argString(args, "dataset"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"tasks": tasks, "count": len(tasks)}, nil
	case "ingestMedia":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		n, err := s.GetMultiModalPipeline().IngestItems(ctx, ds, []MultiModalItem{{
			ID:        argString(args, "id"),
			Modality:  MediaModality(argString(args, "modality")),
			Caption:   argString(args, "caption"),
			SourceURI: argString(args, "sourceUri"),
		}})
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "upserted": n}, nil
	case "rerank":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		res, _, err := s.GetRerankManager().Rerank(ctx, argString(args, "strategy"), argString(args, "query"), nil, resolveTenantIDFromCtx(ctx, ""))
		return res, err
	default:
		return nil, fmt.Errorf("unknown GraphQL mutation field %q", field)
	}
}

// GraphQLHTTPHandler exposes POST / GET /api/v1/graphql endpoint.
func (s *VectorStore) GraphQLHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query         string         `json:"query"`
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		switch r.Method {
		case http.MethodGet:
			req.Query = r.URL.Query().Get("query")
			req.OperationName = r.URL.Query().Get("operationName")
			if rawVars := r.URL.Query().Get("variables"); rawVars != "" {
				_ = json.Unmarshal([]byte(rawVars), &req.Variables)
			}
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "graphql", r.Method, http.StatusBadRequest, err.Error())
				return
			}
		default:
			writeRESTError(w, "graphql", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if strings.TrimSpace(req.Query) == "" {
			writeRESTError(w, "graphql", r.Method, http.StatusBadRequest, "missing GraphQL query")
			return
		}
		resp := s.ExecuteGraphQL(r.Context(), req.Query, req.Variables, req.OperationName)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		metrics.RESTRequestsTotal.WithLabelValues("graphql", r.Method, "200").Inc()
	})
}

func (s *VectorStore) handleGraphQLAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	if action.Type != "GraphQLQuery" {
		return false, nil
	}
	var req struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
	}
	res := s.ExecuteGraphQL(stream.Context(), req.Query, req.Variables, req.OperationName)
	return true, sendJSONActionResult(stream, res)
}
