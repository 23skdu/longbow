package actions

import (
	"context"
	"encoding/json"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/store/rbac"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// ActionHost is the narrow RBAC capability surface used by action handlers.
// It does not reference *VectorStore or *Dataset, so the actions domain cannot
// form an import cycle with the store engine.
type ActionHost interface {
	AuthorizeDatasetAccess(context.Context, rbac.Permission, string) (*security.TenantContext, error)
}

func PermissionAndDatasetForAction(action *flight.Action) (rbac.Permission, string) {
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
		"Rerank", "RewriteQuery", "EvaluateReranker", "Recommend", "MultiModalSearch", "ValidateSchema", "GetSchemaRegistry", "GetTenantUsage",
		"ListStreamConnectors", "GetStreamStats", "GetCloudCluster", "ListCloudClusters", "GetCloudSLAReport",
		"GraphQLQuery":
		return rbac.PermRead, target
	case "IngestStreamEvent", "MultiModalIngest", "MultiModalIngestParquet", "RecordProvenance", "ReplicateClusterMutation", "RegisterFederatedCollection",
		"add-edge":
		return rbac.PermWrite, target
	case "delete", "Delete", "drop", "Drop", "ResetDataset", "delete-dataset", "delete-vector":
		return rbac.PermDelete, target
	case "ForceSnapshot", "TriggerBackup", "RestoreBackup", "ListBackups", "VerifyBackup", "Compact", "TieredOffload":
		return rbac.PermBackup, target
	case "CreateAPIKey", "RotateAPIKey", "RevokeAPIKey", "ListAPIKeys":
		return rbac.PermManageKeys, target
	case "SetRLSPolicy", "CreateNamespace", "DeleteNamespace", "SetSchemaPolicy", "RegisterSchema",
		"RefreshMaterializedView", "CreateMaterializedView", "SetTenantQuota", "ExportBillingReport",
		"CreateStreamConnector", "StopStreamConnector", "RegisterStreamSchema", "TuneBM25", "MigrateSchema",
		"CreateCloudCluster", "ScaleCloudCluster", "AddReplicaRegion", "PromoteClusterRegion", "DeleteCloudCluster":
		return rbac.PermManageTenant, target
	case "GetAuditLog", "GetAuditDenialCounts", "ReplicateWAL", "cluster-status", "MeshIdentity", "MeshStatus", "DiscoveryStatus",
		"GetAutoScaleConfig", "SetAutoScaleConfig", "TriggerShardSplit", "TriggerShardMerge",
		"TriggerRebalance", "GetClusterTopology",
		"ListOIDCProviders", "AddOIDCProvider", "RemoveOIDCProvider":
		return rbac.PermAdmin, ""
	case "ExchangeOIDCToken":
		return rbac.PermRead, ""
	default:
		return rbac.PermAdmin, target
	}
}

// AuthorizeAction authorizes a Flight action through the host RBAC interface.
func AuthorizeAction(ctx context.Context, host ActionHost, action *flight.Action) error {
	perm, target := PermissionAndDatasetForAction(action)
	_, err := host.AuthorizeDatasetAccess(ctx, perm, target)
	return err
}
