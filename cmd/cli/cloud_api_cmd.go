package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/metadata"
)

func executeCloudOrGraphQLAction(ctx context.Context, uri, apiKey, actionType string, payload any) {
	sc := mustGetClient(uri)
	defer func() { _ = sc.Close() }()

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: actionType, Body: raw})
	if err != nil {
		log.Fatalf("%s failed: %v", actionType, err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%s stream error: %v", actionType, err)
		}
		fmt.Println(string(res.Body))
	}
}

func runCloudCreateCluster(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-create-cluster", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID")
	name := fs.String("name", "", "Cluster name (required)")
	tenant := fs.String("tenant", "default", "Tenant ID")
	tier := fs.String("tier", "dedicated", "Cluster tier (serverless|dedicated|enterprise)")
	region := fs.String("primary-region", "us-east-1", "Primary region")
	nodes := fs.Int("nodes", 3, "Node count")
	mode := fs.String("replication-mode", "semi_sync", "Replication mode (async|semi_sync|sync)")
	_ = fs.Parse(args)
	if *name == "" {
		log.Fatal("-name is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "CreateCloudCluster", map[string]any{
		"id":               *id,
		"name":             *name,
		"tenant_id":        *tenant,
		"tier":             *tier,
		"primary_region":   *region,
		"nodes":            *nodes,
		"replication_mode": *mode,
	})
}

func runCloudListClusters(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-list-clusters", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	tenant := fs.String("tenant", "", "Filter by tenant ID")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ListCloudClusters", map[string]any{"tenant_id": *tenant})
}

func runCloudScaleCluster(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-scale", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	nodes := fs.Int("nodes", 0, "Target node count")
	tier := fs.String("tier", "", "Target tier")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ScaleCloudCluster", map[string]any{
		"id":    *id,
		"nodes": *nodes,
		"tier":  *tier,
	})
}

func runCloudPromoteRegion(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-promote", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	region := fs.String("region", "", "Target region to promote (optional)")
	reason := fs.String("reason", "operator failover", "Failover reason")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "PromoteClusterRegion", map[string]any{
		"cluster_id":    *id,
		"target_region": *region,
		"reason":        *reason,
	})
}

func runCloudSLAReport(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cloud-sla", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	id := fs.String("id", "", "Cluster ID (required)")
	p99 := fs.Float64("p99-ms", 0, "Observed P99 latency in ms")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetCloudSLAReport", map[string]any{
		"cluster_id":      *id,
		"observed_p99_ms": *p99,
	})
}

func runGraphQLCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("graphql", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	query := fs.String("query", "{ health { status activeDatasets namespaces } }", "GraphQL query or mutation")
	varsJSON := fs.String("variables", "", "Optional JSON variables map")
	opName := fs.String("operation", "", "Optional GraphQL operation name")
	_ = fs.Parse(args)

	var vars map[string]any
	if *varsJSON != "" {
		if err := json.Unmarshal([]byte(*varsJSON), &vars); err != nil {
			log.Fatalf("invalid -variables JSON: %v", err)
		}
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GraphQLQuery", map[string]any{
		"query":         *query,
		"variables":     vars,
		"operationName": *opName,
	})
}

func runSetRLSPolicy(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("set-rls-policy", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	tenant := fs.String("tenant", "", "Tenant ID")
	namespace := fs.String("namespace", "", "Namespace")
	dataset := fs.String("dataset", "", "Dataset")
	filtersJSON := fs.String("filters", "{}", "Required metadata filters JSON map")
	_ = fs.Parse(args)
	var filters map[string]string
	_ = json.Unmarshal([]byte(*filtersJSON), &filters)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "SetRLSPolicy", map[string]any{
		"tenant_id":        *tenant,
		"namespace":        *namespace,
		"dataset":          *dataset,
		"required_filters": filters,
	})
}

func runAuditLog(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("audit-log", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	limit := fs.Int("limit", 100, "Max audit entries")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetAuditLog", map[string]any{"limit": *limit})
}

func runListBackups(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("list-backups", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Optional dataset filter")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ListBackups", map[string]any{"dataset": *dataset})
}

func runVerifyBackup(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("verify-backup", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	backupID := fs.String("backup-id", "", "Backup ID (required)")
	dataset := fs.String("dataset", "", "Dataset name")
	_ = fs.Parse(args)
	if *backupID == "" {
		log.Fatal("-backup-id is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "VerifyBackup", map[string]any{
		"backup_id": *backupID,
		"dataset":   *dataset,
	})
}

func runStreamCreate(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stream-create", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	name := fs.String("name", "", "Connector name (required)")
	dataset := fs.String("dataset", "", "Target dataset (required)")
	sourceType := fs.String("source", "memory", "Source type (memory|kafka|pulsar)")
	brokers := fs.String("brokers", "", "Broker addresses")
	topic := fs.String("topic", "", "Source topic")
	format := fs.String("format", "json", "Payload format (json|avro|protobuf)")
	start := fs.Bool("start", false, "Start connector immediately")
	_ = fs.Parse(args)
	if *name == "" || *dataset == "" {
		log.Fatal("-name and -dataset are required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "CreateStreamConnector", map[string]any{
		"name":        *name,
		"dataset":     *dataset,
		"source_type": *sourceType,
		"brokers":     *brokers,
		"topic":       *topic,
		"format":      *format,
		"start":       *start,
	})
}

func runStreamStop(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stream-stop", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	name := fs.String("name", "", "Connector name (required)")
	_ = fs.Parse(args)
	if *name == "" {
		log.Fatal("-name is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "StopStreamConnector", map[string]any{"name": *name})
}

func runStreamList(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stream-list", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ListStreamConnectors", map[string]any{})
}

func runStreamIngest(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stream-ingest", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	connector := fs.String("connector", "", "Connector name (required)")
	envelopesJSON := fs.String("envelopes", "[]", "Envelopes JSON array")
	_ = fs.Parse(args)
	var envs []any
	_ = json.Unmarshal([]byte(*envelopesJSON), &envs)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "IngestStreamEvent", map[string]any{
		"connector": *connector,
		"envelopes": envs,
	})
}

func runMultiModalIngest(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("multimodal-ingest", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	parquetPath := fs.String("parquet", "", "Parquet file path")
	itemsJSON := fs.String("items", "[]", "MultiModalItem JSON array")
	_ = fs.Parse(args)
	if *dataset == "" {
		log.Fatal("-dataset is required")
	}
	if *parquetPath != "" {
		executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "MultiModalIngestParquet", map[string]any{
			"dataset":      *dataset,
			"parquet_path": *parquetPath,
		})
		return
	}
	var items []any
	_ = json.Unmarshal([]byte(*itemsJSON), &items)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "MultiModalIngest", map[string]any{
		"dataset": *dataset,
		"items":   items,
	})
}

func runEvalReranker(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("eval-reranker", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name")
	rtype := fs.String("type", "cross_encoder", "Reranker type")
	queriesJSON := fs.String("queries", "[]", "Evaluation queries JSON array")
	_ = fs.Parse(args)
	var queries []any
	_ = json.Unmarshal([]byte(*queriesJSON), &queries)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "EvaluateReranker", map[string]any{
		"dataset": *dataset,
		"type":    *rtype,
		"queries": queries,
	})
}

func runTenantUsage(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("usage", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	tenant := fs.String("tenant", "", "Optional tenant ID")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetTenantUsage", map[string]any{"tenant_id": *tenant})
}

func runSetQuota(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("set-quota", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	tenant := fs.String("tenant", "", "Tenant ID (required)")
	maxQueries := fs.Int64("max-queries", 0, "Max queries")
	maxVectors := fs.Int64("max-vectors", 0, "Max vectors")
	maxBytes := fs.Int64("max-storage-bytes", 0, "Max storage bytes")
	_ = fs.Parse(args)
	if *tenant == "" {
		log.Fatal("-tenant is required")
	}
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "SetTenantQuota", map[string]any{
		"tenant_id":         *tenant,
		"max_queries":       *maxQueries,
		"max_vectors":       *maxVectors,
		"max_storage_bytes": *maxBytes,
	})
}

func runExportBilling(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("export-billing", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	format := fs.String("format", "json", "Billing report format (json|csv|stripe)")
	tenant := fs.String("tenant", "", "Optional tenant ID")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ExportBillingReport", map[string]any{
		"format":    *format,
		"tenant_id": *tenant,
	})
}

func runShardTopology(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("shard-topology", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetClusterTopology", map[string]any{})
}

func runShardSplit(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("shard-split", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	shardID := fs.String("shard-id", "", "Shard ID (required)")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "TriggerShardSplit", map[string]any{
		"dataset":  *dataset,
		"shard_id": *shardID,
	})
}

func runShardMerge(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("shard-merge", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	left := fs.String("left", "", "Left shard ID (required)")
	right := fs.String("right", "", "Right shard ID (required)")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "TriggerShardMerge", map[string]any{
		"dataset":        *dataset,
		"left_shard_id":  *left,
		"right_shard_id": *right,
	})
}

func runShardRebalance(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("shard-rebalance", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "TriggerRebalance", map[string]any{"dataset": *dataset})
}

func runAutoScaleConfig(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("autoscale-config", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	policyJSON := fs.String("set", "", "Optional JSON policy to update")
	_ = fs.Parse(args)
	if *policyJSON == "" {
		executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetAutoScaleConfig", map[string]any{})
		return
	}
	var pol map[string]any
	_ = json.Unmarshal([]byte(*policyJSON), &pol)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "SetAutoScaleConfig", pol)
}

func runRegisterSchema(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("register-schema", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	compat := fs.String("compatibility", "BACKWARD", "Compatibility mode (NONE|BACKWARD|FORWARD|FULL)")
	constraintsJSON := fs.String("constraints", "[]", "FieldConstraint JSON array")
	_ = fs.Parse(args)
	var constraints []any
	_ = json.Unmarshal([]byte(*constraintsJSON), &constraints)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "RegisterSchema", map[string]any{
		"dataset":       *dataset,
		"compatibility": *compat,
		"constraints":   constraints,
	})
}

func runGetSchema(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("get-schema", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetSchemaRegistry", map[string]any{"dataset": *dataset})
}

func runValidateSchema(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("validate-schema", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	vectorsJSON := fs.String("vectors", "[]", "Vectors JSON array")
	_ = fs.Parse(args)
	var vectors []any
	_ = json.Unmarshal([]byte(*vectorsJSON), &vectors)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ValidateSchema", map[string]any{
		"dataset": *dataset,
		"vectors": vectors,
	})
}

func runMigrateSchema(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("migrate-schema", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	compat := fs.String("compatibility", "BACKWARD", "Compatibility mode")
	addColsJSON := fs.String("add-cols", "{}", "Default values for new string columns JSON map")
	_ = fs.Parse(args)
	var addCols map[string]string
	_ = json.Unmarshal([]byte(*addColsJSON), &addCols)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "MigrateSchema", map[string]any{
		"target": map[string]any{
			"dataset":       *dataset,
			"compatibility": *compat,
		},
		"add_string_cols": addCols,
	})
}

func runCreateView(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("create-view", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	name := fs.String("name", "", "View name (required)")
	source := fs.String("source", "", "Source dataset (required)")
	groupByJSON := fs.String("group-by", "[]", "GroupBy fields JSON array")
	mode := fs.String("refresh-mode", "streaming", "Refresh mode (manual|periodic|streaming)")
	_ = fs.Parse(args)
	var groupBy []string
	_ = json.Unmarshal([]byte(*groupByJSON), &groupBy)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "CreateMaterializedView", map[string]any{
		"name":           *name,
		"source_dataset": *source,
		"group_by":       groupBy,
		"refresh_mode":   *mode,
	})
}

func runRefreshView(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("refresh-view", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	name := fs.String("name", "", "View name (required)")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "RefreshMaterializedView", map[string]any{"name": *name})
}

func runQueryView(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("query-view", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	name := fs.String("name", "", "View name (required)")
	filterJSON := fs.String("filter", "{}", "Optional group filter JSON map")
	_ = fs.Parse(args)
	var filter map[string]string
	_ = json.Unmarshal([]byte(*filterJSON), &filter)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "QueryMaterializedView", map[string]any{
		"name":         *name,
		"group_filter": filter,
	})
}

func runListViews(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("list-views", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "ListMaterializedViews", map[string]any{})
}

func runTuneBM25(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("tune-bm25", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	k1 := fs.Float64("k1", 1.2, "BM25 k1 parameter")
	b := fs.Float64("b", 0.75, "BM25 b parameter")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "TuneBM25", map[string]any{
		"dataset": *dataset,
		"k1":      *k1,
		"b":       *b,
	})
}

func runSearchFacets(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("search-facets", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	query := fs.String("query", "", "Text query")
	fieldsJSON := fs.String("fields", "[]", "Facet fields JSON array")
	_ = fs.Parse(args)
	var fields []string
	_ = json.Unmarshal([]byte(*fieldsJSON), &fields)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "SearchFacets", map[string]any{
		"dataset":      *dataset,
		"query":        *query,
		"facet_fields": fields,
	})
}

func runSearchAnalytics(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("search-analytics", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	limit := fs.Int("limit", 20, "Top query limit")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "SearchAnalytics", map[string]any{"limit": *limit})
}

func runRecordProvenance(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("record-provenance", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	vecID := fs.String("vector-id", "", "Vector ID (required)")
	docID := fs.String("source-doc", "", "Source document ID")
	sourceURI := fs.String("source-uri", "", "Source URI")
	model := fs.String("model", "", "Embedding model")
	version := fs.String("model-version", "", "Model version")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "RecordProvenance", map[string]any{
		"dataset":            *dataset,
		"vector_id":          *vecID,
		"source_document_id": *docID,
		"source_uri":         *sourceURI,
		"embedding_model":    *model,
		"model_version":      *version,
	})
}

func runGetProvenance(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("get-provenance", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	apiKey := fs.String("api-key", os.Getenv("LONGBOW_API_KEY"), "RBAC API Key")
	dataset := fs.String("dataset", "", "Dataset name (required)")
	vecID := fs.String("vector-id", "", "Optional Vector ID")
	limit := fs.Int("limit", 50, "Max records when listing")
	_ = fs.Parse(args)
	executeCloudOrGraphQLAction(ctx, *uri, *apiKey, "GetProvenance", map[string]any{
		"dataset":   *dataset,
		"vector_id": *vecID,
		"limit":     *limit,
	})
}
