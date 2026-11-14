package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/pkg/version"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	ctx := context.Background()

	switch command {
	case "import":
		runImport(ctx, os.Args[2:])
	case "export":
		runExport(ctx, os.Args[2:])
	case "search":
		runSearch(ctx, os.Args[2:])
	case "create-namespace":
		runCreateNamespace(ctx, os.Args[2:])
	case "create-dataset":
		runCreateDataset(ctx, os.Args[2:])
	case "delete-namespace":
		runDeleteNamespace(ctx, os.Args[2:])
	case "list-namespaces":
		runListNamespaces(ctx, os.Args[2:])
	case "list-datasets-in-namespace":
		runListDatasetsInNamespace(ctx, os.Args[2:])
	case "stats":
		runStats(ctx, os.Args[2:])
	case "geo-search":
		runGeoSearch(ctx, os.Args[2:])
	case "recommend":
		runRecommend(ctx, os.Args[2:])
	case "delete":
		runDelete(ctx, os.Args[2:])
	case "snapshot":
		runSnapshot(ctx, os.Args[2:])
	case "add-edge":
		runAddEdge(ctx, os.Args[2:])
	case "traverse":
		runTraverse(ctx, os.Args[2:])
	case "get-graph-stats":
		runGetGraphStats(ctx, os.Args[2:])
	case "pagerank":
		runPageRank(ctx, os.Args[2:])
	case "detect-communities":
		runDetectCommunities(ctx, os.Args[2:])
	case "temporal-search":
		runTemporalSearch(ctx, os.Args[2:])
	case "range-search":
		runRangeSearch(ctx, os.Args[2:])
	case "federated-search":
		runFederatedSearch(ctx, os.Args[2:])
	case "text-search":
		runTextSearch(ctx, os.Args[2:])
	case "multimodal-search":
		runMultiModalSearch(ctx, os.Args[2:])
	case "explain-search":
		runExplainSearch(ctx, os.Args[2:])
	case "rerank":
		runRerank(ctx, os.Args[2:])
	case "rewrite-query":
		runRewriteQuery(ctx, os.Args[2:])
	case "drop":
		runDrop(ctx, os.Args[2:])
	case "download-model":
		runDownloadModel(ctx, os.Args[2:])
	case "create-api-key":
		runCreateAPIKey(ctx, os.Args[2:])
	case "rotate-api-key":
		runRotateAPIKey(ctx, os.Args[2:])
	case "revoke-api-key":
		runRevokeAPIKey(ctx, os.Args[2:])
	case "list-api-keys":
		runListAPIKeys(ctx, os.Args[2:])
	case "backup":
		runBackup(ctx, os.Args[2:])
	case "restore":
		runRestore(ctx, os.Args[2:])
	case "cloud-create-cluster":
		runCloudCreateCluster(ctx, os.Args[2:])
	case "cloud-list-clusters":
		runCloudListClusters(ctx, os.Args[2:])
	case "cloud-scale":
		runCloudScaleCluster(ctx, os.Args[2:])
	case "cloud-promote":
		runCloudPromoteRegion(ctx, os.Args[2:])
	case "cloud-sla":
		runCloudSLAReport(ctx, os.Args[2:])
	case "graphql":
		runGraphQLCommand(ctx, os.Args[2:])
	case "set-rls-policy":
		runSetRLSPolicy(ctx, os.Args[2:])
	case "audit-log":
		runAuditLog(ctx, os.Args[2:])
	case "list-backups":
		runListBackups(ctx, os.Args[2:])
	case "verify-backup":
		runVerifyBackup(ctx, os.Args[2:])
	case "stream-create":
		runStreamCreate(ctx, os.Args[2:])
	case "stream-stop":
		runStreamStop(ctx, os.Args[2:])
	case "stream-list", "stream-stats":
		runStreamList(ctx, os.Args[2:])
	case "stream-ingest":
		runStreamIngest(ctx, os.Args[2:])
	case "multimodal-ingest":
		runMultiModalIngest(ctx, os.Args[2:])
	case "eval-reranker":
		runEvalReranker(ctx, os.Args[2:])
	case "usage":
		runTenantUsage(ctx, os.Args[2:])
	case "set-quota":
		runSetQuota(ctx, os.Args[2:])
	case "export-billing":
		runExportBilling(ctx, os.Args[2:])
	case "shard-topology":
		runShardTopology(ctx, os.Args[2:])
	case "shard-split":
		runShardSplit(ctx, os.Args[2:])
	case "shard-merge":
		runShardMerge(ctx, os.Args[2:])
	case "shard-rebalance":
		runShardRebalance(ctx, os.Args[2:])
	case "autoscale-config":
		runAutoScaleConfig(ctx, os.Args[2:])
	case "register-schema":
		runRegisterSchema(ctx, os.Args[2:])
	case "get-schema":
		runGetSchema(ctx, os.Args[2:])
	case "validate-schema":
		runValidateSchema(ctx, os.Args[2:])
	case "migrate-schema":
		runMigrateSchema(ctx, os.Args[2:])
	case "create-view":
		runCreateView(ctx, os.Args[2:])
	case "refresh-view":
		runRefreshView(ctx, os.Args[2:])
	case "query-view":
		runQueryView(ctx, os.Args[2:])
	case "list-views":
		runListViews(ctx, os.Args[2:])
	case "tune-bm25":
		runTuneBM25(ctx, os.Args[2:])
	case "search-facets":
		runSearchFacets(ctx, os.Args[2:])
	case "search-analytics":
		runSearchAnalytics(ctx, os.Args[2:])
	case "record-provenance":
		runRecordProvenance(ctx, os.Args[2:])
	case "get-provenance":
		runGetProvenance(ctx, os.Args[2:])
	case "version", "-v", "--version":
		version.Print()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Longbow CLI - Vector Store Management Tool

Usage:
  longbow-cli <command> [options]

Commands:
  import           Import parquet, npy, or arrow files into a dataset
  export           Export a dataset to an arrow file
  search            Search vectors (dense, sparse, filtered, hybrid, by-id, range)
  range-search      Search all vectors within a distance threshold
  federated-search  Search across multiple collections or routed tags with RRF
  text-search       Advanced full-text search (boolean AST, fuzzy, snippets, facets)
  multimodal-search Cross-modal search (text, image, audio)
  explain-search    Return search provenance and traversal hop trace
  rerank            Rerank candidate results (heuristic, cross_encoder, mmr)
  rewrite-query     Expand or rewrite a search query
  create-namespace  Create a new dataset namespace
  delete-namespace  Delete a dataset namespace
  list-namespaces   List all dataset namespaces
  list-datasets-in-namespace List datasets in a namespace
  stats             Show dataset statistics
  geo-search        Search vectors with geospatial constraints (radius, box, hybrid)
  recommend         Get recommendations based on seed IDs (vector-graph)
  delete            Delete specific IDs from a dataset
  snapshot          Trigger a manual snapshot
  add-edge          Add a directed edge to the graph
  traverse          Traverse the graph from a start node
  get-graph-stats   Show graph connectivity statistics
  pagerank          Calculate PageRank centrality
  detect-communities Run community detection (LPA)
  temporal-search   Search temporal index (as_of, range, sliding_window, sliding_window_time)
  drop             Explicitly drop a dataset from memory
  download-model   Download an ONNX model from Hugging Face
  cloud-create-cluster Provision a managed cloud cluster
  cloud-list-clusters  List managed cloud clusters
  cloud-scale          Scale cloud cluster nodes or tier
  cloud-promote        Promote a replica region in failover
  cloud-sla            Evaluate cluster SLA compliance
  graphql              Execute a GraphQL query or mutation
  set-rls-policy       Register row-level security policy
  audit-log            Fetch RBAC security audit log
  list-backups         List online snapshots/backups
  verify-backup        Verify backup integrity
  stream-create        Create streaming CDC connector
  stream-stop          Stop streaming CDC connector
  stream-list          List streaming connectors & stats
  stream-ingest        Ingest streaming envelopes
  multimodal-ingest    Ingest multi-modal items or Parquet file
  eval-reranker        Evaluate reranker NDCG@10/MRR@10/Recall@10
  usage                Get tenant usage & metering metrics
  set-quota            Set tenant usage quota
  export-billing       Export billing report (json|csv|stripe)
  shard-topology       Inspect elastic shard ring topology
  shard-split          Split a hot shard
  shard-merge          Merge adjacent shards
  shard-rebalance      Rebalance shards across nodes
  autoscale-config     Get or update elastic autoscale policy
  register-schema      Register dataset schema & constraints
  get-schema           Inspect registered dataset schema
  validate-schema      Validate vector records against dataset schema
  migrate-schema       Migrate dataset schema & backfill columns
  create-view          Create a materialized view
  refresh-view         Refresh a materialized view
  query-view           Query a materialized view
  list-views           List materialized views
  tune-bm25            Configure dataset BM25 k1/b parameters
  search-facets        Compute search facet counts
  search-analytics     Inspect search query analytics
  record-provenance    Record vector provenance lineage
  get-provenance       Inspect vector provenance lineage

Global Options:
  -uri string    Longbow server URI (default: grpc://127.0.0.1:3000)

Examples:
  # Import parquet file
  longbow-cli import -dataset mydata -input vectors.parquet

  # Search with dense vectors
  longbow-cli search -dataset mydata -mode dense -vector "0.1,0.2,0.3" -k 10

  # Create TurboQuant dataset
  longbow-cli create-namespace -name mytq -dims 768 -data_type turboquant2


  # Search with compound filters
  longbow-cli search -dataset mydata -mode filtered -vector "0.1,0.2" -filters '{
    "logic": "AND",
    "filters": [
      {"field": "id", "operator": ">", "value": "10"},
      {"logic": "OR", "filters": [
        {"field": "category", "operator": "=", "value": "1"},
        {"field": "status", "operator": "=", "value": "2"}
      ]}
    ]
  }'

  # Hybrid search
  longbow-cli search -dataset mydata -mode hybrid -vector "0.1,0.2" -text "search query" -alpha 0.5

  # Export dataset to Arrow file
  longbow-cli export -dataset mydata -output dataset.arrow -compression lz4

  # Download ONNX model from Hugging Face
  longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-MiniLM-L6-v2

Use "longbow-cli <command> --help" for more information about a command.`)
}

func mustGetClient(uri string) *client.SmartClient {
	// Sanitize URI for logging to prevent log injection (G706)
	re := regexp.MustCompile(`[\r\n]`)
	safeURI := re.ReplaceAllString(uri, "_")

	sc, err := client.NewSmartClient(uri)
	if err != nil {
		log.Fatalf("Failed to connect to Longbow at %s: %v", safeURI, err) // #nosec G706
	}
	return sc
}
