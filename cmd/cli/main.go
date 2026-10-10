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
	case "drop":
		runDrop(ctx, os.Args[2:])
	case "download-model":
		runDownloadModel(ctx, os.Args[2:])
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
  search           Search vectors with Dense, Sparse, Filtered, or Hybrid modes
  create-namespace Create a new dataset namespace
  delete-namespace Delete a dataset namespace
  list-namespaces  List all dataset namespaces
  list-datasets-in-namespace List datasets in a namespace
  stats            Show dataset statistics
  geo-search       Search vectors with geospatial constraints
  recommend        Get recommendations based on seed IDs
  delete           Delete specific IDs from a dataset
  snapshot         Trigger a manual snapshot
  add-edge         Add a directed edge to the graph
  traverse         Traverse the graph from a start node
  get-graph-stats  Show graph connectivity statistics
  pagerank         Calculate PageRank centrality
  detect-communities Run community detection (LPA)
  temporal-search  Search temporal index (as-of, range, window)
  drop             Explicitly drop a dataset from memory
  download-model   Download an ONNX model from Hugging Face

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
