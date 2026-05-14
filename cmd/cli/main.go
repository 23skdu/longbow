package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/sbinet/npyio"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/internal/onnx"
	"github.com/23skdu/longbow/pkg/version"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func runImport(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Target dataset name (required)")
	input := fs.String("input", "", "Input file path (alias for --file)")
	file := fs.String("file", "", "Input file path (Supports .parquet, .npy, .arrow, and s3://bucket/key)")
	dim := fs.Int("dim", 128, "Vector dimension (used for demo data)")
	count := fs.Int("count", 1000, "Number of vectors to generate (used for demo data if no input file)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *file == "" && *input != "" {
		file = input
	}

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli import --dataset <name> --file <file> [-dim <n>] [-count <n>]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	if *file != "" {
		if strings.HasPrefix(*file, "s3://") {
			runImportS3(ctx, sc, *dataset, *file)
			return
		}
		ext := strings.ToLower(*file)
		if strings.HasSuffix(ext, ".parquet") {
			runImportParquet(ctx, sc, *dataset, *file)
		} else if strings.HasSuffix(ext, ".npy") {
			runImportNpy(ctx, sc, *dataset, *file)
		} else if strings.HasSuffix(ext, ".arrow") {
			runImportArrow(ctx, sc, *dataset, *file)
		} else {
			log.Fatalf("Unsupported file format: %s. Only .parquet, .npy, and .arrow are supported.\n", *file)
		}
		return
	}

	// Demo mode
	rec, sch := generateDemoData(*dim, *count)
	defer rec.Release()
	fmt.Printf("Importing %d demo rows (dim: %d) to dataset %s...\n", rec.NumRows(), *dim, *dataset)

	start := time.Now()
	if err := uploadData(ctx, sc, *dataset, rec, sch); err != nil {
		log.Fatalf("Upload failed: %v\n", err)
	}
	fmt.Printf("Successfully imported %d rows in %v\n", rec.NumRows(), time.Since(start))
}

func runImportParquet(ctx context.Context, sc *client.SmartClient, dataset, inputPath string) {
	start := time.Now()
	fmt.Printf("Importing Parquet file %s to dataset %s...\n", inputPath, dataset)

	f, err := os.Open(filepath.Clean(inputPath)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to open parquet file: %v\n", err)
	}
	defer f.Close()

	rdr, err := file.NewParquetReader(f)
	if err != nil {
		log.Fatalf("Failed to create parquet reader: %v\n", err)
	}
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{Parallel: true}, memory.DefaultAllocator)
	if err != nil {
		log.Fatalf("Failed to create pqarrow reader: %v\n", err)
	}

	tbl, err := arrowRdr.ReadTable(ctx)
	if err != nil {
		log.Fatalf("Failed to read table from parquet: %v\n", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 10000)
	defer tr.Release()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}

	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		log.Fatalf("DoPut stream failed: %v\n", err)
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(tbl.Schema()))
	writer.SetFlightDescriptor(desc)

	totalRows := int64(0)
	for tr.Next() {
		rec := tr.Record()
		if err := writer.Write(rec); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}
	if tr.Err() != nil {
		log.Fatalf("Table reader error: %v\n", tr.Err())
	}

	_ = writer.Close()
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Failed to close flight stream: %v\n", err)
	}
	_, _ = stream.Recv()

	fmt.Printf("Successfully imported %d rows in %v\n", totalRows, time.Since(start))
}

func runImportNpy(ctx context.Context, sc *client.SmartClient, dataset, inputPath string) {
	start := time.Now()
	fmt.Printf("Importing NumPy file %s to dataset %s...\n", inputPath, dataset)

	f, err := os.Open(filepath.Clean(inputPath)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to open npy file: %v\n", err)
	}
	defer f.Close()

	r, err := npyio.NewReader(f)
	if err != nil {
		log.Fatalf("Failed to create npy reader: %v\n", err)
	}

	shape := r.Header.Descr.Shape
	if len(shape) != 2 {
		log.Fatalf("Expected 2D numpy array, got %dD array\n", len(shape))
	}

	count := int(shape[0])
	dim := int(shape[1])

	var data []float32
	err = r.Read(&data)
	if err != nil {
		log.Fatalf("Failed to read npy payload: %v\n", err)
	}

	mem := memory.NewGoAllocator()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)}, // #nosec G115
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32) // #nosec G115
	defer listBuilder.Release()
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.Reserve(count)
	listBuilder.Reserve(count)
	vecBuilder.Reserve(count * dim)

	for i := 0; i < count; i++ {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
	}
	vecBuilder.AppendValues(data, nil)

	idArr := idBuilder.NewArray()
	defer idArr.Release()
	vecArr := listBuilder.NewArray()
	defer vecArr.Release()

	rec := array.NewRecordBatch(sch, []arrow.Array{idArr, vecArr}, int64(count))
	defer rec.Release()

	if err := uploadData(ctx, sc, dataset, rec, sch); err != nil {
		log.Fatalf("Upload failed: %v\n", err)
	}

	fmt.Printf("Successfully imported %d rows in %v\n", count, time.Since(start))
}

func generateDemoData(dim, count int) (arrow.Record, *arrow.Schema) {
	mem := memory.NewGoAllocator()

	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)}, // #nosec G115
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32) // #nosec G115
	defer listBuilder.Release()

	catBuilder := array.NewInt64Builder(mem)
	defer catBuilder.Release()

	scoreBuilder := array.NewFloat32Builder(mem)
	defer scoreBuilder.Release()

	idBuilder.Reserve(count)
	listBuilder.Reserve(count)
	catBuilder.Reserve(count)
	scoreBuilder.Reserve(count)

	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
	vecBuilder.Reserve(count * dim)

	for i := 0; i < count; i++ {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
		for j := 0; j < dim; j++ {
			vecBuilder.Append(rand.Float32())
		}
		catBuilder.Append(int64(i % 5))
		scoreBuilder.Append(rand.Float32() * 100)
	}

	idArr := idBuilder.NewArray()
	defer idArr.Release()
	vecArr := listBuilder.NewArray()
	defer vecArr.Release()
	catArr := catBuilder.NewArray()
	defer catArr.Release()
	scoreArr := scoreBuilder.NewArray()
	defer scoreArr.Release()

	rec := array.NewRecordBatch(sch, []arrow.Array{idArr, vecArr, catArr, scoreArr}, int64(count))
	return rec, sch
} // #nosec G404

func uploadData(ctx context.Context, sc *client.SmartClient, dataset string, rec arrow.Record, sch *arrow.Schema) error {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}

	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(sch))
	writer.SetFlightDescriptor(desc)

	if err := writer.Write(rec); err != nil {
		_ = writer.Close()
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	_, _ = stream.Recv()
	return nil
}

func runSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	mode := fs.String("mode", "dense", "Search mode: dense, sparse, filtered, hybrid")
	vector := fs.String("vector", "", "Query vector as comma-separated floats")
	textQuery := fs.String("text", "", "Text query for sparse/hybrid search")
	alpha := fs.Float64("alpha", 0.5, "Alpha for hybrid search (0=sparse, 1=dense)")
	k := fs.Int("k", 10, "Number of results")
	filters := fs.String("filters", "", "JSON filter expression (inline or file path)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli search -dataset <name> -mode <dense|sparse|filtered|hybrid> [options]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset": *dataset,
		"k":       *k,
	}

	switch strings.ToLower(*mode) {
	case "dense":
		if *vector == "" {
			log.Fatal("Dense search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)

	case "sparse":
		if *textQuery == "" {
			log.Fatal("Sparse search requires -text flag")
		}
		req["text_query"] = *textQuery
		req["alpha"] = 0.0

	case "filtered":
		if *vector == "" {
			log.Fatal("Filtered search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)
		if *filters != "" {
			filterExpr, err := parseFilterExpression(*filters)
			if err != nil {
				log.Fatalf("Failed to parse filters: %v", err)
			}
			req["filters"] = filterExpr
		}

	case "hybrid":
		if *vector == "" {
			log.Fatal("Hybrid search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)
		req["alpha"] = *alpha
		if *textQuery != "" {
			req["text_query"] = *textQuery
		}

	default:
		log.Fatalf("Unknown search mode: %s", *mode)
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"search": req})

	start := time.Now()
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	var totalRows int64
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
		printResults(rec)
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Error reading results: %v", err)
	}

	fmt.Printf("\nFound %d results in %v\n", totalRows, time.Since(start))
}

func parseFloats(s string) []float32 {
	parts := strings.Split(s, ",")
	result := make([]float32, len(parts))
	for i, p := range parts {
		var f float64
		_, _ = fmt.Sscanf(strings.TrimSpace(p), "%f", &f)
		result[i] = float32(f)
	}
	return result
}

func parseFilterExpression(s string) (interface{}, error) {
	data, err := os.ReadFile(filepath.Clean(s))
	if err != nil {
		return json.RawMessage(s), nil
	}

	var filter interface{}
	if err := json.Unmarshal(data, &filter); err != nil {
		return nil, err
	}
	return filter, nil
}

func printResults(rec arrow.Record) {
	for i := int64(0); i < rec.NumRows(); i++ {
		for j := int64(0); j < rec.NumCols(); j++ {
			col := rec.Column(int(j))
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s=%v", rec.Schema().Field(int(j)).Name, extractValue(col, i))
		}
		fmt.Println()
	}
}

func extractValue(col arrow.Array, idx int64) interface{} {
	if col.IsNull(int(idx)) {
		return nil
	}
	switch col.DataType().ID() {
	case arrow.INT64:
		return col.(*array.Int64).Value(int(idx))
	case arrow.FLOAT32:
		return col.(*array.Float32).Value(int(idx))
	case arrow.FLOAT64:
		return col.(*array.Float64).Value(int(idx))
	case arrow.STRING:
		return col.(*array.String).Value(int(idx))
	default:
		return fmt.Sprintf("<%s>", col.DataType().Name())
	}
}

func runCreateNamespace(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("create-namespace", flag.ExitOnError)
	name := fs.String("name", "", "Namespace name (required)")
	dims := fs.Int("dims", 128, "Vector dimensions")
	dtype := fs.String("data_type", "float32", "Data type (float32, int8, turboquant)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli create-namespace -name <name> [-dims <n>] [-data_type <type>]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"name":      *name,
		"dims":      *dims,
		"data_type": *dtype,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "CreateNamespace", Body: actionBody}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to create namespace: %v", err)
	}

	for {
		result, err := stream.Recv()
		if err != nil {
			break
		}
		if len(result.Body) > 0 {
			fmt.Printf("%s\n", string(result.Body))
		}
	}

	fmt.Printf("Namespace '%s' created successfully\n", *name)
}

func runDeleteNamespace(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("delete-namespace", flag.ExitOnError)
	name := fs.String("name", "", "Namespace name (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli delete-namespace -name <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	actionBody, _ := json.Marshal(map[string]string{"namespace": *name})
	action := &flight.Action{Type: "delete_namespace", Body: actionBody}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to delete namespace: %v", err)
	}

	for {
		result, err := stream.Recv()
		if err != nil {
			break
		}
		if len(result.Body) > 0 {
			fmt.Printf("%s\n", string(result.Body))
		}
	}

	fmt.Printf("Namespace '%s' deleted successfully\n", *name)
}

func runListNamespaces(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("list-namespaces", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)
	sc := mustGetClient(*uri)
	defer sc.Close()

	action := &flight.Action{Type: "list_actions"}
	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to list namespaces: %v", err)
	}

	fmt.Println("Namespaces:")
	for {
		result, err := stream.Recv()
		if err != nil {
			break
		}
		if len(result.Body) > 0 {
			fmt.Printf("  %s\n", string(result.Body))
		}
	}
}

func runListDatasetsInNamespace(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("list-datasets-in-namespace", flag.ExitOnError)
	name := fs.String("namespace", "default", "Namespace name")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	body, err := json.Marshal(map[string]string{"name": *name})
	if err != nil {
		log.Fatalf("Failed to marshal request: %v", err)
	}

	action := &flight.Action{Type: "ListDatasetsInNamespace", Body: body}
	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to list datasets in namespace: %v", err)
	}

	result, err := stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive response: %v", err)
	}

	var resp map[string][]string
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	datasets, ok := resp["datasets"]
	if !ok {
		log.Fatalf("Invalid response format")
	}

	fmt.Printf("Datasets in namespace '%s':\n", *name)
	if len(datasets) == 0 {
		fmt.Println("  (none)")
	} else {
		for _, ds := range datasets {
			fmt.Printf("  %s\n", ds)
		}
	}
}

func runStats(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	name := fs.String("dataset", "", "Dataset name (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli stats -dataset <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	actionBody, _ := json.Marshal(map[string]string{"dataset": *name})
	action := &flight.Action{Type: "check_readiness", Body: actionBody}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	for {
		result, err := stream.Recv()
		if err != nil {
			break
		}
		if len(result.Body) > 0 {
			var stats map[string]interface{}
			if err := json.Unmarshal(result.Body, &stats); err == nil {
				prettyJSON, _ := json.MarshalIndent(stats, "", "  ")
				fmt.Printf("%s\n", string(prettyJSON))
			} else {
				fmt.Printf("%s\n", string(result.Body))
			}
		}
	}

	// Display Load Balancing Hints
	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{*name}}
	_, _ = sc.GetFlightInfo(ctx, desc)
	hints := sc.GetLastLoadHints()
	if hints != nil {
		fmt.Printf("\nLoad Balancing Hints:\n")
		fmt.Printf("  CPU Load:    %d%%\n", hints.CPULoad)
		fmt.Printf("  Memory Load: %d%%\n", hints.MemLoad)
		fmt.Printf("  Queue Depth: %d\n", hints.QueueDepth)
		fmt.Printf("  Health:      %d%%\n\n", hints.Health)
	}
}

func runImportS3(ctx context.Context, sc *client.SmartClient, dataset, s3Path string) {
	// Parse s3://bucket/key
	u := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(u, "/", 2)
	if len(parts) < 2 {
		log.Fatalf("Invalid S3 path: %s. Expected s3://bucket/key\n", s3Path)
	}
	bucket, key := parts[0], parts[1]

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v\n", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	// Get file size
	head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("Failed to head S3 object: %v\n", err)
	}
	size := *head.ContentLength

	fmt.Printf("Importing S3 Parquet file %s (size: %d bytes) to dataset %s...\n", s3Path, size, dataset)

	readerAt := &s3ReaderAt{
		s3:     s3Client,
		bucket: bucket,
		key:    key,
		size:   size,
	}

	rdr, err := file.NewParquetReader(readerAt)
	if err != nil {
		log.Fatalf("Failed to create parquet reader from S3: %v\n", err)
	}
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{Parallel: true}, memory.DefaultAllocator)
	if err != nil {
		log.Fatalf("Failed to create pqarrow reader: %v\n", err)
	}

	tbl, err := arrowRdr.ReadTable(ctx)
	if err != nil {
		log.Fatalf("Failed to read table from S3 parquet: %v\n", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 10000)
	defer tr.Release()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}

	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		log.Fatalf("DoPut stream failed: %v\n", err)
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(tbl.Schema()))
	writer.SetFlightDescriptor(desc)

	totalRows := int64(0)
	for tr.Next() {
		rec := tr.Record()
		if err := writer.Write(rec); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}

	_ = writer.Close()
	_ = stream.CloseSend()
	_, _ = stream.Recv()

	fmt.Printf("Successfully imported %d rows from S3 in %v\n", totalRows, time.Now())
}

type s3ReaderAt struct {
	s3            *s3.Client
	bucket        string
	key           string
	size          int64
	currentOffset int64
}

func (r *s3ReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)
	out, err := r.s3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
		Range:  &rangeHeader,
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	return io.ReadFull(out.Body, p)
}

func (r *s3ReaderAt) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.currentOffset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset: %d", newOffset)
	}
	r.currentOffset = newOffset
	return newOffset, nil
}

func (r *s3ReaderAt) Read(p []byte) (n int, err error) {
	n, err = r.ReadAt(p, r.currentOffset)
	r.currentOffset += int64(n)
	return n, err
}

func runGeoSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("geo-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	lat := fs.Float64("lat", 0, "Center latitude")
	lon := fs.Float64("lon", 0, "Center longitude")
	radius := fs.Float64("radius", 1.0, "Search radius in km")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":     *dataset,
		"k":           *k,
		"center":      map[string]float64{"lat": *lat, "lon": *lon},
		"radius_km":   *radius,
		"search_type": "radius",
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"geo_search": req})
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Geo-Search failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		printResults(reader.Record())
	}
}

func runRecommend(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("recommend", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	seeds := fs.String("seeds", "", "Comma-separated seed IDs")
	k := fs.Int("k", 10, "Number of results")
	alpha := fs.Float64("alpha", 0.5, "Hybrid blend alpha")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" || *seeds == "" {
		log.Fatal("Dataset and seeds are required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":  *dataset,
		"seed_ids": strings.Split(*seeds, ","),
		"k":        *k,
		"alpha":    *alpha,
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"recommend": req})
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Recommend failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		printResults(reader.Record())
	}
}

func runDelete(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	id := fs.String("id", "", "Vector ID to delete")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" || *id == "" {
		log.Fatal("Dataset and ID are required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]string{"dataset": *dataset, "id": *id}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "delete", Body: actionBody}

	_, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Deleted ID %s from %s\n", *id, *dataset)
}

func runSnapshot(_ context.Context, args []string) {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)
	sc := mustGetClient(*uri)
	defer sc.Close()

	action := &flight.Action{Type: "ForceSnapshot", Body: []byte{}}
	_, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Snapshot failed: %v", err)
	}
	fmt.Println("Manual snapshot triggered")
}

func runAddEdge(_ context.Context, args []string) {
	fs := flag.NewFlagSet("add-edge", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	sub := fs.Int("subject", 0, "Subject ID")
	pred := fs.String("predicate", "related", "Predicate")
	obj := fs.Int("object", 0, "Object ID")
	weight := fs.Float64("weight", 1.0, "Edge weight")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":   *dataset,
		"subject":   *sub,
		"predicate": *pred,
		"object":    *obj,
		"weight":    *weight,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "add-edge", Body: actionBody}
	_, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Add edge failed: %v", err)
	}
	fmt.Printf("Added edge: %d --[%s]--> %d\n", *sub, *pred, *obj)
}

func runTraverse(_ context.Context, args []string) {
	fs := flag.NewFlagSet("traverse", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	start := fs.Int("start", 0, "Start node ID")
	hops := fs.Int("hops", 2, "Max hops")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":  *dataset,
		"start":    *start,
		"max_hops": *hops,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "traverse-graph", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Traverse failed: %v", err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("%s\n", string(res.Body))
	}
}

func runGetGraphStats(_ context.Context, args []string) {
	fs := flag.NewFlagSet("get-graph-stats", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]string{"dataset": *dataset}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "GetGraphStats", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Get graph stats failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}

func runPageRank(_ context.Context, args []string) {
	fs := flag.NewFlagSet("pagerank", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	iter := fs.Int("iterations", 20, "Max iterations")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{"dataset": *dataset, "max_iterations": *iter}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "calculate-pagerank", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("PageRank failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}

func runDetectCommunities(_ context.Context, args []string) {
	fs := flag.NewFlagSet("detect-communities", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]string{"dataset": *dataset}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "detect-communities", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Community detection failed: %v", err)
	}

	res, _ := stream.Recv()
	fmt.Printf("%s\n", string(res.Body))
}

func runTemporalSearch(_ context.Context, args []string) {
	fs := flag.NewFlagSet("temporal-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	searchType := fs.String("type", "as_of", "Search type: as_of, range, window")
	ts := fs.Int64("ts", 0, "Timestamp for as_of")
	start := fs.Int64("start", 0, "Start time")
	end := fs.Int64("end", 0, "End time")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":     *dataset,
		"search_type": *searchType,
		"timestamp":   *ts,
		"start_time":  *start,
		"end_time":    *end,
		"k":           *k,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "TemporalSearch", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Temporal search failed: %v", err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("%s\n", string(res.Body))
	}
}

func runCreateDataset(_ context.Context, args []string) {
	fs := flag.NewFlagSet("create-dataset", flag.ExitOnError)
	name := fs.String("name", "", "Dataset name (required)")
	dims := fs.Int("dims", 128, "Dimensions")
	vtype := fs.String("type", "float32", "Vector type")
	geo := fs.Bool("geo", false, "Enable geo index")
	m := fs.Int("m", 32, "HNSW M parameter")
	ef := fs.Int("ef", 400, "HNSW efConstruction parameter")
	shards := fs.Int("shards", 0, "Number of shards (0 for auto)")
	tqBits := fs.Int("tq_bits", 8, "TurboQuant bits (4 or 8)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *name == "" {
		log.Fatal("Dataset name is required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"name":            *name,
		"dimension":       *dims,
		"vector_type":     *vtype,
		"geo_enabled":     *geo,
		"hnsw_m":          *m,
		"hnsw_ef":         *ef,
		"num_shards":      *shards,
		"turboquant_bits": *tqBits,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "CreateDataset", Body: actionBody}
	_, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Create dataset failed: %v", err)
	}
	fmt.Printf("Dataset '%s' created\n", *name)
}

func runDrop(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("drop", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name to drop (required)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli drop -dataset <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	actionBody, _ := json.Marshal(map[string]string{"dataset": *dataset})
	action := &flight.Action{Type: "drop", Body: actionBody}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		log.Fatalf("Failed to drop dataset: %v", err)
	}

	result, err := stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive response: %v", err)
	}

	fmt.Printf("Dataset '%s' dropped successfully: %s\n", *dataset, string(result.Body))
}

func runImportArrow(ctx context.Context, sc *client.SmartClient, dataset, inputPath string) {
	start := time.Now()
	fmt.Printf("Importing Arrow file %s to dataset %s...\n", inputPath, dataset)

	f, err := os.Open(filepath.Clean(inputPath)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to open arrow file: %v\n", err)
	}
	defer f.Close()

	rdr, err := ipc.NewFileReader(f)
	if err != nil {
		log.Fatalf("Failed to create arrow reader: %v\n", err)
	}

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}

	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		log.Fatalf("DoPut stream failed: %v\n", err)
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(rdr.Schema()))
	writer.SetFlightDescriptor(desc)

	totalRows := int64(0)
	for i := 0; i < rdr.NumRecords(); i++ {
		rec, err := rdr.Record(i)
		if err != nil {
			log.Fatalf("Failed to read record %d: %v\n", i, err)
		}
		if err := writer.Write(rec); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}

	_ = writer.Close()
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Failed to close flight stream: %v\n", err)
	}
	_, _ = stream.Recv()

	fmt.Printf("Successfully imported %d rows from Arrow in %v\n", totalRows, time.Since(start))
}

func runExport(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Target dataset name (required)")
	output := fs.String("output", "", "Output file path (alias for --file)")
	fileFlag := fs.String("file", "", "Output file path (required)")
	compression := fs.String("compression", "", "Compression codec: lz4, zstd")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *fileFlag == "" && *output != "" {
		fileFlag = output
	}

	if *dataset == "" || *fileFlag == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli export --dataset <name> --file <output.arrow> [--compression lz4|zstd]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	// Connect to dataset via DoGet with dataset name ticket
	ticketBytes := []byte(*dataset)
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("DoGet failed: %v\n", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to create record reader: %v\n", err)
	}
	defer reader.Release()

	// Prepare output file
	f, err := os.Create(filepath.Clean(*fileFlag)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to create output file: %v\n", err)
	}
	defer f.Close()

	var opts []ipc.Option
	opts = append(opts, ipc.WithSchema(reader.Schema()))

	if *compression != "" {
		switch strings.ToLower(*compression) {
		case "lz4":
			opts = append(opts, ipc.WithLZ4())
		case "zstd":
			opts = append(opts, ipc.WithZstd())
		default:
			log.Fatalf("Unsupported compression codec: %s. Use 'lz4' or 'zstd'.\n", *compression)
		}
	}

	writer, err := ipc.NewFileWriter(f, opts...)
	if err != nil {
		log.Fatalf("Failed to create arrow writer: %v\n", err)
	}
	defer writer.Close()

	start := time.Now()
	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		if err := writer.Write(rec); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v\n", err)
	}

	fmt.Printf("Successfully exported %d rows to %s in %v\n", totalRows, *fileFlag, time.Since(start))
}

func runDownloadModel(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("download-model", flag.ExitOnError)
	repo := fs.String("repo", "", "Hugging Face repo ID (e.g., sentence-transformers/all-MiniLM-L6-v2) (required)")
	dest := fs.String("dest", "models", "Destination directory")
	_ = fs.Parse(args)

	if *repo == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli download-model -repo <repo_id> [-dest <path>]\n")
		os.Exit(1)
	}

	if err := onnx.DownloadModel(*repo, *dest); err != nil {
		log.Fatalf("Failed to download model: %v", err)
	}

	fmt.Printf("Successfully downloaded model %s to %s\n", *repo, *dest)
}
