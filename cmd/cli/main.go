package main

import (
	"context"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/sbinet/npyio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	case "search":
		runSearch(ctx, os.Args[2:])
	case "create-namespace":
		runCreateNamespace(ctx, os.Args[2:])
	case "delete-namespace":
		runDeleteNamespace(ctx, os.Args[2:])
	case "list-namespaces":
		runListNamespaces(ctx, os.Args[2:])
	case "stats":
		runStats(ctx, os.Args[2:])
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
  import           Import parquet or npy files into a dataset
  search           Search vectors with Dense, Sparse, Filtered, or Hybrid modes
  create-namespace Create a new dataset namespace
  delete-namespace Delete a dataset namespace
  list-namespaces  List all dataset namespaces
  stats            Show dataset statistics

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

Use "longbow-cli <command> --help" for more information about a command.`)
}

func getClientURI(args []string) (string, []string) {
	uri := "127.0.0.1:3000"
	var remaining []string

	for i := 0; i < len(args); i++ {
		if args[i] == "-uri" && i+1 < len(args) {
			uri = args[i+1]
			i++
		} else if strings.HasPrefix(args[i], "-uri=") {
			uri = strings.TrimPrefix(args[i], "-uri=")
		} else {
			remaining = append(remaining, args[i])
		}
	}

	return uri, remaining
}

func mustGetClient(uri string) *client.SmartClient {
	sc, err := client.NewSmartClient(uri)
	if err != nil {
		log.Fatalf("Failed to connect to Longbow at %s: %v", uri, err)
	}
	return sc
}

func runImport(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Target dataset name (required)")
	input := fs.String("input", "", "Input file path. Supports .parquet and .npy")
	dim := fs.Int("dim", 128, "Vector dimension (used for demo data)")
	count := fs.Int("count", 1000, "Number of vectors to generate (used for demo data if no input file)")
	fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli import -dataset <name> [-input <file>] [-dim <n>] [-count <n>]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
	defer sc.Close()

	if *input != "" {
		ext := strings.ToLower(*input)
		if strings.HasSuffix(ext, ".parquet") {
			runImportParquet(ctx, sc, *dataset, *input)
		} else if strings.HasSuffix(ext, ".npy") {
			runImportNpy(ctx, sc, *dataset, *input)
		} else {
			log.Fatalf("Unsupported file format: %s. Only .parquet and .npy are supported.\n", *input)
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

	f, err := os.Open(inputPath)
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

	writer.Close()
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Failed to close flight stream: %v\n", err)
	}
	stream.Recv()

	fmt.Printf("Successfully imported %d rows in %v\n", totalRows, time.Since(start))
}

func runImportNpy(ctx context.Context, sc *client.SmartClient, dataset, inputPath string) {
	start := time.Now()
	fmt.Printf("Importing NumPy file %s to dataset %s...\n", inputPath, dataset)

	f, err := os.Open(inputPath)
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
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32)
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
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32)
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
}

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
		writer.Close()
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
	fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli search -dataset <name> -mode <dense|sparse|filtered|hybrid> [options]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
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
		fmt.Sscanf(strings.TrimSpace(p), "%f", &f)
		result[i] = float32(f)
	}
	return result
}

func parseFilterExpression(s string) (interface{}, error) {
	data, err := os.ReadFile(s)
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
	fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli create-namespace -name <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
	defer sc.Close()

	actionBody, _ := json.Marshal(map[string]string{"namespace": *name})
	action := &flight.Action{Type: "create_namespace", Body: actionBody}

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
	fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli delete-namespace -name <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
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


func runAlterNamespace(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("alter-namespace", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	action := fs.String("action", "", "add or drop (required)")
	column := fs.String("column", "", "Column name (required)")
	typ := fs.String("type", "", "Data type if adding (e.g. float32, string, int64)")
	fs.Parse(args)

	if *dataset == "" || *action == "" || *column == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli alter-namespace -dataset <name> -action <add|drop> -column <name> [-type <type>]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
	defer sc.Close()

	payload := map[string]string{
		"dataset": *dataset,
		"action":  *action,
		"column":  *column,
		"type":    *typ,
	}
	actionBody, _ := json.Marshal(payload)
	act := &flight.Action{Type: "alter_schema", Body: actionBody}

	res, err := sc.DoAction(ctx, act)
	if err != nil {
		log.Fatalf("Alter namespace failed: %v\n", err)
	}
	
	msg := ""
	for {
		r, err := res.Recv()
		if err != nil { break }
		msg += string(r.Body)
	}
	fmt.Printf("Success: %s\n", msg)
}

func runListNamespaces(ctx context.Context, args []string) {
	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
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

func runStats(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	name := fs.String("dataset", "", "Dataset name (required)")
	fs.Parse(args)

	if *name == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli stats -dataset <name> [-uri <uri>]\n")
		os.Exit(1)
	}

	uri, _ := getClientURI(args)
	sc := mustGetClient(uri)
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
}

func generateRandomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}
