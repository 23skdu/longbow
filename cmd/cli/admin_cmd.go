package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func runCreateNamespace(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("create-namespace", flag.ExitOnError)
	name := fs.String("name", "", "Namespace name (required)")
	dims := fs.Int("dims", 128, "Vector dimensions")
	dtype := fs.String("data_type", "float32", "Data type (float32, int8, turboquant2, turboquant4, turboquant8)")
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

	actionBody, _ := json.Marshal(map[string]string{"name": *name})
	action := &flight.Action{Type: "DeleteNamespace", Body: actionBody}

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

	action := &flight.Action{Type: "ListNamespaces"}

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
	action := &flight.Action{Type: "DiscoveryStatus", Body: actionBody}

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
	action := &flight.Action{Type: "Delete", Body: actionBody}

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
	action := &flight.Action{Type: "DropDataset", Body: actionBody}

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

	originalFileFlag := *fileFlag
	if strings.HasPrefix(originalFileFlag, "s3://") || strings.HasPrefix(originalFileFlag, "gs://") {
		tmp, err := os.CreateTemp("", "longbow-export-*.arrow")
		if err != nil {
			log.Fatalf("Failed to create temp file for remote export: %v\n", err)
		}
		// Close the initial file handle and replace it with the temp file
		_ = f.Close() // #nosec G104
		f = tmp
	}

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

	if strings.HasPrefix(*fileFlag, "s3://") {
		// Upload temp file to S3
		_ = f.Close() // #nosec G104
		runExportS3(ctx, *fileFlag, f.Name())
	} else if strings.HasPrefix(*fileFlag, "gs://") {
		// Upload temp file to GCS
		_ = f.Close() // #nosec G104
		runExportGCS(ctx, *fileFlag, f.Name())
	}

	fmt.Printf("Successfully exported %d rows to %s in %v\n", totalRows, *fileFlag, time.Since(start))
}

func runExportS3(ctx context.Context, s3Path, localPath string) {
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

	f, err := os.Open(filepath.Clean(localPath)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to open local file: %v\n", err)
	}
	defer f.Close()

	fmt.Printf("Uploading exported data to S3: %s\n", s3Path)
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		log.Fatalf("Failed to upload to S3: %v\n", err)
	}
	fmt.Printf("Successfully uploaded export to %s\n", s3Path)
}

func runExportGCS(ctx context.Context, gcsPath, localPath string) {
	// Parse gs://bucket/key
	u := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(u, "/", 2)
	if len(parts) < 2 {
		log.Fatalf("Invalid GCS path: %s. Expected gs://bucket/key\n", gcsPath)
	}
	bucket, key := parts[0], parts[1]

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v\n", err)
	}
	defer gcsClient.Close()

	f, err := os.Open(filepath.Clean(localPath)) // #nosec G304
	if err != nil {
		log.Fatalf("Failed to open local file: %v\n", err)
	}
	defer f.Close()

	fmt.Printf("Uploading exported data to GCS: %s\n", gcsPath)
	w := gcsClient.Bucket(bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		log.Fatalf("Failed to upload to GCS: %v\n", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("Failed to close GCS writer: %v\n", err)
	}
	fmt.Printf("Successfully uploaded export to %s\n", gcsPath)
}
