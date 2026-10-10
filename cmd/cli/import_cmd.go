package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/internal/onnx"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sbinet/npyio"
)

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
		if strings.HasPrefix(*file, "gs://") {
			runImportGCS(ctx, sc, *dataset, *file)
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
	uploader, err := sc.NewStreamUploader(ctx, dataset, sch)
	if err != nil {
		return err
	}
	defer uploader.Close()

	if err := uploader.WriteChunked(rec, 10000); err != nil {
		return err
	}
	return uploader.Close()
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

	uploader, err := sc.NewStreamUploader(ctx, dataset, rdr.Schema())
	if err != nil {
		log.Fatalf("Failed to create stream uploader: %v\n", err)
	}
	defer uploader.Close()

	totalRows := int64(0)
	for i := 0; i < rdr.NumRecords(); i++ {
		rec, err := rdr.Record(i)
		if err != nil {
			log.Fatalf("Failed to read record %d: %v\n", i, err)
		}
		if err := uploader.WriteChunked(rec, 10000); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}

	if err := uploader.Close(); err != nil {
		log.Fatalf("Failed to close stream uploader: %v\n", err)
	}

	fmt.Printf("Successfully imported %d rows from Arrow in %v\n", totalRows, time.Since(start))
}

func runDownloadModel(_ context.Context, args []string) {
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

func runImportGCS(ctx context.Context, sc *client.SmartClient, dataset, gcsPath string) {
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

	obj := gcsClient.Bucket(bucket).Object(key)
	r, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatalf("Failed to create GCS reader: %v\n", err)
	}
	defer r.Close()

	// Since we don't know if it's Parquet, Arrow, etc. from just gs://,
	// we'll assume Parquet for now or check extension if possible.
	ext := strings.ToLower(key)
	if strings.HasSuffix(ext, ".parquet") {
		// ParquetReader needs a ReaderAt and size, so we buffer to local temp file
		tmp, err := os.CreateTemp("", "longbow-gcs-*.parquet")
		if err != nil {
			log.Fatalf("Failed to create temp file: %v\n", err)
		}
		defer os.Remove(tmp.Name())
		defer tmp.Close()

		fmt.Printf("Buffering GCS object to temp file...\n")
		if _, err := io.Copy(tmp, r); err != nil {
			log.Fatalf("Failed to buffer GCS object: %v\n", err)
		}

		runImportParquet(ctx, sc, dataset, tmp.Name())
	} else if strings.HasSuffix(ext, ".arrow") {
		// Arrow stream can be read directly
		runImportArrowFromReader(ctx, sc, dataset, r)
	} else {
		log.Fatalf("Unsupported GCS file extension: %s. Only .parquet and .arrow are supported.\n", key)
	}
}

func runImportArrowFromReader(ctx context.Context, sc *client.SmartClient, dataset string, r io.Reader) {
	start := time.Now()
	fmt.Printf("Importing Arrow stream to dataset %s...\n", dataset)

	reader, err := ipc.NewReader(r)
	if err != nil {
		log.Fatalf("Failed to create arrow reader: %v\n", err)
	}
	defer reader.Release()

	uploader, err := sc.NewStreamUploader(ctx, dataset, reader.Schema())
	if err != nil {
		log.Fatalf("Failed to create stream uploader: %v\n", err)
	}
	defer uploader.Close()

	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		if err := uploader.WriteChunked(rec, 10000); err != nil {
			log.Fatalf("Failed to write record batch: %v\n", err)
		}
		totalRows += rec.NumRows()
	}

	if err := uploader.Close(); err != nil {
		log.Fatalf("Failed to close stream uploader: %v\n", err)
	}

	fmt.Printf("Successfully imported %d rows in %v\n", totalRows, time.Since(start))
}
