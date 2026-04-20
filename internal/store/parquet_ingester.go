package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/parquet-go/parquet-go"
)

// ParquetIngester handles high-performance ingestion from Parquet files
type ParquetIngester struct {
	dataset    *Dataset
	ringReader *UringReader
	batchSize  int
}

func NewParquetIngester(ds *Dataset, batchSize int) *ParquetIngester {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}
	return &ParquetIngester{
		dataset:   ds,
		batchSize: batchSize,
	}
}

// Ingest performs high-throughput ingestion from a Parquet file.
// It uses io_uring for zero-copy reads on Linux if available.
func (pi *ParquetIngester) Ingest(ctx context.Context, path string) (int64, error) {
	start := time.Now()
	
	// 1. Try to use io_uring reader if on Linux
	var readerAt io.ReaderAt
	uring, err := NewUringReader(path)
	if err == nil {
		defer uring.Close()
		readerAt = uring
		pi.ringReader = uring
	} else {
		// Fallback to standard os.File
		f, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()
		readerAt = f
	}

	info, _ := os.Stat(path)
	pf, err := parquet.OpenFile(readerAt, info.Size())
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet: %w", err)
	}

	// 2. Use Generic Reader for type-safe ingestion
	pr := parquet.NewGenericReader[DatasetParquetRecord](pf)
	
	totalRows := int64(0)
	rows := make([]DatasetParquetRecord, pi.batchSize)
	
	for {
		select {
		case <-ctx.Done():
			return totalRows, ctx.Err()
		default:
		}

		n, err := pr.Read(rows)
		if n > 0 {
			// Ingest current batch
			batchRows := rows[:n]
			if err := pi.ingestBatch(ctx, batchRows); err != nil {
				return totalRows, fmt.Errorf("batch ingestion failed: %w", err)
			}
			totalRows += int64(n)
			
			// Update metrics
			metrics.IngestionTotal.WithLabelValues(pi.dataset.Name, "parquet").Add(float64(n))
		}
		
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalRows, fmt.Errorf("parquet read error: %w", err)
		}
	}

	duration := time.Since(start)
	metrics.IngestionDurationSeconds.WithLabelValues(pi.dataset.Name, "parquet").Observe(duration.Seconds())
	
	return totalRows, nil
}

func (pi *ParquetIngester) ingestBatch(ctx context.Context, batch []DatasetParquetRecord) error {
	// Conver DatasetParquetRecord to Arrow RecordBatch or directly into HNSW
	// For high throughput, we bypass gRPC and go straight to the indexer
	
	// 1. Prepare records for the dataset
	// (Implementation similar to readParquetToRecords but batched)
	// We'll use a temporary DatasetIO helper for now but optimized for this batch
	ioHelper := NewDatasetIO(nil) // vs is only used for logging/metrics in some paths
	
	// Map the batch to the dataset's record batches
	// Reusing the logic from dataset_io.go but avoiding the full-file read
	// In a real implementation, we'd have a more direct 'AppendParquetBatch' method
	_ = ioHelper
	
	// For this task, I'll update the dataset_io.go to expose a more efficient batch method
	// or implement it here.
	
	return pi.dataset.IngestBatch(batch)
}
