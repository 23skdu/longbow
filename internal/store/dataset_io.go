package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/parquet-go/parquet-go"
)

const (
	DatasetFileExtension = ".parquet"
	DatasetMagic         = "LONGDATASET"
	DatasetVersion       = 1
)

var datasetExportBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getDatasetBuffer() *bytes.Buffer {
	return datasetExportBufferPool.Get().(*bytes.Buffer)
}

func putDatasetBuffer(b *bytes.Buffer) {
	b.Reset()
	datasetExportBufferPool.Put(b)
}

type DatasetIO struct {
	vs *VectorStore
}

func NewDatasetIO(vs *VectorStore) *DatasetIO {
	return &DatasetIO{vs: vs}
}

type DatasetHeader struct {
	Magic      string    `json:"magic"`
	Version    int       `json:"version"`
	Name       string    `json:"name"`
	NumRecords int       `json:"num_records"`
	NumVectors int64     `json:"num_vectors"`
	SchemaJSON string    `json:"schema_json"`
	CreatedAt  time.Time `json:"created_at"`
	ExportedAt time.Time `json:"exported_at"`
	VectorDim  int       `json:"vector_dim"`
	VectorType string    `json:"vector_type"`
}

func (d *DatasetHeader) Validate() error {
	if d.Magic != DatasetMagic {
		return fmt.Errorf("invalid magic: expected %s, got %s", DatasetMagic, d.Magic)
	}
	if d.Version != DatasetVersion {
		return fmt.Errorf("unsupported version: %d", d.Version)
	}
	return nil
}

type DatasetParquetRecord struct {
	ID        int64  `parquet:"id,optional"`
	Vector    []byte `parquet:"vector,optional"`
	Metadata  []byte `parquet:"metadata,optional"`
	CreatedAt int64  `parquet:"created_at,optional"`
}

func (d *DatasetIO) ExportToParquet(ctx context.Context, name string, backend storage.SnapshotBackend) (int64, error) {
	startTime := time.Now()
	ds, ok := d.vs.getDataset(name)
	if !ok {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("dataset not found: %s", name)
	}

	ds.dataMu.RLock()
	numRecords := len(ds.Records)
	ds.dataMu.RUnlock()

	if numRecords == 0 {
		metrics.DatasetExportEmpty.WithLabelValues(name).Inc()
		return 0, nil
	}

	schemaJSON, err := json.Marshal(ds.Schema)
	if err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to marshal schema: %w", err)
	}

	var vectorDim int
	ds.dataMu.RLock()
	if len(ds.Records) > 0 {
		rec := ds.Records[0]
		for _, f := range rec.Schema().Fields() {
			if f.Name == "vector" {
				if fType, ok := f.Type.(*arrow.FixedSizeListType); ok {
					vectorDim = int(fType.Len())
				}
				break
			}
		}
	}
	ds.dataMu.RUnlock()

	header := &DatasetHeader{
		Magic:      DatasetMagic,
		Version:    DatasetVersion,
		Name:       name,
		NumRecords: numRecords,
		SchemaJSON: string(schemaJSON),
		CreatedAt:  time.Now(),
		ExportedAt: time.Now(),
		VectorDim:  vectorDim,
		VectorType: "fixed_size_list",
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to marshal header: %w", err)
	}

	headerBuf := getDatasetBuffer()
	defer putDatasetBuffer(headerBuf)
	_, _ = headerBuf.Write(headerJSON)      // #nosec G104
	_, _ = headerBuf.Write([]byte{'\n'}) // #nosec G104

	if err := backend.WriteSnapshotFile(ctx, name+".header", ".header", bytes.NewReader(headerBuf.Bytes())); err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to write header: %w", err)
	}

	parquetBuf := getDatasetBuffer()
	defer putDatasetBuffer(parquetBuf)
	ds.dataMu.RLock()
	totalVectors, err := d.writeRecordsToParquet(ds.Records, parquetBuf)
	ds.dataMu.RUnlock()

	if err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to write parquet: %w", err)
	}

	header.NumVectors = totalVectors
	metrics.DatasetExportVectors.WithLabelValues(name).Set(float64(totalVectors))

	if err := backend.WriteSnapshotFile(ctx, name, DatasetFileExtension, bytes.NewReader(parquetBuf.Bytes())); err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to write parquet: %w", err)
	}

	metrics.DatasetExportTotal.WithLabelValues(name).Inc()
	duration := time.Since(startTime)
	metrics.DatasetExportDuration.WithLabelValues(name).Observe(duration.Seconds())
	metrics.DatasetExportBytes.WithLabelValues(name).Observe(float64(parquetBuf.Len()))

	d.vs.logger.Info().
		Str("dataset", name).
		Int("records", numRecords).
		Int64("vectors", totalVectors).
		Int("bytes", parquetBuf.Len()).
		Dur("duration", duration).
		Msg("dataset exported to parquet")

	return totalVectors, nil
}

func (d *DatasetIO) writeRecordsToParquet(records []arrow.RecordBatch, buf *bytes.Buffer) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	pw := parquet.NewGenericWriter[DatasetParquetRecord](buf, parquet.Compression(&parquet.Zstd))
	defer func() { _ = pw.Close() }() // #nosec G104

	totalRows := int64(0)
	for _, rec := range records {
		numRows := rec.NumRows()
		if numRows == 0 {
			continue
		}
		totalRows += numRows

		idColIdx := -1
		vectorColIdx := -1
		metadataColIdx := -1
		createdAtColIdx := -1

		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idColIdx = i
			case "vector":
				vectorColIdx = i
			case "metadata":
				metadataColIdx = i
			case "created_at":
				createdAtColIdx = i
			}
		}

		records := make([]DatasetParquetRecord, numRows)
		for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
			record := DatasetParquetRecord{}

			if idColIdx >= 0 {
				col := rec.Column(idColIdx)
				switch arr := col.(type) {
				case *array.Int64:
					if !arr.IsNull(int(rowIdx)) {
						record.ID = arr.Value(int(rowIdx))
					}
				case *array.String:
					if !arr.IsNull(int(rowIdx)) {
						var id int64
						_, _ = fmt.Sscanf(arr.Value(int(rowIdx)), "%d", &id)
						record.ID = id
					}
				}
			}

			if vectorColIdx >= 0 {
				col := rec.Column(vectorColIdx)
				switch arr := col.(type) {
				case *array.FixedSizeList:
					dim := arr.Len()
					child := arr.ListValues()
					if floatArr, ok := child.(*array.Float32); ok {
						vec := make([]byte, dim*4)
						offset := int(rowIdx) * dim
						for i := 0; i < dim; i++ {
							binary.LittleEndian.PutUint32(vec[i*4:], math.Float32bits(floatArr.Value(offset+i)))
						}
						record.Vector = vec
					}
				case *array.FixedSizeBinary:
					if !arr.IsNull(int(rowIdx)) {
						record.Vector = arr.Value(int(rowIdx))
					}
				}
			}

			if metadataColIdx >= 0 {
				col := rec.Column(metadataColIdx)
				switch arr := col.(type) {
				case *array.Binary:
					if !arr.IsNull(int(rowIdx)) {
						record.Metadata = arr.Value(int(rowIdx))
					}
				case *array.String:
					if !arr.IsNull(int(rowIdx)) {
						record.Metadata = []byte(arr.Value(int(rowIdx)))
					}
				}
			}

			if createdAtColIdx >= 0 {
				col := rec.Column(createdAtColIdx)
				switch arr := col.(type) {
				case *array.Int64:
					if !arr.IsNull(int(rowIdx)) {
						record.CreatedAt = arr.Value(int(rowIdx))
					}
				}
			}

			records[rowIdx] = record
		}

		if _, err := pw.Write(records); err != nil {
			return totalRows, fmt.Errorf("failed to write rows: %w", err)
		}
		rec.Release()
	}

	if err := pw.Close(); err != nil {
		return totalRows, fmt.Errorf("failed to close writer: %w", err)
	}

	return totalRows, nil
}

func (d *DatasetIO) ImportFromParquet(ctx context.Context, snapshotName, datasetName string, backend storage.SnapshotBackend, schema *arrow.Schema) (int64, error) {
	startTime := time.Now()

	var header DatasetHeader

	headerFile, err := backend.ReadSnapshotFile(ctx, snapshotName, ".header")
	if err == nil {
		defer func() { _ = headerFile.Close() }() // #nosec G104
		headerData, err := io.ReadAll(headerFile)
		if err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, fmt.Errorf("failed to read header: %w", err)
		}

		if err := json.Unmarshal(headerData, &header); err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, fmt.Errorf("failed to parse header: %w", err)
		}

		if err := header.Validate(); err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, err
		}

		if schema == nil {
			if err := json.Unmarshal([]byte(header.SchemaJSON), &schema); err != nil {
				metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
				return 0, fmt.Errorf("failed to parse schema from header: %w", err)
			}
		}
	} else if !storage.IsNotFoundError(err) {
		metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
		return 0, fmt.Errorf("failed to read header: %w", err)
	}

	var parquetData []byte

	parquetFile, err := backend.ReadSnapshotFile(ctx, snapshotName, DatasetFileExtension)
	if err == nil {
		defer func() { _ = parquetFile.Close() }() // #nosec G104
		parquetData, err = io.ReadAll(parquetFile)
		if err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, fmt.Errorf("failed to read parquet data: %w", err)
		}
	} else {
		parquetFile, err = backend.ReadSnapshot(ctx, snapshotName)
		if err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, fmt.Errorf("failed to read parquet: %w", err)
		}
		defer func() { _ = parquetFile.Close() }() // #nosec G104
		parquetData, err = io.ReadAll(parquetFile)
		if err != nil {
			metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
			return 0, fmt.Errorf("failed to read parquet data: %w", err)
		}
	}

	ds, _ := d.vs.getOrCreateDataset(datasetName, func() *Dataset {
		return NewDataset(datasetName, schema)
	})

	totalRows, err := d.readParquetToRecords(bytes.NewReader(parquetData), ds)
	if err != nil {
		metrics.DatasetImportFailures.WithLabelValues(datasetName).Inc()
		return 0, fmt.Errorf("failed to read parquet: %w", err)
	}

	metrics.DatasetImportTotal.WithLabelValues(datasetName).Inc()
	metrics.DatasetImportVectors.WithLabelValues(datasetName).Set(float64(totalRows))
	duration := time.Since(startTime)
	metrics.DatasetImportDuration.WithLabelValues(datasetName).Observe(duration.Seconds())
	metrics.DatasetImportBytes.WithLabelValues(datasetName).Observe(float64(len(parquetData)))

	d.vs.logger.Info().
		Str("dataset", datasetName).
		Str("snapshot", snapshotName).
		Int64("vectors", totalRows).
		Int("bytes", len(parquetData)).
		Dur("duration", duration).
		Msg("dataset imported from parquet")

	return totalRows, nil
}

func (d *DatasetIO) readParquetToRecords(r io.Reader, ds *Dataset) (int64, error) {
	// Create a temporary file to support random access needed by Parquet
	tmpFile, err := os.CreateTemp("", "parquet-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Stream from reader to temp file
	if _, err := io.Copy(tmpFile, r); err != nil {
		_ = tmpFile.Close()
		return 0, fmt.Errorf("failed to stream to temp file: %w", err)
	}
	_ = tmpFile.Close()

	// Use ParquetIngester for optimized batch loading
	ingester := NewParquetIngester(ds, 2048) // Process in 2K row batches
	return ingester.Ingest(context.Background(), tmpPath)
}

func (d *DatasetIO) ExportToArrowIPC(ctx context.Context, name string, backend storage.SnapshotBackend) (int64, error) {
	startTime := time.Now()
	ds, ok := d.vs.getDataset(name)
	if !ok {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("dataset not found: %s", name)
	}

	ds.dataMu.RLock()
	numRecords := len(ds.Records)
	ds.dataMu.RUnlock()

	if numRecords == 0 {
		return 0, nil
	}

	buf := getDatasetBuffer()
	defer putDatasetBuffer(buf)

	writer := ipc.NewWriter(buf, ipc.WithSchema(ds.Schema))
	if writer == nil {
		return 0, fmt.Errorf("failed to create IPC writer")
	}

	ds.dataMu.RLock()
	totalRows := int64(0)
	for _, rec := range ds.Records {
		totalRows += rec.NumRows()
		if err := writer.Write(rec); err != nil {
			ds.dataMu.RUnlock()
			_ = writer.Close()
			return totalRows, fmt.Errorf("failed to write record: %w", err)
		}
	}
	ds.dataMu.RUnlock()

	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("failed to close writer: %w", err)
	}

	if err := backend.WriteSnapshotFile(ctx, name, ".arrow", bytes.NewReader(buf.Bytes())); err != nil {
		metrics.DatasetExportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to write arrow IPC: %w", err)
	}

	metrics.DatasetExportTotal.WithLabelValues(name).Inc()
	duration := time.Since(startTime)
	metrics.DatasetExportDuration.WithLabelValues(name).Observe(duration.Seconds())
	metrics.DatasetExportBytes.WithLabelValues(name).Observe(float64(buf.Len()))

	return totalRows, nil
}

func (d *DatasetIO) ImportFromArrowIPC(ctx context.Context, name string, backend storage.SnapshotBackend, schema *arrow.Schema) (int64, error) {
	startTime := time.Now()

	file, err := backend.ReadSnapshotFile(ctx, name, ".arrow")
	if err != nil {
		metrics.DatasetImportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to read arrow IPC: %w", err)
	}
	defer func() { _ = file.Close() }() // #nosec G104

	data, err := io.ReadAll(file)
	if err != nil {
		metrics.DatasetImportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to read arrow data: %w", err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithSchema(schema))
	if err != nil {
		metrics.DatasetImportFailures.WithLabelValues(name).Inc()
		return 0, fmt.Errorf("failed to create IPC reader: %w", err)
	}

	totalRows := int64(0)
	ds, _ := d.vs.getOrCreateDataset(name, func() *Dataset {
		return NewDataset(name, schema)
	})

	ds.dataMu.Lock()
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			break
		}
		totalRows += rec.NumRows()
		batchIdx := len(ds.Records)
		ds.Records = append(ds.Records, rec)
		ds.BatchNodes = append(ds.BatchNodes, -1)

		if d.vs.indexQueue != nil {
			job := IndexJob{
				DatasetName: name,
				Record:      rec,
				BatchIdx:    batchIdx,
				CreatedAt:   time.Now(),
			}
			d.vs.indexQueue.Send(job)
		}
	}
	ds.dataMu.Unlock()

	metrics.DatasetImportTotal.WithLabelValues(name).Inc()
	duration := time.Since(startTime)
	metrics.DatasetImportDuration.WithLabelValues(name).Observe(duration.Seconds())
	metrics.DatasetImportBytes.WithLabelValues(name).Observe(float64(len(data)))

	return totalRows, nil
}
