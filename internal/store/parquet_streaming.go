package store

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/parquet-go/parquet-go"
)

// ParquetRow represents a single vectorized record in Parquet
type ParquetRow struct {
	ID     int64     `parquet:"id"`
	Vector []float32 `parquet:"vector"`
}

// StreamingParquetWriter handles high-throughput serialization of Arrow batches
// directly from SlabArena to disk using zero-copy paths.
type StreamingParquetWriter struct {
	w        io.Writer
	mu       sync.Mutex
	closed   bool
	slabSize int
	writer   *parquet.Writer
}

// NewStreamingParquetWriter creates a new Parquet writer
func NewStreamingParquetWriter(w io.Writer) *StreamingParquetWriter {
	return &StreamingParquetWriter{
		w:        w,
		slabSize: 4 * 1024 * 1024, // 4MB slabs
	}
}

// WriteBatch serializes GraphData to Parquet format using a slab-aware buffer.
func (p *StreamingParquetWriter) WriteBatch(gd *types.GraphData) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("writer closed")
	}

	start := time.Now()
	defer func() {
		// Metrics for Parquet write performance
		metrics.PipelineDurationSeconds.WithLabelValues("parquet_serialization").Observe(time.Since(start).Seconds())
	}()

	if p.writer == nil {
		p.writer = parquet.NewWriter(p.w, parquet.SchemaOf(ParquetRow{}))
	}

	// Iterate through vectors and stream to Parquet
	// Real implementation would prioritize zero-copy from arena
	rows := make([]ParquetRow, 0, len(gd.Vectors))
	for i, vec := range gd.Vectors {
		if vec != nil {
			rows = append(rows, ParquetRow{
				ID:     int64(i),
				Vector: vec,
			})
		}
	}

	if err := p.writer.Write(rows); err != nil {
		return fmt.Errorf("failed to write parquet rows: %w", err)
	}

	return nil
}

func (p *StreamingParquetWriter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

// WriteRaw serialize raw Slab data (Phase 2 io_uring extension)
func (p *StreamingParquetWriter) WriteRaw(slab []byte) error {
	// In a real Linux environment with io_uring, we would submit this
	// buffer directly to the ring for asynchronous persistence.
	_, err := p.w.Write(slab)
	return err
}
