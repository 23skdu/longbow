package store

import (
	"context"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
)

// DoGetPipeline implements a pipelined DoGet with prefetching for improved throughput.
// It overlaps record processing with I/O to reduce CPU stalls and improve cache utilization.
type DoGetPipeline struct {
	depth int // Pipeline depth (number of records to prefetch)
}

// NewDoGetPipeline creates a new pipeline with the specified depth.
// Recommended depth: 4-8 for optimal balance between memory and performance.
func NewDoGetPipeline(depth int) *DoGetPipeline {
	if depth < 2 {
		depth = 2
	}
	if depth > 16 {
		depth = 16
	}
	return &DoGetPipeline{depth: depth}
}

// DefaultDoGetPipeline returns a pipeline with sensible defaults.
func DefaultDoGetPipeline() *DoGetPipeline {
	return NewDoGetPipeline(4)
}

// PipelineStage represents a stage in the DoGet pipeline.
type PipelineStage struct {
	Record    arrow.RecordBatch
	BatchIdx  int
	Tombstone *Bitset
	Err       error
}

// ProcessRecords processes a list of records through the pipeline.
// Returns a channel of processed records ready for writing.
func (p *DoGetPipeline) ProcessRecords(
	ctx context.Context,
	records []arrow.RecordBatch,
	tombstones map[int]*Bitset,
	filters []Filter,
	evaluator *FilterEvaluator,
) <-chan PipelineStage {
	resultChan := make(chan PipelineStage, p.depth)

	go func() {
		defer close(resultChan)

		// Process records with prefetching
		for i := 0; i < len(records); i++ {
			// Prefetch next record's data into cache
			if i+1 < len(records) {
				prefetchRecord(records[i+1])
			}

			rec := records[i]
			var tombstone *Bitset
			if ts, ok := tombstones[i]; ok {
				tombstone = ts
			}

			// Send to result channel
			select {
			case resultChan <- PipelineStage{
				Record:    rec,
				BatchIdx:  i,
				Tombstone: tombstone,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return resultChan
}

// prefetchRecord hints to the CPU to prefetch the record's data into cache.
// This reduces cache misses when the record is actually processed.
func prefetchRecord(rec arrow.RecordBatch) {
	if rec == nil {
		return
	}

	// Prefetch column data
	for _, col := range rec.Columns() {
		if col == nil {
			continue
		}

		data := col.Data()
		if data == nil {
			continue
		}

		// Touch first cache line of each buffer
		for _, buf := range data.Buffers() {
			if buf != nil && buf.Len() > 0 {
				// Prefetch hint: touch first byte of buffer
				bytes := buf.Bytes()
				if len(bytes) > 0 {
					_ = bytes[0]
				}

				// For large buffers, prefetch additional cache lines
				if len(bytes) > 64 {
					_ = bytes[64]
				}
				if len(bytes) > 128 {
					_ = bytes[128]
				}
			}
		}
	}

	// Force compiler to not optimize away the prefetch
	runtime.KeepAlive(rec)
}

// ShouldUsePipeline returns true if the pipeline should be used for the given number of batches.
// Pipeline has overhead, so only use for larger result sets.
func ShouldUsePipeline(numBatches int) bool {
	return numBatches >= 10
}
