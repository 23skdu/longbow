package streaming

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BuildStreamArrowBatch converts stream events into an Arrow record batch.
// It is exported for host packages that need the encoding while keeping the
// streaming domain free of *VectorStore/*Dataset references.
func BuildStreamArrowBatch(alloc memory.Allocator, events []StreamVectorEvent) (arrow.RecordBatch, error) {
	return buildStreamArrowBatch(alloc, events)
}

// IngestEvents applies a batch of stream events through the host interface.
// This is the narrow helper used by the API handler domain.
func (c *StreamConnector) IngestEvents(ctx context.Context, events []StreamVectorEvent) error {
	return c.ingestEvents(ctx, events)
}
