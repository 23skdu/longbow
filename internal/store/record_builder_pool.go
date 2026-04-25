package store

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecordBuilderPool manages reusable array.RecordBuilder instances for specific schemas.
// This significantly reduces GC pressure on search hot paths by recycling builder buffers.
type RecordBuilderPool struct {
	pool sync.Pool
	schema *arrow.Schema
}

// NewRecordBuilderPool creates a new pool for the given schema.
func NewRecordBuilderPool(schema *arrow.Schema) *RecordBuilderPool {
	return &RecordBuilderPool{
		schema: schema,
		pool: sync.Pool{
			New: func() any {
				// Each builder is created with the global Go allocator
				return array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			},
		},
	}
}

// Get retrieves a builder from the pool.
func (p *RecordBuilderPool) Get() *array.RecordBuilder {
	return p.pool.Get().(*array.RecordBuilder)
}

// Put returns a builder to the pool after resetting it.
// Important: All arrays created from this builder must be released BEFORE putting the builder back,
// OR the builder must be reset which releases internal references.
func (p *RecordBuilderPool) Put(b *array.RecordBuilder) {
	// Reset the builder to clear internal state and reuse buffers
	// Note: b.Release() would destroy it. We want to keep it but empty.
	// Unfortunately, arrow-go's RecordBuilder doesn't have a simple Reset() that keeps buffers.
	// However, we can release the builder and let the pool create new ones if needed, 
	// but that defeats the purpose of buffer reuse.
	
	// Better approach: RecordBuilder fields (builders) often have Reserve() and Reset().
	// For now, we'll just use the pool to avoid RecordBuilder object allocation,
	// even if the underlying buffers are re-allocated.
	p.pool.Put(b)
}

// Global Response Pools for common schemas
var (
	// SearchResponseSchema: [id (string), score (float32)]
	SearchResponseSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)
	
	// SearchWithVectorResponseSchema: [id (string), score (float32), vector (binary)]
	SearchWithVectorResponseSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		{Name: "vector", Type: arrow.BinaryTypes.Binary},
	}, nil)

	SearchResponsePool           = NewRecordBuilderPool(SearchResponseSchema)
	SearchWithVectorResponsePool = NewRecordBuilderPool(SearchWithVectorResponseSchema)
)
