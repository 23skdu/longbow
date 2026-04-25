package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockDoActionServerCoverage struct {
	grpc.ServerStream
	results []*flight.Result
}

func (m *mockDoActionServerCoverage) Send(res *flight.Result) error {
	m.results = append(m.results, res)
	return nil
}

func (m *mockDoActionServerCoverage) Context() context.Context {
	return context.Background()
}

func TestVectorStore_DoAction_Extended(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer s.Close()

	// Create a dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	
	s.getOrCreateDataset("test-ds", func() *Dataset {
		ds := NewDataset("test-ds", schema)
		
		// Add some data
		b := array.NewRecordBuilder(mem, schema)
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{101, 102}, nil)
		b.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, nil)
		rec := b.NewRecordBatch()
		ds.Records = []arrow.RecordBatch{rec}
		
		return ds
	})

	t.Run("check_readiness", func(t *testing.T) {
		stream := &mockDoActionServerCoverage{}
		body, _ := json.Marshal(map[string]string{"dataset": "test-ds"})
		err := s.DoAction(&flight.Action{Type: "check_readiness", Body: body}, stream)
		require.NoError(t, err)
		assert.Len(t, stream.results, 1)
		
		var resp map[string]any
		json.Unmarshal(stream.results[0].Body, &resp)
		assert.Equal(t, "READY", resp["status"])
	})

	t.Run("delete_linear_scan", func(t *testing.T) {
		stream := &mockDoActionServerCoverage{}
		// 101 exists in test-ds
		body, _ := json.Marshal(map[string]string{"dataset": "test-ds", "id": "101"})
		err := s.DoAction(&flight.Action{Type: "delete", Body: body}, stream)
		require.NoError(t, err)
		assert.Equal(t, "deleted", string(stream.results[0].Body))
		
		ds, _ := s.getDataset("test-ds")
		assert.True(t, ds.Tombstones[0].Contains(0))
	})

	t.Run("alter_schema_add", func(t *testing.T) {
		stream := &mockDoActionServerCoverage{}
		body, _ := json.Marshal(map[string]string{
			"dataset": "test-ds",
			"action":  "add",
			"column":  "new_col",
			"type":    "int32",
		})
		err := s.DoAction(&flight.Action{Type: "alter_schema", Body: body}, stream)
		require.NoError(t, err)
		
		ds, _ := s.getDataset("test-ds")
		indices := ds.Schema.FieldIndices("new_col")
		assert.NotEmpty(t, indices)
	})

	t.Run("delete-dataset", func(t *testing.T) {
		stream := &mockDoActionServerCoverage{}
		body, _ := json.Marshal(map[string]string{"dataset": "test-ds"})
		err := s.DoAction(&flight.Action{Type: "delete-dataset", Body: body}, stream)
		require.NoError(t, err)
		
		_, found := s.getDataset("test-ds")
		assert.False(t, found)
	})
}
