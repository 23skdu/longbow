package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockVectorSearchDoActionServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

func (m *mockVectorSearchDoActionServer) Send(res *flight.Result) error {
	m.results = append(m.results, res)
	return nil
}

func (m *mockVectorSearchDoActionServer) Context() context.Context {
	return context.Background()
}

func TestVectorSearchAction_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024, 1024*1024, time.Second)
	defer vs.Close()

	req := query.VectorSearchRequest{
		Dataset: "test_dataset",
		Vector:  []float32{1.0, 2.0, 3.0},
		K:       10,
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	stream := &mockVectorSearchDoActionServer{}
	err = vs.handleVectorSearchAction(action, stream)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
