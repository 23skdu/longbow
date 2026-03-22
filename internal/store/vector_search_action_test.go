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

func TestVectorSearchAction_EfSearchValidation(t *testing.T) {
	tests := []struct {
		name        string
		efSearch    int
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid efSearch 16",
			efSearch:    16,
			expectError: false,
		},
		{
			name:        "valid efSearch 4096",
			efSearch:    4096,
			expectError: false,
		},
		{
			name:        "valid efSearch 256",
			efSearch:    256,
			expectError: false,
		},
		{
			name:        "invalid efSearch below range",
			efSearch:    15,
			expectError: true,
			errorMsg:    "ef_search must be between 16 and 4096",
		},
		{
			name:        "invalid efSearch above range",
			efSearch:    4097,
			expectError: true,
			errorMsg:    "ef_search must be between 16 and 4096",
		},
		{
			name:        "invalid efSearch negative",
			efSearch:    -1,
			expectError: true,
			errorMsg:    "ef_search must be between 16 and 4096",
		},
		{
			name:        "zero efSearch uses default",
			efSearch:    0,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewGoAllocator()
			logger := zerolog.Nop()
			vs := NewVectorStore(mem, logger, 1024*1024, 1024*1024, time.Second)
			defer vs.Close()

			req := query.VectorSearchRequest{
				Dataset:  "test_dataset",
				Vector:   []float32{1.0, 2.0, 3.0},
				K:        10,
				EfSearch: tt.efSearch,
			}
			reqBytes, err := json.Marshal(req)
			require.NoError(t, err)

			action := &flight.Action{
				Type: "VectorSearch",
				Body: reqBytes,
			}

			stream := &mockVectorSearchDoActionServer{}
			err = vs.handleVectorSearchAction(action, stream)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "not found")
			}
		})
	}
}
