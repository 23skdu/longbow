package store

import (
	"encoding/json"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func FuzzEfSearchParameter(f *testing.F) {
	f.Fuzz(func(t *testing.T, efSearch int) {
		mem := memory.NewGoAllocator()
		logger := zerolog.Nop()
		vs := NewVectorStore(mem, logger, 1024*1024, 1024*1024, 0)
		defer vs.Close()

		req := query.VectorSearchRequest{
			Dataset:  "test_dataset",
			Vector:   []float32{1.0, 2.0, 3.0},
			K:        10,
			EfSearch: efSearch,
		}
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		action := &flight.Action{
			Type: "VectorSearch",
			Body: reqBytes,
		}

		stream := &mockVectorSearchDoActionServer{}
		_ = vs.handleVectorSearchAction(action, stream)
	})
}

func FuzzVectorSearchRequestParsing(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var req query.VectorSearchRequest
		_ = json.Unmarshal(data, &req)

		if req.EfSearch < 16 || req.EfSearch > 4096 {
			t.Logf("efSearch out of range: %d", req.EfSearch)
		}
	})
}
