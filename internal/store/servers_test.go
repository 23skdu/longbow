package store

import (
"context"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
"google.golang.org/grpc/codes"
"google.golang.org/grpc/status"
)

func TestDataServer_Unimplemented(t *testing.T) {
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, time.Hour)
server := NewDataServer(store)

err := server.ListFlights(nil, nil)
assert.Error(t, err)
st, ok := status.FromError(err)
assert.True(t, ok)
assert.Equal(t, codes.Unimplemented, st.Code())

_, err = server.GetFlightInfo(context.Background(), nil)
assert.Error(t, err)
st, ok = status.FromError(err)
assert.True(t, ok)
assert.Equal(t, codes.Unimplemented, st.Code())
}

func TestMetaServer_Unimplemented(t *testing.T) {
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, time.Hour)
server := NewMetaServer(store)

err := server.DoGet(nil, nil)
assert.Error(t, err)
st, ok := status.FromError(err)
assert.True(t, ok)
assert.Equal(t, codes.Unimplemented, st.Code())

err = server.DoPut(nil)
assert.Error(t, err)
st, ok = status.FromError(err)
assert.True(t, ok)
assert.Equal(t, codes.Unimplemented, st.Code())

}
