package store

import (
"context"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
)

func TestStore_UpdateConfig(t *testing.T) {
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

store.UpdateConfig(2048, 0, 30*time.Minute)
store.globalMu.RLock()
assert.Equal(t, int64(2048), store.maxMemory)
store.globalMu.RUnlock()
}

func TestStore_DoAction(t *testing.T) {
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

// Test get_stats
action := &flight.Action{Type: "get_stats"}
stream := &mockDoActionServer{}
err := store.DoAction(action, stream)
assert.NoError(t, err)
assert.Contains(t, string(stream.lastResult.Body), "datasets")

// Test drop_dataset
actionDrop := &flight.Action{Type: "drop_dataset", Body: []byte("test")}
err = store.DoAction(actionDrop, stream)
assert.NoError(t, err)
assert.Contains(t, string(stream.lastResult.Body), "dataset dropped")

// Test unknown action
actionUnknown := &flight.Action{Type: "unknown"}
err = store.DoAction(actionUnknown, stream)
assert.Error(t, err)
}

type mockDoActionServer struct {
flight.FlightService_DoActionServer
lastResult *flight.Result
}

func (m *mockDoActionServer) Send(r *flight.Result) error {
m.lastResult = r
return nil
}

func (m *mockDoActionServer) Context() context.Context {
return context.Background()
}
