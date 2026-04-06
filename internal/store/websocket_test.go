package store

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketServer_NewWebSocketServer(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	assert.NotNil(t, server)
	assert.NotNil(t, server.conns)
	assert.NotNil(t, server.stopChan)
}

func TestWebSocketServer_Start(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	err := server.Start(":9876")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:9876/ws")
	if err == nil {
		resp.Body.Close()
	}

	err = server.Stop()
	require.NoError(t, err)
}

func TestWebSocketServer_HandleMessage(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)
	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())
	defer wsConn.cancel()

	t.Run("ping", func(t *testing.T) {
		msg := WSMessage{Type: WSTypePing}
		data, _ := json.Marshal(msg)

		err := server.handleMessage(wsConn, data)
		require.NoError(t, err)
	})

	t.Run("unknown type", func(t *testing.T) {
		msg := WSMessage{Type: "unknown"}
		data, _ := json.Marshal(msg)

		err := server.handleMessage(wsConn, data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown message type")
	})

	t.Run("invalid json", func(t *testing.T) {
		err := server.handleMessage(wsConn, []byte("invalid json"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid message format")
	})
}

func TestWebSocketServer_HandleSubscribe(t *testing.T) {
	store := &VectorStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)
	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())
	defer wsConn.cancel()

	t.Run("valid subscribe", func(t *testing.T) {
		payload := WSSubscribePayload{
			Dataset:    "test-dataset",
			Columns:    []string{"vector"},
			EventTypes: []string{"INSERT"},
		}
		data, _ := json.Marshal(payload)

		msg := WSMessage{
			Type:    WSTypeSubscribe,
			Payload: data,
		}
		msgData, _ := json.Marshal(msg)

		err := server.handleMessage(wsConn, msgData)
		require.NoError(t, err)

		wsConn.mu.RLock()
		sub, ok := wsConn.subs["test-dataset"]
		wsConn.mu.RUnlock()
		assert.True(t, ok)
		assert.NotNil(t, sub)
	})

	t.Run("missing dataset", func(t *testing.T) {
		payload := WSSubscribePayload{
			Dataset: "",
		}
		data, _ := json.Marshal(payload)

		msg := WSMessage{
			Type:    WSTypeSubscribe,
			Payload: data,
		}
		msgData, _ := json.Marshal(msg)

		err := server.handleMessage(wsConn, msgData)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset is required")
	})
}

func TestWebSocketServer_HandleUnsubscribe(t *testing.T) {
	store := &VectorStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)
	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())
	defer wsConn.cancel()

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 64)
	require.NoError(t, err)

	wsConn.mu.Lock()
	wsConn.subs["test-dataset"] = sub
	wsConn.mu.Unlock()

	payload := struct {
		Dataset string `json:"dataset"`
	}{Dataset: "test-dataset"}
	data, _ := json.Marshal(payload)

	msg := WSMessage{
		Type:    WSTypeUnsubscribe,
		Payload: data,
	}
	msgData, _ := json.Marshal(msg)

	err = server.handleMessage(wsConn, msgData)
	require.NoError(t, err)

	wsConn.mu.RLock()
	_, ok := wsConn.subs["test-dataset"]
	wsConn.mu.RUnlock()
	assert.False(t, ok)
}

func TestWebSocketServer_ConnectionCount(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	count := server.GetConnectionCount()
	assert.Equal(t, 0, count)

	ids := server.GetConnectionIDs()
	assert.Empty(t, ids)
}

func TestWSTypeConstants(t *testing.T) {
	assert.Equal(t, WSMessageType("subscribe"), WSTypeSubscribe)
	assert.Equal(t, WSMessageType("unsubscribe"), WSTypeUnsubscribe)
	assert.Equal(t, WSMessageType("data"), WSTypeData)
	assert.Equal(t, WSMessageType("error"), WSTypeError)
	assert.Equal(t, WSMessageType("ping"), WSTypePing)
	assert.Equal(t, WSMessageType("pong"), WSTypePong)
}

func TestWSMessage_JSON(t *testing.T) {
	msg := WSMessage{
		Type:    WSTypeData,
		Payload: json.RawMessage(`{"test":true}`),
		Error:   "",
	}

	data, err := json.Marshal(msg)
	require.NoError(t, err)

	var parsed WSMessage
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, WSTypeData, parsed.Type)
	assert.Equal(t, `{"test":true}`, string(parsed.Payload))
}

func TestWSSubscribePayload_JSON(t *testing.T) {
	payload := WSSubscribePayload{
		Dataset:    "my-dataset",
		Columns:    []string{"col1", "col2"},
		EventTypes: []string{"INSERT", "UPDATE"},
	}

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var parsed WSSubscribePayload
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "my-dataset", parsed.Dataset)
	assert.Equal(t, []string{"col1", "col2"}, parsed.Columns)
	assert.Equal(t, []string{"INSERT", "UPDATE"}, parsed.EventTypes)
}

func TestWebSocketServer_WebSocketUpgrade(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	err := server.Start(":9878")
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	count := server.GetConnectionCount()
	assert.Equal(t, 0, count)
}

func TestWSConnection_Close(t *testing.T) {
	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())

	assert.False(t, wsConn.closed)

	wsConn.Close()
	assert.True(t, wsConn.closed)
}

func TestWSConnection_MultipleClose(t *testing.T) {
	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())

	wsConn.Close()
	assert.True(t, wsConn.closed)

	wsConn.Close()
	assert.True(t, wsConn.closed)
}

func TestWebSocketServer_SendError(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	wsConn := &WSConnection{
		id:        "test-conn",
		writeChan: make(chan []byte, 64),
		closed:    false,
	}

	server.sendError(wsConn, "test error")

	select {
	case msg := <-wsConn.writeChan:
		var wsMsg WSMessage
		err := json.Unmarshal(msg, &wsMsg)
		require.NoError(t, err)
		assert.Equal(t, WSTypeError, wsMsg.Type)
		assert.Equal(t, "test error", wsMsg.Error)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error message")
	}
}

func TestWebSocketServer_RemoveConnection(t *testing.T) {
	store := &VectorStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	server := NewWebSocketServer(logger, cdc)

	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 64),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 64)
	require.NoError(t, err)

	wsConn.mu.Lock()
	wsConn.subs["test-dataset"] = sub
	wsConn.mu.Unlock()

	server.removeConnection(wsConn)

	assert.True(t, wsConn.closed)
	assert.Equal(t, 0, server.GetConnectionCount())
}
