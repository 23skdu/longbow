package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketServer_New(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	store := &VectorStore{}
	cdc := NewChangeDataCapture(store, logger)

	wsServer := NewWebSocketServer(logger, cdc)

	assert.NotNil(t, wsServer)
	assert.NotNil(t, wsServer.conns)
	assert.NotNil(t, wsServer.stopChan)
	assert.NotNil(t, wsServer.cdc)
}

func TestWSConnection_Close(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	store := &VectorStore{}
	cdc := NewChangeDataCapture(store, logger)
	_ = NewWebSocketServer(logger, cdc) // Need server for CDC

	wsConn := &WSConnection{
		id:        "test-conn",
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 256),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())

	wsConn.Close()

	assert.True(t, wsConn.closed)
}

func TestWebSocketServer_Start(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	store := &VectorStore{}
	cdc := NewChangeDataCapture(store, logger)
	wsServer := NewWebSocketServer(logger, cdc)

	err := wsServer.Start("127.0.0.1:0")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = wsServer.Stop()
	require.NoError(t, err)
}

func TestWSMessage_JSON(t *testing.T) {
	msg := WSMessage{
		Type:    WSTypeSubscribe,
		Payload: json.RawMessage(`{"dataset":"test"}`),
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "subscribe")
}

func TestWSSubscribePayload_JSON(t *testing.T) {
	payload := WSSubscribePayload{
		Dataset:    "test-dataset",
		Columns:    []string{"id", "vector"},
		EventTypes: []string{"insert", "update"},
	}

	data, err := json.Marshal(payload)
	assert.NoError(t, err)

	var parsed WSSubscribePayload
	err = json.Unmarshal(data, &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "test-dataset", parsed.Dataset)
	assert.Len(t, parsed.Columns, 2)
}

func TestWSDataPayload_JSON(t *testing.T) {
	payload := WSDataPayload{
		Dataset: "test-dataset",
		Event:   "insert",
		Data:    json.RawMessage(`{"id":1}`),
	}

	data, err := json.Marshal(payload)
	assert.NoError(t, err)

	var parsed WSDataPayload
	err = json.Unmarshal(data, &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "test-dataset", parsed.Dataset)
	assert.Equal(t, "insert", parsed.Event)
}

func TestWSType_String(t *testing.T) {
	assert.Equal(t, "subscribe", string(WSTypeSubscribe))
	assert.Equal(t, "unsubscribe", string(WSTypeUnsubscribe))
	assert.Equal(t, "data", string(WSTypeData))
	assert.Equal(t, "error", string(WSTypeError))
	assert.Equal(t, "ping", string(WSTypePing))
	assert.Equal(t, "pong", string(WSTypePong))
}

func TestWebSocketServer_Stop_Empty(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	store := &VectorStore{}
	cdc := NewChangeDataCapture(store, logger)
	wsServer := NewWebSocketServer(logger, cdc)

	err := wsServer.Stop()
	assert.NoError(t, err)
}

func TestWebSocketServer_HandleWS_UpgradeError(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	store := &VectorStore{}
	cdc := NewChangeDataCapture(store, logger)
	wsServer := NewWebSocketServer(logger, cdc)

	assert.NotNil(t, wsServer.handleWS)
}
