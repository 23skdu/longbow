// alias-only: not re-exported
package apihandlers

import (
	"context"
	"net/http"
)

// WebSocketServerState reports whether the internal websocket server state was
// initialized. It is a narrow helper for host white-box tests.
type WebSocketServerState struct {
	HasConns    bool
	HasStopChan bool
	HasCDC      bool
}

// InspectWebSocketServer exposes the websocket server's initialized state to
// host white-box tests.
func InspectWebSocketServer(server *WebSocketServer) WebSocketServerState {
	return WebSocketServerState{
		HasConns:    server.conns != nil,
		HasStopChan: server.stopChan != nil,
		HasCDC:      server.cdc != nil,
	}
}

// NewTestWSConnection builds a websocket connection for host white-box tests.
func NewTestWSConnection(id string) *WSConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSConnection{
		id:        id,
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 256),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// WSConnectionClosed reports whether a websocket connection has been closed.
func WSConnectionClosed(conn *WSConnection) bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	return conn.closed
}

// HandleWS exposes the websocket upgrade handler to host white-box tests.
func HandleWS(server *WebSocketServer, w http.ResponseWriter, r *http.Request) {
	server.handleWS(w, r)
}

// HandleWSFunc exposes the websocket upgrade handler as a function value.
func HandleWSFunc(server *WebSocketServer) func(http.ResponseWriter, *http.Request) {
	return server.handleWS
}

// NewWSConnectionWithContext builds a websocket connection carrying a tenant
// context. It is a narrow helper for host white-box tests.
func NewWSConnectionWithContext(ctx context.Context) *WSConnection {
	testCtx, cancel := context.WithCancel(ctx)
	return &WSConnection{ctx: testCtx, cancel: cancel}
}

// AuthorizeDatasetSubscription exposes websocket subscription authorization to
// host white-box tests.
func AuthorizeDatasetSubscription(server *WebSocketServer, conn *WSConnection, dataset string) error {
	return server.authorizeDatasetSubscription(conn, dataset)
}
