package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type WSMessageType string

const (
	WSTypeSubscribe   WSMessageType = "subscribe"
	WSTypeUnsubscribe WSMessageType = "unsubscribe"
	WSTypeData        WSMessageType = "data"
	WSTypeError       WSMessageType = "error"
	WSTypePing        WSMessageType = "ping"
	WSTypePong        WSMessageType = "pong"
)

type WSMessage struct {
	Type    WSMessageType   `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type WSSubscribePayload struct {
	Dataset    string   `json:"dataset"`
	Columns    []string `json:"columns,omitempty"`
	EventTypes []string `json:"event_types,omitempty"`
}

type WSDataPayload struct {
	Dataset string          `json:"dataset"`
	Event   string          `json:"event"`
	Data    json.RawMessage `json:"data"`
}

type WebSocketServer struct {
	logger     zerolog.Logger
	cdc        *ChangeDataCapture
	conns      map[*websocket.Conn]*WSConnection
	connMu     sync.RWMutex
	httpServer *http.Server
	serverWg   sync.WaitGroup
	stopChan   chan struct{}
	pool       *WSConnectionPool
}

type WSConnectionPool struct {
	mu          sync.Mutex
	available   []*WSConnection
	active      map[string]*WSConnection
	maxSize     int
	idleTimeout time.Duration
}

func NewWSConnectionPool(maxSize int, idleTimeout time.Duration) *WSConnectionPool {
	return &WSConnectionPool{
		active:      make(map[string]*WSConnection),
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
	}
}

func (p *WSConnectionPool) Add(conn *WSConnection) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.active) >= p.maxSize {
		return fmt.Errorf("connection pool exhausted")
	}

	p.active[conn.id] = conn
	return nil
}

func (p *WSConnectionPool) Remove(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.active[id]; ok {
		delete(p.active, id)
		conn.Close()
	}
}

func (p *WSConnectionPool) Get(id string) (*WSConnection, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, ok := p.active[id]
	return conn, ok
}

func (p *WSConnectionPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active)
}

func (p *WSConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.active {
		conn.Close()
	}
	p.active = make(map[string]*WSConnection)
}

type WSConnection struct {
	id        string
	conn      *websocket.Conn
	subs      map[string]*CDCSubscription
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	writeChan chan []byte
	closed    bool
}

func (ws *WSConnection) Close() {
	ws.mu.Lock()
	if ws.closed {
		ws.mu.Unlock()
		return
	}
	ws.closed = true
	ws.cancel()
	if ws.conn != nil {
		ws.conn.Close()
	}
	close(ws.writeChan)
	ws.mu.Unlock()
}

const (
	DefaultWSMaxConnections = 1000
	DefaultWSIdleTimeout    = 5 * time.Minute
)

func NewWebSocketServer(logger zerolog.Logger, cdc *ChangeDataCapture) *WebSocketServer {
	return &WebSocketServer{
		logger:   logger,
		cdc:      cdc,
		conns:    make(map[*websocket.Conn]*WSConnection),
		stopChan: make(chan struct{}),
		pool:     NewWSConnectionPool(DefaultWSMaxConnections, DefaultWSIdleTimeout),
	}
}

func (w *WebSocketServer) Start(addr string) error {
	w.logger.Info().Str("addr", addr).Msg("Starting WebSocket server")

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", w.handleWS)

	w.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		if err := w.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			w.logger.Error().Err(err).Msg("WebSocket server error")
		}
	}()

	return nil
}

func (w *WebSocketServer) Stop() error {
	close(w.stopChan)

	w.connMu.Lock()
	for conn := range w.conns {
		conn.Close()
	}
	w.conns = make(map[*websocket.Conn]*WSConnection)
	w.connMu.Unlock()

	if w.httpServer != nil {
		return w.httpServer.Close()
	}
	return nil
}

func (w *WebSocketServer) handleWS(wr http.ResponseWriter, req *http.Request) {
	conn, err := upgrader.Upgrade(wr, req, nil)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to upgrade WebSocket")
		return
	}

	wsConn := &WSConnection{
		id:        fmt.Sprintf("ws-%d", time.Now().UnixNano()),
		conn:      conn,
		subs:      make(map[string]*CDCSubscription),
		writeChan: make(chan []byte, 256),
		closed:    false,
	}
	wsConn.ctx, wsConn.cancel = context.WithCancel(context.Background())

	w.connMu.Lock()
	w.conns[conn] = wsConn
	w.connMu.Unlock()

	w.logger.Info().Str("id", wsConn.id).Msg("New WebSocket connection")

	w.serverWg.Add(1)
	go w.writePump(wsConn)
	go w.readPump(wsConn)
}

func (w *WebSocketServer) readPump(wsConn *WSConnection) {
	defer func() {
		w.removeConnection(wsConn)
		wsConn.conn.Close()
	}()

	wsConn.conn.SetReadLimit(8192)
	wsConn.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	wsConn.conn.SetPongHandler(func(string) error {
		wsConn.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := wsConn.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				w.logger.Error().Err(err).Str("id", wsConn.id).Msg("WebSocket read error")
			}
			break
		}

		if err := w.handleMessage(wsConn, message); err != nil {
			w.sendError(wsConn, err.Error())
		}
	}
}

func (w *WebSocketServer) writePump(wsConn *WSConnection) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		w.removeConnection(wsConn)
		wsConn.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-wsConn.writeChan:
			wsConn.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				wsConn.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := wsConn.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			wsConn.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := wsConn.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-wsConn.ctx.Done():
			return
		}
	}
}

func (w *WebSocketServer) handleMessage(wsConn *WSConnection, data []byte) error {
	var msg WSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("invalid message format: %w", err)
	}

	switch msg.Type {
	case WSTypeSubscribe:
		return w.handleSubscribe(wsConn, msg.Payload)
	case WSTypeUnsubscribe:
		return w.handleUnsubscribe(wsConn, msg.Payload)
	case WSTypePing:
		return w.handlePing(wsConn)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

func (w *WebSocketServer) handleSubscribe(wsConn *WSConnection, payload []byte) error {
	var subPayload WSSubscribePayload
	if err := json.Unmarshal(payload, &subPayload); err != nil {
		return fmt.Errorf("invalid subscription payload: %w", err)
	}

	if subPayload.Dataset == "" {
		return fmt.Errorf("dataset is required")
	}

	filter := CDCFilter{}
	if len(subPayload.EventTypes) > 0 {
		for _, et := range subPayload.EventTypes {
			switch et {
			case "INSERT":
				filter.EventTypes = append(filter.EventTypes, CDCEventInsert)
			case "UPDATE":
				filter.EventTypes = append(filter.EventTypes, CDCEventUpdate)
			case "DELETE":
				filter.EventTypes = append(filter.EventTypes, CDCEventDelete)
			}
		}
	}
	if len(subPayload.Columns) > 0 {
		filter.Columns = subPayload.Columns
	}

	sub, err := w.cdc.Subscribe(subPayload.Dataset, filter, 64)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	wsConn.mu.Lock()
	wsConn.subs[subPayload.Dataset] = sub
	wsConn.mu.Unlock()

	w.logger.Info().Str("id", wsConn.id).Str("dataset", subPayload.Dataset).Msg("Subscribed to dataset")

	w.serverWg.Add(1)
	go w.forwardToConnection(wsConn, sub)

	return nil
}

func (w *WebSocketServer) handleUnsubscribe(wsConn *WSConnection, payload []byte) error {
	var payloadStruct struct {
		Dataset string `json:"dataset"`
	}
	if err := json.Unmarshal(payload, &payloadStruct); err != nil {
		return fmt.Errorf("invalid unsubscribe payload: %w", err)
	}

	wsConn.mu.RLock()
	sub, ok := wsConn.subs[payloadStruct.Dataset]
	wsConn.mu.RUnlock()

	if !ok {
		return fmt.Errorf("not subscribed to dataset: %s", payloadStruct.Dataset)
	}

	if err := w.cdc.Unsubscribe(sub.ID); err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	wsConn.mu.Lock()
	delete(wsConn.subs, payloadStruct.Dataset)
	wsConn.mu.Unlock()

	w.logger.Info().Str("id", wsConn.id).Str("dataset", payloadStruct.Dataset).Msg("Unsubscribed from dataset")

	return nil
}

func (w *WebSocketServer) handlePing(wsConn *WSConnection) error {
	msg := WSMessage{Type: WSTypePong}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case wsConn.writeChan <- data:
	default:
		return fmt.Errorf("failed to send pong: channel full")
	}
	return nil
}

func (w *WebSocketServer) forwardToConnection(wsConn *WSConnection, sub *CDCSubscription) {
	defer w.serverWg.Done()

	for {
		select {
		case batch, ok := <-sub.Ch:
			if !ok {
				return
			}

			jsonData, err := w.cdc.EventToJSON(CDCEvent{
				EventType: CDCEventInsert,
				Dataset:   sub.Dataset,
				Batch:     batch,
			})
			if err != nil {
				w.logger.Error().Err(err).Msg("Failed to serialize CDC event")
				batch.Release()
				continue
			}

			dataPayload := WSDataPayload{
				Dataset: sub.Dataset,
				Event:   "INSERT",
				Data:    jsonData,
			}

			msg := WSMessage{
				Type:    WSTypeData,
				Payload: dataPayloadToJSON(dataPayload),
			}

			msgData, err := json.Marshal(msg)
			if err != nil {
				batch.Release()
				continue
			}

			select {
			case wsConn.writeChan <- msgData:
			default:
				w.logger.Warn().Str("id", wsConn.id).Msg("WebSocket channel full, dropping event")
			}
			batch.Release()

		case <-wsConn.ctx.Done():
			return
		}
	}
}

func dataPayloadToJSON(payload WSDataPayload) json.RawMessage {
	data, _ := json.Marshal(payload)
	return data
}

func (w *WebSocketServer) sendError(wsConn *WSConnection, errMsg string) {
	msg := WSMessage{
		Type:  WSTypeError,
		Error: errMsg,
	}
	data, _ := json.Marshal(msg)

	select {
	case wsConn.writeChan <- data:
	default:
	}
}

func (w *WebSocketServer) removeConnection(wsConn *WSConnection) {
	wsConn.cancel()

	wsConn.mu.Lock()
	for _, sub := range wsConn.subs {
		w.cdc.Unsubscribe(sub.ID)
	}
	wsConn.subs = make(map[string]*CDCSubscription)
	wsConn.closed = true
	wsConn.mu.Unlock()

	w.connMu.Lock()
	delete(w.conns, wsConn.conn)
	w.connMu.Unlock()

	w.logger.Info().Str("id", wsConn.id).Msg("WebSocket connection removed")
}

func (w *WebSocketServer) GetConnectionCount() int {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return len(w.conns)
}

func (w *WebSocketServer) GetConnectionIDs() []string {
	w.connMu.RLock()
	defer w.connMu.RUnlock()

	ids := make([]string, 0, len(w.conns))
	for _, conn := range w.conns {
		ids = append(ids, conn.id)
	}
	return ids
}
