package store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestWebSocketUpgradeRequiresAuthentication(t *testing.T) {
	vs, _, _, _, adminRaw := newNamespacePayloadStore(t)
	wsServer := NewWebSocketServer(zerolog.Nop(), NewChangeDataCapture(vs, zerolog.Nop()))
	wsServer.SetAuth(vs.DynamicAccessValidator(), vs.AuditLogger())

	srv := httptest.NewServer(http.HandlerFunc(wsServer.handleWS))
	defer srv.Close()

	wsURL := "ws" + srv.URL[len("http"):] + "/ws"

	_, httpResp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	conn, httpResp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": []string{"Bearer " + adminRaw},
	})
	require.NoError(t, err)
	require.True(t, conn != nil)
	defer func() { _ = conn.Close() }()

	_, httpResp, err = websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": []string{"Bearer " + adminRaw},
		"Origin":        []string{"https://evil.example"},
	})
	require.Error(t, err)
	require.Equal(t, http.StatusForbidden, httpResp.StatusCode)
}

func TestWebSocketSubscriptionUsesTenantScope(t *testing.T) {
	vs, mgr, _, _, _ := newNamespacePayloadStore(t)
	key, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "ws-reader",
		Role:      RoleReader,
		Namespace: "allowed",
	})
	require.NoError(t, err)
	require.NotNil(t, key)

	tc, err := mgr.AuthorizeToken(context.Background(), raw, string(PermRead), "", "")
	require.NoError(t, err)
	wsConn := &WSConnection{ctx: security.ContextWithTenant(context.Background(), tc)}
	wsServer := NewWebSocketServer(zerolog.Nop(), NewChangeDataCapture(vs, zerolog.Nop()))
	wsServer.SetAuth(vs.DynamicAccessValidator(), nil)

	require.NoError(t, wsServer.authorizeDatasetSubscription(wsConn, "allowed/data"))
	require.Error(t, wsServer.authorizeDatasetSubscription(wsConn, "other/data"))

	wsConn.ctx = context.Background()
	require.Error(t, wsServer.authorizeDatasetSubscription(wsConn, "allowed/data"))
}
