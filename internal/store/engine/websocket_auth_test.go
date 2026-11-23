package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/store/apihandlers"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestWebSocketUpgradeRequiresAuthentication runs the test web socket upgrade requires authentication operation.
func TestWebSocketUpgradeRequiresAuthentication(t *testing.T) {
	vs, _, _, _, adminRaw := newNamespacePayloadStore(t)
	wsServer := NewWebSocketServer(zerolog.Nop(), NewChangeDataCapture(vs, zerolog.Nop()))
	wsServer.SetAuth(vs.DynamicAccessValidator(), vs.AuditLogger())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apihandlers.HandleWS(wsServer, w, r)
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[len("http"):] + "/ws"

	_, httpResp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, httpResp.StatusCode)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
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

// TestWebSocketSubscriptionUsesTenantScope runs the test web socket subscription uses tenant scope operation.
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
	wsConn := apihandlers.NewWSConnectionWithContext(security.ContextWithTenant(context.Background(), tc))
	wsServer := NewWebSocketServer(zerolog.Nop(), NewChangeDataCapture(vs, zerolog.Nop()))
	wsServer.SetAuth(vs.DynamicAccessValidator(), nil)

	require.NoError(t, apihandlers.AuthorizeDatasetSubscription(wsServer, wsConn, "allowed/data"))
	require.Error(t, apihandlers.AuthorizeDatasetSubscription(wsServer, wsConn, "other/data"))

	wsConn = apihandlers.NewWSConnectionWithContext(context.Background())
	require.Error(t, apihandlers.AuthorizeDatasetSubscription(wsServer, wsConn, "allowed/data"))
}
