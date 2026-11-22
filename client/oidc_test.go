package client

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type mockOIDCExchangeServer struct {
	flight.BaseFlightServer
	seenAuth string
}

// DoAction runs the do action operation.
func (m *mockOIDCExchangeServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action.Type != "ExchangeOIDCToken" {
		return nil
	}
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			m.seenAuth = vals[0]
		}
	}
	body, _ := json.Marshal(map[string]any{"token": "lbk_short_session_123"})
	return stream.Send(&flight.Result{Body: body})
}

// TestAcquireAWSWebIdentityToken runs the test acquire aws web identity token operation.
func TestAcquireAWSWebIdentityToken(t *testing.T) {
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	_, err := AcquireAWSWebIdentityToken()
	require.Error(t, err)

	tmp := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tmp, []byte("header.payload.sig\n"), 0600))
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tmp)

	tok, err := AcquireAWSWebIdentityToken()
	require.NoError(t, err)
	require.Equal(t, "header.payload.sig", tok)
}

// TestAcquireGoogleIDToken runs the test acquire google id token operation.
func TestAcquireGoogleIDToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Google", r.Header.Get("Metadata-Flavor"))
		require.Equal(t, "longbow", r.URL.Query().Get("audience"))
		_, _ = w.Write([]byte("gcp.id.token\n"))
	}))
	defer srv.Close()

	tok, err := acquireGoogleIDTokenFromEndpoint(context.Background(), srv.URL, "longbow")
	require.NoError(t, err)
	require.Equal(t, "gcp.id.token", tok)
}

// TestSmartClientExchangeOIDCToken runs the test smart client exchange oidc token operation.
func TestSmartClientExchangeOIDCToken(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	mock := &mockOIDCExchangeServer{}
	flight.RegisterFlightServiceServer(gs, mock)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	sc, err := NewSmartClientWithConfig(ClientConfig{
		Addr:     lis.Addr().String(),
		TenantID: "acme",
	})
	require.NoError(t, err)
	defer func() { _ = sc.Close() }()

	sessTok, err := sc.ExchangeOIDCToken(context.Background(), "oidc.jwt.token", 10*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "lbk_short_session_123", sessTok)
	require.Equal(t, "Bearer oidc.jwt.token", mock.seenAuth)

	md, ok := metadata.FromOutgoingContext(sc.WithAuthContext(context.Background()))
	require.True(t, ok)
	require.Equal(t, []string{"Bearer lbk_short_session_123"}, md.Get("authorization"))
	require.Equal(t, []string{"acme"}, md.Get("x-tenant-id"))
}
