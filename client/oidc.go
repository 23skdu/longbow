package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/metadata"
)

const defaultGoogleMetadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"

// SetAPIKey updates the bearer token / API key used by WithAuthContext.
func (c *SmartClient) SetAPIKey(token string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.apiKey = token
	c.mu.Unlock()
}

// AcquireAWSWebIdentityToken reads an EKS/IRSA web identity JWT from
// AWS_WEB_IDENTITY_TOKEN_FILE.
func AcquireAWSWebIdentityToken() (string, error) {
	tokenFile := strings.TrimSpace(os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"))
	if tokenFile == "" {
		return "", errors.New("AWS_WEB_IDENTITY_TOKEN_FILE is not set")
	}
	raw, err := os.ReadFile(filepath.Clean(tokenFile)) // #nosec G304 G703 - standard AWS IRSA token file path from env
	if err != nil {
		return "", fmt.Errorf("read AWS web identity token file: %w", err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return "", errors.New("AWS web identity token file is empty")
	}
	return tok, nil
}

// AcquireGoogleIDToken fetches a Google Cloud IAM identity token from the GCE
// metadata server for the requested audience.
func AcquireGoogleIDToken(ctx context.Context, audience string) (string, error) {
	return acquireGoogleIDTokenFromEndpoint(ctx, defaultGoogleMetadataURL, audience)
}

func acquireGoogleIDTokenFromEndpoint(ctx context.Context, endpoint, audience string) (string, error) {
	if strings.TrimSpace(audience) == "" {
		return "", errors.New("audience is required")
	}
	u := endpoint + "?audience=" + url.QueryEscape(audience) + "&format=full"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil) // #nosec G107 G704 - GCE metadata endpoint
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	httpc := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpc.Do(req) // #nosec G704 - GCE metadata endpoint
	if err != nil {
		return "", fmt.Errorf("fetch Google ID token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("google metadata status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(body))
	if tok == "" {
		return "", errors.New("empty Google ID token response")
	}
	return tok, nil
}

// ExchangeOIDCToken exchanges a federated OIDC JWT for a short-lived Longbow
// session token via the ExchangeOIDCToken Flight action and updates SmartClient's
// active bearer credential.
func (c *SmartClient) ExchangeOIDCToken(ctx context.Context, oidcToken string, ttl time.Duration) (string, error) {
	if c == nil {
		return "", errors.New("nil SmartClient")
	}
	if strings.TrimSpace(oidcToken) == "" {
		return "", errors.New("oidcToken is required")
	}
	payload, err := json.Marshal(map[string]any{
		"token":       oidcToken,
		"ttl_seconds": int64(ttl.Seconds()),
	})
	if err != nil {
		return "", err
	}
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+oidcToken)
	c.mu.RLock()
	tenantID := c.tenantID
	c.mu.RUnlock()
	if tenantID != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, "x-tenant-id", tenantID)
	}

	stream, err := c.DoAction(callCtx, &flight.Action{
		Type: "ExchangeOIDCToken",
		Body: payload,
	})
	if err != nil {
		return "", fmt.Errorf("ExchangeOIDCToken action: %w", err)
	}
	res, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("ExchangeOIDCToken recv: %w", err)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(res.Body, &out); err != nil {
		return "", fmt.Errorf("ExchangeOIDCToken decode: %w", err)
	}
	if out.Token == "" {
		return "", errors.New("ExchangeOIDCToken returned empty token")
	}
	c.SetAPIKey(out.Token)
	return out.Token, nil
}
