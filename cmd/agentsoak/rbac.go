package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/internal/security"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// agentKeys holds the per-agent credentials. The writer covers owned and
// common namespaces, the reader covers other agents' shared namespaces, and
// the federated reader covers every readable collection in one request.
type agentKeys struct {
	Writer    string
	Reader    string
	Federated string
}

func newClient(cfg *soakConfig, apiKey string) (*client.SmartClient, error) {
	tlsCfg := security.TLSConfig{
		Enabled:    cfg.Security.TLS.Enabled,
		CertFile:   cfg.Security.TLS.CertFile,
		KeyFile:    cfg.Security.TLS.KeyFile,
		CAFile:     cfg.Security.TLS.CAFile,
		ServerName: cfg.Security.TLS.ServerName,
		MinVersion: cfg.Security.TLS.MinVersion,
	}
	return client.NewSmartClientWithConfig(client.ClientConfig{
		Addr:     cfg.URI,
		APIKey:   apiKey,
		TenantID: cfg.Security.TenantID,
		TLS:      tlsCfg,
	})
}

// applyOIDC exchanges a workload-identity JWT for a short-lived Longbow
// session key. Namespace RBAC still applies to the exchanged key; this never
// grants an admin role or disables the scoped API keys provisioned for agents.
func applyOIDC(ctx context.Context, c *client.SmartClient, cfg *soakConfig) error {
	mode := cfg.Security.OIDC.Mode
	if mode == "" || mode == "none" {
		return nil
	}
	var token string
	var err error
	switch mode {
	case "aws":
		env := cfg.Security.OIDC.TokenFileEnv
		if env == "" {
			env = "AWS_WEB_IDENTITY_TOKEN_FILE"
		}
		path := strings.TrimSpace(os.Getenv(env))
		if path == "" {
			return fmt.Errorf("oidc mode aws requires %s", env)
		}
		raw, readErr := os.ReadFile(filepath.Clean(path)) // #nosec G304 G703 - workload identity token path from environment
		if readErr != nil {
			return fmt.Errorf("read OIDC token: %w", readErr)
		}
		token = strings.TrimSpace(string(raw))
	case "google":
		audience := cfg.Security.OIDC.Audience
		if audience == "" {
			audience = "longbow"
		}
		token, err = client.AcquireGoogleIDToken(ctx, audience)
	default:
		return fmt.Errorf("unsupported oidc mode %q", mode)
	}
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("oidc mode %s returned an empty token", mode)
	}
	ttl := time.Duration(cfg.Security.OIDC.TTLSeconds) * time.Second
	if _, err := c.ExchangeOIDCToken(ctx, token, ttl); err != nil {
		return fmt.Errorf("exchange OIDC token: %w", err)
	}
	return nil
}

func actionJSON(ctx context.Context, c *client.SmartClient, actionType string, req any) (map[string]any, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	stream, err := c.DoAction(c.WithAuthContext(ctx), &flight.Action{Type: actionType, Body: body})
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend() // #nosec G104
	result, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var resp map[string]any
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func createNamespace(ctx context.Context, c *client.SmartClient, ns string, dim int) error {
	// The data plane replies with plain text for CreateNamespace, so drain the
	// stream without assuming a JSON result body.
	return doActionDrain(ctx, c, "CreateNamespace", map[string]any{
		"name": ns, "dims": dim, "data_type": "float32",
	})
}

func createAPIKey(ctx context.Context, c *client.SmartClient, name, role string, datasets []string) (string, error) {
	resp, err := actionJSON(ctx, c, "CreateAPIKey", map[string]any{
		"name": name, "role": role, "datasets": datasets,
	})
	if err != nil {
		return "", err
	}
	key, _ := resp["api_key"].(string)
	if key == "" {
		return "", fmt.Errorf("CreateAPIKey for %s returned no api_key", name)
	}
	return key, nil
}

func registerFederatedCollection(ctx context.Context, c *client.SmartClient, ns string) error {
	_, err := actionJSON(ctx, c, "RegisterFederatedCollection", map[string]any{
		"dataset": ns, "name": ns, "description": "AgentSoak collection",
		"tags": []string{"soak"}, "metadata": map[string]any{},
	})
	return err
}

// seedGraphEdges gives each namespace a small numeric graph so graphrag,
// traversal, PageRank, and community ops have real topology to exercise.
func seedGraphEdges(ctx context.Context, c *client.SmartClient, ns string, count uint32) error {
	for i := uint32(1); i < count; i++ {
		if err := doActionDrain(ctx, c, "add-edge", map[string]any{
			"dataset": ns, "subject": i, "predicate": "related",
			"object": i + 1, "weight": 1.0,
		}); err != nil {
			return err
		}
	}
	return nil
}

// provision creates every namespace, registers federated collections, and
// issues per-agent scoped API keys using the bootstrap admin credential.
func provision(cfg *soakConfig, topo *topology, adminKey string) (map[int]agentKeys, error) {
	admin, err := newClient(cfg, adminKey)
	if err != nil {
		return nil, fmt.Errorf("admin connect: %w", err)
	}
	defer admin.Close() // #nosec G104

	ctx := context.Background()
	if cfg.Security.OIDC.Mode != "" && cfg.Security.OIDC.Mode != "none" {
		// Exchange on a disposable client. ExchangeOIDCToken replaces the
		// client's active credential with a session key that is normally a
		// reader; using the admin client here would strip its bootstrap rights
		// before any namespace or API-key provisioning runs.
		oidcProbe, oidcErr := newClient(cfg, adminKey)
		if oidcErr != nil {
			return nil, fmt.Errorf("OIDC identity connect: %w", oidcErr)
		}
		oidcErr = applyOIDC(ctx, oidcProbe, cfg)
		_ = oidcProbe.Close() // #nosec G104
		if oidcErr != nil {
			return nil, fmt.Errorf("admin OIDC identity: %w", oidcErr)
		}
	}
	rng := rand.New(rand.NewSource(cfg.Seed)) // #nosec G404
	for _, ns := range topo.Namespaces() {
		if err := createNamespace(ctx, admin, ns, cfg.Dim); err != nil {
			if *verbose {
				log.Printf("[provision] create %s: %v", ns, err)
			}
		}
		seedID := fmt.Sprintf("seed_%s", ns)
		if err := doInsert(ctx, admin, ns, seedID, randVec(rng, cfg.Dim), 1, cfg.Dim, cfg.WorkloadOptions.Geo.Lat, cfg.WorkloadOptions.Geo.Lon); err != nil {
			if *verbose {
				log.Printf("[provision] seed %s: %v", ns, err)
			}
		}
		if err := seedGraphEdges(ctx, admin, ns, 16); err != nil {
			if *verbose {
				log.Printf("[provision] graph edges %s: %v", ns, err)
			}
		}
		if err := registerFederatedCollection(ctx, admin, ns); err != nil {
			if *verbose {
				log.Printf("[provision] federate %s: %v", ns, err)
			}
		}
	}

	keys := make(map[int]agentKeys, topo.agents)
	for i := 0; i < topo.agents; i++ {
		writer, err := createAPIKey(ctx, admin, fmt.Sprintf("agent_%d_writer", i), "writer", topo.Writable(i))
		if err != nil {
			return nil, fmt.Errorf("provision writer key for agent %d: %w", i, err)
		}
		reader, err := createAPIKey(ctx, admin, fmt.Sprintf("agent_%d_reader", i), "reader", topo.ReadOnly(i))
		if err != nil {
			return nil, fmt.Errorf("provision reader key for agent %d: %w", i, err)
		}
		federated, err := createAPIKey(ctx, admin, fmt.Sprintf("agent_%d_federated", i), "reader", topo.Readable(i))
		if err != nil {
			return nil, fmt.Errorf("provision federated key for agent %d: %w", i, err)
		}
		keys[i] = agentKeys{Writer: writer, Reader: reader, Federated: federated}
	}
	return keys, nil
}

// verifyRBAC probes forbidden operations with real scoped keys and fails fast
// if the server is not enforcing the namespace boundaries.
func verifyRBAC(cfg *soakConfig, topo *topology, keys map[int]agentKeys) error {
	rng := rand.New(rand.NewSource(cfg.Seed + 1)) // #nosec G404
	ctx := context.Background()
	for i := 0; i < topo.agents; i++ {
		forbiddenRead, forbiddenWrite := topo.forbidden(i)
		c, err := newClient(cfg, keys[i].Writer)
		if err != nil {
			return fmt.Errorf("verify agent %d connect: %w", i, err)
		}
		err = doSearch(ctx, c, forbiddenRead, randVec(rng, cfg.Dim))
		if err == nil {
			c.Close() // #nosec G104
			return fmt.Errorf("agent %d can read forbidden namespace %s", i, forbiddenRead)
		}
		c.SetAPIKey(keys[i].Reader)
		err = doInsert(ctx, c, forbiddenWrite, fmt.Sprintf("rbac_probe_%d", i), randVec(rng, cfg.Dim), 1, cfg.Dim, cfg.WorkloadOptions.Geo.Lat, cfg.WorkloadOptions.Geo.Lon)
		if err == nil {
			c.Close() // #nosec G104
			return fmt.Errorf("agent %d can write forbidden namespace %s", i, forbiddenWrite)
		}
		c.Close() // #nosec G104
	}
	log.Printf("RBAC boundaries verified: %d agents, %d namespaces", topo.agents, len(topo.Namespaces()))
	return nil
}

// verifyAuditDenials confirms that the negative RBAC probes produced queryable
// audit records. Denied operations are expected and must not be counted as
// client-visible soak errors.
func verifyAuditDenials(cfg *soakConfig, adminKey string) (map[string]int, error) {
	admin, err := newClient(cfg, adminKey)
	if err != nil {
		return nil, fmt.Errorf("audit query connect: %w", err)
	}
	defer admin.Close() // #nosec G104
	resp, err := actionJSON(context.Background(), admin, "GetAuditDenialCounts", map[string]any{})
	if err != nil {
		return nil, fmt.Errorf("query audit denials: %w", err)
	}
	total := 0
	if value, ok := resp["total"].(float64); ok {
		total = int(value)
	}
	if total <= 0 {
		return nil, fmt.Errorf("expected RBAC denial audit records, got total=%d", total)
	}
	counts := make(map[string]int)
	if raw, ok := resp["counts"].(map[string]any); ok {
		for key, value := range raw {
			if n, ok := value.(float64); ok {
				counts[key] = int(n)
			}
		}
	}
	return counts, nil
}
