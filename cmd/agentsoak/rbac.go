package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// agentKeys holds the two RBAC credentials an agent switches between: the
// writer key grants read/write/delete on its own namespaces and the common
// namespace; the reader key grants read-only access to other agents' shared
// namespaces.
type agentKeys struct {
	Writer string
	Reader string
}

func newClient(addr, apiKey string) (*client.SmartClient, error) {
	return client.NewSmartClientWithConfig(client.ClientConfig{Addr: addr, APIKey: apiKey})
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
	_, err := actionJSON(ctx, c, "CreateNamespace", map[string]any{
		"name": ns, "dims": dim, "data_type": "float32",
	})
	return err
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

// provision creates every namespace and issues per-agent scoped API keys using
// the bootstrap admin credential. Existing namespaces are fine; the seed write
// is best-effort so repeat runs against a populated server still work.
func provision(cfg *soakConfig, topo *topology, adminKey string) (map[int]agentKeys, error) {
	admin, err := newClient(cfg.URI, adminKey)
	if err != nil {
		return nil, fmt.Errorf("admin connect: %w", err)
	}
	defer admin.Close() // #nosec G104

	ctx := context.Background()
	rng := rand.New(rand.NewSource(cfg.Seed)) // #nosec G404
	for _, ns := range topo.Namespaces() {
		if err := createNamespace(ctx, admin, ns, cfg.Dim); err != nil {
			if *verbose {
				log.Printf("[provision] create %s: %v", ns, err)
			}
		}
		seedID := fmt.Sprintf("seed_%s", ns)
		if err := doInsert(ctx, admin, ns, seedID, randVec(rng, cfg.Dim), 1, cfg.Dim); err != nil {
			if *verbose {
				log.Printf("[provision] seed %s: %v", ns, err)
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
		keys[i] = agentKeys{Writer: writer, Reader: reader}
	}
	return keys, nil
}

// verifyRBAC probes forbidden operations with real scoped keys and fails fast
// if the server is not enforcing the namespace boundaries.
func verifyRBAC(cfg *soakConfig, topo *topology, keys map[int]agentKeys) error {
	rng := rand.New(rand.NewSource(cfg.Seed + 1)) // #nosec G404
	ctx := context.Background()
	for i := 0; i < topo.agents; i++ {
		other := (i + 1) % topo.agents
		c, err := newClient(cfg.URI, keys[i].Writer)
		if err != nil {
			return fmt.Errorf("verify agent %d connect: %w", i, err)
		}
		err = doSearch(ctx, c, topo.private[other], randVec(rng, cfg.Dim))
		if err == nil {
			c.Close() // #nosec G104
			return fmt.Errorf("agent %d can read agent %d's private namespace %s", i, other, topo.private[other])
		}
		c.SetAPIKey(keys[i].Reader)
		err = doInsert(ctx, c, topo.shared[other], fmt.Sprintf("rbac_probe_%d", i), randVec(rng, cfg.Dim), 1, cfg.Dim)
		if err == nil {
			c.Close() // #nosec G104
			return fmt.Errorf("agent %d can write agent %d's shared namespace %s", i, other, topo.shared[other])
		}
		c.Close() // #nosec G104
	}
	log.Printf("RBAC boundaries verified: %d agents, %d namespaces", topo.agents, len(topo.Namespaces()))
	return nil
}
