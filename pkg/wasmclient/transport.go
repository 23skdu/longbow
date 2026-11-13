package wasmclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClientConfig configures an offline-capable WASM / edge Longbow client.
type ClientConfig struct {
	Endpoint         string           `json:"endpoint"`
	Dataset          string           `json:"dataset"`
	APIKey           string           `json:"api_key,omitempty"`
	TenantID         string           `json:"tenant_id,omitempty"`
	ClientID         string           `json:"client_id,omitempty"`
	Metric           Metric           `json:"metric,omitempty"`
	ConflictStrategy ConflictStrategy `json:"conflict_strategy,omitempty"`
	OfflineFirst     bool             `json:"offline_first"`
	Timeout          time.Duration    `json:"timeout,omitempty"`
}

// EdgeClient provides hybrid local-HNSW + remote HTTP/gRPC-web transport and offline sync.
type EdgeClient struct {
	cfg        ClientConfig
	httpClient *http.Client
	index      *EdgeIndex
	sync       *SyncEngine
	nowFn      func() int64
}

// NewEdgeClient constructs an edge/WASM Longbow client.
func NewEdgeClient(cfg ClientConfig) (*EdgeClient, error) {
	if cfg.Dataset == "" {
		return nil, errors.New("dataset is required")
	}
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("invalid endpoint URL: %q", cfg.Endpoint)
		}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &EdgeClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: timeout},
		index:      NewEdgeIndex(cfg.Metric, 16, 64),
		sync:       NewSyncEngine(cfg.ClientID, cfg.ConflictStrategy),
		nowFn:      func() int64 { return time.Now().UnixNano() },
	}, nil
}

// LocalIndex exposes the embedded pure-Go/WASM HNSW index.
func (c *EdgeClient) LocalIndex() *EdgeIndex {
	return c.index
}

// SyncEngine exposes the offline-first queue and conflict resolver.
func (c *EdgeClient) SyncEngine() *SyncEngine {
	return c.sync
}

// Upsert inserts or updates locally immediately (if OfflineFirst or disconnected) and queues sync.
func (c *EdgeClient) Upsert(ctx context.Context, doc EdgeDocument) error {
	if doc.UpdatedAt == 0 {
		doc.UpdatedAt = c.nowFn()
	}
	if err := c.index.Upsert(doc); err != nil {
		return err
	}
	c.sync.RecordLocal("upsert", doc, doc.UpdatedAt)
	if !c.cfg.OfflineFirst && c.cfg.Endpoint != "" {
		return c.FlushSync(ctx)
	}
	return nil
}

// Delete removes locally and queues a tombstone mutation for sync.
func (c *EdgeClient) Delete(ctx context.Context, id string) error {
	c.index.Delete(id)
	ts := c.nowFn()
	c.sync.RecordLocal("delete", EdgeDocument{ID: id, Deleted: true, UpdatedAt: ts}, ts)
	if !c.cfg.OfflineFirst && c.cfg.Endpoint != "" {
		return c.FlushSync(ctx)
	}
	return nil
}

// Search queries local WASM HNSW when OfflineFirst (or on upstream failure) and falls back/queries upstream HTTP endpoint.
func (c *EdgeClient) Search(ctx context.Context, query []float32, k int) ([]EdgeSearchResult, error) {
	if c.cfg.OfflineFirst || c.cfg.Endpoint == "" {
		return c.index.Search(query, k)
	}
	remoteRes, err := c.remoteSearch(ctx, query, k)
	if err == nil {
		return remoteRes, nil
	}
	if c.index.Len() > 0 {
		return c.index.Search(query, k)
	}
	return nil, err
}

// FlushSync pushes queued local mutations to the upstream HTTP/gRPC-web sync endpoint.
func (c *EdgeClient) FlushSync(ctx context.Context) error {
	if c.cfg.Endpoint == "" {
		return errors.New("cannot sync: endpoint is empty")
	}
	muts := c.sync.Drain()
	if len(muts) == 0 {
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"dataset":   c.cfg.Dataset,
		"client_id": c.cfg.ClientID,
		"mutations": muts,
	})
	if err != nil {
		c.sync.Requeue(muts)
		return err
	}
	endpoint := strings.TrimRight(c.cfg.Endpoint, "/") + "/api/v1/edge/sync"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		c.sync.Requeue(muts)
		return err
	}
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req) // #nosec G107 -- endpoint scheme validated in NewEdgeClient
	if err != nil {
		c.sync.Requeue(muts)
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		c.sync.Requeue(muts)
		return fmt.Errorf("edge sync returned HTTP %d", resp.StatusCode)
	}
	var syncResp struct {
		RemoteDocs []EdgeDocument `json:"remote_docs,omitempty"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&syncResp)
	for _, rdoc := range syncResp.RemoteDocs {
		localDoc, exists := c.index.docs[rdoc.ID]
		if exists {
			winner, _ := c.sync.ResolveConflict(localDoc, nil, rdoc, nil, "server")
			_ = c.index.Upsert(winner)
		} else {
			_ = c.index.Upsert(rdoc)
		}
	}
	return nil
}

func (c *EdgeClient) remoteSearch(ctx context.Context, query []float32, k int) ([]EdgeSearchResult, error) {
	payload, err := json.Marshal(map[string]any{
		"dataset": c.cfg.Dataset,
		"vector":  query,
		"k":       k,
	})
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(c.cfg.Endpoint, "/") + "/api/v1/search"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	c.setAuthHeaders(req)
	resp, err := c.httpClient.Do(req) // #nosec G107 -- endpoint scheme validated in NewEdgeClient
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("edge remote search HTTP %d", resp.StatusCode)
	}
	var out struct {
		Results []EdgeSearchResult `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Results, nil
}

func (c *EdgeClient) setAuthHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	if c.cfg.TenantID != "" {
		req.Header.Set("X-Longbow-Tenant", c.cfg.TenantID)
	}
}
