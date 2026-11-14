package wasmclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	UseGRPCWeb       bool             `json:"use_grpc_web,omitempty"`
	Timeout          time.Duration    `json:"timeout,omitempty"`
}

// EncodeGRPCWebFrame wraps a message payload in a standard 5-byte gRPC-Web data frame
// (flag 0x00 + 4-byte BigEndian length) and optional trailer frame (flag 0x80).
func EncodeGRPCWebFrame(payload []byte, includeOkTrailer bool) []byte {
	trailer := []byte("grpc-status: 0\r\n")
	total := 5 + len(payload)
	if includeOkTrailer {
		total += 5 + len(trailer)
	}
	out := make([]byte, total)
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload))) // #nosec G115
	copy(out[5:5+len(payload)], payload)
	if includeOkTrailer {
		offset := 5 + len(payload)
		out[offset] = 0x80
		binary.BigEndian.PutUint32(out[offset+1:offset+5], uint32(len(trailer))) // #nosec G115
		copy(out[offset+5:], trailer)
	}
	return out
}

// DecodeGRPCWebFrame extracts the data frame payload and optional trailers from a gRPC-Web body.
func DecodeGRPCWebFrame(framed []byte) ([]byte, string, error) {
	if len(framed) < 5 {
		return nil, "", errors.New("grpc-web frame shorter than 5-byte header")
	}
	var dataPayload []byte
	var trailers string
	pos := 0
	for pos+5 <= len(framed) {
		flag := framed[pos]
		length := int(binary.BigEndian.Uint32(framed[pos+1 : pos+5]))
		pos += 5
		if length < 0 || pos+length > len(framed) {
			return nil, "", errors.New("truncated grpc-web frame payload")
		}
		chunk := framed[pos : pos+length]
		pos += length
		if flag&0x80 != 0 {
			trailers = string(chunk)
		} else {
			dataPayload = append(dataPayload, chunk...)
		}
	}
	return dataPayload, trailers, nil
}

// EncodeArrowColumnarDocs serializes EdgeDocuments into a compact binary Arrow-layout columnar buffer
// (magic "LBAR", uint32 count, uint32 dim, contiguous float32 vector buffer + length-prefixed IDs).
func EncodeArrowColumnarDocs(docs []EdgeDocument) ([]byte, error) {
	var dim int
	for _, d := range docs {
		if len(d.Vector) > 0 {
			if dim == 0 {
				dim = len(d.Vector)
			} else if len(d.Vector) != dim {
				return nil, fmt.Errorf("inconsistent vector dim: %d vs %d", len(d.Vector), dim)
			}
		}
	}
	var buf bytes.Buffer
	buf.WriteString("LBAR")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(docs))) // #nosec G115
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dim))       // #nosec G115
	// Column 0: length-prefixed UTF-8 IDs + int64 timestamps
	for _, d := range docs {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.ID))) // #nosec G115
		buf.WriteString(d.ID)
		_ = binary.Write(&buf, binary.LittleEndian, d.UpdatedAt)
	}
	// Column 1: contiguous float32 FixedSizeList vector values buffer
	for _, d := range docs {
		for i := 0; i < dim; i++ {
			var v float32
			if i < len(d.Vector) {
				v = d.Vector[i]
			}
			_ = binary.Write(&buf, binary.LittleEndian, math.Float32bits(v))
		}
	}
	return buf.Bytes(), nil
}

// DecodeArrowColumnarDocs deserializes a binary columnar buffer produced by EncodeArrowColumnarDocs.
func DecodeArrowColumnarDocs(raw []byte) ([]EdgeDocument, error) {
	if len(raw) < 12 || string(raw[:4]) != "LBAR" {
		return nil, errors.New("invalid columnar magic header")
	}
	count := int(binary.LittleEndian.Uint32(raw[4:8]))
	dim := int(binary.LittleEndian.Uint32(raw[8:12]))
	if count < 0 || count > 1000000 || dim < 0 || dim > 65536 {
		return nil, errors.New("columnar batch dimensions out of bounds")
	}
	docs := make([]EdgeDocument, count)
	pos := 12
	for i := 0; i < count; i++ {
		if pos+4 > len(raw) {
			return nil, errors.New("truncated id length")
		}
		idLen := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
		pos += 4
		if idLen < 0 || pos+idLen+8 > len(raw) {
			return nil, errors.New("truncated id or timestamp")
		}
		docs[i].ID = string(raw[pos : pos+idLen])
		pos += idLen
		docs[i].UpdatedAt = int64(binary.LittleEndian.Uint64(raw[pos : pos+8])) // #nosec G115
		pos += 8
	}
	for i := 0; i < count; i++ {
		if dim == 0 {
			continue
		}
		if pos+dim*4 > len(raw) {
			return nil, errors.New("truncated vector column buffer")
		}
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			bits := binary.LittleEndian.Uint32(raw[pos : pos+4])
			vec[j] = math.Float32frombits(bits)
			pos += 4
		}
		docs[i].Vector = vec
	}
	return docs, nil
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
	bodyBytes := payload
	if c.cfg.UseGRPCWeb {
		bodyBytes = EncodeGRPCWebFrame(payload, false)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
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
	rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/grpc-web") || (len(rawResp) >= 5 && rawResp[0] == 0x00) {
		unframed, _, fErr := DecodeGRPCWebFrame(rawResp)
		if fErr == nil {
			rawResp = unframed
		}
	}
	var out struct {
		Results []EdgeSearchResult `json:"results"`
	}
	if err := json.Unmarshal(rawResp, &out); err != nil {
		return nil, err
	}
	return out.Results, nil
}

func (c *EdgeClient) setAuthHeaders(req *http.Request) {
	if c.cfg.UseGRPCWeb {
		req.Header.Set("Content-Type", "application/grpc-web+json")
		req.Header.Set("X-Grpc-Web", "1")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	if c.cfg.TenantID != "" {
		req.Header.Set("X-Longbow-Tenant", c.cfg.TenantID)
	}
}
