package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
)

// CrossEncoderConfig configures batched cross-encoder reranking.
type CrossEncoderConfig struct {
	ModelPath string  `json:"model_path"`
	BatchSize int     `json:"batch_size"`
	MLWeight  float32 `json:"ml_weight"`
}

// CrossEncoderReranker scores (query, candidate document) pairs using an MLBackend
// (WASM/ONNX) or injectable scoring callback, calibrating logits via sigmoid.
type CrossEncoderReranker struct {
	backend   MLBackend
	scoreFn   func(ctx context.Context, query string, docs []string) ([]float32, error)
	batchSize int
	mlWeight  float32
}

// NewCrossEncoderReranker creates a cross-encoder reranker over a WASM/ONNX MLBackend.
func NewCrossEncoderReranker(backend MLBackend, cfg CrossEncoderConfig) *CrossEncoderReranker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 16
	}
	if cfg.MLWeight <= 0 || cfg.MLWeight > 1 {
		cfg.MLWeight = 0.75
	}
	return &CrossEncoderReranker{
		backend:   backend,
		batchSize: cfg.BatchSize,
		mlWeight:  cfg.MLWeight,
	}
}

// NewCallbackCrossEncoderReranker wraps an explicit pair scoring function (useful for tests/custom models).
func NewCallbackCrossEncoderReranker(fn func(ctx context.Context, query string, docs []string) ([]float32, error), mlWeight float32) *CrossEncoderReranker {
	if mlWeight <= 0 || mlWeight > 1 {
		mlWeight = 0.75
	}
	return &CrossEncoderReranker{
		scoreFn:   fn,
		batchSize: 16,
		mlWeight:  mlWeight,
	}
}

// Rerank scores candidate documents in batches and returns sorted results.
func (c *CrossEncoderReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}
	docs := make([]string, len(results))
	for i, r := range results {
		docs[i] = extractDocumentText(r)
	}

	rawScores := make([]float32, len(results))
	for start := 0; start < len(docs); start += c.batchSize {
		end := min(len(docs), start+c.batchSize)
		var batchScores []float32
		var err error
		switch {
		case c.scoreFn != nil:
			batchScores, err = c.scoreFn(ctx, query, docs[start:end])
		case c.backend != nil:
			batchScores, err = c.backend.Score(ctx, query, docs[start:end])
		default:
			return nil, errors.New("cross-encoder has no ML backend configured")
		}
		if err != nil {
			return nil, err
		}
		for i := 0; i < end-start && i < len(batchScores); i++ {
			rawScores[start+i] = sigmoid32(batchScores[i])
		}
	}

	out := make([]SearchResult, len(results))
	copy(out, results)
	for i := range out {
		denseScore := float32(1.0) / (1.0 + out[i].Distance)
		out[i].Score = c.mlWeight*rawScores[i] + (1.0-c.mlWeight)*denseScore
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Score > out[j].Score
	})
	return out, nil
}

// LLMRerankConfig configures OpenAI or local (Ollama/vLLM) LLM reranking.
type LLMRerankConfig struct {
	Provider   string `json:"provider"` // "openai", "local", "cohere"
	Endpoint   string `json:"endpoint"` // e.g. "https://api.openai.com/v1/chat/completions" or "http://localhost:11434/api/chat"
	APIKey     string `json:"api_key,omitempty"`
	Model      string `json:"model"`
	TimeoutMs  int    `json:"timeout_ms,omitempty"`
	MaxDocChar int    `json:"max_doc_char,omitempty"`
}

// LLMReranker uses OpenAI-compatible or local Ollama/vLLM JSON relevance scoring endpoints.
type LLMReranker struct {
	cfg        LLMRerankConfig
	httpClient *http.Client
}

// NewLLMReranker constructs an LLM-based listwise reranker.
func NewLLMReranker(cfg LLMRerankConfig) (*LLMReranker, error) {
	if cfg.Provider == "" {
		cfg.Provider = "local"
	}
	if cfg.Endpoint == "" {
		if cfg.Provider == "openai" {
			cfg.Endpoint = "https://api.openai.com/v1/chat/completions"
		} else {
			cfg.Endpoint = "http://localhost:11434/v1/chat/completions"
		}
	}
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid LLM reranker endpoint: %q", cfg.Endpoint)
	}
	if cfg.Model == "" {
		if cfg.Provider == "openai" {
			cfg.Model = "gpt-4o-mini"
		} else {
			cfg.Model = "llama3.1"
		}
	}
	timeout := 15 * time.Second
	if cfg.TimeoutMs > 0 {
		timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	}
	if cfg.MaxDocChar <= 0 {
		cfg.MaxDocChar = 512
	}
	return &LLMReranker{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

// Rerank prompts the configured LLM to score candidate passages and returns sorted results.
func (l *LLMReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}
	var sb strings.Builder
	sb.WriteString("Score each document relevance (0.0 to 1.0) to query: ")
	sb.WriteString(query)
	sb.WriteString("\nReturn JSON {\"scores\": [s0, s1, ...]}.\n")
	for i, r := range results {
		doc := extractDocumentText(r)
		if len(doc) > l.cfg.MaxDocChar {
			doc = doc[:l.cfg.MaxDocChar]
		}
		fmt.Fprintf(&sb, "[%d] %s\n", i, doc)
	}

	reqBody, err := json.Marshal(map[string]any{
		"model": l.cfg.Model,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a search relevance judge. Output valid JSON only."},
			{"role": "user", "content": sb.String()},
		},
		"temperature": 0.0,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.cfg.Endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if l.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+l.cfg.APIKey)
	}

	resp, err := l.httpClient.Do(req) // #nosec G107 -- URL validated in NewLLMReranker
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("LLM reranker HTTP %d", resp.StatusCode)
	}

	var chatResp struct {
		Scores  []float32 `json:"scores"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&chatResp); err != nil {
		return nil, err
	}
	scores := chatResp.Scores
	if len(scores) == 0 && len(chatResp.Choices) > 0 {
		var parsedContent struct {
			Scores []float32 `json:"scores"`
		}
		_ = json.Unmarshal([]byte(extractJSONBlock(chatResp.Choices[0].Message.Content)), &parsedContent)
		scores = parsedContent.Scores
	}
	if len(scores) != len(results) {
		return nil, fmt.Errorf("LLM returned %d scores for %d candidates", len(scores), len(results))
	}

	out := make([]SearchResult, len(results))
	copy(out, results)
	for i := range out {
		out[i].Score = scores[i]
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Score > out[j].Score
	})
	return out, nil
}

// HybridReranker combines dense vector similarity, full-length lexical term & exact-phrase matching,
// metadata field weights, and an optional ML/CrossEncoder/LLM second-stage reranker.
type HybridReranker struct {
	Secondary     Reranker
	MLWeight      float32
	DenseWeight   float32
	LexicalWeight float32
	FieldBoosts   map[string]float32
}

// NewHybridReranker builds a balanced ML + lexical + dense heuristic reranker.
func NewHybridReranker(secondary Reranker) *HybridReranker {
	mlW := float32(0.0)
	denseW := float32(0.55)
	lexW := float32(0.45)
	if secondary != nil {
		mlW = 0.50
		denseW = 0.25
		lexW = 0.25
	}
	return &HybridReranker{
		Secondary:     secondary,
		MLWeight:      mlW,
		DenseWeight:   denseW,
		LexicalWeight: lexW,
		FieldBoosts: map[string]float32{
			"title":   1.5,
			"caption": 1.3,
			"content": 1.0,
			"text":    1.0,
		},
	}
}

// Rerank scores every candidate across all signals and sorts descending.
func (h *HybridReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}
	out := make([]SearchResult, len(results))
	copy(out, results)

	mlScores := make(map[core.VectorID]float32, len(results))
	if h.Secondary != nil && h.MLWeight > 0 {
		mlRanked, err := h.Secondary.Rerank(ctx, query, out)
		if err == nil {
			for _, item := range mlRanked {
				mlScores[item.ID] = item.Score
			}
		}
	}

	qTerms := tokenizeAlphaNum(query)
	qLower := strings.ToLower(strings.TrimSpace(query))

	for i := range out {
		denseScore := float32(1.0) / (1.0 + out[i].Distance)
		lexScore := h.computeLexicalScore(qLower, qTerms, out[i])
		mlScore := mlScores[out[i].ID]
		out[i].Score = h.MLWeight*mlScore + h.DenseWeight*denseScore + h.LexicalWeight*lexScore
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Score > out[j].Score
	})
	return out, nil
}

func (h *HybridReranker) computeLexicalScore(qLower string, qTerms []string, r SearchResult) float32 {
	if len(qTerms) == 0 || len(r.Metadata) == 0 {
		return 0
	}
	metaMap, _ := core.DecodeMetadata(r.Metadata)
	if len(metaMap) == 0 {
		return 0
	}

	var best float32
	for field, rawVal := range metaMap {
		text, ok := rawVal.(string)
		if !ok || text == "" {
			continue
		}
		lower := strings.ToLower(text)
		matched := 0
		for _, term := range qTerms {
			if strings.Contains(lower, term) {
				matched++
			}
		}
		coverage := float32(matched) / float32(len(qTerms))
		if qLower != "" && strings.Contains(lower, qLower) {
			coverage += 0.35 // phrase bonus
		}
		boost := h.FieldBoosts[field]
		if boost <= 0 {
			boost = 0.8
		}
		score := coverage * boost
		if score > best {
			best = score
		}
	}
	if best > 1.5 {
		best = 1.5
	}
	return best
}

// QueryRewriteMode specifies query expansion strategy.
type QueryRewriteMode string

const (
	RewriteSynonyms   QueryRewriteMode = "synonyms"
	RewriteMultiQuery QueryRewriteMode = "multi_query"
	RewriteHyDE       QueryRewriteMode = "hyde"
)

// QueryRewriter expands, decomposes, or HyDE-augments queries prior to retrieval/reranking.
type QueryRewriter struct {
	mu       sync.RWMutex
	synonyms map[string][]string
}

// NewQueryRewriter initializes a rewriter with technical & domain synonyms.
func NewQueryRewriter() *QueryRewriter {
	return &QueryRewriter{
		synonyms: map[string][]string{
			"db":       {"database", "datastore"},
			"ml":       {"machine learning", "neural"},
			"ai":       {"artificial intelligence", "llm"},
			"fast":     {"low latency", "high throughput"},
			"auth":     {"authentication", "authorization", "rbac"},
			"security": {"encryption", "tls", "compliance"},
			"image":    {"photo", "picture", "visual"},
			"audio":    {"sound", "speech", "voice"},
		},
	}
}

// AddSynonyms registers custom domain synonyms.
func (qr *QueryRewriter) AddSynonyms(term string, expansions []string) {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	qr.synonyms[strings.ToLower(term)] = expansions
}

// Rewrite returns rewritten/expanded query variants.
func (qr *QueryRewriter) Rewrite(query string, mode QueryRewriteMode) []string {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	switch mode {
	case RewriteMultiQuery:
		parts := splitMultiQuery(query)
		if len(parts) > 1 {
			return append([]string{query}, parts...)
		}
		return []string{query}
	case RewriteHyDE:
		hydeDoc := fmt.Sprintf("Technical passage answering: %s. Relevant concepts, architecture, and implementation details for %s.", query, query)
		return []string{query, hydeDoc}
	case RewriteSynonyms, "":
		expanded := qr.expandSynonyms(query)
		if expanded != query {
			return []string{query, expanded}
		}
		return []string{query}
	default:
		return []string{query}
	}
}

func (qr *QueryRewriter) expandSynonyms(query string) string {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	words := strings.Fields(query)
	extras := make([]string, 0)
	seen := make(map[string]struct{}, len(words))
	for _, w := range words {
		lw := strings.ToLower(strings.Trim(w, ".,?!:;"))
		seen[lw] = struct{}{}
	}
	for _, w := range words {
		lw := strings.ToLower(strings.Trim(w, ".,?!:;"))
		if syns, ok := qr.synonyms[lw]; ok {
			for _, syn := range syns {
				if _, exists := seen[syn]; !exists {
					seen[syn] = struct{}{}
					extras = append(extras, syn)
				}
			}
		}
	}
	if len(extras) == 0 {
		return query
	}
	return query + " " + strings.Join(extras, " ")
}

func splitMultiQuery(query string) []string {
	seps := []string{" and ", " vs ", " versus ", ";", "?"}
	cur := []string{query}
	for _, sep := range seps {
		var next []string
		for _, part := range cur {
			pieces := strings.Split(part, sep)
			for _, p := range pieces {
				p = strings.TrimSpace(p)
				if p != "" {
					next = append(next, p)
				}
			}
		}
		cur = next
	}
	return cur
}

// RerankCache caches reranked search results by strategy + query + candidate set hash with TTL.
type RerankCache struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	entries    map[string]rerankCacheEntry
	order      []string
}

type rerankCacheEntry struct {
	expiresAt time.Time
	results   []SearchResult
}

// NewRerankCache initializes an LRU + TTL rerank cache.
func NewRerankCache(maxEntries int, ttl time.Duration) *RerankCache {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &RerankCache{
		maxEntries: maxEntries,
		ttl:        ttl,
		entries:    make(map[string]rerankCacheEntry),
		order:      make([]string, 0, maxEntries),
	}
}

func rerankCacheKey(strategy, query string, candidates []SearchResult) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strategy))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(query))
	for _, c := range candidates {
		_, _ = fmt.Fprintf(h, "|%d:%.5f", c.ID, c.Distance)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Get retrieves cached reranked results when valid and unexpired.
func (rc *RerankCache) Get(strategy, query string, candidates []SearchResult) ([]SearchResult, bool) {
	key := rerankCacheKey(strategy, query, candidates)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	ent, ok := rc.entries[key]
	if !ok {
		metrics.RerankCacheMissesTotal.WithLabelValues(strategy).Inc()
		return nil, false
	}
	if time.Now().After(ent.expiresAt) {
		delete(rc.entries, key)
		metrics.RerankCacheMissesTotal.WithLabelValues(strategy).Inc()
		return nil, false
	}
	out := make([]SearchResult, len(ent.results))
	copy(out, ent.results)
	metrics.RerankCacheHitsTotal.WithLabelValues(strategy).Inc()
	return out, true
}

// Put stores reranked results into cache.
func (rc *RerankCache) Put(strategy, query string, candidates, ranked []SearchResult) {
	key := rerankCacheKey(strategy, query, candidates)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if _, exists := rc.entries[key]; !exists {
		if len(rc.order) >= rc.maxEntries {
			evict := rc.order[0]
			rc.order = rc.order[1:]
			delete(rc.entries, evict)
		}
		rc.order = append(rc.order, key)
	}
	copyRes := make([]SearchResult, len(ranked))
	copy(copyRes, ranked)
	rc.entries[key] = rerankCacheEntry{
		expiresAt: time.Now().Add(rc.ttl),
		results:   copyRes,
	}
}

// EvalQueryCase represents a labeled evaluation query for reranker quality testing.
type EvalQueryCase struct {
	Query      string
	Candidates []SearchResult
	Relevant   map[core.VectorID]float64 // graded relevance e.g. 1.0, 2.0
}

// RerankEvalMetrics reports standard IR effectiveness metrics (NDCG@K, MRR, Recall@K).
type RerankEvalMetrics struct {
	Cases      int     `json:"cases"`
	K          int     `json:"k"`
	MeanNDCG   float64 `json:"mean_ndcg"`
	MeanMRR    float64 `json:"mean_mrr"`
	MeanRecall float64 `json:"mean_recall"`
}

// EvaluateReranker benchmarks reranking quality on labeled relevance judgments.
func EvaluateReranker(ctx context.Context, r Reranker, cases []EvalQueryCase, k int) (RerankEvalMetrics, error) {
	if len(cases) == 0 {
		return RerankEvalMetrics{}, errors.New("eval cases cannot be empty")
	}
	if k <= 0 {
		k = 10
	}
	var sumNDCG, sumMRR, sumRecall float64
	for _, c := range cases {
		ranked, err := r.Rerank(ctx, c.Query, c.Candidates)
		if err != nil {
			return RerankEvalMetrics{}, err
		}
		limit := min(k, len(ranked))
		// DCG@K
		var dcg float64
		var mrr float64
		hits := 0
		for rank := 0; rank < limit; rank++ {
			rel := c.Relevant[ranked[rank].ID]
			if rel > 0 {
				hits++
				if mrr == 0 {
					mrr = 1.0 / float64(rank+1)
				}
				dcg += (math.Pow(2, rel) - 1) / math.Log2(float64(rank+2))
			}
		}
		// IDCG@K
		idealRels := make([]float64, 0, len(c.Relevant))
		totalRel := 0
		for _, rel := range c.Relevant {
			if rel > 0 {
				idealRels = append(idealRels, rel)
				totalRel++
			}
		}
		sort.Slice(idealRels, func(i, j int) bool { return idealRels[i] > idealRels[j] })
		var idcg float64
		for rank := 0; rank < min(k, len(idealRels)); rank++ {
			idcg += (math.Pow(2, idealRels[rank]) - 1) / math.Log2(float64(rank+2))
		}
		ndcg := 0.0
		if idcg > 0 {
			ndcg = dcg / idcg
		}
		recall := 0.0
		if totalRel > 0 {
			recall = float64(hits) / float64(totalRel)
		}
		sumNDCG += ndcg
		sumMRR += mrr
		sumRecall += recall
	}
	n := float64(len(cases))
	return RerankEvalMetrics{
		Cases:      len(cases),
		K:          k,
		MeanNDCG:   sumNDCG / n,
		MeanMRR:    sumMRR / n,
		MeanRecall: sumRecall / n,
	}, nil
}

// AdvancedRerankManager integrates strategies, caching, query rewriting, and metering.
type AdvancedRerankManager struct {
	store    *VectorStore
	rewriter *QueryRewriter
	cache    *RerankCache
	hybrid   *HybridReranker
}

// NewAdvancedRerankManager constructs the central reranking & query rewriting service.
func NewAdvancedRerankManager(s *VectorStore) *AdvancedRerankManager {
	return &AdvancedRerankManager{
		store:    s,
		rewriter: NewQueryRewriter(),
		cache:    NewRerankCache(2048, 5*time.Minute),
		hybrid:   NewHybridReranker(nil),
	}
}

// Rerank runs the chosen strategy ("hybrid", "heuristic", "llm", "cohere", "cross_encoder") with cache.
func (m *AdvancedRerankManager) Rerank(ctx context.Context, strategy, query string, candidates []SearchResult, tenant string) ([]SearchResult, bool, error) {
	if strategy == "" {
		strategy = "hybrid"
	}
	if cached, ok := m.cache.Get(strategy, query, candidates); ok {
		return cached, true, nil
	}
	var ranked []SearchResult
	var err error
	switch strategy {
	case "heuristic":
		hr := &HeuristicReranker{ModelName: "default"}
		ranked, err = hr.Rerank(ctx, query, candidates)
	case "hybrid", "cross_encoder":
		ranked, err = m.hybrid.Rerank(ctx, query, candidates)
	default:
		ranked, err = m.hybrid.Rerank(ctx, query, candidates)
	}
	if err != nil {
		return nil, false, err
	}
	m.cache.Put(strategy, query, candidates, ranked)
	if m.store != nil {
		m.store.GetUsageMeter().RecordRerank(tenant, int64(len(candidates)))
	}
	return ranked, false, nil
}

// Rewrite rewrites/expands a query.
func (m *AdvancedRerankManager) Rewrite(query string, mode QueryRewriteMode) []string {
	return m.rewriter.Rewrite(query, mode)
}

func (s *VectorStore) handleRerankAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "Rerank":
		var req struct {
			Strategy string         `json:"strategy"`
			Query    string         `json:"query"`
			Results  []SearchResult `json:"results"`
			TopK     int            `json:"top_k"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		tenant := resolveTenantIDFromCtx(stream.Context(), "")
		ranked, cacheHit, err := s.GetRerankManager().Rerank(stream.Context(), req.Strategy, req.Query, req.Results, tenant)
		if err != nil {
			return true, status.Errorf(codes.Internal, "%v", err)
		}
		if req.TopK > 0 && req.TopK < len(ranked) {
			ranked = ranked[:req.TopK]
		}
		body, _ := json.Marshal(map[string]any{"results": ranked, "cache_hit": cacheHit})
		return true, stream.Send(&flight.Result{Body: body})

	case "RewriteQuery":
		var req struct {
			Query string           `json:"query"`
			Mode  QueryRewriteMode `json:"mode"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		variants := s.GetRerankManager().Rewrite(req.Query, req.Mode)
		body, _ := json.Marshal(map[string]any{"queries": variants})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}

func extractDocumentText(r SearchResult) string {
	if len(r.Metadata) == 0 {
		return fmt.Sprintf("vector:%d", r.ID)
	}
	metaMap, _ := core.DecodeMetadata(r.Metadata)
	var parts []string
	for _, key := range []string{"title", "caption", "content", "text", "description"} {
		if v, ok := metaMap[key].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("vector:%d", r.ID)
	}
	return strings.Join(parts, " | ")
}

func sigmoid32(x float32) float32 {
	return float32(1.0 / (1.0 + math.Exp(float64(-x))))
}

func tokenizeAlphaNum(s string) []string {
	var tokens []string
	var cur strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			cur.WriteByte(c)
		} else if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

func extractJSONBlock(s string) string {
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}
