package store

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/security"
)

// PricingModel defines unit billing prices used to compute estimated tenant charges.
type PricingModel struct {
	CostPer1KQueries      float64 `json:"cost_per_1k_queries"`
	CostPer1MVectors      float64 `json:"cost_per_1m_vectors"`
	CostPerGBMonthStorage float64 `json:"cost_per_gb_month_storage"`
	CostPerGBEgress       float64 `json:"cost_per_gb_egress"`
	CostPer1KReranks      float64 `json:"cost_per_1k_reranks"`
	Currency              string  `json:"currency"`
}

// DefaultPricingModel returns baseline SaaS unit pricing in USD.
func DefaultPricingModel() PricingModel {
	return PricingModel{
		CostPer1KQueries:      0.002,
		CostPer1MVectors:      0.50,
		CostPerGBMonthStorage: 0.25,
		CostPerGBEgress:       0.09,
		CostPer1KReranks:      0.05,
		Currency:              "USD",
	}
}

// TenantQuota defines hard and soft usage limits per tenant.
type TenantQuota struct {
	MaxVectors              int64   `json:"max_vectors,omitempty"`
	MaxStorageBytes         int64   `json:"max_storage_bytes,omitempty"`
	MaxQueriesPerWindow     int64   `json:"max_queries_per_window,omitempty"`
	MaxEgressBytesPerWindow int64   `json:"max_egress_bytes_per_window,omitempty"`
	WindowSeconds           int64   `json:"window_seconds,omitempty"`
	SoftLimitRatio          float64 `json:"soft_limit_ratio,omitempty"`
	EnforceHardLimits       bool    `json:"enforce_hard_limits"`
}

// QuotaDecision describes whether an operation is permitted and whether a soft warning fired.
type QuotaDecision struct {
	Allowed     bool    `json:"allowed"`
	SoftWarning bool    `json:"soft_warning"`
	Resource    string  `json:"resource,omitempty"`
	Reason      string  `json:"reason,omitempty"`
	Utilization float64 `json:"utilization,omitempty"`
}

// UsageBucket stores time-windowed usage totals for billing rollups.
type UsageBucket struct {
	WindowStart     time.Time `json:"window_start"`
	Queries         int64     `json:"queries"`
	VectorsIngested int64     `json:"vectors_ingested"`
	StorageBytesMax int64     `json:"storage_bytes_max"`
	EgressBytes     int64     `json:"egress_bytes"`
	Reranks         int64     `json:"reranks"`
}

// TenantUsageReport summarizes tenant resource consumption, quota state, and estimated cost.
type TenantUsageReport struct {
	TenantID            string        `json:"tenant_id"`
	Queries             int64         `json:"queries"`
	WindowQueries       int64         `json:"window_queries"`
	VectorsIngested     int64         `json:"vectors_ingested"`
	CurrentVectors      int64         `json:"current_vectors"`
	CurrentStorageBytes int64         `json:"current_storage_bytes"`
	EgressBytes         int64         `json:"egress_bytes"`
	WindowEgressBytes   int64         `json:"window_egress_bytes"`
	Reranks             int64         `json:"reranks"`
	Embeddings          int64         `json:"embeddings"`
	SoftWarnings        int64         `json:"soft_warnings"`
	HardRejections      int64         `json:"hard_rejections"`
	EstimatedCost       float64       `json:"estimated_cost"`
	Currency            string        `json:"currency"`
	Quota               *TenantQuota  `json:"quota,omitempty"`
	Buckets             []UsageBucket `json:"buckets,omitempty"`
	LastActivity        time.Time     `json:"last_activity"`
}

// BillingExporter receives billing usage snapshots for downstream accounting systems.
type BillingExporter interface {
	ExportBilling(ctx context.Context, reports []TenantUsageReport) error
}

// WebhookBillingExporter posts JSON usage reports to a billing webhook URL.
type WebhookBillingExporter struct {
	URL        string
	HTTPClient *http.Client
}

// ExportBilling sends usage reports via HTTP POST JSON.
func (w *WebhookBillingExporter) ExportBilling(ctx context.Context, reports []TenantUsageReport) error {
	if w == nil || w.URL == "" {
		return errors.New("webhook URL is empty")
	}
	parsed, err := url.Parse(w.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("invalid billing webhook URL: %q", w.URL)
	}
	payload, err := json.Marshal(map[string]any{
		"generated_at": time.Now().UTC(),
		"tenants":      reports,
	})
	if err != nil {
		return err
	}
	client := w.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req) // #nosec G107 -- URL scheme verified above
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("billing webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

type tenantUsageState struct {
	queries             int64
	windowQueries       int64
	vectorsIngested     int64
	currentVectors      int64
	currentStorageBytes int64
	egressBytes         int64
	windowEgressBytes   int64
	reranks             int64
	embeddings          int64
	softWarnings        int64
	hardRejections      int64
	windowStart         time.Time
	lastActivity        time.Time
	quota               *TenantQuota
	buckets             []UsageBucket
}

// UsageMeter tracks per-tenant consumption, evaluates soft/hard quotas, exports
// Prometheus + OpenTelemetry metrics, and generates billing reports.
type UsageMeter struct {
	mu      sync.RWMutex
	logger  zerolog.Logger
	pricing PricingModel
	tenants map[string]*tenantUsageState
	nowFn   func() time.Time

	otelQueries otelmetric.Int64Counter
	otelVectors otelmetric.Int64Counter
	otelEgress  otelmetric.Int64Counter
}

// NewUsageMeter initializes a per-tenant usage meter with default pricing and OTel counters.
func NewUsageMeter(logger zerolog.Logger) *UsageMeter {
	m := &UsageMeter{
		logger:  logger,
		pricing: DefaultPricingModel(),
		tenants: make(map[string]*tenantUsageState),
		nowFn:   time.Now,
	}
	meter := otel.GetMeterProvider().Meter("longbow.billing")
	m.otelQueries, _ = meter.Int64Counter("longbow.billing.queries")
	m.otelVectors, _ = meter.Int64Counter("longbow.billing.vectors_ingested")
	m.otelEgress, _ = meter.Int64Counter("longbow.billing.egress_bytes")
	return m
}

// SetPricing overrides the pricing model.
func (m *UsageMeter) SetPricing(p PricingModel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.Currency == "" {
		p.Currency = "USD"
	}
	m.pricing = p
}

// SetQuota sets a soft/hard resource quota for a tenant.
func (m *UsageMeter) SetQuota(tenantID string, q TenantQuota) error {
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if q.WindowSeconds <= 0 {
		q.WindowSeconds = 3600
	}
	if q.SoftLimitRatio <= 0 || q.SoftLimitRatio >= 1 {
		q.SoftLimitRatio = 0.80
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.getOrCreateLocked(tenantID, m.nowFn())
	copyQ := q
	st.quota = &copyQ
	return nil
}

func (m *UsageMeter) getOrCreateLocked(tenantID string, now time.Time) *tenantUsageState {
	if tenantID == "" {
		tenantID = "default"
	}
	st, ok := m.tenants[tenantID]
	if !ok {
		st = &tenantUsageState{
			windowStart:  now,
			lastActivity: now,
		}
		m.tenants[tenantID] = st
	}
	return st
}

func (m *UsageMeter) rollWindowAndBucketLocked(tenantID string, st *tenantUsageState, now time.Time) {
	winDur := time.Hour
	if st.quota != nil && st.quota.WindowSeconds > 0 {
		winDur = time.Duration(st.quota.WindowSeconds) * time.Second
	}
	if now.Sub(st.windowStart) >= winDur {
		st.windowStart = now
		st.windowQueries = 0
		st.windowEgressBytes = 0
	}
	hourStart := now.UTC().Truncate(time.Hour)
	if len(st.buckets) == 0 || !st.buckets[len(st.buckets)-1].WindowStart.Equal(hourStart) {
		if len(st.buckets) >= 168 {
			st.buckets = st.buckets[1:]
		}
		st.buckets = append(st.buckets, UsageBucket{
			WindowStart:     hourStart,
			StorageBytesMax: st.currentStorageBytes,
		})
	}
	_ = tenantID
}

// CheckAndConsumeQuery validates query/egress quotas and records a query execution.
func (m *UsageMeter) CheckAndConsumeQuery(ctx context.Context, tenantID string, estimatedEgressBytes int64) QuotaDecision {
	if tenantID == "" {
		tenantID = "default"
	}
	now := m.nowFn()
	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.getOrCreateLocked(tenantID, now)
	m.rollWindowAndBucketLocked(tenantID, st, now)

	decision := QuotaDecision{Allowed: true}
	if st.quota != nil {
		if st.quota.MaxQueriesPerWindow > 0 {
			util := float64(st.windowQueries+1) / float64(st.quota.MaxQueriesPerWindow)
			if st.windowQueries+1 > st.quota.MaxQueriesPerWindow {
				decision = QuotaDecision{
					Allowed:     !st.quota.EnforceHardLimits,
					SoftWarning: !st.quota.EnforceHardLimits,
					Resource:    "queries",
					Reason:      fmt.Sprintf("tenant %q exceeded max queries per window (%d)", tenantID, st.quota.MaxQueriesPerWindow),
					Utilization: util,
				}
				if !decision.Allowed {
					st.hardRejections++
					metrics.BillingTenantQuotaRejectionsTotal.WithLabelValues(tenantID, "queries").Inc()
					return decision
				}
			} else if util >= st.quota.SoftLimitRatio {
				decision.SoftWarning = true
				decision.Resource = "queries"
				decision.Utilization = util
				decision.Reason = fmt.Sprintf("tenant %q reached %.0f%% of query quota", tenantID, util*100)
			}
		}
		if st.quota.MaxEgressBytesPerWindow > 0 && estimatedEgressBytes > 0 {
			util := float64(st.windowEgressBytes+estimatedEgressBytes) / float64(st.quota.MaxEgressBytesPerWindow)
			if st.windowEgressBytes+estimatedEgressBytes > st.quota.MaxEgressBytesPerWindow && st.quota.EnforceHardLimits {
				st.hardRejections++
				metrics.BillingTenantQuotaRejectionsTotal.WithLabelValues(tenantID, "egress").Inc()
				return QuotaDecision{
					Allowed:     false,
					Resource:    "egress",
					Reason:      fmt.Sprintf("tenant %q exceeded max egress bytes per window (%d)", tenantID, st.quota.MaxEgressBytesPerWindow),
					Utilization: util,
				}
			} else if util >= st.quota.SoftLimitRatio {
				decision.SoftWarning = true
				if decision.Resource == "" {
					decision.Resource = "egress"
					decision.Utilization = util
				}
			}
		}
	}

	if decision.SoftWarning {
		st.softWarnings++
		metrics.BillingTenantQuotaWarningsTotal.WithLabelValues(tenantID, decision.Resource).Inc()
		m.logger.Warn().Str("tenant", tenantID).Str("resource", decision.Resource).Float64("utilization", decision.Utilization).Msg("Tenant approaching usage quota")
	}

	st.queries++
	st.windowQueries++
	st.lastActivity = now
	if len(st.buckets) > 0 {
		st.buckets[len(st.buckets)-1].Queries++
	}
	metrics.BillingTenantQueriesTotal.WithLabelValues(tenantID).Inc()
	if m.otelQueries != nil {
		m.otelQueries.Add(ctx, 1, otelmetric.WithAttributes(attribute.String("tenant", tenantID)))
	}
	if estimatedEgressBytes > 0 {
		st.egressBytes += estimatedEgressBytes
		st.windowEgressBytes += estimatedEgressBytes
		if len(st.buckets) > 0 {
			st.buckets[len(st.buckets)-1].EgressBytes += estimatedEgressBytes
		}
		metrics.BillingTenantEgressBytesTotal.WithLabelValues(tenantID).Add(float64(estimatedEgressBytes))
		if m.otelEgress != nil {
			m.otelEgress.Add(ctx, estimatedEgressBytes, otelmetric.WithAttributes(attribute.String("tenant", tenantID)))
		}
	}
	return decision
}

// CheckAndConsumeIngest validates vector & storage quotas and records ingestion usage.
func (m *UsageMeter) CheckAndConsumeIngest(ctx context.Context, tenantID string, vectors, storageBytes int64) QuotaDecision {
	if tenantID == "" {
		tenantID = "default"
	}
	now := m.nowFn()
	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.getOrCreateLocked(tenantID, now)
	m.rollWindowAndBucketLocked(tenantID, st, now)

	decision := QuotaDecision{Allowed: true}
	if st.quota != nil {
		if st.quota.MaxVectors > 0 && vectors > 0 {
			nextVecs := st.currentVectors + vectors
			util := float64(nextVecs) / float64(st.quota.MaxVectors)
			if nextVecs > st.quota.MaxVectors && st.quota.EnforceHardLimits {
				st.hardRejections++
				metrics.BillingTenantQuotaRejectionsTotal.WithLabelValues(tenantID, "vectors").Inc()
				return QuotaDecision{
					Allowed:     false,
					Resource:    "vectors",
					Reason:      fmt.Sprintf("tenant %q exceeded max vectors quota (%d)", tenantID, st.quota.MaxVectors),
					Utilization: util,
				}
			} else if util >= st.quota.SoftLimitRatio {
				decision.SoftWarning = true
				decision.Resource = "vectors"
				decision.Utilization = util
			}
		}
		if st.quota.MaxStorageBytes > 0 && storageBytes > 0 {
			nextBytes := st.currentStorageBytes + storageBytes
			util := float64(nextBytes) / float64(st.quota.MaxStorageBytes)
			if nextBytes > st.quota.MaxStorageBytes && st.quota.EnforceHardLimits {
				st.hardRejections++
				metrics.BillingTenantQuotaRejectionsTotal.WithLabelValues(tenantID, "storage").Inc()
				return QuotaDecision{
					Allowed:     false,
					Resource:    "storage",
					Reason:      fmt.Sprintf("tenant %q exceeded max storage quota (%d bytes)", tenantID, st.quota.MaxStorageBytes),
					Utilization: util,
				}
			} else if util >= st.quota.SoftLimitRatio {
				decision.SoftWarning = true
				if decision.Resource == "" {
					decision.Resource = "storage"
					decision.Utilization = util
				}
			}
		}
	}

	if decision.SoftWarning {
		st.softWarnings++
		resName := decision.Resource
		if resName == "" {
			resName = "vectors"
		}
		metrics.BillingTenantQuotaWarningsTotal.WithLabelValues(tenantID, resName).Inc()
		m.logger.Warn().Str("tenant", tenantID).Str("resource", resName).Float64("utilization", decision.Utilization).Msg("Tenant approaching ingest quota")
	}

	if vectors > 0 {
		st.vectorsIngested += vectors
		st.currentVectors += vectors
		if len(st.buckets) > 0 {
			st.buckets[len(st.buckets)-1].VectorsIngested += vectors
		}
		metrics.BillingTenantVectorsTotal.WithLabelValues(tenantID).Add(float64(vectors))
		if m.otelVectors != nil {
			m.otelVectors.Add(ctx, vectors, otelmetric.WithAttributes(attribute.String("tenant", tenantID)))
		}
	}
	if storageBytes > 0 {
		st.currentStorageBytes += storageBytes
		if len(st.buckets) > 0 && st.currentStorageBytes > st.buckets[len(st.buckets)-1].StorageBytesMax {
			st.buckets[len(st.buckets)-1].StorageBytesMax = st.currentStorageBytes
		}
		metrics.BillingTenantStorageBytes.WithLabelValues(tenantID).Set(float64(st.currentStorageBytes))
	}
	st.lastActivity = now
	return decision
}

// RecordEgress adds post-response egress bytes to a tenant's usage.
func (m *UsageMeter) RecordEgress(ctx context.Context, tenantID string, egressBytes int64) {
	if egressBytes <= 0 {
		return
	}
	if tenantID == "" {
		tenantID = "default"
	}
	now := m.nowFn()
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.getOrCreateLocked(tenantID, now)
	m.rollWindowAndBucketLocked(tenantID, st, now)
	st.egressBytes += egressBytes
	st.windowEgressBytes += egressBytes
	st.lastActivity = now
	if len(st.buckets) > 0 {
		st.buckets[len(st.buckets)-1].EgressBytes += egressBytes
	}
	metrics.BillingTenantEgressBytesTotal.WithLabelValues(tenantID).Add(float64(egressBytes))
	if m.otelEgress != nil {
		m.otelEgress.Add(ctx, egressBytes, otelmetric.WithAttributes(attribute.String("tenant", tenantID)))
	}
}

// RecordRerank records reranking calls for a tenant.
func (m *UsageMeter) RecordRerank(tenantID string, count int64) {
	if count <= 0 {
		return
	}
	if tenantID == "" {
		tenantID = "default"
	}
	now := m.nowFn()
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.getOrCreateLocked(tenantID, now)
	m.rollWindowAndBucketLocked(tenantID, st, now)
	st.reranks += count
	st.lastActivity = now
	if len(st.buckets) > 0 {
		st.buckets[len(st.buckets)-1].Reranks += count
	}
}

func (m *UsageMeter) computeCostLocked(st *tenantUsageState) float64 {
	const gb = float64(1024 * 1024 * 1024)
	cost := (float64(st.queries)/1000.0)*m.pricing.CostPer1KQueries +
		(float64(st.vectorsIngested)/1000000.0)*m.pricing.CostPer1MVectors +
		(float64(st.currentStorageBytes)/gb)*m.pricing.CostPerGBMonthStorage +
		(float64(st.egressBytes)/gb)*m.pricing.CostPerGBEgress +
		(float64(st.reranks)/1000.0)*m.pricing.CostPer1KReranks
	return cost
}

func (m *UsageMeter) snapshotLocked(tenantID string, st *tenantUsageState) TenantUsageReport {
	var qCopy *TenantQuota
	if st.quota != nil {
		q := *st.quota
		qCopy = &q
	}
	buckets := make([]UsageBucket, len(st.buckets))
	copy(buckets, st.buckets)
	return TenantUsageReport{
		TenantID:            tenantID,
		Queries:             st.queries,
		WindowQueries:       st.windowQueries,
		VectorsIngested:     st.vectorsIngested,
		CurrentVectors:      st.currentVectors,
		CurrentStorageBytes: st.currentStorageBytes,
		EgressBytes:         st.egressBytes,
		WindowEgressBytes:   st.windowEgressBytes,
		Reranks:             st.reranks,
		Embeddings:          st.embeddings,
		SoftWarnings:        st.softWarnings,
		HardRejections:      st.hardRejections,
		EstimatedCost:       m.computeCostLocked(st),
		Currency:            m.pricing.Currency,
		Quota:               qCopy,
		Buckets:             buckets,
		LastActivity:        st.lastActivity,
	}
}

// GetTenantUsage returns the live usage and cost report for one tenant.
func (m *UsageMeter) GetTenantUsage(tenantID string) TenantUsageReport {
	if tenantID == "" {
		tenantID = "default"
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.tenants[tenantID]
	if !ok {
		return TenantUsageReport{TenantID: tenantID, Currency: m.pricing.Currency}
	}
	return m.snapshotLocked(tenantID, st)
}

// GetAllUsage returns sorted usage reports across all known tenants.
func (m *UsageMeter) GetAllUsage() []TenantUsageReport {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.tenants))
	for k := range m.tenants {
		names = append(names, k)
	}
	sort.Strings(names)
	reports := make([]TenantUsageReport, 0, len(names))
	for _, name := range names {
		reports = append(reports, m.snapshotLocked(name, m.tenants[name]))
	}
	return reports
}

// ExportReport formats usage data as "json" or "csv".
func (m *UsageMeter) ExportReport(format, tenantID string) ([]byte, string, error) {
	var reports []TenantUsageReport
	if tenantID != "" {
		reports = []TenantUsageReport{m.GetTenantUsage(tenantID)}
	} else {
		reports = m.GetAllUsage()
	}

	switch format {
	case "", "json":
		b, err := json.MarshalIndent(map[string]any{
			"generated_at": time.Now().UTC(),
			"tenants":      reports,
		}, "", "  ")
		return b, "application/json", err
	case "csv":
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		_ = w.Write([]string{
			"tenant_id", "queries", "vectors_ingested", "current_vectors",
			"current_storage_bytes", "egress_bytes", "reranks",
			"soft_warnings", "hard_rejections", "estimated_cost", "currency",
		})
		for _, r := range reports {
			_ = w.Write([]string{
				r.TenantID,
				strconv.FormatInt(r.Queries, 10),
				strconv.FormatInt(r.VectorsIngested, 10),
				strconv.FormatInt(r.CurrentVectors, 10),
				strconv.FormatInt(r.CurrentStorageBytes, 10),
				strconv.FormatInt(r.EgressBytes, 10),
				strconv.FormatInt(r.Reranks, 10),
				strconv.FormatInt(r.SoftWarnings, 10),
				strconv.FormatInt(r.HardRejections, 10),
				strconv.FormatFloat(r.EstimatedCost, 'f', 6, 64),
				r.Currency,
			})
		}
		w.Flush()
		return buf.Bytes(), "text/csv", w.Error()
	default:
		return nil, "", fmt.Errorf("unsupported billing export format: %q", format)
	}
}

// HTTPHandler serves GET /api/v1/usage?tenant=<id>.
func (m *UsageMeter) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := r.URL.Query().Get("tenant")
		w.Header().Set("Content-Type", "application/json")
		if tenant != "" {
			_ = json.NewEncoder(w).Encode(m.GetTenantUsage(tenant))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenants": m.GetAllUsage(),
		})
	})
}

// ExportHTTPHandler serves GET /api/v1/usage/export?format=json|csv&tenant=<id>.
func (m *UsageMeter) ExportHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		format := r.URL.Query().Get("format")
		tenant := r.URL.Query().Get("tenant")
		data, contentType, err := m.ExportReport(format, tenant)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(data)
	})
}

func resolveTenantIDFromCtx(ctx context.Context, datasetPath string) string {
	if tc, ok := security.TenantFromContext(ctx); ok && tc != nil && tc.TenantID != "" {
		return tc.TenantID
	}
	ns, _ := ParseNamespacedPath(datasetPath)
	if ns != "" {
		return ns
	}
	return "default"
}

func (s *VectorStore) enforceTenantQueryQuota(ctx context.Context, datasetPath string, estimatedEgress int64) error {
	tenant := resolveTenantIDFromCtx(ctx, datasetPath)
	dec := s.GetUsageMeter().CheckAndConsumeQuery(ctx, tenant, estimatedEgress)
	if !dec.Allowed {
		return status.Error(codes.ResourceExhausted, dec.Reason)
	}
	return nil
}

func (s *VectorStore) enforceTenantIngestQuota(ctx context.Context, datasetPath string, vectors, storageBytes int64) error {
	tenant := resolveTenantIDFromCtx(ctx, datasetPath)
	dec := s.GetUsageMeter().CheckAndConsumeIngest(ctx, tenant, vectors, storageBytes)
	if !dec.Allowed {
		return status.Error(codes.ResourceExhausted, dec.Reason)
	}
	return nil
}

func (s *VectorStore) handleUsageAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "GetTenantUsage":
		var req struct {
			TenantID string `json:"tenant_id"`
		}
		_ = json.Unmarshal(action.Body, &req)
		if req.TenantID != "" {
			rep := s.GetUsageMeter().GetTenantUsage(req.TenantID)
			body, _ := json.Marshal(rep)
			return true, stream.Send(&flight.Result{Body: body})
		}
		reps := s.GetUsageMeter().GetAllUsage()
		body, _ := json.Marshal(map[string]any{"tenants": reps})
		return true, stream.Send(&flight.Result{Body: body})

	case "SetTenantQuota":
		var req struct {
			TenantID string `json:"tenant_id"`
			TenantQuota
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid quota json: %v", err)
		}
		if err := s.GetUsageMeter().SetQuota(req.TenantID, req.TenantQuota); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"quota_set"}`)})

	case "ExportBillingReport":
		var req struct {
			Format   string `json:"format"`
			TenantID string `json:"tenant_id"`
		}
		_ = json.Unmarshal(action.Body, &req)
		data, _, err := s.GetUsageMeter().ExportReport(req.Format, req.TenantID)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: data})
	}
	return false, nil
}
