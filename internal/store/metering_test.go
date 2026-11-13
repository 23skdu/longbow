package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestUsageMeterQuotasBillingAndHTTPExport(t *testing.T) {
	meter := NewUsageMeter(zerolog.Nop())
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	meter.nowFn = func() time.Time { return now }

	require.NoError(t, meter.SetQuota("acme", TenantQuota{
		MaxVectors:              10,
		MaxStorageBytes:         2048,
		MaxQueriesPerWindow:     5,
		MaxEgressBytesPerWindow: 4096,
		WindowSeconds:           60,
		SoftLimitRatio:          0.80,
		EnforceHardLimits:       true,
	}))

	ctx := context.Background()

	// Ingest 8 vectors -> triggers 80% soft warning, still allowed
	dIngest := meter.CheckAndConsumeIngest(ctx, "acme", 8, 1024)
	require.True(t, dIngest.Allowed)
	require.True(t, dIngest.SoftWarning)

	// Ingest 5 more -> exceeds MaxVectors=10 hard limit
	dBlock := meter.CheckAndConsumeIngest(ctx, "acme", 5, 512)
	require.False(t, dBlock.Allowed)
	require.Equal(t, "vectors", dBlock.Resource)

	// Run 5 allowed queries, 4th/5th hit soft limit, 6th blocked by hard limit
	for i := 1; i <= 5; i++ {
		dq := meter.CheckAndConsumeQuery(ctx, "acme", 100)
		require.True(t, dq.Allowed)
		if i >= 4 {
			require.True(t, dq.SoftWarning)
		}
	}
	dq6 := meter.CheckAndConsumeQuery(ctx, "acme", 100)
	require.False(t, dq6.Allowed)
	require.Equal(t, "queries", dq6.Resource)

	// Advancing window resets query window counters
	now = now.Add(61 * time.Second)
	dqReset := meter.CheckAndConsumeQuery(ctx, "acme", 100)
	require.True(t, dqReset.Allowed)

	rep := meter.GetTenantUsage("acme")
	require.Equal(t, int64(6), rep.Queries)
	require.Equal(t, int64(8), rep.CurrentVectors)
	require.Equal(t, int64(600), rep.EgressBytes)
	require.Greater(t, rep.EstimatedCost, 0.0)
	require.GreaterOrEqual(t, rep.HardRejections, int64(2))

	// HTTP /api/v1/usage endpoint check
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?tenant=acme", nil)
	rec := httptest.NewRecorder()
	meter.HTTPHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var decoded TenantUsageReport
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
	require.Equal(t, "acme", decoded.TenantID)

	// CSV billing export check
	csvReq := httptest.NewRequest(http.MethodGet, "/api/v1/usage/export?format=csv", nil)
	csvRec := httptest.NewRecorder()
	meter.ExportHTTPHandler().ServeHTTP(csvRec, csvReq)
	require.Equal(t, http.StatusOK, csvRec.Code)
	require.True(t, strings.Contains(csvRec.Body.String(), "tenant_id,queries"))
	require.True(t, strings.Contains(csvRec.Body.String(), "acme,6"))

	// Webhook exporter integration check
	var webhookPayloads [][]byte
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var tmp map[string]any
		_ = json.NewDecoder(r.Body).Decode(&tmp)
		b, _ := json.Marshal(tmp)
		webhookPayloads = append(webhookPayloads, b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer webhookSrv.Close()

	exp := &WebhookBillingExporter{URL: webhookSrv.URL}
	require.NoError(t, exp.ExportBilling(ctx, meter.GetAllUsage()))
	require.Len(t, webhookPayloads, 1)
}

func FuzzUsageMeterAndQuotaDecisions(f *testing.F) {
	f.Add("tenant-1", int64(5), int64(100), int64(10))
	f.Add("", int64(-1), int64(0), int64(0))

	f.Fuzz(func(t *testing.T, tenant string, maxQ int64, vectors int64, egress int64) {
		m := NewUsageMeter(zerolog.Nop())
		_ = m.SetQuota(tenant, TenantQuota{
			MaxQueriesPerWindow:     maxQ,
			MaxVectors:              vectors,
			MaxEgressBytesPerWindow: egress,
			WindowSeconds:           60,
			EnforceHardLimits:       true,
		})
		_ = m.CheckAndConsumeQuery(context.Background(), tenant, egress)
		_ = m.CheckAndConsumeIngest(context.Background(), tenant, vectors, egress)
		_, _, err := m.ExportReport("csv", tenant)
		if err != nil {
			t.Fatalf("csv export failed: %v", err)
		}
	})
}
