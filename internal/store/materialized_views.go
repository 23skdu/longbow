package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// ViewRefreshPolicy controls how a materialized view stays synchronized with base datasets.
type ViewRefreshPolicy string

const (
	RefreshManual    ViewRefreshPolicy = "manual"
	RefreshScheduled ViewRefreshPolicy = "scheduled"
	RefreshStreaming ViewRefreshPolicy = "streaming"
)

// AggOp enumerates supported columnar & metadata aggregation functions.
type AggOp string

const (
	AggCount AggOp = "count"
	AggSum   AggOp = "sum"
	AggAvg   AggOp = "avg"
	AggMin   AggOp = "min"
	AggMax   AggOp = "max"
)

// AggSpec defines one computed metric column inside a materialized view.
type AggSpec struct {
	Alias string `json:"alias"`
	Field string `json:"field,omitempty"`
	Op    AggOp  `json:"op"`
}

// MaterializedViewSpec declares a materialized aggregation view over a dataset.
type MaterializedViewSpec struct {
	Name              string            `json:"name"`
	SourceDataset     string            `json:"source_dataset"`
	GroupBy           []string          `json:"group_by"`
	Aggregations      []AggSpec         `json:"aggregations"`
	FilterEquals      map[string]string `json:"filter_equals,omitempty"`
	RefreshPolicy     ViewRefreshPolicy `json:"refresh_policy"`
	RefreshIntervalMs int               `json:"refresh_interval_ms,omitempty"`
}

// MaterializedViewRow holds one aggregated group bucket and computed metrics.
type MaterializedViewRow struct {
	GroupKey string             `json:"group_key"`
	Group    map[string]string  `json:"group"`
	Metrics  map[string]float64 `json:"metrics"`
}

// MaterializedViewSnapshot stores a point-in-time consistent materialized view state.
type MaterializedViewSnapshot struct {
	Spec        MaterializedViewSpec  `json:"spec"`
	Version     uint64                `json:"version"`
	RefreshedAt time.Time             `json:"refreshed_at"`
	SourceRows  int64                 `json:"source_rows"`
	Rows        []MaterializedViewRow `json:"rows"`
}

type mvEntry struct {
	spec     MaterializedViewSpec
	snapshot *MaterializedViewSnapshot
	stopCh   chan struct{}
}

// MaterializedViewManager manages view definitions, count/sum/avg/min/max aggregation,
// refresh policies (manual/scheduled/streaming), and query planner integration.
type MaterializedViewManager struct {
	mu    sync.RWMutex
	store *VectorStore
	views map[string]*mvEntry
}

// NewMaterializedViewManager initializes a materialized view manager.
func NewMaterializedViewManager(s *VectorStore) *MaterializedViewManager {
	return &MaterializedViewManager{
		store: s,
		views: make(map[string]*mvEntry),
	}
}

// CreateView registers a materialized view, computes its initial snapshot, and starts
// scheduled background refresh loops if configured.
func (m *MaterializedViewManager) CreateView(ctx context.Context, spec MaterializedViewSpec) (*MaterializedViewSnapshot, error) {
	if spec.Name == "" || spec.SourceDataset == "" {
		return nil, errors.New("view name and source_dataset are required")
	}
	if len(spec.Aggregations) == 0 {
		return nil, errors.New("at least one aggregation is required")
	}
	for i := range spec.Aggregations {
		if spec.Aggregations[i].Alias == "" {
			spec.Aggregations[i].Alias = fmt.Sprintf("%s_%s", spec.Aggregations[i].Op, spec.Aggregations[i].Field)
		}
	}
	if spec.RefreshPolicy == "" {
		spec.RefreshPolicy = RefreshManual
	}

	m.mu.Lock()
	if old, exists := m.views[spec.Name]; exists && old.stopCh != nil {
		close(old.stopCh)
	}
	ent := &mvEntry{spec: spec}
	if spec.RefreshPolicy == RefreshScheduled {
		ent.stopCh = make(chan struct{})
	}
	m.views[spec.Name] = ent
	m.mu.Unlock()

	snap, err := m.RefreshView(ctx, spec.Name)
	if err != nil {
		return nil, err
	}
	if spec.RefreshPolicy == RefreshScheduled {
		interval := time.Duration(max(50, spec.RefreshIntervalMs)) * time.Millisecond
		go m.runScheduledLoop(spec.Name, interval, ent.stopCh)
	}
	return snap, nil
}

func (m *MaterializedViewManager) runScheduledLoop(name string, interval time.Duration, stopCh chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			_, _ = m.RefreshView(context.Background(), name)
		}
	}
}

// RefreshView recomputes a materialized view consistently over the source dataset's
// active Arrow batches while respecting tombstones.
func (m *MaterializedViewManager) RefreshView(ctx context.Context, name string) (*MaterializedViewSnapshot, error) {
	_ = ctx
	m.mu.RLock()
	ent, ok := m.views[name]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("materialized view %q not found", name)
	}
	spec := ent.spec
	m.mu.RUnlock()

	var batches []arrow.RecordBatch
	var tombstones map[int]*types.Bitset
	if m.store != nil {
		if ds, exists := m.store.getDataset(spec.SourceDataset); exists {
			ds.dataMu.RLock()
			records := ds.Records.Read()
			batches = make([]arrow.RecordBatch, len(records))
			for i, r := range records {
				r.Retain()
				batches[i] = r
			}
			if len(ds.Tombstones) > 0 {
				tombstones = make(map[int]*types.Bitset, len(ds.Tombstones))
				for k, v := range ds.Tombstones {
					tombstones[k] = v
				}
			}
			ds.dataMu.RUnlock()
		}
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	rows, sourceCount := AggregateBatches(batches, tombstones, spec)

	m.mu.Lock()
	defer m.mu.Unlock()
	ent, ok = m.views[name]
	if !ok {
		return nil, fmt.Errorf("materialized view %q removed", name)
	}
	var nextVer uint64 = 1
	if ent.snapshot != nil {
		nextVer = ent.snapshot.Version + 1
	}
	snap := &MaterializedViewSnapshot{
		Spec:        spec,
		Version:     nextVer,
		RefreshedAt: time.Now().UTC(),
		SourceRows:  sourceCount,
		Rows:        rows,
	}
	ent.snapshot = snap
	metrics.MaterializedViewRefreshesTotal.WithLabelValues(name, string(spec.RefreshPolicy)).Inc()
	return snap, nil
}

// OnDatasetIngest refreshes all streaming materialized views dependent on dataset.
func (m *MaterializedViewManager) OnDatasetIngest(ctx context.Context, dataset string) {
	m.mu.RLock()
	var streamingNames []string
	for name, ent := range m.views {
		if ent.spec.SourceDataset == dataset && ent.spec.RefreshPolicy == RefreshStreaming {
			streamingNames = append(streamingNames, name)
		}
	}
	m.mu.RUnlock()
	for _, name := range streamingNames {
		_, _ = m.RefreshView(ctx, name)
	}
}

// QueryView returns the materialized snapshot, optionally filtered by exact group field equality.
func (m *MaterializedViewManager) QueryView(name string, groupFilter map[string]string) (*MaterializedViewSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ent, ok := m.views[name]
	if !ok || ent.snapshot == nil {
		return nil, fmt.Errorf("materialized view %q not found", name)
	}
	if len(groupFilter) == 0 {
		return ent.snapshot, nil
	}
	filtered := make([]MaterializedViewRow, 0)
	for _, r := range ent.snapshot.Rows {
		match := true
		for k, want := range groupFilter {
			if r.Group[k] != want {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, r)
		}
	}
	cp := *ent.snapshot
	cp.Rows = filtered
	return &cp, nil
}

// TryPlanQuery implements query-planner integration: if a matching materialized view
// covers dataset + groupBy + aggs, serves the precomputed rows directly.
func (m *MaterializedViewManager) TryPlanQuery(dataset string, groupBy []string, aggs []AggSpec) (*MaterializedViewSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, ent := range m.views {
		if ent.snapshot == nil || ent.spec.SourceDataset != dataset || len(ent.spec.FilterEquals) > 0 {
			continue
		}
		if !stringSlicesEqual(ent.spec.GroupBy, groupBy) {
			continue
		}
		if coversAggs(ent.spec.Aggregations, aggs) {
			return ent.snapshot, true
		}
	}
	return nil, false
}

// AggregateBatches computes exact grouped count/sum/avg/min/max over Arrow RecordBatches + JSON metadata.
func AggregateBatches(batches []arrow.RecordBatch, tombstones map[int]*types.Bitset, spec MaterializedViewSpec) ([]MaterializedViewRow, int64) {
	type aggAcc struct {
		count int64
		sum   float64
		min   float64
		max   float64
		has   bool
	}
	type groupState struct {
		group map[string]string
		accs  []aggAcc
	}
	groups := make(map[string]*groupState)
	var sourceRows int64

	for bIdx, rec := range batches {
		ts := tombstones[bIdx]
		schema := rec.Schema()
		colIdx := make(map[string]int, schema.NumFields())
		for i, f := range schema.Fields() {
			colIdx[f.Name] = i
		}
		metaIdx, hasMeta := colIdx["metadata"]

		for row := 0; row < int(rec.NumRows()); row++ {
			if ts != nil && ts.Contains(row) {
				continue
			}
			var meta map[string]any
			if hasMeta {
				if sa, ok := rec.Column(metaIdx).(*array.String); ok && !sa.IsNull(row) {
					_ = json.Unmarshal([]byte(sa.Value(row)), &meta)
				}
			}

			// Optional equality filter check
			if !rowMatchesEquals(rec, colIdx, meta, row, spec.FilterEquals) {
				continue
			}

			sourceRows++
			gMap := make(map[string]string, len(spec.GroupBy))
			keyParts := make([]string, 0, len(spec.GroupBy))
			for _, gField := range spec.GroupBy {
				val := readRowStringValue(rec, colIdx, meta, row, gField)
				gMap[gField] = val
				keyParts = append(keyParts, gField+"="+val)
			}
			gKey := strings.Join(keyParts, "|")
			gs, ok := groups[gKey]
			if !ok {
				accs := make([]aggAcc, len(spec.Aggregations))
				for i := range accs {
					accs[i].min = math.Inf(1)
					accs[i].max = math.Inf(-1)
				}
				gs = &groupState{group: gMap, accs: accs}
				groups[gKey] = gs
			}

			for i, agg := range spec.Aggregations {
				acc := &gs.accs[i]
				if agg.Op == AggCount {
					acc.count++
					acc.has = true
					continue
				}
				num, ok := readRowNumericValue(rec, colIdx, meta, row, agg.Field)
				if !ok {
					continue
				}
				acc.count++
				acc.sum += num
				if !acc.has || num < acc.min {
					acc.min = num
				}
				if !acc.has || num > acc.max {
					acc.max = num
				}
				acc.has = true
			}
		}
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([]MaterializedViewRow, 0, len(keys))
	for _, k := range keys {
		gs := groups[k]
		metricsMap := make(map[string]float64, len(spec.Aggregations))
		for i, agg := range spec.Aggregations {
			acc := gs.accs[i]
			switch agg.Op {
			case AggCount:
				metricsMap[agg.Alias] = float64(acc.count)
			case AggSum:
				metricsMap[agg.Alias] = acc.sum
			case AggAvg:
				if acc.count > 0 {
					metricsMap[agg.Alias] = acc.sum / float64(acc.count)
				}
			case AggMin:
				if acc.has {
					metricsMap[agg.Alias] = acc.min
				}
			case AggMax:
				if acc.has {
					metricsMap[agg.Alias] = acc.max
				}
			}
		}
		rows = append(rows, MaterializedViewRow{
			GroupKey: k,
			Group:    gs.group,
			Metrics:  metricsMap,
		})
	}
	return rows, sourceRows
}

func rowMatchesEquals(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int, filters map[string]string) bool {
	for k, want := range filters {
		if readRowStringValue(rec, colIdx, meta, row, k) != want {
			return false
		}
	}
	return true
}

func readRowStringValue(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int, field string) string {
	if idx, ok := colIdx[field]; ok {
		col := rec.Column(idx)
		if !col.IsNull(row) {
			if sa, ok := col.(*array.String); ok {
				return sa.Value(row)
			}
			return fmt.Sprintf("%v", col.GetOneForMarshal(row))
		}
	}
	if meta != nil {
		if v, ok := meta[field]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func readRowNumericValue(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int, field string) (float64, bool) {
	if idx, ok := colIdx[field]; ok {
		col := rec.Column(idx)
		if !col.IsNull(row) {
			switch a := col.(type) {
			case *array.Float64:
				return a.Value(row), true
			case *array.Float32:
				return float64(a.Value(row)), true
			case *array.Int64:
				return float64(a.Value(row)), true
			case *array.Int32:
				return float64(a.Value(row)), true
			case *array.Uint64:
				return float64(a.Value(row)), true
			case *array.Uint32:
				return float64(a.Value(row)), true
			}
		}
	}
	if meta != nil {
		if v, ok := meta[field]; ok {
			return toFloat64(v)
		}
	}
	return 0, false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func coversAggs(have, want []AggSpec) bool {
	for _, w := range want {
		found := false
		for _, h := range have {
			if h.Op == w.Op && h.Field == w.Field {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (s *VectorStore) handleMaterializedViewAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "CreateMaterializedView":
		var spec MaterializedViewSpec
		if err := json.Unmarshal(action.Body, &spec); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		snap, err := s.GetMaterializedViewManager().CreateView(stream.Context(), spec)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(snap)
		return true, stream.Send(&flight.Result{Body: body})

	case "RefreshMaterializedView":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		snap, err := s.GetMaterializedViewManager().RefreshView(stream.Context(), req.Name)
		if err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		body, _ := json.Marshal(snap)
		return true, stream.Send(&flight.Result{Body: body})

	case "QueryMaterializedView":
		var req struct {
			Name        string            `json:"name"`
			GroupFilter map[string]string `json:"group_filter,omitempty"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		snap, err := s.GetMaterializedViewManager().QueryView(req.Name, req.GroupFilter)
		if err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		body, _ := json.Marshal(snap)
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
