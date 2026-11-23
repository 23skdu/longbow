package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	configPath     = flag.String("config", defaultConfigPath, "Shared JSON config (flags override values from the file)")
	uri            = flag.String("uri", "", "Data plane address (overrides config)")
	agents         = flag.Int("agents", 0, "Number of agents (overrides config)")
	agentsPerNS    = flag.Int("agents-per-namespace", 0, "Agents sharing one namespace (overrides config; 0 keeps the private-per-agent layout)")
	duration       = flag.Duration("duration", 0, "Soak duration (overrides config duration_hours)")
	dim            = flag.Int("dim", 0, "Vector dimension (overrides config)")
	seed           = flag.Int64("seed", 0, "Random seed (overrides config; 0 is a valid seed)")
	thinkTime      = flag.Duration("think-time", 0, "Think time between ops (overrides config)")
	sampleInterval = flag.Duration("sample-interval", 0, "Metrics sample interval (overrides config)")
	privatePrefix  = flag.String("private-prefix", "", "Private namespace prefix (overrides config)")
	sharedPrefix   = flag.String("shared-prefix", "", "Shared namespace prefix (overrides config)")
	common         = flag.String("common", "", "Common namespace name (overrides config)")
	adminAPIKey    = flag.String("admin-api-key", "", "Bootstrap admin API key (overrides config env var)")
	workloadFlag   = flag.String("workload", "", "Comma-separated op=weight workload override")
	federatedOnly  = flag.Bool("federated-only", false, "Run only the federated operation")
	tlsEnabled     = flag.Bool("tls-enabled", false, "Enable TLS for client connections")
	tlsCert        = flag.String("tls-cert", "", "Client certificate for mTLS")
	tlsKey         = flag.String("tls-key", "", "Client key for mTLS")
	tlsCA          = flag.String("tls-ca", "", "Trusted CA bundle")
	tlsServerName  = flag.String("tls-server-name", "", "TLS server name override")
	tlsMinVersion  = flag.String("tls-min-version", "", "Minimum TLS version (1.2 or 1.3)")
	oidcMode       = flag.String("oidc-mode", "", "OIDC mode: none, aws, or google")
	oidcAudience   = flag.String("oidc-audience", "", "OIDC audience for Google identity tokens")
	oidcTTL        = flag.Duration("oidc-ttl", 0, "OIDC session key TTL")
	tenantID       = flag.String("tenant-id", "", "Tenant ID header")
	outputJSON     = flag.String("json", "", "Output JSON file")
	verbose        = flag.Bool("verbose", false, "Verbose output")
)

// flagSet reports whether a flag was explicitly provided on the command line,
// so zero values (e.g. -seed 0) still override the config file. It is a var so
// tests can exercise override precedence without mutating the global flag set.
var flagSet = func(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

type opStats struct {
	Ops    int64
	Errors int64
	Lats   []float64
	// Windows maps minute buckets from soak start to operation counts. It is
	// used to detect a >50% QPS drop sustained for two minutes.
	Windows map[int]int64
	Last    time.Time
}

type globalMetrics struct {
	Ops    atomic.Int64
	Errors atomic.Int64
	Start  time.Time
	mu     sync.Mutex
	Lats   []float64
	ByOp   map[string]*opStats
	ByErr  map[string]int64
}

func newGlobalMetrics() *globalMetrics {
	return &globalMetrics{
		ByOp:  make(map[string]*opStats),
		ByErr: make(map[string]int64),
		Start: time.Now(),
	}
}

func (g *globalMetrics) record(op string, lat float64, ok bool) {
	g.Ops.Add(1)
	if !ok {
		g.Errors.Add(1)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Lats = append(g.Lats, lat)
	st := g.ByOp[op]
	if st == nil {
		st = &opStats{Windows: make(map[int]int64), Last: time.Now()}
		g.ByOp[op] = st
	}
	st.Ops++
	st.Lats = append(st.Lats, lat)
	st.Last = time.Now()
	bucket := int(time.Since(g.Start).Minutes())
	st.Windows[bucket]++
	if !ok {
		g.ByErr[op]++
		st.Errors++
	}
}

// percentileMS returns the p-th percentile in milliseconds from an already
// sorted latency slice (seconds).
func percentileMS(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100.0 * float64(len(sorted)-1))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	if idx < 0 {
		idx = 0
	}
	return sorted[idx] * 1000
}

func (g *globalMetrics) byOpStats(now time.Time) map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]any, len(g.ByOp))
	elapsed := now.Sub(g.Start).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	for op, st := range g.ByOp {
		lats := append([]float64{}, st.Lats...)
		sort.Float64s(lats)
		opErrorRate := float64(0)
		if st.Ops > 0 {
			opErrorRate = float64(st.Errors) / float64(st.Ops)
		}
		out[op] = map[string]any{
			"ops":        st.Ops,
			"errors":     st.Errors,
			"error_rate": opErrorRate,
			"qps":        float64(st.Ops) / elapsed,
			"p50_ms":     percentileMS(lats, 50),
			"p95_ms":     percentileMS(lats, 95),
			"p99_ms":     percentileMS(lats, 99),
			"last_op_s":  now.Sub(st.Last).Seconds(),
		}
	}
	return out
}

// stalls returns operations whose two-minute moving QPS is at most half the
// preceding two-minute baseline. Sparse operations are ignored because their
// bucket counts are too noisy for this signal.
func (g *globalMetrics) stalls(now time.Time) []map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []map[string]any
	for op, st := range g.ByOp {
		latest := -1
		for bucket := range st.Windows {
			if bucket > latest {
				latest = bucket
			}
		}
		if latest < 3 {
			continue
		}
		baseline := st.Windows[latest-3] + st.Windows[latest-2]
		current := st.Windows[latest-1] + st.Windows[latest]
		if baseline < 4 || current > baseline/2 {
			continue
		}
		baseQPS := float64(baseline) / 120.0
		curQPS := float64(current) / 120.0
		drop := 0.0
		if baseQPS > 0 {
			drop = (1 - curQPS/baseQPS) * 100
		}
		out = append(out, map[string]any{
			"op":           op,
			"baseline_qps": baseQPS,
			"current_qps":  curQPS,
			"drop_pct":     drop,
			"duration_s":   120,
			"at_s":         now.Sub(g.Start).Seconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i]["op"].(string) < out[j]["op"].(string)
	})
	return out
}

func buildSchema(d int) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: arrow.FixedSizeListOf(int32(d), arrow.PrimitiveTypes.Float32)},
		{Name: "importance", Type: arrow.PrimitiveTypes.Int64},
		// geo_point is required by GeoSearch so every seeded namespace has a
		// real geospatial index instead of a FailedPrecondition.
		{Name: "geo_point", Type: arrow.FixedSizeListOf(int32(2), arrow.PrimitiveTypes.Float64)},
		// modality lets MultiModalSearch bind its modality filter.
		{Name: "modality", Type: arrow.BinaryTypes.String},
	}, nil)
}

func randVec(rng *rand.Rand, d int) []float32 {
	v := make([]float32, d)
	var n float64
	for i := 0; i < d; i++ {
		x := rng.Float64()*2 - 1
		v[i] = float32(x)
		n += x * x
	}
	if n == 0 {
		n = 1
	}
	n = 1 / (n) // placeholder
	_ = n
	// normalize
	var nn float64
	for i := 0; i < d; i++ {
		nn += float64(v[i]) * float64(v[i])
	}
	nn = 1.0 / (nn + 1e-12)
	for i := 0; i < d; i++ {
		v[i] = float32(float64(v[i]) * nn)
	}
	return v
}

func doInsert(ctx context.Context, c *client.SmartClient, dataset string, id string, vec []float32, importance int64, d int, geoLat, geoLon float64) error {
	ctx = c.WithAuthContext(ctx)
	schema := buildSchema(d)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	impB := b.Field(2).(*array.Int64Builder)
	geoB := b.Field(3).(*array.FixedSizeListBuilder)
	geoValB := geoB.ValueBuilder().(*array.Float64Builder)
	modalityB := b.Field(4).(*array.StringBuilder)

	idB.Append(id)
	vecB.Append(true)
	for _, x := range vec {
		valB.Append(x)
	}
	impB.Append(importance)
	// Keep every point inside the configured search radius while spreading
	// IDs deterministically so radius, box, and hybrid searches have hits.
	var idSum int64
	for _, r := range id {
		idSum += int64(r)
	}
	jitter := rand.New(rand.NewSource(idSum)) // #nosec G404
	geoB.Append(true)
	geoValB.Append(geoLat + (jitter.Float64()-0.5)*0.02)
	geoValB.Append(geoLon + (jitter.Float64()-0.5)*0.02)
	modalityB.Append("text")
	rec := b.NewRecordBatch()
	defer rec.Release()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{dataset}}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return err
	}
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	writer.SetFlightDescriptor(desc)
	if err := writer.Write(rec); err != nil {
		stream.CloseSend() // #nosec G104
		return err
	}
	if err := writer.Close(); err != nil {
		stream.CloseSend() // #nosec G104
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	_, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err // #nosec G104
}

func doSearch(ctx context.Context, c *client.SmartClient, dataset string, vec []float32) error {
	return doSearchWith(ctx, c, dataset, vec, 5, nil)
}

func doDelete(ctx context.Context, c *client.SmartClient, dataset, id string) error {
	ctx = c.WithAuthContext(ctx)
	body, _ := json.Marshal(map[string]any{"dataset": dataset, "id": id})
	stream, err := c.DoAction(ctx, &flight.Action{Type: "delete", Body: body})
	if err != nil {
		return err
	}
	defer stream.CloseSend() // #nosec G104
	_, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err
}

type agent struct {
	id       int
	cfg      *soakConfig
	topo     *topology
	keys     agentKeys
	metrics  *globalMetrics
	stop     *atomic.Bool
	selector *weightedSelector
	ids      map[string][]string
	mu       sync.Mutex
	verified atomic.Int64
}

// setKeyFor switches to the credential that authorizes the target operation.
func (a *agent) setKeyFor(c *client.SmartClient, ns, op string) {
	if op == "federated" {
		c.SetAPIKey(a.keys.Federated)
		return
	}
	if a.topo.contains(a.topo.ReadOnly(a.id), ns) || op == "cross_read" {
		c.SetAPIKey(a.keys.Reader)
		return
	}
	c.SetAPIKey(a.keys.Writer)
}

func (a *agent) popID(ns string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := a.ids[ns]
	if len(ids) == 0 {
		return ""
	}
	id := ids[len(ids)-1]
	a.ids[ns] = ids[:len(ids)-1]
	return id
}

func (a *agent) pushID(ns, id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ids[ns] = append(a.ids[ns], id)
}

func (a *agent) run(ctx context.Context) {
	rng := rand.New(rand.NewSource(a.cfg.Seed + int64(a.id)*9973)) // #nosec G404
	c, err := newClient(a.cfg, a.keys.Writer)
	if err != nil {
		if *verbose {
			log.Printf("[agent %d] connect err: %v", a.id, err)
		}
		return
	}
	defer c.Close() // #nosec G104
	if err := applyOIDC(ctx, c, a.cfg); err != nil {
		if *verbose {
			log.Printf("[agent %d] OIDC identity err: %v", a.id, err)
		}
		return
	}

	readable := a.topo.Readable(a.id)
	writable := a.topo.Writable(a.id)
	opts := a.cfg.WorkloadOptions
	for _, ns := range a.topo.Namespaces() {
		a.ids[ns] = nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		op := a.selector.pick(rng)
		// A slice of dense reads is used for cross-agent verification so the
		// shared workload schema and the verification invariant stay aligned.
		if op == "dense" && a.topo.agents > 1 && rng.Intn(4) == 0 {
			op = "cross_read"
		}
		t0 := time.Now()
		err := a.execute(ctx, c, rng, op, readable, writable, opts)
		if !shouldRecordOperation(ctx, err) {
			return
		}
		a.metrics.record(op, time.Since(t0).Seconds(), err == nil)
		if err == nil && a.isCrossVerification(op) {
			a.verified.Add(1)
		}
		if err != nil && *verbose {
			log.Printf("[agent %d] %s err: %v", a.id, op, err)
		}
		time.Sleep(time.Duration(a.cfg.ThinkTimeMS) * time.Millisecond)
	}
}

// shouldRecordOperation drops only cancellations caused by the bounded soak
// deadline. Requests already in flight when the session ends are expected to
// fail; counting them would make short, low-volume operations breach the 1%
// per-operation gate at shutdown.
func shouldRecordOperation(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return true
	}
	// gRPC converts context cancellation into a status error, so errors.Is
	// alone does not recognize the in-flight requests aborted at shutdown.
	code := status.Code(err)
	return code != codes.Canceled && code != codes.DeadlineExceeded &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// retryIDNotFound tolerates the server's asynchronous ingest visibility for
// IDs created earlier in the same soak. Only NotFound is retried; any other
// failure is returned immediately, and a persistent NotFound still fails the
// operation once the bounded window expires.
func retryIDNotFound(ctx context.Context, fn func() error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err := fn()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil || status.Code(err) != codes.NotFound {
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		err = fn()
		if err == nil || status.Code(err) != codes.NotFound {
			return err
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// isCrossVerification reports whether a successful operation proves that an
// agent could read another agent's namespace. Federated searches fan out over
// every readable collection, so they also provide that proof in a
// federated-only soak, where no dense reads exist to carve into cross_read.
func (a *agent) isCrossVerification(op string) bool {
	switch op {
	case "cross_read":
		return true
	case "federated":
		return len(a.topo.ReadOnly(a.id)) > 0
	default:
		return false
	}
}

func (a *agent) execute(ctx context.Context, c *client.SmartClient, rng *rand.Rand, op string, readable, writable []string, opts soakWorkloadOptions) error {
	vec := randVec(rng, a.cfg.Dim)
	pickReadable := func() string { return readable[rng.Intn(len(readable))] }
	pickWritable := func() string { return writable[rng.Intn(len(writable))] }

	switch op {
	case "dense":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doSearch(ctx, c, ns, vec)
	case "cross_read":
		readonly := a.topo.ReadOnly(a.id)
		if len(readonly) == 0 {
			return fmt.Errorf("no cross-agent readable namespace")
		}
		ns := readonly[rng.Intn(len(readonly))]
		a.setKeyFor(c, ns, op)
		return doSearch(ctx, c, ns, vec)
	case "hybrid":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doSearchWith(ctx, c, ns, vec, opts.K, map[string]any{
			"text_query": "memory agent", "alpha": opts.Alpha,
		})
	case "sparse":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doSearchWith(ctx, c, ns, vec, opts.K, map[string]any{
			"text_query": "memory", "alpha": 0.0,
		})
	case "filtered":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doSearchWith(ctx, c, ns, vec, opts.K, map[string]any{
			"filters": []map[string]any{{"field": "importance", "operator": ">=", "value": 1}},
		})
	case "range":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doRangeSearch(ctx, c, ns, vec, opts.K)
	case "byid", "get_vector":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		id := a.popID(ns)
		if id == "" {
			id = fmt.Sprintf("seed_%s", ns)
		}
		return retryIDNotFound(ctx, func() error {
			return doByID(ctx, c, ns, id, opts.K)
		})
	case "temporal_asof", "temporal_range", "temporal_window":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doTemporalSearch(ctx, c, ns, temporalSearchType(op), opts.TemporalWindowSize, vec, opts.K)
	case "geo_radius", "geo_box", "geo_hybrid":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doGeoSearch(ctx, c, ns, strings.TrimPrefix(op, "geo_"), opts.Geo, vec, opts.K)
	case "graphrag", "traverse", "pagerank", "communities", "graph_stats":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		nodeID := opts.Graph.StartNode + uint32(rng.Intn(8))
		return doGraphSearch(ctx, c, ns, op, opts.Graph, nodeID)
	case "recommend":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		id := a.popID(ns)
		if id == "" {
			id = fmt.Sprintf("seed_%s", ns)
		}
		return retryIDNotFound(ctx, func() error {
			return doRecommend(ctx, c, ns, id, opts.K, opts.Alpha, opts.Graph.MaxHops)
		})
	case "multimodal":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doMultiModal(ctx, c, ns, opts.K)
	case "text":
		ns := pickReadable()
		a.setKeyFor(c, ns, op)
		return doTextSearch(ctx, c, ns, opts.K)
	case "federated":
		a.setKeyFor(c, "", op)
		return doFederated(ctx, c, readable, opts.FederatedTags, vec, opts.K, opts.RRFK)
	case "insert":
		ns := pickWritable()
		a.setKeyFor(c, ns, op)
		id := fmt.Sprintf("m_%d_%d", a.id, rng.Intn(1<<30))
		if err := doInsert(ctx, c, ns, id, vec, int64(rng.Intn(10)+1), a.cfg.Dim, a.cfg.WorkloadOptions.Geo.Lat, a.cfg.WorkloadOptions.Geo.Lon); err != nil {
			return err
		}
		a.pushID(ns, id)
		return nil
	case "upsert":
		ns := pickWritable()
		a.setKeyFor(c, ns, op)
		id := a.popID(ns)
		if id == "" {
			id = fmt.Sprintf("u_%d_%d", a.id, rng.Intn(1<<30))
		}
		if err := doInsert(ctx, c, ns, id, vec, int64(rng.Intn(10)+1), a.cfg.Dim, a.cfg.WorkloadOptions.Geo.Lat, a.cfg.WorkloadOptions.Geo.Lon); err != nil {
			return err
		}
		a.pushID(ns, id)
		return nil
	case "delete":
		ns := pickWritable()
		a.setKeyFor(c, ns, op)
		id := a.popID(ns)
		if id == "" {
			return nil
		}
		return retryIDNotFound(ctx, func() error {
			return doDelete(ctx, c, ns, id)
		})
	case "delete_ns":
		// Bounded session cleanup: remove every tracked ID in one owned
		// namespace without dropping the namespace itself, so other agents'
		// scoped keys stay valid for the rest of the soak.
		ns := pickWritable()
		a.setKeyFor(c, ns, op)
		for {
			id := a.popID(ns)
			if id == "" {
				return nil
			}
			if err := retryIDNotFound(ctx, func() error {
				return doDelete(ctx, c, ns, id)
			}); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported workload op %q", op)
	}
}

// resolveConfig loads the shared config and applies explicitly set flags over
// it, so zero-valued flags (-seed 0) remain valid overrides.
func resolveConfig() (*soakConfig, error) {
	cfg, err := loadConfig(*configPath)
	if err != nil {
		if flagSet("config") {
			return nil, err
		}
		log.Printf("shared config %s unavailable (%v); using built-in defaults", *configPath, err)
		cfg, err = loadConfig("")
		if err != nil {
			return nil, err
		}
	}
	if flagSet("agents") {
		cfg.Agents = *agents
	}
	if flagSet("agents-per-namespace") {
		cfg.AgentsPerNamespace = *agentsPerNS
	}
	if flagSet("uri") {
		cfg.URI = *uri
	}
	if flagSet("duration") {
		cfg.DurationHours = duration.Hours()
	}
	if flagSet("dim") {
		cfg.Dim = *dim
	}
	if flagSet("seed") {
		cfg.Seed = *seed
	}
	if flagSet("think-time") {
		cfg.ThinkTimeMS = int(thinkTime.Milliseconds())
	}
	if flagSet("sample-interval") {
		cfg.SampleIntervalS = int(sampleInterval.Seconds())
	}
	if flagSet("private-prefix") {
		cfg.Namespaces.PrivatePrefix = *privatePrefix
	}
	if flagSet("shared-prefix") {
		cfg.Namespaces.SharedPrefix = *sharedPrefix
	}
	if flagSet("common") {
		cfg.Namespaces.Common = *common
	}
	if flagSet("workload") {
		override, err := parseWorkloadOverride(*workloadFlag)
		if err != nil {
			return nil, err
		}
		cfg.Workload = override
	}
	if *federatedOnly {
		cfg.Workload = map[string]float64{"federated": 1}
	}
	if flagSet("tls-enabled") {
		cfg.Security.TLS.Enabled = *tlsEnabled
	}
	if flagSet("tls-cert") {
		cfg.Security.TLS.CertFile = *tlsCert
	}
	if flagSet("tls-key") {
		cfg.Security.TLS.KeyFile = *tlsKey
	}
	if flagSet("tls-ca") {
		cfg.Security.TLS.CAFile = *tlsCA
	}
	if flagSet("tls-server-name") {
		cfg.Security.TLS.ServerName = *tlsServerName
	}
	if flagSet("tls-min-version") {
		cfg.Security.TLS.MinVersion = *tlsMinVersion
	}
	if flagSet("oidc-mode") {
		cfg.Security.OIDC.Mode = *oidcMode
	}
	if flagSet("oidc-audience") {
		cfg.Security.OIDC.Audience = *oidcAudience
	}
	if flagSet("oidc-ttl") {
		cfg.Security.OIDC.TTLSeconds = int(oidcTTL.Seconds())
	}
	if flagSet("tenant-id") {
		cfg.Security.TenantID = *tenantID
	}
	if cfg.Security.TLS.Enabled {
		switch cfg.Security.Variant {
		case "plaintext":
			cfg.Security.Variant = "tls"
		case "oidc", "encrypted":
			cfg.Security.Variant = "combined"
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func main() {
	flag.Parse()
	cfg, err := resolveConfig()
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}
	topo, err := newTopology(cfg)
	if err != nil {
		log.Fatalf("topology: %v", err)
	}
	selector, err := newWeightedSelector(cfg.Workload)
	if err != nil {
		log.Fatalf("workload: %v", err)
	}

	adminKey := *adminAPIKey
	if adminKey == "" {
		adminKey = os.Getenv(cfg.RBAC.AdminAPIKeyEnv)
	}
	if strings.TrimSpace(adminKey) == "" {
		log.Fatalf("RBAC is always enforced: set %s or pass -admin-api-key", cfg.RBAC.AdminAPIKeyEnv)
	}
	keys, err := provision(cfg, topo, adminKey)
	if err != nil {
		log.Fatalf("RBAC provisioning failed: %v", err)
	}
	if err := verifyRBAC(cfg, topo, keys); err != nil {
		log.Fatalf("RBAC verification failed: %v", err)
	}
	auditDenials, err := verifyAuditDenials(cfg, adminKey)
	if err != nil {
		log.Fatalf("permission audit assertion failed: %v", err)
	}
	log.Printf("permission audit denials recorded: total=%d", len(auditDenials))

	gm := newGlobalMetrics()
	var stop atomic.Bool
	runDuration := time.Duration(cfg.DurationHours * float64(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	agentsList := make([]*agent, cfg.Agents)
	for i := 0; i < cfg.Agents; i++ {
		a := &agent{
			id: i, cfg: cfg, topo: topo, keys: keys[i], metrics: gm,
			stop: &stop, selector: selector, ids: make(map[string][]string),
		}
		agentsList[i] = a
		wg.Add(1)
		go func(ag *agent) {
			defer wg.Done()
			ag.run(ctx)
		}(a)
	}

	ticker := time.NewTicker(time.Duration(cfg.SampleIntervalS) * time.Second)
	defer ticker.Stop()
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-ticker.C:
			elapsed := time.Since(gm.Start).Seconds()
			qps := float64(gm.Ops.Load()) / elapsed
			errRate := float64(0)
			if gm.Ops.Load() > 0 {
				errRate = float64(gm.Errors.Load()) / float64(gm.Ops.Load())
			}
			degraded := make([]string, 0)
			for _, s := range gm.stalls(time.Now()) {
				op, _ := s["op"].(string)
				drop, _ := s["drop_pct"].(float64)
				degraded = append(degraded, fmt.Sprintf("%s:-%.0f%%", op, drop))
			}
			log.Printf("ops=%d qps=%.1f err_rate=%.4f degraded_ops=%v",
				gm.Ops.Load(), qps, errRate, degraded)
		}
	}
	wg.Wait()
	finished := time.Now()

	gm.mu.Lock()
	lats := append([]float64{}, gm.Lats...)
	gm.mu.Unlock()
	sort.Float64s(lats)
	pct := func(p float64) float64 { return percentileMS(lats, p) }
	verified := int64(0)
	for _, a := range agentsList {
		verified += a.verified.Load()
	}
	errorRate := float64(0)
	if gm.Ops.Load() > 0 {
		errorRate = float64(gm.Errors.Load()) / float64(gm.Ops.Load())
	}
	stalls := gm.stalls(finished)
	byOpStats := gm.byOpStats(finished)
	failures := make([]string, 0)
	if errorRate > 0.01 {
		failures = append(failures, fmt.Sprintf("error_rate %.4f exceeds 1%%", errorRate))
	}
	opNames := make([]string, 0, len(byOpStats))
	for op := range byOpStats {
		opNames = append(opNames, op)
	}
	sort.Strings(opNames)
	for _, op := range opNames {
		stats, ok := byOpStats[op].(map[string]any)
		if !ok {
			continue
		}
		opRate, ok := stats["error_rate"].(float64)
		if ok && opRate > 0.01 {
			failures = append(failures, fmt.Sprintf("operation %s error_rate %.4f exceeds 1%%", op, opRate))
		}
	}
	if verified == 0 {
		failures = append(failures, "no cross-agent verification hits")
	}
	if len(stalls) > 0 {
		failures = append(failures, fmt.Sprintf("%d operation stalls detected", len(stalls)))
	}
	result := map[string]any{
		"agents":           cfg.Agents,
		"namespaces":       len(topo.Namespaces()),
		"namespace_layout": topo.Namespaces(),
		"rbac_enforced":    true,
		"security_variant": cfg.Security.Variant,
		"workload":         cfg.Workload,
		"duration_s":       finished.Sub(gm.Start).Seconds(),
		"ops":              gm.Ops.Load(),
		"errors":           gm.Errors.Load(),
		"error_rate":       errorRate,
		"qps":              float64(gm.Ops.Load()) / finished.Sub(gm.Start).Seconds(),
		"p50_ms":           pct(50),
		"p95_ms":           pct(95),
		"p99_ms":           pct(99),
		"cross_verified":   verified,
		"by_op":            gm.byOpCounts(),
		"by_op_errors":     gm.ByErr,
		"by_op_stats":      byOpStats,
		"stalls":           stalls,
		"audit_denials":    auditDenials,
		"failures":         failures,
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	if *outputJSON != "" {
		if werr := os.WriteFile(*outputJSON, data, 0600); werr != nil {
			log.Printf("write output: %v", werr)
		}
	}
	fmt.Println(string(data))
	if len(failures) > 0 {
		os.Exit(1)
	}
}

func (g *globalMetrics) byOpCounts() map[string]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int64, len(g.ByOp))
	for op, st := range g.ByOp {
		out[op] = st.Ops
	}
	return out
}
