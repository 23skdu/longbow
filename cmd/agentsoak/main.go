package main

import (
	"context"
	"encoding/json"
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
)

var (
	configPath    = flag.String("config", defaultConfigPath, "Shared JSON config (flags override values from the file)")
	uri           = flag.String("uri", "", "Data plane address (overrides config)")
	agents        = flag.Int("agents", 0, "Number of agents (overrides config)")
	duration      = flag.Duration("duration", 0, "Soak duration (overrides config duration_hours)")
	dim           = flag.Int("dim", 0, "Vector dimension (overrides config)")
	seed          = flag.Int64("seed", 0, "Random seed (overrides config; 0 is a valid seed)")
	thinkTime     = flag.Duration("think-time", 0, "Think time between ops (overrides config)")
	privatePrefix = flag.String("private-prefix", "", "Private namespace prefix (overrides config)")
	sharedPrefix  = flag.String("shared-prefix", "", "Shared namespace prefix (overrides config)")
	common        = flag.String("common", "", "Common namespace name (overrides config)")
	adminAPIKey   = flag.String("admin-api-key", "", "Bootstrap admin API key (overrides config env var)")
	outputJSON    = flag.String("json", "", "Output JSON file")
	verbose       = flag.Bool("verbose", false, "Verbose output")
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

type globalMetrics struct {
	Ops    atomic.Int64
	Errors atomic.Int64
	Start  time.Time
	mu     sync.Mutex
	Lats   []float64
	ByOp   map[string]int64
	ByErr  map[string]int64
}

func newGlobalMetrics() *globalMetrics {
	return &globalMetrics{ByOp: make(map[string]int64), ByErr: make(map[string]int64), Start: time.Now()}
}

func (g *globalMetrics) record(op string, lat float64, ok bool) {
	g.Ops.Add(1)
	if !ok {
		g.Errors.Add(1)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Lats = append(g.Lats, lat)
	g.ByOp[op]++
	if !ok {
		g.ByErr[op]++
	}
}

func buildSchema(d int) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: arrow.FixedSizeListOf(int32(d), arrow.PrimitiveTypes.Float32)},
		{Name: "importance", Type: arrow.PrimitiveTypes.Int64},
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

func doInsert(ctx context.Context, c *client.SmartClient, dataset string, id string, vec []float32, importance int64, d int) error {
	ctx = c.WithAuthContext(ctx)
	schema := buildSchema(d)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	impB := b.Field(2).(*array.Int64Builder)

	idB.Append(id)
	vecB.Append(true)
	for _, x := range vec {
		valB.Append(x)
	}
	impB.Append(importance)
	rec := b.NewRecordBatch()
	defer rec.Release()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{dataset}}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return err
	}
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
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
	return err // #nosec G104
}

func doSearch(ctx context.Context, c *client.SmartClient, dataset string, vec []float32) error {
	ctx = c.WithAuthContext(ctx)
	req := map[string]any{"dataset": dataset, "k": 5, "vector": vec}
	body, _ := json.Marshal(req)
	action := &flight.Action{Type: "VectorSearch", Body: body}
	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
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
	ids      map[string][]string
	mu       sync.Mutex
	verified atomic.Int64
}

// setKeyFor switches to the reader credential for another agent's shared
// namespace and back to the writer credential for owned/common namespaces.
func (a *agent) setKeyFor(c *client.SmartClient, ns string) {
	if a.topo.contains(a.topo.ReadOnly(a.id), ns) {
		c.SetAPIKey(a.keys.Reader)
		return
	}
	c.SetAPIKey(a.keys.Writer)
}

func (a *agent) run(ctx context.Context) {
	rng := rand.New(rand.NewSource(a.cfg.Seed + int64(a.id)*9973)) // #nosec G404
	c, err := newClient(a.cfg.URI, a.keys.Writer)
	if err != nil {
		if *verbose {
			log.Printf("[agent %d] connect err: %v", a.id, err)
		}
		return
	}
	defer c.Close() // #nosec G104

	mine := a.topo.private[a.id]
	readable := a.topo.Readable(a.id)
	writable := a.topo.Writable(a.id)
	for _, ns := range a.topo.Namespaces() {
		a.ids[ns] = nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		op := rng.Intn(5)
		t0 := time.Now()
		var err error
		switch op {
		case 0:
			ns := writable[rng.Intn(len(writable))]
			a.setKeyFor(c, ns)
			id := fmt.Sprintf("m_%d_%d", a.id, rng.Intn(1<<30))
			vec := randVec(rng, a.cfg.Dim)
			err = doInsert(ctx, c, ns, id, vec, int64(rng.Intn(10)+1), a.cfg.Dim)
			if err == nil {
				a.mu.Lock()
				a.ids[ns] = append(a.ids[ns], id)
				a.mu.Unlock()
			}
			a.metrics.record("insert", time.Since(t0).Seconds(), err == nil)
		case 1:
			ns := readable[rng.Intn(len(readable))]
			a.setKeyFor(c, ns)
			vec := randVec(rng, a.cfg.Dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("search", time.Since(t0).Seconds(), err == nil)
		case 2:
			// cross-agent memory verification: search another agent's shared ns
			other := rng.Intn(a.topo.agents)
			if other == a.id {
				continue
			}
			ns := a.topo.shared[other]
			a.setKeyFor(c, ns)
			vec := randVec(rng, a.cfg.Dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("cross_read", time.Since(t0).Seconds(), err == nil)
			if err == nil {
				a.verified.Add(1)
			}
		case 3:
			ns := writable[rng.Intn(len(writable))]
			a.setKeyFor(c, ns)
			a.mu.Lock()
			ids := a.ids[ns]
			var id string
			if len(ids) > 0 {
				id = ids[len(ids)-1]
				a.ids[ns] = ids[:len(ids)-1]
			}
			a.mu.Unlock()
			if id == "" {
				continue
			}
			err = doDelete(ctx, c, ns, id)
			a.metrics.record("delete", time.Since(t0).Seconds(), err == nil)
		case 4:
			ns := mine
			a.setKeyFor(c, ns)
			vec := randVec(rng, a.cfg.Dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("federated", time.Since(t0).Seconds(), err == nil)
		}
		if err != nil && *verbose {
			log.Printf("[agent %d] err: %v", a.id, err)
		}
		time.Sleep(time.Duration(a.cfg.ThinkTimeMS) * time.Millisecond)
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
	if flagSet("private-prefix") {
		cfg.Namespaces.PrivatePrefix = *privatePrefix
	}
	if flagSet("shared-prefix") {
		cfg.Namespaces.SharedPrefix = *sharedPrefix
	}
	if flagSet("common") {
		cfg.Namespaces.Common = *common
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

	gm := newGlobalMetrics()
	var stop atomic.Bool
	runDuration := time.Duration(cfg.DurationHours * float64(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var wg sync.WaitGroup
	agentsList := make([]*agent, cfg.Agents)
	for i := 0; i < cfg.Agents; i++ {
		a := &agent{id: i, cfg: cfg, topo: topo, keys: keys[i], metrics: gm, stop: &stop, ids: make(map[string][]string)}
		agentsList[i] = a
		wg.Add(1)
		go func(ag *agent) {
			defer wg.Done()
			ag.run(ctx)
		}(a)
	}

	// progress ticker
	ticker := time.NewTicker(10 * time.Second)
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
			log.Printf("ops=%d qps=%.1f err_rate=%.4f", gm.Ops.Load(), qps, errRate)
		}
	}
	wg.Wait()

	// build JSON output
	gm.mu.Lock()
	lats := append([]float64{}, gm.Lats...)
	gm.mu.Unlock()
	sort.Float64s(lats)
	pct := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		idx := int(p / 100.0 * float64(len(lats)-1))
		if idx >= len(lats) {
			idx = len(lats) - 1
		}
		return lats[idx] * 1000
	}
	verified := int64(0)
	for _, a := range agentsList {
		verified += a.verified.Load()
	}
	result := map[string]any{
		"agents":           cfg.Agents,
		"namespaces":       len(topo.Namespaces()),
		"namespace_layout": topo.Namespaces(),
		"rbac_enforced":    true,
		"duration_s":       time.Since(gm.Start).Seconds(),
		"ops":              gm.Ops.Load(),
		"errors":           gm.Errors.Load(),
		"error_rate": func() float64 {
			if gm.Ops.Load() == 0 {
				return 0
			}
			return float64(gm.Errors.Load()) / float64(gm.Ops.Load())
		}(),
		"qps":            float64(gm.Ops.Load()) / time.Since(gm.Start).Seconds(),
		"p50_ms":         pct(50),
		"p95_ms":         pct(95),
		"p99_ms":         pct(99),
		"cross_verified": verified,
		"by_op":          gm.ByOp,
		"by_op_errors":   gm.ByErr,
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	if *outputJSON != "" {
		if werr := os.WriteFile(*outputJSON, data, 0600); werr != nil {
			log.Printf("write output: %v", werr)
		}
	}
	fmt.Println(string(data))
}
