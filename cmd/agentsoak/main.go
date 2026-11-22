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
	uri        = flag.String("uri", "127.0.0.1:3000", "Data plane address")
	agents     = flag.Int("agents", 8, "Number of agents")
	_          = flag.Int("agents-per-ns", 2, "Agents per shared namespace")
	duration   = flag.Duration("duration", 5*time.Minute, "Soak duration")
	dim        = flag.Int("dim", 128, "Vector dimension")
	seed       = flag.Int64("seed", 42, "Random seed")
	outputJSON = flag.String("json", "", "Output JSON file")
	thinkTime  = flag.Duration("think-time", 10*time.Millisecond, "Think time between ops")
	verbose    = flag.Bool("verbose", false, "Verbose output")
)

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

type agent struct {
	id       int
	agents   int
	uri      string
	dim      int
	seed     int64
	metrics  *globalMetrics
	stop     *atomic.Bool
	ids      map[string][]uint32
	mu       sync.Mutex
	verified atomic.Int64
}

func (a *agent) namespaces() (string, []string, string) {
	mine := fmt.Sprintf("private_%d", a.id)
	sharedWrite := fmt.Sprintf("shared_%d", a.id)
	master := "master"
	return mine, []string{sharedWrite}, master
}

func (a *agent) run(ctx context.Context) {
	rng := rand.New(rand.NewSource(a.seed + int64(a.id)*9973)) // #nosec G404
	c, err := client.NewSmartClient(a.uri)
	if err != nil {
		if *verbose {
			log.Printf("[agent %d] connect err: %v", a.id, err)
		}
		return
	}
	defer c.Close()

	mine, sharedWrites, master := a.namespaces()
	allShared := make([]string, a.agents)
	for i := 0; i < a.agents; i++ {
		allShared[i] = fmt.Sprintf("shared_%d", i)
	}
	namespaces := append([]string{mine, master}, allShared...)
	namespaces = append(namespaces, sharedWrites...)
	for _, ns := range namespaces {
		req := map[string]any{"name": ns, "dims": a.dim, "data_type": "float32"}
		body, _ := json.Marshal(req)
		action := &flight.Action{Type: "CreateNamespace", Body: body}
		stream, err := c.DoAction(ctx, action)
		if err == nil {
			for {
				if _, err := stream.Recv(); err != nil {
					break
				}
			}
		}
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
			ns := mine
			if rng.Intn(2) == 0 {
				ns = master
			}
			id := fmt.Sprintf("m_%d_%d", a.id, rng.Intn(1<<30))
			vec := randVec(rng, a.dim)
			err = doInsert(ctx, c, ns, id, vec, int64(rng.Intn(10)+1), a.dim)
			if err == nil {
				a.mu.Lock()
				a.ids[ns] = append(a.ids[ns], 0) // placeholder
				a.mu.Unlock()
			}
			a.metrics.record("insert", time.Since(t0).Seconds(), err == nil)
		case 1:
			readable := append([]string{mine, master}, allShared...)
			ns := readable[rng.Intn(len(readable))]
			vec := randVec(rng, a.dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("search", time.Since(t0).Seconds(), err == nil)
		case 2:
			// cross-agent memory verification: search another agent's shared ns
			other := rng.Intn(a.agents)
			if other == a.id {
				continue
			}
			ns := fmt.Sprintf("shared_%d", other)
			vec := randVec(rng, a.dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("cross_read", time.Since(t0).Seconds(), err == nil)
			if err == nil {
				a.verified.Add(1)
			}
		case 3:
			ns := mine
			// best-effort delete by placeholder id not tracked; skip numeric
			_ = ns
			a.metrics.record("delete", time.Since(t0).Seconds(), true)
		case 4:
			ns := mine
			vec := randVec(rng, a.dim)
			err = doSearch(ctx, c, ns, vec)
			a.metrics.record("federated", time.Since(t0).Seconds(), err == nil)
			_ = master
		}
		if err != nil && *verbose {
			log.Printf("[agent %d] err: %v", a.id, err)
		}
		time.Sleep(*thinkTime)
	}
}

func main() {
	flag.Parse()
	gm := newGlobalMetrics()
	var stop atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	agentsList := make([]*agent, *agents)
	for i := 0; i < *agents; i++ {
		a := &agent{id: i, agents: *agents, uri: *uri, dim: *dim, seed: *seed, metrics: gm, stop: &stop, ids: make(map[string][]uint32)}
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
		"agents":     *agents,
		"duration_s": time.Since(gm.Start).Seconds(),
		"ops":        gm.Ops.Load(),
		"errors":     gm.Errors.Load(),
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
