package metrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe" // required by go:linkname

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//go:linkname runtimeProcPin runtime.procPin
func runtimeProcPin() int

//go:linkname runtimeProcUnpin runtime.procUnpin
func runtimeProcUnpin()

const (
	// DefaultShardedFlushInterval is the publish cadence of a ShardedCounter
	// when ShardedCounterOptions.FlushInterval is zero.
	DefaultShardedFlushInterval = 100 * time.Millisecond

	// counterCacheLineBytes is the padding granularity of every shard so that
	// two accumulators never share a cache line. It mirrors the padding style
	// of store.CacheLinePadding and types.PaddedMutex.
	counterCacheLineBytes = 64

	// maxShardedCounterShards bounds the shard allocation for pathological
	// GOMAXPROCS values.
	maxShardedCounterShards = 1 << 16
)

var (
	// ErrShardedCounterDecrease is the error reported when a negative value is
	// added to a sharded counter, matching prometheus.Counter.Add.
	ErrShardedCounterDecrease = errors.New("metrics: sharded counter cannot decrease in value")

	// ErrShardedCounterNilTarget is the error reported when a sharded counter
	// is built without a publish target.
	ErrShardedCounterNilTarget = errors.New("metrics: sharded counter target must not be nil")

	// ErrShardedCounterStopped is returned by Start once Stop has been called.
	ErrShardedCounterStopped = errors.New("metrics: sharded counter already stopped")
)

// counterShard is a single accumulation slot.
//
// The layout mirrors prometheus.(*counter): an exact integer accumulator for
// the common integral case and a float64 accumulator (kept as raw
// math.Float64bits) for everything else. Padding is added so consecutive shards
// start on separate cache lines and a write on one P cannot invalidate the
// line a neighbouring P is writing.
type counterShard struct {
	intAcc   atomic.Uint64
	floatAcc atomic.Uint64
	_        [counterCacheLineBytes - 16]byte
}

// ShardedCounterOptions configures a ShardedCounter.
type ShardedCounterOptions struct {
	// Name is a diagnostic identifier surfaced by Stats. It is not exported as
	// a metric label.
	Name string
	// Shards is the number of cache-line padded accumulation slots. Zero
	// selects the next power of two of runtime.GOMAXPROCS(0); any other value
	// is rounded up to the next power of two.
	Shards int
	// FlushInterval is the publish cadence. Zero selects
	// DefaultShardedFlushInterval.
	FlushInterval time.Duration
}

// ShardedCounterStats is a point in time snapshot of a ShardedCounter.
type ShardedCounterStats struct {
	// Name is the diagnostic identifier of the counter.
	Name string
	// Shards is the number of accumulation slots.
	Shards int
	// Pending is the value accumulated but not yet published.
	Pending float64
	// PublishedTotal is the value handed to the target so far.
	PublishedTotal float64
	// Flushes is the number of completed publish cycles.
	Flushes uint64
	// FlushDurationSum is the total time spent draining and publishing.
	FlushDurationSum time.Duration
	// LastFlushUnixNano is the UnixNano timestamp of the last publish cycle.
	LastFlushUnixNano int64
}

// ShardedCounter is a per-P, cache-line padded accumulator in front of a
// prometheus.Counter.
//
// A query hot path calls Inc/Add/AddInt, which touch only the accumulator
// belonging to the calling P, so N concurrent query goroutines no longer
// serialize on a single shared Prometheus atomic (and on the false sharing
// that comes with it). A background goroutine drains every accumulator and
// adds the aggregate into the target once per FlushInterval.
//
// Drain protocol: every accumulator is drained with a single atomic
// read-modify-write. The integral accumulator uses Swap(0); the float
// accumulator uses a CAS loop that only ever stores zero. Both producers
// (Add/Inc) and the flusher are read-modify-writes on the same address, so the
// memory model totally orders every producer against the drain of its shard:
//
//   - producer before drain: its value is inside the value the drain swapped
//     out and is added to the target in this cycle;
//   - producer after drain: its value is in the freshly zeroed accumulator and
//     is published by the next cycle.
//
// There is no third case, so no value is ever lost or published twice. Values
// added between the drain of a shard and the publish are simply published one
// cycle later, which also makes concurrent Flush calls safe by construction. A
// crash between the drain and the publish loses one cycle of values, the same
// trade-off any buffered metric carries.
type ShardedCounter struct {
	name          string
	target        prometheus.Counter
	shards        []counterShard
	mask          uint32
	flushInterval time.Duration

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	flushes        atomic.Uint64
	flushNanos     atomic.Uint64
	lastFlushNanos atomic.Int64
	published      atomic.Uint64
}

// NewShardedCounter wraps target in a sharded accumulator. The target is never
// registered by the constructor or by Start, so repeated Start calls cannot
// double register a collector; use NewRegisteredShardedCounter when the target
// still has to be created and registered.
func NewShardedCounter(target prometheus.Counter, opts ShardedCounterOptions) *ShardedCounter {
	if target == nil {
		panic(ErrShardedCounterNilTarget)
	}

	n := opts.Shards
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	if n > maxShardedCounterShards {
		n = maxShardedCounterShards
	}
	n = nextPowerOfTwo(n)
	if n < 1 {
		n = 1
	}

	interval := opts.FlushInterval
	if interval <= 0 {
		interval = DefaultShardedFlushInterval
	}

	return &ShardedCounter{
		name:          opts.Name,
		target:        target,
		shards:        make([]counterShard, n),
		mask:          uint32(n - 1), // #nosec G115 -- n is a small power of two
		flushInterval: interval,
	}
}

// NewRegisteredShardedCounter creates a prometheus.Counter, registers it with
// reg and wraps it in a ShardedCounter. If the metric is already registered reg
// returns an existing collector and that collector is reused instead of
// registering a duplicate.
func NewRegisteredShardedCounter(reg prometheus.Registerer, counterOpts prometheus.CounterOpts, opts ShardedCounterOptions) (*ShardedCounter, error) {
	if reg == nil {
		return nil, errors.New("metrics: nil prometheus registerer")
	}

	target := prometheus.NewCounter(counterOpts)
	if err := reg.Register(target); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			return nil, fmt.Errorf("metrics: register %s: %w", counterOpts.Name, err)
		}
		existing, ok := already.ExistingCollector.(prometheus.Counter)
		if !ok {
			return nil, fmt.Errorf("metrics: %s already registered as %T", counterOpts.Name, already.ExistingCollector)
		}
		target = existing
	}

	if opts.Name == "" {
		opts.Name = counterOpts.Name
	}
	return NewShardedCounter(target, opts), nil
}

// Name returns the diagnostic name of the counter.
func (a *ShardedCounter) Name() string { return a.name }

// Shards returns the number of accumulation slots.
func (a *ShardedCounter) Shards() int { return len(a.shards) }

// FlushInterval returns the configured publish cadence.
func (a *ShardedCounter) FlushInterval() time.Duration { return a.flushInterval }

// shard returns the accumulator of the calling P. The P is pinned only for the
// index computation so preemption stays enabled for the caller.
func (a *ShardedCounter) shard() *counterShard {
	p := runtimeProcPin()
	idx := uint32(p) & a.mask // #nosec G115 -- procPin returns a non-negative P id
	runtimeProcUnpin()
	return &a.shards[idx]
}

// Inc adds one to the accumulator of the calling P.
func (a *ShardedCounter) Inc() {
	a.shard().intAcc.Add(1)
}

// AddInt adds v to the integral accumulator of the calling P.
func (a *ShardedCounter) AddInt(v int64) {
	if v < 0 {
		panic(ErrShardedCounterDecrease)
	}
	if v == 0 {
		return
	}
	a.shard().intAcc.Add(uint64(v))
}

// Add adds v to the accumulator of the calling P. Integral values take a
// single atomic add, everything else a CAS loop on the same per-P cache line.
func (a *ShardedCounter) Add(v float64) {
	if v < 0 {
		panic(ErrShardedCounterDecrease)
	}
	if v == 0 {
		return
	}
	if ival := uint64(v); float64(ival) == v {
		a.shard().intAcc.Add(ival)
		return
	}

	s := a.shard()
	for {
		old := s.floatAcc.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if s.floatAcc.CompareAndSwap(old, next) {
			return
		}
	}
}

// Pending returns the accumulated value that has not been published yet.
func (a *ShardedCounter) Pending() float64 {
	var total float64
	for i := range a.shards {
		total += pendingCounterShard(&a.shards[i])
	}
	return total
}

// Flush drains every accumulator and adds the aggregate to the target. It
// returns the published value, which is zero when nothing was pending.
func (a *ShardedCounter) Flush() float64 {
	start := time.Now()

	var total float64
	for i := range a.shards {
		total += drainCounterShard(&a.shards[i])
	}
	if total != 0 {
		a.target.Add(total)
	}

	elapsed := time.Since(start)
	a.flushes.Add(1)
	a.flushNanos.Add(uint64(elapsed)) // #nosec G115 -- elapsed is a non-negative duration
	a.lastFlushNanos.Store(time.Now().UnixNano())
	addFloatCAS(&a.published, total)
	ShardedCounterFlushesTotal.Inc()
	ShardedCounterFlushDurationSeconds.Observe(elapsed.Seconds())
	return total
}

// addFloatCAS accumulates v into a float64 kept as raw bits. Float bit patterns
// cannot be summed directly, so the running total is updated with a CAS loop.
// Only the flusher calls it, once per cycle, so the retries are irrelevant.
func addFloatCAS(acc *atomic.Uint64, v float64) {
	if v == 0 {
		return
	}
	for {
		old := acc.Load()
		if acc.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// pendingCounterShard reads a shard without modifying it.
func pendingCounterShard(s *counterShard) float64 {
	return float64(s.intAcc.Load()) + math.Float64frombits(s.floatAcc.Load())
}

// drainCounterShard atomically returns the value of a shard and resets it to
// zero. Every store is a read-modify-write, so a concurrent Add either lands
// before the drain and is returned, or lands after it and stays pending.
func drainCounterShard(s *counterShard) float64 {
	total := float64(s.intAcc.Swap(0))
	for {
		old := s.floatAcc.Load()
		if old == 0 {
			return total
		}
		if s.floatAcc.CompareAndSwap(old, 0) {
			return total + math.Float64frombits(old)
		}
	}
}

// Start launches the background flusher. It is idempotent: a second call while
// running returns immediately without spawning another goroutine or touching
// the registry. Cancelling ctx stops the flusher after a final flush. Start
// after Stop returns ErrShardedCounterStopped.
func (a *ShardedCounter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stopped {
		return ErrShardedCounterStopped
	}
	if a.started {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	a.cancel = cancel
	a.done = done
	a.started = true

	go a.run(runCtx, done)
	return nil
}

// Stop stops the background flusher, waiting for the in-flight flush and the
// final flush to complete. It is idempotent and safe to call on a counter that
// was never started, in which case pending values are published inline. If ctx
// expires before the flusher exits, ctx.Err() is returned and the final flush
// still happens in the background.
func (a *ShardedCounter) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	started, cancel, done := a.started, a.cancel, a.done
	a.started, a.cancel, a.done = false, nil, nil
	a.mu.Unlock()

	if !started {
		a.Flush()
		return nil
	}

	cancel()
	if done == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run publishes on every tick and once more on shutdown. The final flush runs
// before done is closed, so a Stop that observes a closed done knows the
// pending values reached the target.
func (a *ShardedCounter) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer a.Flush() // LIFO: flushes before done is closed, and a panic still closes done

	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.Flush()
		}
	}
}

// Stats returns a point in time snapshot of the accumulator.
func (a *ShardedCounter) Stats() ShardedCounterStats {
	return ShardedCounterStats{
		Name:              a.name,
		Shards:            len(a.shards),
		Pending:           a.Pending(),
		PublishedTotal:    math.Float64frombits(a.published.Load()),
		Flushes:           a.flushes.Load(),
		FlushDurationSum:  time.Duration(a.flushNanos.Load()), // #nosec G115 -- round-trips the uint64 nanosecond accumulator
		LastFlushUnixNano: a.lastFlushNanos.Load(),
	}
}

// CounterVecResolver maps label values to the prometheus.Counter a
// ShardedCounterVec child publishes into. It runs at most once per distinct
// label combination, from For, and is expected to memoize the resolved handle
// exactly like GetMetricsCache does for the SIMD kernel metrics.
type CounterVecResolver func(labelValues []string) prometheus.Counter

// ShardedCounterVec fans sharded accumulators out over the label space of a
// prometheus.CounterVec. One accumulator is created per distinct label
// combination and a single background flusher publishes all of them, mirroring
// the pre-resolved handle pattern of GetMetricsCache while removing the shared
// atomic from the query hot path.
type ShardedCounterVec struct {
	opts    ShardedCounterOptions
	resolve CounterVecResolver
	vec     *prometheus.CounterVec

	children sync.Map // string -> *ShardedCounter

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewShardedCounterVec binds a prometheus.CounterVec to sharded accumulators.
// resolve is called once per label combination, off the hot path, and must
// return the child counter for those label values (typically
// vec.WithLabelValues(labelValues...)); returning nil makes For return nil.
func NewShardedCounterVec(vec *prometheus.CounterVec, resolve CounterVecResolver, opts ShardedCounterOptions) *ShardedCounterVec {
	if vec == nil {
		panic(ErrShardedCounterNilTarget)
	}
	if resolve == nil {
		panic(errors.New("metrics: sharded counter vec resolver must not be nil"))
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultShardedFlushInterval
	}
	return &ShardedCounterVec{opts: opts, resolve: resolve, vec: vec}
}

// Vec returns the backing prometheus.CounterVec.
func (v *ShardedCounterVec) Vec() *prometheus.CounterVec { return v.vec }

// For returns the accumulator for labelValues, creating it on first use. The
// label lookup is memoized, so a hot path should hoist the call out of its loop
// and keep the returned handle, the same way GetMetricsCache is used today. For
// returns nil when the resolver cannot map the label values.
func (v *ShardedCounterVec) For(labelValues ...string) *ShardedCounter {
	key := labelValuesKey(labelValues)
	if child, ok := v.children.Load(key); ok {
		return child.(*ShardedCounter)
	}

	target := v.resolve(labelValues)
	if target == nil {
		return nil
	}

	childOpts := v.opts
	childOpts.Name = v.opts.Name + "{" + strings.Join(labelValues, ",") + "}"
	child := NewShardedCounter(target, childOpts)
	if existing, loaded := v.children.LoadOrStore(key, child); loaded {
		return existing.(*ShardedCounter)
	}
	return child
}

// Labels returns the label combinations with a live accumulator, sorted so the
// result is deterministic.
func (v *ShardedCounterVec) Labels() []string {
	keys := make([]string, 0, v.childrenCount())
	v.children.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})
	slices.Sort(keys)
	return keys
}

func (v *ShardedCounterVec) childrenCount() int {
	n := 0
	v.children.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// Flush drains and publishes every child accumulator and returns the total
// published value.
func (v *ShardedCounterVec) Flush() float64 {
	var total float64
	v.children.Range(func(_, val any) bool {
		total += val.(*ShardedCounter).Flush()
		return true
	})
	return total
}

// Start launches the single background flusher of the vector. Like
// ShardedCounter.Start it is idempotent and registers nothing.
func (v *ShardedCounterVec) Start(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.stopped {
		return ErrShardedCounterStopped
	}
	if v.started {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	v.cancel = cancel
	v.done = done
	v.started = true

	go v.run(runCtx, done)
	return nil
}

// Stop stops the background flusher after flushing every child, including
// children created after Start.
func (v *ShardedCounterVec) Stop(ctx context.Context) error {
	v.mu.Lock()
	if v.stopped {
		v.mu.Unlock()
		return nil
	}
	v.stopped = true
	started, cancel, done := v.started, v.cancel, v.done
	v.started, v.cancel, v.done = false, nil, nil
	v.mu.Unlock()

	if !started {
		v.Flush()
		return nil
	}

	cancel()
	if done == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *ShardedCounterVec) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer v.Flush() // LIFO: final flush completes before done is closed

	ticker := time.NewTicker(v.opts.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.Flush()
		}
	}
}

func labelValuesKey(labelValues []string) string {
	return strings.Join(labelValues, "\xff")
}

func nextPowerOfTwo(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

var (
	// ShardedCounterFlushesTotal counts publish cycles across all sharded counters.
	ShardedCounterFlushesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_sharded_counter_flushes_total",
			Help: "Total number of sharded counter drain and publish cycles",
		},
	)

	// ShardedCounterFlushDurationSeconds measures the cost of one drain and publish.
	ShardedCounterFlushDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "longbow_sharded_counter_flush_duration_seconds",
			Help: "Time spent draining and publishing sharded metric accumulators",
			Buckets: []float64{
				1e-6, // 1µs
				5e-6, // 5µs
				1e-5, // 10µs
				5e-5, // 50µs
				1e-4, // 100µs
				5e-4, // 500µs
				1e-3, // 1ms
				5e-3, // 5ms
			},
		},
	)
)
