package store

import (
"context"
"encoding/json"
"errors"
"fmt"
"sync"
"sync/atomic"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/ipc"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/23skdu/longbow/internal/metrics"
	"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
)

// =============================================================================
// Flight Client Pool - Connection pooling for peer-to-peer Arrow Flight
// =============================================================================

// FlightClientPoolConfig configures the Flight client connection pool.
type FlightClientPoolConfig struct {
// MaxConnsPerHost is the maximum connections per peer host (default: 10)
MaxConnsPerHost int
// MinConnsPerHost is the minimum idle connections to maintain (default: 1)
MinConnsPerHost int
// ConnTimeout is the connection establishment timeout (default: 10s)
ConnTimeout time.Duration
// IdleTimeout is how long idle connections are kept (default: 5m)
IdleTimeout time.Duration
// MaxConnLifetime is the maximum lifetime of any connection (default: 1h)
MaxConnLifetime time.Duration
// HealthCheckInterval is how often to check connection health (default: 30s)
HealthCheckInterval time.Duration
// EnableCompression enables gzip compression for Flight streams
EnableCompression bool
}

// DefaultFlightClientPoolConfig returns sensible defaults for Flight client pooling.
func DefaultFlightClientPoolConfig() FlightClientPoolConfig {
return FlightClientPoolConfig{
MaxConnsPerHost:     10,
MinConnsPerHost:     1,
ConnTimeout:         10 * time.Second,
IdleTimeout:         5 * time.Minute,
MaxConnLifetime:     time.Hour,
HealthCheckInterval: 30 * time.Second,
EnableCompression:   true,
}
}

// Validate checks if the configuration is valid.
func (c FlightClientPoolConfig) Validate() error {
if c.MaxConnsPerHost <= 0 {
return errors.New("MaxConnsPerHost must be positive")
}
if c.MinConnsPerHost < 0 {
return errors.New("MinConnsPerHost cannot be negative")
}
if c.MinConnsPerHost > c.MaxConnsPerHost {
return fmt.Errorf("MinConnsPerHost (%d) cannot exceed MaxConnsPerHost (%d)",
c.MinConnsPerHost, c.MaxConnsPerHost)
}
if c.ConnTimeout <= 0 {
return errors.New("ConnTimeout must be positive")
}
if c.IdleTimeout < 0 {
return errors.New("IdleTimeout cannot be negative")
}
return nil
}

// FlightClientPoolStats provides statistics about the connection pool.
type FlightClientPoolStats struct {
TotalHosts       int
TotalConnections int
ActiveConns      int
IdleConns        int
Gets             int64
Puts             int64
Hits             int64
Misses           int64
Timeouts         int64
Errors           int64
}

// PooledFlightClient wraps a Flight client with pool metadata.
type PooledFlightClient struct {
client    flight.Client
host      string
createdAt time.Time
lastUsed  time.Time
inUse     bool
}

// Host returns the peer host address.
func (c *PooledFlightClient) Host() string {
return c.host
}

// CreatedAt returns when the connection was established.
func (c *PooledFlightClient) CreatedAt() time.Time {
return c.createdAt
}

// LastUsed returns when the connection was last used.
func (c *PooledFlightClient) LastUsed() time.Time {
return c.lastUsed
}

// IsExpired checks if the connection exceeds max lifetime.
func (c *PooledFlightClient) IsExpired(maxLifetime time.Duration) bool {
return time.Since(c.createdAt) > maxLifetime
}

// Client returns the underlying Flight client.
func (c *PooledFlightClient) Client() flight.Client {
return c.client
}

// hostPool manages connections to a single peer host.
type hostPool struct {
mu          sync.Mutex
host        string
maxConns    int
minConns    int
conns       []*PooledFlightClient // idle connections
activeCount int32                 // in-use connections
}

func newHostPool(host string, maxConns, minConns int) *hostPool {
return &hostPool{
host:     host,
maxConns: maxConns,
minConns: minConns,
conns:    make([]*PooledFlightClient, 0, maxConns),
}
}

// tryCreate attempts to create a new connection slot (not actual connection).
// Returns a placeholder PooledFlightClient if slot available, nil if at max.
func (hp *hostPool) tryCreate() *PooledFlightClient {
poolLockStart1 := time.Now()
hp.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("healthcheck").Observe(time.Since(poolLockStart1).Seconds())
defer hp.mu.Unlock()

active := int(atomic.LoadInt32(&hp.activeCount))
if active >= hp.maxConns {
return nil
}

// Reserve a slot
atomic.AddInt32(&hp.activeCount, 1)
return &PooledFlightClient{
host:      hp.host,
createdAt: time.Now(),
lastUsed:  time.Now(),
inUse:     true,
}
}

// getIdle retrieves an idle connection if available.
func (hp *hostPool) getIdle() *PooledFlightClient {
poolLockStart2 := time.Now()
hp.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("healthcheck").Observe(time.Since(poolLockStart2).Seconds())
defer hp.mu.Unlock()

if len(hp.conns) == 0 {
return nil
}

// Pop from end (LIFO for better cache locality)
conn := hp.conns[len(hp.conns)-1]
hp.conns = hp.conns[:len(hp.conns)-1]
conn.inUse = true
conn.lastUsed = time.Now()
atomic.AddInt32(&hp.activeCount, 1)
return conn
}

// returnConn returns a connection to the idle pool.
func (hp *hostPool) returnConn(conn *PooledFlightClient) {
poolLockStart3 := time.Now()
hp.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("healthcheck").Observe(time.Since(poolLockStart3).Seconds())
defer hp.mu.Unlock()

atomic.AddInt32(&hp.activeCount, -1)
conn.inUse = false
conn.lastUsed = time.Now()

// Add to idle pool if not at capacity
if len(hp.conns) < hp.maxConns {
hp.conns = append(hp.conns, conn)
} else if conn.client != nil {
// Close excess connection
_ = conn.client.Close()
}
}

// close closes all connections in the pool.
func (hp *hostPool) close() {
poolLockStart4 := time.Now()
hp.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("healthcheck").Observe(time.Since(poolLockStart4).Seconds())
defer hp.mu.Unlock()

for _, conn := range hp.conns {
if conn.client != nil {
_ = conn.client.Close()
}
}
hp.conns = nil
}

// stats returns pool statistics.
func (hp *hostPool) stats() (idle, active int) {
poolLockStart5 := time.Now()
hp.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("healthcheck").Observe(time.Since(poolLockStart5).Seconds())
defer hp.mu.Unlock()
return len(hp.conns), int(atomic.LoadInt32(&hp.activeCount))
}

// FlightClientPool manages pooled connections to multiple peer Flight servers.
type FlightClientPool struct {
mu       sync.RWMutex
config   FlightClientPoolConfig
hosts    map[string]*hostPool
closed   bool
allocator memory.Allocator

// Statistics
gets     int64
puts     int64
hits     int64
misses   int64
timeouts int64
errors   int64
}

// NewFlightClientPool creates a new connection pool.
func NewFlightClientPool(cfg FlightClientPoolConfig) *FlightClientPool {
return &FlightClientPool{
config:    cfg,
hosts:     make(map[string]*hostPool),
allocator: memory.NewGoAllocator(),
}
}

// AddHost adds a peer host to the pool.
func (p *FlightClientPool) AddHost(host string) error {
poolLockStart6 := time.Now()
p.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("pool").Observe(time.Since(poolLockStart6).Seconds())
defer p.mu.Unlock()

if p.closed {
return errors.New("pool is closed")
}

if _, exists := p.hosts[host]; exists {
return fmt.Errorf("host %s already exists", host)
}

p.hosts[host] = newHostPool(host, p.config.MaxConnsPerHost, p.config.MinConnsPerHost)
return nil
}

// RemoveHost removes a peer host from the pool and closes its connections.
func (p *FlightClientPool) RemoveHost(host string) error {
poolLockStart7 := time.Now()
p.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("pool").Observe(time.Since(poolLockStart7).Seconds())
defer p.mu.Unlock()

hp, exists := p.hosts[host]
if !exists {
return fmt.Errorf("host %s not found", host)
}

hp.close()
delete(p.hosts, host)
return nil
}

// Get acquires a connection to the specified host.
// If no idle connection is available, creates a new one.
func (p *FlightClientPool) Get(ctx context.Context, host string) (*PooledFlightClient, error) {
startTime := time.Now()
atomic.AddInt64(&p.gets, 1)

poolLockStart8 := time.Now()
p.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("pool").Observe(time.Since(poolLockStart8).Seconds())
if p.closed {
p.mu.Unlock()
return nil, errors.New("pool is closed")
}

// Auto-add host if not exists
hp, exists := p.hosts[host]
if !exists {
hp = newHostPool(host, p.config.MaxConnsPerHost, p.config.MinConnsPerHost)
p.hosts[host] = hp
}
p.mu.Unlock()

// Try to get idle connection
if conn := hp.getIdle(); conn != nil && conn.client != nil {
atomic.AddInt64(&p.hits, 1)
metrics.FlightPoolWaitDuration.WithLabelValues(host).Observe(time.Since(startTime).Seconds())
metrics.FlightPoolConnectionsActive.WithLabelValues(host).Inc()
return conn, nil
}

// Create new connection
atomic.AddInt64(&p.misses, 1)

conn := hp.tryCreate()
if conn == nil {
atomic.AddInt64(&p.timeouts, 1)
return nil, fmt.Errorf("max connections reached for host %s", host)
}

// Establish actual Flight connection
connCtx, cancel := context.WithTimeout(ctx, p.config.ConnTimeout)
defer cancel()

opts := []grpc.DialOption{
grpc.WithTransportCredentials(insecure.NewCredentials()),
}

client, err := flight.NewClientWithMiddleware(host, nil, nil, opts...)
if err != nil {
atomic.AddInt32(&hp.activeCount, -1)
atomic.AddInt64(&p.errors, 1)
return nil, fmt.Errorf("failed to connect to %s: %w", host, err)
}

// Verify connection with a simple call
_, _ = client.GetFlightInfo(connCtx, &flight.FlightDescriptor{
Type: flight.DescriptorCMD,
Cmd:  []byte("ping"),
})
// Ignore error - server may not support ping, connection is still valid

conn.client = client
conn.createdAt = time.Now()
metrics.FlightPoolWaitDuration.WithLabelValues(host).Observe(time.Since(startTime).Seconds())
metrics.FlightPoolConnectionsActive.WithLabelValues(host).Inc()
return conn, nil
}

// Put returns a connection to the pool.
func (p *FlightClientPool) Put(conn *PooledFlightClient) {
if conn == nil {
return
}
metrics.FlightPoolConnectionsActive.WithLabelValues(conn.host).Dec()

atomic.AddInt64(&p.puts, 1)

poolLockStart9 := time.Now()
p.mu.RLock()
metrics.PoolLockWaitDuration.WithLabelValues("read").Observe(time.Since(poolLockStart9).Seconds())
hp, exists := p.hosts[conn.host]
p.mu.RUnlock()

if !exists {
// Host was removed, close connection
if conn.client != nil {
_ = conn.client.Close()
}
return
}

// Check if connection is expired
if conn.IsExpired(p.config.MaxConnLifetime) {
if conn.client != nil {
_ = conn.client.Close()
}
atomic.AddInt32(&hp.activeCount, -1)
return
}

hp.returnConn(conn)
}

// Stats returns current pool statistics.
func (p *FlightClientPool) Stats() FlightClientPoolStats {
poolLockStart10 := time.Now()
p.mu.RLock()
metrics.PoolLockWaitDuration.WithLabelValues("read").Observe(time.Since(poolLockStart10).Seconds())
defer p.mu.RUnlock()

stats := FlightClientPoolStats{
TotalHosts: len(p.hosts),
Gets:       atomic.LoadInt64(&p.gets),
Puts:       atomic.LoadInt64(&p.puts),
Hits:       atomic.LoadInt64(&p.hits),
Misses:     atomic.LoadInt64(&p.misses),
Timeouts:   atomic.LoadInt64(&p.timeouts),
Errors:     atomic.LoadInt64(&p.errors),
}

for _, hp := range p.hosts {
idle, active := hp.stats()
stats.IdleConns += idle
stats.ActiveConns += active
}
stats.TotalConnections = stats.IdleConns + stats.ActiveConns

return stats
}

// Close closes all connections and shuts down the pool.
func (p *FlightClientPool) Close() error {
poolLockStart11 := time.Now()
p.mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("pool").Observe(time.Since(poolLockStart11).Seconds())
defer p.mu.Unlock()

if p.closed {
return nil
}

p.closed = true
for _, hp := range p.hosts {
hp.close()
}
p.hosts = nil
return nil
}

// =============================================================================
// Peer-to-Peer Arrow Flight Operations
// =============================================================================

// DoGetFromPeer retrieves Arrow RecordBatches from a peer via Flight DoGet.
func (p *FlightClientPool) DoGetFromPeer(ctx context.Context, host, dataset string, filters []Filter) ([]arrow.RecordBatch, error) {
conn, err := p.Get(ctx, host)
if err != nil {
return nil, err
}
defer p.Put(conn)

// Build ticket
ticket := TicketQuery{
Name: dataset,
Filters: filters,
}
ticketBytes, err := json.Marshal(ticket)
if err != nil {
return nil, fmt.Errorf("failed to marshal ticket: %w", err)
}

// Execute DoGet
stream, err := conn.client.DoGet(ctx, &flight.Ticket{Ticket: ticketBytes})
if err != nil {
return nil, fmt.Errorf("DoGet failed: %w", err)
}

// Read all record batches
reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(p.allocator))
if err != nil {
return nil, fmt.Errorf("failed to create reader: %w", err)
}
defer reader.Release()

records := make([]arrow.RecordBatch, 0, 8)
for reader.Next() {
rec := reader.RecordBatch()
rec.Retain() // Keep reference
records = append(records, rec)
}

if err := reader.Err(); err != nil {
return nil, fmt.Errorf("reader error: %w", err)
}

return records, nil
}

// DoPutToPeer sends Arrow RecordBatches to a peer via Flight DoPut.
func (p *FlightClientPool) DoPutToPeer(ctx context.Context, host, dataset string, records []arrow.RecordBatch) error {
if len(records) == 0 {
return nil
}

conn, err := p.Get(ctx, host)
if err != nil {
return err
}
defer p.Put(conn)

// Create Flight descriptor
desc := &flight.FlightDescriptor{
Type: flight.DescriptorPATH,
Path: []string{dataset},
}

// Start DoPut stream
stream, err := conn.client.DoPut(ctx)
if err != nil {
return fmt.Errorf("DoPut failed: %w", err)
}

// Create writer with schema from first record
writer := flight.NewRecordWriter(stream, ipc.WithSchema(records[0].Schema()))
writer.SetFlightDescriptor(desc)

// Write all records
for _, rec := range records {
if err := writer.Write(rec); err != nil {
_ = writer.Close()
return fmt.Errorf("failed to write record: %w", err)
}
}

// Close and receive acknowledgment
if err := writer.Close(); err != nil {
return fmt.Errorf("failed to close writer: %w", err)
}

// Drain put results
for {
_, err := stream.Recv()
if err != nil {
break // End of stream or error
}
}

return nil
}

// ReplicateToPeers replicates data to multiple peers concurrently.
// Returns a map of host -> error for failed replications.
func (p *FlightClientPool) ReplicateToPeers(ctx context.Context, peers []string, dataset string, records []arrow.RecordBatch) map[string]error {
if len(peers) == 0 || len(records) == 0 {
return nil
}

errMap := make(map[string]error)
var mu sync.Mutex
var wg sync.WaitGroup

for _, peer := range peers {
wg.Add(1)
go func(host string) {
defer wg.Done()
if err := p.DoPutToPeer(ctx, host, dataset, records); err != nil {
poolLockStart12 := time.Now()
mu.Lock()
metrics.PoolLockWaitDuration.WithLabelValues("global").Observe(time.Since(poolLockStart12).Seconds())
errMap[host] = err
mu.Unlock()
}
}(peer)
}

wg.Wait()
return errMap
}
