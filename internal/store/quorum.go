package store

import (
"context"
"errors"
"strings"
"sync"
"sync/atomic"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// ConsistencyLevel defines the consistency requirements for read/write operations
type ConsistencyLevel int

const (
ConsistencyOne    ConsistencyLevel = iota // At least one node must acknowledge
ConsistencyQuorum                         // Majority of nodes must acknowledge
ConsistencyAll                            // All nodes must acknowledge
)

// String returns the string representation of ConsistencyLevel
func (cl ConsistencyLevel) String() string {
switch cl {
case ConsistencyOne:
return "ONE"
case ConsistencyQuorum:
return "QUORUM"
case ConsistencyAll:
return "ALL"
default:
return "UNKNOWN"
}
}

// ParseConsistencyLevel parses a string into ConsistencyLevel
func ParseConsistencyLevel(s string) (ConsistencyLevel, error) {
switch strings.ToUpper(s) {
case "ONE":
return ConsistencyOne, nil
case "QUORUM":
return ConsistencyQuorum, nil
case "ALL":
return ConsistencyAll, nil
default:
return ConsistencyOne, errors.New("invalid consistency level: " + s)
}
}

// =============================================================================
// QuorumCalculator - Calculates quorum requirements
// =============================================================================

// QuorumCalculator handles quorum arithmetic
type QuorumCalculator struct{}

// NewQuorumCalculator creates a new QuorumCalculator
func NewQuorumCalculator() *QuorumCalculator {
return &QuorumCalculator{}
}

// RequiredNodes returns the number of nodes required for the given consistency level
func (qc *QuorumCalculator) RequiredNodes(totalNodes int, level ConsistencyLevel) int {
if totalNodes <= 0 {
return 0
}

switch level {
case ConsistencyOne:
return 1
case ConsistencyQuorum:
return (totalNodes / 2) + 1
case ConsistencyAll:
return totalNodes
default:
return 1
}
}

// IsSatisfied returns true if the number of acks meets the consistency requirement
func (qc *QuorumCalculator) IsSatisfied(acks, totalNodes int, level ConsistencyLevel) bool {
required := qc.RequiredNodes(totalNodes, level)
return acks >= required
}

// =============================================================================
// QuorumManager - Manages quorum-based operations
// =============================================================================

// QuorumConfig holds configuration for quorum operations
type QuorumConfig struct {
DefaultReadLevel  ConsistencyLevel
DefaultWriteLevel ConsistencyLevel
Timeout           time.Duration
}

// QuorumResult holds the result of a quorum operation
type QuorumResult struct {
SuccessCount int
FailureCount int
QuorumMet    bool
Err          error
Responses    map[string][]byte // For read operations
}

// QuorumManager manages quorum-based read/write operations
type QuorumManager struct {
config     QuorumConfig
calculator *QuorumCalculator
}

// NewQuorumManager creates a new QuorumManager
func NewQuorumManager(cfg QuorumConfig) *QuorumManager {
if cfg.Timeout == 0 {
cfg.Timeout = 5 * time.Second
}
return &QuorumManager{
config:     cfg,
calculator: NewQuorumCalculator(),
}
}

// GetDefaultReadLevel returns the default read consistency level
func (qm *QuorumManager) GetDefaultReadLevel() ConsistencyLevel {
return qm.config.DefaultReadLevel
}

// GetDefaultWriteLevel returns the default write consistency level
func (qm *QuorumManager) GetDefaultWriteLevel() ConsistencyLevel {
return qm.config.DefaultWriteLevel
}

// WriteFn is the function signature for write operations
type WriteFn func(ctx context.Context, peer string) error

// ReadFn is the function signature for read operations
type ReadFn func(ctx context.Context, peer string) ([]byte, error)

// ExecuteWrite executes a write operation across peers with quorum semantics
func (qm *QuorumManager) ExecuteWrite(
ctx context.Context,
peers []string,
level ConsistencyLevel,
writeFn WriteFn,
) QuorumResult {
start := time.Now()
defer func() {
metrics.QuorumOperationDuration.WithLabelValues("write", level.String()).Observe(time.Since(start).Seconds())
}()

if len(peers) == 0 {
return QuorumResult{Err: errors.New("no peers provided")}
}

required := qm.calculator.RequiredNodes(len(peers), level)
var successCount int64
var failureCount int64

ctx, cancel := context.WithTimeout(ctx, qm.config.Timeout)
defer cancel()

var wg sync.WaitGroup
resultChan := make(chan bool, len(peers))

for _, peer := range peers {
wg.Add(1)
go func(p string) {
defer wg.Done()
err := writeFn(ctx, p)
if err != nil {
atomic.AddInt64(&failureCount, 1)
resultChan <- false
} else {
atomic.AddInt64(&successCount, 1)
resultChan <- true
}
}(peer)
}

// Wait for completion in background
go func() {
wg.Wait()
close(resultChan)
}()

// Wait for quorum or completion
for range resultChan {
if int(atomic.LoadInt64(&successCount)) >= required {
break
}
}

// Drain remaining results
for range resultChan {
}

successes := int(atomic.LoadInt64(&successCount))
failures := int(atomic.LoadInt64(&failureCount))
quorumMet := successes >= required

if quorumMet {
metrics.QuorumSuccessTotal.WithLabelValues("write", level.String()).Inc()
} else {
metrics.QuorumFailureTotal.WithLabelValues("write", level.String()).Inc()
}

return QuorumResult{
SuccessCount: successes,
FailureCount: failures,
QuorumMet:    quorumMet,
}
}

// ExecuteRead executes a read operation across peers with quorum semantics
func (qm *QuorumManager) ExecuteRead(
ctx context.Context,
peers []string,
level ConsistencyLevel,
readFn ReadFn,
) QuorumResult {
start := time.Now()
defer func() {
metrics.QuorumOperationDuration.WithLabelValues("read", level.String()).Observe(time.Since(start).Seconds())
}()

if len(peers) == 0 {
return QuorumResult{Err: errors.New("no peers provided")}
}

required := qm.calculator.RequiredNodes(len(peers), level)
var successCount int64
var failureCount int64

ctx, cancel := context.WithTimeout(ctx, qm.config.Timeout)
defer cancel()

type readResult struct {
peer string
data []byte
err  error
}

resultChan := make(chan readResult, len(peers))
var wg sync.WaitGroup

for _, peer := range peers {
wg.Add(1)
go func(p string) {
defer wg.Done()
data, err := readFn(ctx, p)
resultChan <- readResult{peer: p, data: data, err: err}
}(peer)
}

// Wait for completion in background
go func() {
wg.Wait()
close(resultChan)
}()

responses := make(map[string][]byte)
var mu sync.Mutex

for res := range resultChan {
if res.err != nil {
atomic.AddInt64(&failureCount, 1)
} else {
atomic.AddInt64(&successCount, 1)
mu.Lock()
responses[res.peer] = res.data
mu.Unlock()
}
// Continue collecting - quorum tracking happens after loop
}

successes := int(atomic.LoadInt64(&successCount))
failures := int(atomic.LoadInt64(&failureCount))
quorumMet := successes >= required

if quorumMet {
metrics.QuorumSuccessTotal.WithLabelValues("read", level.String()).Inc()
} else {
metrics.QuorumFailureTotal.WithLabelValues("read", level.String()).Inc()
}

return QuorumResult{
SuccessCount: successes,
FailureCount: failures,
QuorumMet:    quorumMet,
Responses:    responses,
}
}
