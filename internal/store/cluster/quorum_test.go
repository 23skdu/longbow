package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// ConsistencyLevel Tests
// =============================================================================

// TestConsistencyLevel_String runs the test consistency level_string operation.
func TestConsistencyLevel_String(t *testing.T) {
	tests := []struct {
		level    ConsistencyLevel
		expected string
	}{
		{ConsistencyOne, "ONE"},
		{ConsistencyQuorum, "QUORUM"},
		{ConsistencyAll, "ALL"},
	}

	for _, tt := range tests {
		if tt.level.String() != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.level.String())
		}
	}
}

// TestParseConsistencyLevel runs the test parse consistency level operation.
func TestParseConsistencyLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected ConsistencyLevel
		hasError bool
	}{
		{"ONE", ConsistencyOne, false},
		{"one", ConsistencyOne, false},
		{"QUORUM", ConsistencyQuorum, false},
		{"quorum", ConsistencyQuorum, false},
		{"ALL", ConsistencyAll, false},
		{"all", ConsistencyAll, false},
		{"invalid", ConsistencyOne, true},
	}

	for _, tt := range tests {
		level, err := ParseConsistencyLevel(tt.input)
		if tt.hasError && err == nil {
			t.Errorf("expected error for input %s", tt.input)
		}
		if !tt.hasError && level != tt.expected {
			t.Errorf("expected %v for input %s, got %v", tt.expected, tt.input, level)
		}
	}
}

// =============================================================================
// QuorumCalculator Tests
// =============================================================================

// TestQuorumCalculator_RequiredNodes runs the test quorum calculator_required nodes operation.
func TestQuorumCalculator_RequiredNodes(t *testing.T) {
	tests := []struct {
		totalNodes int
		level      ConsistencyLevel
		expected   int
	}{
		// ONE always requires 1
		{1, ConsistencyOne, 1},
		{3, ConsistencyOne, 1},
		{5, ConsistencyOne, 1},

		// QUORUM requires (N/2)+1
		{1, ConsistencyQuorum, 1},
		{2, ConsistencyQuorum, 2},
		{3, ConsistencyQuorum, 2},
		{5, ConsistencyQuorum, 3},
		{7, ConsistencyQuorum, 4},

		// ALL requires N
		{1, ConsistencyAll, 1},
		{3, ConsistencyAll, 3},
		{5, ConsistencyAll, 5},
	}

	calc := NewQuorumCalculator()
	for _, tt := range tests {
		result := calc.RequiredNodes(tt.totalNodes, tt.level)
		if result != tt.expected {
			t.Errorf("RequiredNodes(%d, %s): expected %d, got %d",
				tt.totalNodes, tt.level, tt.expected, result)
		}
	}
}

// TestQuorumCalculator_IsSatisfied runs the test quorum calculator_is satisfied operation.
func TestQuorumCalculator_IsSatisfied(t *testing.T) {
	calc := NewQuorumCalculator()

	// 3 nodes, QUORUM (needs 2)
	if !calc.IsSatisfied(2, 3, ConsistencyQuorum) {
		t.Error("expected quorum satisfied with 2/3 acks")
	}
	if calc.IsSatisfied(1, 3, ConsistencyQuorum) {
		t.Error("expected quorum NOT satisfied with 1/3 acks")
	}

	// 5 nodes, ALL (needs 5)
	if !calc.IsSatisfied(5, 5, ConsistencyAll) {
		t.Error("expected ALL satisfied with 5/5 acks")
	}
	if calc.IsSatisfied(4, 5, ConsistencyAll) {
		t.Error("expected ALL NOT satisfied with 4/5 acks")
	}
}

// =============================================================================
// QuorumManager Tests
// =============================================================================

// TestQuorumManager_NewQuorumManager runs the test quorum manager_new quorum manager operation.
func TestQuorumManager_NewQuorumManager(t *testing.T) {
	cfg := QuorumConfig{
		DefaultReadLevel:  ConsistencyQuorum,
		DefaultWriteLevel: ConsistencyQuorum,
		Timeout:           5 * time.Second,
	}

	mgr := NewQuorumManager(cfg)
	if mgr == nil {
		t.Fatal("expected non-nil QuorumManager")
	}
	if mgr.GetDefaultReadLevel() != ConsistencyQuorum {
		t.Errorf("expected default read level QUORUM")
	}
	if mgr.GetDefaultWriteLevel() != ConsistencyQuorum {
		t.Errorf("expected default write level QUORUM")
	}
}

// TestQuorumManager_ExecuteWrite_One runs the test quorum manager_execute write_one operation.
func TestQuorumManager_ExecuteWrite_One(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyOne,
		Timeout:           1 * time.Second,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}
	callCount := 0
	var mu sync.Mutex

	writeFn := func(ctx context.Context, peer string) error {
		mu.Lock()
		callCount++
		mu.Unlock()
		return nil
	}

	result := mgr.ExecuteWrite(context.Background(), peers, ConsistencyOne, writeFn)

	if result.Err != nil {
		t.Errorf("expected no error, got %v", result.Err)
	}
	if result.SuccessCount < 1 {
		t.Errorf("expected at least 1 success, got %d", result.SuccessCount)
	}
	if !result.QuorumMet {
		t.Error("expected quorum met")
	}
}

// TestQuorumManager_ExecuteWrite_Quorum runs the test quorum manager_execute write_quorum operation.
func TestQuorumManager_ExecuteWrite_Quorum(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyQuorum,
		Timeout:           1 * time.Second,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}

	writeFn := func(ctx context.Context, peer string) error {
		return nil
	}

	result := mgr.ExecuteWrite(context.Background(), peers, ConsistencyQuorum, writeFn)

	if result.Err != nil {
		t.Errorf("expected no error, got %v", result.Err)
	}
	if result.SuccessCount < 2 {
		t.Errorf("expected at least 2 successes for quorum, got %d", result.SuccessCount)
	}
	if !result.QuorumMet {
		t.Error("expected quorum met")
	}
}

// TestQuorumManager_ExecuteWrite_All runs the test quorum manager_execute write_all operation.
func TestQuorumManager_ExecuteWrite_All(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyAll,
		Timeout:           1 * time.Second,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}

	writeFn := func(ctx context.Context, peer string) error {
		return nil
	}

	result := mgr.ExecuteWrite(context.Background(), peers, ConsistencyAll, writeFn)

	if result.Err != nil {
		t.Errorf("expected no error, got %v", result.Err)
	}
	if result.SuccessCount != 3 {
		t.Errorf("expected 3 successes for ALL, got %d", result.SuccessCount)
	}
	if !result.QuorumMet {
		t.Error("expected quorum met")
	}
}

// TestQuorumManager_ExecuteWrite_QuorumFailed runs the test quorum manager_execute write_quorum failed operation.
func TestQuorumManager_ExecuteWrite_QuorumFailed(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyQuorum,
		Timeout:           500 * time.Millisecond,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}
	failCount := 0
	var mu sync.Mutex

	writeFn := func(ctx context.Context, peer string) error {
		mu.Lock()
		failCount++
		fail := failCount <= 2 // First 2 fail
		mu.Unlock()
		if fail {
			return context.DeadlineExceeded
		}
		return nil
	}

	result := mgr.ExecuteWrite(context.Background(), peers, ConsistencyQuorum, writeFn)

	// Only 1 success out of 3, need 2 for quorum
	if result.QuorumMet {
		t.Error("expected quorum NOT met with only 1/3 success")
	}
	if result.SuccessCount != 1 {
		t.Errorf("expected 1 success, got %d", result.SuccessCount)
	}
}

// TestQuorumManager_ExecuteRead_Quorum runs the test quorum manager_execute read_quorum operation.
func TestQuorumManager_ExecuteRead_Quorum(t *testing.T) {
	cfg := QuorumConfig{
		DefaultReadLevel: ConsistencyQuorum,
		Timeout:          1 * time.Second,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}

	readFn := func(ctx context.Context, peer string) ([]byte, error) {
		return []byte("data-from-" + peer), nil
	}

	result := mgr.ExecuteRead(context.Background(), peers, ConsistencyQuorum, readFn)

	if result.Err != nil {
		t.Errorf("expected no error, got %v", result.Err)
	}
	if result.SuccessCount < 2 {
		t.Errorf("expected at least 2 responses for quorum, got %d", result.SuccessCount)
	}
	if len(result.Responses) < 2 {
		t.Errorf("expected at least 2 responses, got %d", len(result.Responses))
	}
	if !result.QuorumMet {
		t.Error("expected quorum met")
	}
}

// TestQuorumManager_Metrics runs the test quorum manager_metrics operation.
func TestQuorumManager_Metrics(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyQuorum,
		Timeout:           1 * time.Second,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}

	// Execute some writes to increment metrics
	writeFn := func(ctx context.Context, peer string) error {
		return nil
	}

	mgr.ExecuteWrite(context.Background(), peers, ConsistencyQuorum, writeFn)
	mgr.ExecuteWrite(context.Background(), peers, ConsistencyOne, writeFn)

	// Metrics should be incremented - no panic means success
}

// TestQuorumManager_Timeout runs the test quorum manager_timeout operation.
func TestQuorumManager_Timeout(t *testing.T) {
	cfg := QuorumConfig{
		DefaultWriteLevel: ConsistencyAll,
		Timeout:           100 * time.Millisecond,
	}
	mgr := NewQuorumManager(cfg)

	peers := []string{"peer1", "peer2", "peer3"}

	writeFn := func(ctx context.Context, peer string) error {
		if peer == "peer3" {
			// Simulate slow peer that respects context
			select {
			case <-time.After(500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	result := mgr.ExecuteWrite(context.Background(), peers, ConsistencyAll, writeFn)

	// ALL requires all 3, but peer3 times out
	if result.QuorumMet {
		t.Error("expected quorum NOT met due to timeout")
	}
	if result.SuccessCount != 2 {
		t.Errorf("expected 2 successes before timeout, got %d", result.SuccessCount)
	}
}
