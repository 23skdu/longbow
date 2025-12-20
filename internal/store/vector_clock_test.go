package store

import (
	"sync"
	"testing"
)

// =============================================================================
// VectorClock Tests
// =============================================================================

func TestVectorClock_NewVectorClock(t *testing.T) {
	vc := NewVectorClock("node1")
	if vc == nil {
		t.Fatal("expected non-nil VectorClock")
	}
	if vc.NodeID() != "node1" {
		t.Errorf("expected nodeID 'node1', got %s", vc.NodeID())
	}
	if vc.Get("node1") != 0 {
		t.Errorf("expected initial clock 0, got %d", vc.Get("node1"))
	}
}

func TestVectorClock_Increment(t *testing.T) {
	vc := NewVectorClock("node1")
	vc.Increment()
	if vc.Get("node1") != 1 {
		t.Errorf("expected clock 1 after increment, got %d", vc.Get("node1"))
	}
	vc.Increment()
	vc.Increment()
	if vc.Get("node1") != 3 {
		t.Errorf("expected clock 3 after 3 increments, got %d", vc.Get("node1"))
	}
}

func TestVectorClock_Merge(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Increment() // node1: 1
	vc1.Increment() // node1: 2

	vc2 := NewVectorClock("node2")
	vc2.Increment()     // node2: 1
	vc2.Set("node1", 1) // node1: 1, node2: 1

	vc1.Merge(vc2)

	// After merge, vc1 should have max(2,1)=2 for node1, max(0,1)=1 for node2
	if vc1.Get("node1") != 2 {
		t.Errorf("expected node1=2, got %d", vc1.Get("node1"))
	}
	if vc1.Get("node2") != 1 {
		t.Errorf("expected node2=1, got %d", vc1.Get("node2"))
	}
}

func TestVectorClock_Compare_Equal(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 2)
	vc1.Set("node2", 3)

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 2)
	vc2.Set("node2", 3)

	if vc1.Compare(vc2) != ClockEqual {
		t.Errorf("expected ClockEqual, got %v", vc1.Compare(vc2))
	}
}

func TestVectorClock_Compare_Before(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 1)
	vc1.Set("node2", 2)

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 2)
	vc2.Set("node2", 3)

	// vc1 happened before vc2 (all values <=, at least one <)
	if vc1.Compare(vc2) != ClockBefore {
		t.Errorf("expected ClockBefore, got %v", vc1.Compare(vc2))
	}
}

func TestVectorClock_Compare_After(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 3)
	vc1.Set("node2", 4)

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 2)
	vc2.Set("node2", 3)

	// vc1 happened after vc2
	if vc1.Compare(vc2) != ClockAfter {
		t.Errorf("expected ClockAfter, got %v", vc1.Compare(vc2))
	}
}

func TestVectorClock_Compare_Concurrent(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 2)
	vc1.Set("node2", 1)

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 1)
	vc2.Set("node2", 2)

	// Neither happened before the other - concurrent/conflict
	if vc1.Compare(vc2) != ClockConcurrent {
		t.Errorf("expected ClockConcurrent, got %v", vc1.Compare(vc2))
	}
}

func TestVectorClock_Copy(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 5)
	vc1.Set("node2", 3)

	vc2 := vc1.Copy()

	// Modify original
	vc1.Set("node1", 10)

	// Copy should be unaffected
	if vc2.Get("node1") != 5 {
		t.Errorf("copy was affected by original modification")
	}
}

func TestVectorClock_ConcurrentAccess(t *testing.T) {
	vc := NewVectorClock("node1")
	var wg sync.WaitGroup

	// Concurrent increments
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vc.Increment()
		}()
	}

	wg.Wait()

	if vc.Get("node1") != 100 {
		t.Errorf("expected 100 after concurrent increments, got %d", vc.Get("node1"))
	}
}

func TestVectorClock_Serialize(t *testing.T) {
	vc := NewVectorClock("node1")
	vc.Set("node1", 5)
	vc.Set("node2", 3)
	vc.Set("node3", 7)

	data := vc.Serialize()

	vc2 := NewVectorClock("node1")
	err := vc2.Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	if vc.Compare(vc2) != ClockEqual {
		t.Error("deserialized clock not equal to original")
	}
}

// =============================================================================
// VersionedData Tests
// =============================================================================

func TestVersionedData_New(t *testing.T) {
	vc := NewVectorClock("node1")
	vc.Increment()

	vd := NewVersionedData("test-dataset", []byte("test-data"), vc)

	if vd.Dataset != "test-dataset" {
		t.Errorf("expected dataset 'test-dataset', got %s", vd.Dataset)
	}
	if string(vd.Data) != "test-data" {
		t.Errorf("expected data 'test-data', got %s", string(vd.Data))
	}
	if vd.Clock.Get("node1") != 1 {
		t.Errorf("expected clock node1=1, got %d", vd.Clock.Get("node1"))
	}
}

func TestVersionedData_Supersedes(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 1)
	vd1 := NewVersionedData("ds", []byte("v1"), vc1)

	vc2 := NewVectorClock("node1")
	vc2.Set("node1", 2)
	vd2 := NewVersionedData("ds", []byte("v2"), vc2)

	// vd2 supersedes vd1 (happened after)
	if !vd2.Supersedes(vd1) {
		t.Error("expected vd2 to supersede vd1")
	}
	if vd1.Supersedes(vd2) {
		t.Error("expected vd1 to NOT supersede vd2")
	}
}

func TestVersionedData_Conflicts(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc1.Set("node1", 2)
	vc1.Set("node2", 1)
	vd1 := NewVersionedData("ds", []byte("v1"), vc1)

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 1)
	vc2.Set("node2", 2)
	vd2 := NewVersionedData("ds", []byte("v2"), vc2)

	// Concurrent - neither supersedes
	if vd1.Supersedes(vd2) || vd2.Supersedes(vd1) {
		t.Error("concurrent versions should not supersede each other")
	}
	if !vd1.Conflicts(vd2) {
		t.Error("expected conflict between concurrent versions")
	}
}

// =============================================================================
// Prometheus Metrics Tests
// =============================================================================

func TestVectorClock_Metrics(t *testing.T) {
	// Verify metrics are registered and increment correctly
	vc1 := NewVectorClock("node1")
	vc1.Increment()

	vc2 := NewVectorClock("node2")
	vc2.Set("node1", 1)
	vc2.Set("node2", 5)

	// Merge should increment merge counter
	vc1.Merge(vc2)

	// Compare concurrent should increment conflict counter
	vc3 := NewVectorClock("node1")
	vc3.Set("node1", 2)
	vc3.Set("node2", 1)

	vc4 := NewVectorClock("node2")
	vc4.Set("node1", 1)
	vc4.Set("node2", 2)

	vc3.Compare(vc4) // This is concurrent - should increment conflict metric

	// Just verify no panic - actual metric values tested via /metrics endpoint
}
