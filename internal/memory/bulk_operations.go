package memory

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// BulkOperation tracks the state of a batch memory operation.
type BulkOperation struct {
	mu           sync.Mutex
	isActive     bool
	previousGOGC int
}

var bulkOp BulkOperation

// BeginBulkOperation starts a bulk memory operation, disabling GC.
func BeginBulkOperation() {
	bulkOp.mu.Lock()
	if bulkOp.isActive {
		bulkOp.mu.Unlock()
		return
	}
	bulkOp.isActive = true
	bulkOp.previousGOGC = debug.SetGCPercent(-1)
}

// EndBulkOperation ends a bulk memory operation, restoring GC.
func EndBulkOperation() {
	bulkOp.mu.Lock()
	if !bulkOp.isActive {
		bulkOp.mu.Unlock()
		return
	}
	bulkOp.isActive = false
	debug.SetGCPercent(bulkOp.previousGOGC)
	runtime.GC()
	bulkOp.mu.Unlock()
}

// IsBulkOperationActive returns true if a bulk operation is currently in progress.
func IsBulkOperationActive() bool {
	bulkOp.mu.Lock()
	defer bulkOp.mu.Unlock()
	return bulkOp.isActive
}
