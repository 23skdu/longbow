package memory

import (
	"runtime"
	"runtime/debug"
	"sync"
)

type BulkOperation struct {
	mu           sync.Mutex
	isActive     bool
	previousGOGC int
}

var bulkOp BulkOperation

func BeginBulkOperation() {
	bulkOp.mu.Lock()
	if bulkOp.isActive {
		bulkOp.mu.Unlock()
		return
	}
	bulkOp.isActive = true
	bulkOp.previousGOGC = debug.SetGCPercent(-1)
}

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

func IsBulkOperationActive() bool {
	bulkOp.mu.Lock()
	defer bulkOp.mu.Unlock()
	return bulkOp.isActive
}
