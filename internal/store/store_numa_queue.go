package store

import (
	"time"

	"github.com/23skdu/longbow/internal/store/types"
)

// EnqueueIndexJob routes an index job to the preferred NUMA-local queue or global fallback queue.
func (vs *VectorStore) EnqueueIndexJob(job types.IndexJob) bool {
	if job.NodeID >= 0 && job.NodeID < len(vs.numaIndexQueues) && vs.numaIndexQueues[job.NodeID] != nil {
		if vs.numaIndexQueues[job.NodeID].Send(job) {
			return true
		}
	}
	if vs.indexQueue != nil {
		return vs.indexQueue.Send(job)
	}
	return false
}

// BlockEnqueueIndexJob attempts to send to the preferred NUMA-local queue or global queue, blocking up to timeout.
func (vs *VectorStore) BlockEnqueueIndexJob(job types.IndexJob, timeout time.Duration) bool {
	if job.NodeID >= 0 && job.NodeID < len(vs.numaIndexQueues) && vs.numaIndexQueues[job.NodeID] != nil {
		if vs.numaIndexQueues[job.NodeID].Block(job, timeout) {
			return true
		}
	}
	if vs.indexQueue != nil {
		return vs.indexQueue.Block(job, timeout)
	}
	return false
}

// TotalIndexQueueDepth returns the aggregated depth of all NUMA-local queues and the global index queue.
func (vs *VectorStore) TotalIndexQueueDepth() int {
	total := 0
	if vs.indexQueue != nil {
		total += vs.indexQueue.Len()
	}
	for _, q := range vs.numaIndexQueues {
		if q != nil {
			total += q.Len()
		}
	}
	return total
}
