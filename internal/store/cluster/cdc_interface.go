package cluster

import "github.com/apache/arrow-go/v18/arrow"

// CDCStore defines the interface that VectorStore must implement for CDC to attach to it.
type CDCStore interface {
	RegisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch)
	UnregisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch)
}
