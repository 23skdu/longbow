package store

import (
	"github.com/apache/arrow-go/v18/arrow"
)

func estimateBatchSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		size += estimateDataSize(col.Data())
	}
	return size
}

func estimateDataSize(data arrow.ArrayData) int64 {
	size := int64(0)
	if data == nil {
		return 0
	}
	for _, buf := range data.Buffers() {
		if buf != nil {
			size += int64(buf.Len())
		}
	}
	for _, child := range data.Children() {
		if child != nil {
			size += estimateDataSize(child)
		}
	}
	return size
}
