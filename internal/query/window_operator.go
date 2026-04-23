package query

import (
	"sort"

	"github.com/23skdu/longbow/internal/core"
)

// WindowOperator implements execution of analytical window functions
type WindowOperator struct{}

// NewWindowOperator creates a new operator for window functions
func NewWindowOperator() *WindowOperator {
	return &WindowOperator{}
}

// Execute applies window functions to the search results
func (o *WindowOperator) Execute(results []core.SearchResult, functions []WindowFunction) []core.SearchResult {
	if len(functions) == 0 || len(results) == 0 {
		return results
	}

	for _, fn := range functions {
		results = o.applyFunction(results, fn)
	}

	return results
}

func (o *WindowOperator) applyFunction(results []core.SearchResult, fn WindowFunction) []core.SearchResult {
	// If PARTITION BY is specified, we group results
	if len(fn.Over.PartitionBy) > 0 {
		return o.applyPartitioned(results, fn)
	}

	// Global window
	return o.applyGlobal(results, fn)
}

func (o *WindowOperator) applyGlobal(results []core.SearchResult, fn WindowFunction) []core.SearchResult {
	// Copy results to avoid modifying input if necessary, but here we modify in place or return new slice
	// For global window functions that depend on order, we sort first
	if len(fn.Over.OrderBy) > 0 {
		sort.SliceStable(results, func(i, j int) bool {
			return o.compare(results[i], results[j], fn.Over.OrderBy)
		})
	}

	// Decode all metadata first for efficient processing
	metas := make([]map[string]interface{}, len(results))
	for i := range results {
		metas[i], _ = core.DecodeMetadata(results[i].Metadata)
		if metas[i] == nil {
			metas[i] = make(map[string]interface{})
		}
	}

	switch fn.Name {
	case "row_number":
		for i := range metas {
			metas[i][fn.As] = i + 1
		}
	case "rank":
		o.applyRank(results, metas, fn, false)
	case "dense_rank":
		o.applyRank(results, metas, fn, true)
	case "sum":
		sum := 0.0
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				sum += o.toFloat64(val)
			}
		}
		for i := range metas {
			metas[i][fn.As] = sum
		}
	case "avg":
		sum := 0.0
		count := 0
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				sum += o.toFloat64(val)
				count++
			}
		}
		if count > 0 {
			avg := sum / float64(count)
			for i := range metas {
				metas[i][fn.As] = avg
			}
		}
	case "min":
		min := 1e30 // Large number
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				fval := o.toFloat64(val)
				if fval < min {
					min = fval
				}
			}
		}
		for i := range metas {
			metas[i][fn.As] = min
		}
	case "max":
		max := -1e30 // Small number
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				fval := o.toFloat64(val)
				if fval > max {
					max = fval
				}
			}
		}
		for i := range metas {
			metas[i][fn.As] = max
		}
	}

	// Re-encode metadata
	for i := range results {
		results[i].Metadata, _ = core.EncodeMetadata(metas[i])
	}

	return results
}

func (o *WindowOperator) applyPartitioned(results []core.SearchResult, fn WindowFunction) []core.SearchResult {
	// Decode all metadata first
	metas := make([]map[string]interface{}, len(results))
	for i := range results {
		metas[i], _ = core.DecodeMetadata(results[i].Metadata)
		if metas[i] == nil {
			metas[i] = make(map[string]interface{})
		}
	}

	// Group results by partition key
	partitions := make(map[string][]int)
	for i, m := range metas {
		key := o.getPartitionKey(m, fn.Over.PartitionBy)
		partitions[key] = append(partitions[key], i)
	}

	// Apply function to each partition
	for _, indices := range partitions {
		partitionResults := make([]core.SearchResult, len(indices))
		partitionMetas := make([]map[string]interface{}, len(indices))
		for i, idx := range indices {
			partitionResults[i] = results[idx]
			partitionMetas[i] = metas[idx]
		}

		// Apply global logic to this partition (modified to accept decoded metas)
		o.applyGlobalOnDecoded(partitionResults, partitionMetas, fn)

		// Put back
		for i, idx := range indices {
			results[idx] = partitionResults[i]
			metas[idx] = partitionMetas[i]
		}
	}

	// Re-encode metadata
	for i := range results {
		results[i].Metadata, _ = core.EncodeMetadata(metas[i])
	}

	return results
}

func (o *WindowOperator) applyGlobalOnDecoded(results []core.SearchResult, metas []map[string]interface{}, fn WindowFunction) {
	if len(fn.Over.OrderBy) > 0 {
		// Sorting is tricky here because results and metas must stay in sync
		type pair struct {
			res  core.SearchResult
			meta map[string]interface{}
		}
		pairs := make([]pair, len(results))
		for i := range results {
			pairs[i] = pair{results[i], metas[i]}
		}
		sort.SliceStable(pairs, func(i, j int) bool {
			return o.compareInternal(pairs[i].res, pairs[i].meta, pairs[j].res, pairs[j].meta, fn.Over.OrderBy)
		})
		for i := range results {
			results[i] = pairs[i].res
			metas[i] = pairs[i].meta
		}
	}

	switch fn.Name {
	case "row_number":
		for i := range metas {
			metas[i][fn.As] = i + 1
		}
	case "rank":
		o.applyRank(results, metas, fn, false)
	case "dense_rank":
		o.applyRank(results, metas, fn, true)
	case "sum":
		sum := 0.0
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				sum += o.toFloat64(val)
			}
		}
		for i := range metas {
			metas[i][fn.As] = sum
		}
	case "avg":
		sum := 0.0
		count := 0
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				sum += o.toFloat64(val)
				count++
			}
		}
		if count > 0 {
			avg := sum / float64(count)
			for i := range metas {
				metas[i][fn.As] = avg
			}
		}
	case "min":
		min := 1e30
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				fval := o.toFloat64(val)
				if fval < min {
					min = fval
				}
			}
		}
		for i := range metas {
			metas[i][fn.As] = min
		}
	case "max":
		max := -1e30
		for _, m := range metas {
			if val, ok := m[fn.Field]; ok {
				fval := o.toFloat64(val)
				if fval > max {
					max = fval
				}
			}
		}
		for i := range metas {
			metas[i][fn.As] = max
		}
	}
}

func (o *WindowOperator) getPartitionKey(m map[string]interface{}, fields []string) string {
	var key string
	for _, field := range fields {
		if val, ok := m[field]; ok {
			key += ":" + o.valToString(val)
		} else {
			key += ":<nil>"
		}
	}
	return key
}

func (o *WindowOperator) valToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		return ""
	}
}

func (o *WindowOperator) compare(a, b core.SearchResult, orders []WindowOrder) bool {
	metaA, _ := core.DecodeMetadata(a.Metadata)
	metaB, _ := core.DecodeMetadata(b.Metadata)
	return o.compareInternal(a, metaA, b, metaB, orders)
}

func (o *WindowOperator) compareInternal(a core.SearchResult, metaA map[string]interface{}, b core.SearchResult, metaB map[string]interface{}, orders []WindowOrder) bool {
	for _, order := range orders {
		var valA, valB interface{}
		var okA, okB bool

		if order.Field == "distance" {
			valA, okA = a.Distance, true
			valB, okB = b.Distance, true
		} else if order.Field == "score" {
			valA, okA = a.Score, true
			valB, okB = b.Score, true
		} else {
			if metaA != nil {
				valA, okA = metaA[order.Field]
			}
			if metaB != nil {
				valB, okB = metaB[order.Field]
			}
		}

		if !okA && !okB {
			continue
		}
		if !okA {
			return !order.Descending
		}
		if !okB {
			return order.Descending
		}

		if valA == valB {
			continue
		}

		less := o.isLess(valA, valB)
		if order.Descending {
			return !less
		}
		return less
	}
	return false
}

func (o *WindowOperator) isLess(a, b interface{}) bool {
	switch va := a.(type) {
	case float32:
		return va < b.(float32)
	case float64:
		return va < b.(float64)
	case int:
		return va < b.(int)
	case int64:
		return va < b.(int64)
	case string:
		return va < b.(string)
	}
	return false
}

func (o *WindowOperator) applyRank(results []core.SearchResult, metas []map[string]interface{}, fn WindowFunction, dense bool) {
	if len(results) == 0 {
		return
	}

	rank := 1
	for i := range results {
		if i > 0 && o.isEqualInternal(results[i], metas[i], results[i-1], metas[i-1], fn.Over.OrderBy) {
			// Same rank as previous
		} else {
			if dense {
				if i > 0 {
					rank++
				}
			} else {
				rank = i + 1
			}
		}
		metas[i][fn.As] = rank
	}
}

func (o *WindowOperator) toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0.0
	}
}

func (o *WindowOperator) isEqualInternal(a core.SearchResult, metaA map[string]interface{}, b core.SearchResult, metaB map[string]interface{}, orders []WindowOrder) bool {
	if len(orders) == 0 {
		return true
	}
	for _, order := range orders {
		var valA, valB interface{}
		if order.Field == "distance" {
			valA, valB = a.Distance, b.Distance
		} else if order.Field == "score" {
			valA, valB = a.Score, b.Score
		} else {
			if metaA != nil {
				valA = metaA[order.Field]
			}
			if metaB != nil {
				valB = metaB[order.Field]
			}
		}
		if valA != valB {
			return false
		}
	}
	return true
}
