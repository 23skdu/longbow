package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/query"
)

type HNSWPredicate interface {
	CanAcceptFilter(filter query.Filter) bool
	ApplyFilter(filter query.Filter) ([]uint32, error)
	GetIndexedFields() []string
}

type ArrowHNSWPredicate struct {
	hnsw          *ArrowHNSW
	indexedFields map[string]bool
}

func NewArrowHNSWPredicate(hnsw *ArrowHNSW, indexedFields []string) *ArrowHNSWPredicate {
	fields := make(map[string]bool)
	for _, f := range indexedFields {
		fields[f] = true
	}
	return &ArrowHNSWPredicate{
		hnsw:          hnsw,
		indexedFields: fields,
	}
}

func (hp *ArrowHNSWPredicate) CanAcceptFilter(filter query.Filter) bool {
	if hp == nil || hp.hnsw == nil {
		return false
	}
	return hp.indexedFields[filter.Field]
}

func (hp *ArrowHNSWPredicate) ApplyFilter(filter query.Filter) ([]uint32, error) {
	if hp == nil || hp.hnsw == nil {
		return nil, fmt.Errorf("HNSW not initialized")
	}

	if !hp.CanAcceptFilter(filter) {
		return nil, fmt.Errorf("field %q not indexed", filter.Field)
	}

	return nil, nil
}

func (hp *ArrowHNSWPredicate) GetIndexedFields() []string {
	if hp == nil {
		return nil
	}
	fields := make([]string, 0, len(hp.indexedFields))
	for f := range hp.indexedFields {
		fields = append(fields, f)
	}
	return fields
}

type PredicatePushdownOptimizer struct {
	hnswPredicates map[string]*ArrowHNSWPredicate
}

func NewPredicatePushdownOptimizer() *PredicatePushdownOptimizer {
	return &PredicatePushdownOptimizer{
		hnswPredicates: make(map[string]*ArrowHNSWPredicate),
	}
}

func (ppo *PredicatePushdownOptimizer) RegisterHNSW(name string, hnsw *ArrowHNSW, fields []string) {
	ppo.hnswPredicates[name] = NewArrowHNSWPredicate(hnsw, fields)
}

func (ppo *PredicatePushdownOptimizer) Optimize(filters []query.Filter) (pushable []query.Filter, nonPushable []query.Filter) {
	for _, f := range filters {
		pushableToAny := false
		for _, hp := range ppo.hnswPredicates {
			if hp.CanAcceptFilter(f) {
				pushableToAny = true
				break
			}
		}
		if pushableToAny {
			pushable = append(pushable, f)
		} else {
			nonPushable = append(nonPushable, f)
		}
	}
	return pushable, nonPushable
}

func (ppo *PredicatePushdownOptimizer) ApplyPushdown(filters []query.Filter) (map[string][]uint32, error) {
	result := make(map[string][]uint32)

	for name, hp := range ppo.hnswPredicates {
		var pushable []query.Filter
		for _, f := range filters {
			if hp.CanAcceptFilter(f) {
				pushable = append(pushable, f)
			}
		}

		if len(pushable) == 0 {
			continue
		}

		var ids []uint32
		var err error

		if len(pushable) == 1 {
			ids, err = hp.ApplyFilter(pushable[0])
		} else {
			ids, err = hp.ApplyFilter(query.Filter{
				Logic:   "AND",
				Filters: pushable,
			})
		}

		if err != nil {
			return nil, err
		}
		result[name] = ids
	}

	return result, nil
}

func (ppo *PredicatePushdownOptimizer) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	for name, hp := range ppo.hnswPredicates {
		stats[name] = map[string]interface{}{
			"indexed_fields": hp.GetIndexedFields(),
		}
	}
	return stats
}
