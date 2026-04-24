package query

import (
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// ExtractPushablePredicate attempts to extract a SIMD-accelerated predicate from a FilterExpr.
// It currently handles simple numeric range comparisons.
func ExtractPushablePredicate(expr types.FilterExpr, records []arrow.RecordBatch) types.HNSWPredicate {
	if expr == nil || len(records) == 0 {
		return nil
	}

	switch e := expr.(type) {
	case *types.EqExpr:
		return NewHNSWSIMDPredicate(records, e.Field, simd.CompareEq, e.Value)
	case *types.GtExpr:
		return NewHNSWSIMDPredicate(records, e.Field, simd.CompareGt, e.Value)
	case *types.GeExpr:
		return NewHNSWSIMDPredicate(records, e.Field, simd.CompareGe, e.Value)
	case *types.LtExpr:
		return NewHNSWSIMDPredicate(records, e.Field, simd.CompareLt, e.Value)
	case *types.LeExpr:
		return NewHNSWSIMDPredicate(records, e.Field, simd.CompareLe, e.Value)
	}

	return nil
}
