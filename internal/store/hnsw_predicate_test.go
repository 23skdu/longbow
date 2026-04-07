package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/stretchr/testify/require"
)

func TestPredicatePushdownOptimizer(t *testing.T) {
	optimizer := NewPredicatePushdownOptimizer()

	t.Run("EmptyOptimizer", func(t *testing.T) {
		filters := []query.Filter{
			{Field: "id", Operator: ">", Value: "10"},
		}
		pushable, nonPushable := optimizer.Optimize(filters)
		require.Empty(t, pushable)
		require.Equal(t, 1, len(nonPushable))
	})

	t.Run("RegisterHNSW", func(t *testing.T) {
		optimizer.RegisterHNSW("test-hnsw", nil, []string{"category", "status"})

		filters := []query.Filter{
			{Field: "category", Operator: "=", Value: "1"},
			{Field: "id", Operator: ">", Value: "10"},
		}

		pushable, nonPushable := optimizer.Optimize(filters)
		require.Equal(t, 1, len(pushable))
		require.Equal(t, 1, len(nonPushable))
		require.Equal(t, "category", pushable[0].Field)
		require.Equal(t, "id", nonPushable[0].Field)
	})

	t.Run("ApplyPushdown", func(t *testing.T) {
		optimizer.RegisterHNSW("test-hnsw", nil, []string{"category"})

		filters := []query.Filter{
			{Field: "category", Operator: "=", Value: "1"},
		}

		result, err := optimizer.ApplyPushdown(filters)
		require.NoError(t, err)
		require.Contains(t, result, "test-hnsw")
	})

	t.Run("GetStats", func(t *testing.T) {
		optimizer.RegisterHNSW("test-hnsw", nil, []string{"category", "status"})

		stats := optimizer.GetStats()
		require.Contains(t, stats, "test-hnsw")
	})
}

func TestArrowHNSWPredicate(t *testing.T) {
	t.Run("CanAcceptFilter", func(t *testing.T) {
		hp := NewArrowHNSWPredicate(nil, []string{"category", "status"})

		require.True(t, hp.CanAcceptFilter(query.Filter{Field: "category"}))
		require.True(t, hp.CanAcceptFilter(query.Filter{Field: "status"}))
		require.False(t, hp.CanAcceptFilter(query.Filter{Field: "id"}))
	})

	t.Run("GetIndexedFields", func(t *testing.T) {
		hp := NewArrowHNSWPredicate(nil, []string{"category", "status"})

		fields := hp.GetIndexedFields()
		require.ElementsMatch(t, []string{"category", "status"}, fields)
	})

	t.Run("NilHNSW", func(t *testing.T) {
		hp := NewArrowHNSWPredicate(nil, []string{"category"})

		_, err := hp.ApplyFilter(query.Filter{Field: "category", Operator: "=", Value: "1"})
		require.Error(t, err)
	})
}

func TestPredicatePushdownOptimizer_ComplexFilters(t *testing.T) {
	optimizer := NewPredicatePushdownOptimizer()
	optimizer.RegisterHNSW("test-hnsw", nil, []string{"category", "status", "id"})

	filters := []query.Filter{
		{Field: "id", Operator: ">", Value: "100"},
		{Field: "category", Operator: "=", Value: "1"},
		{Field: "status", Operator: "=", Value: "active"},
	}

	pushable, nonPushable := optimizer.Optimize(filters)
	require.Equal(t, 2, len(pushable))
	require.Equal(t, 1, len(nonPushable))
}
