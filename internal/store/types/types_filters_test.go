package types

import (
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildMockLazyMetadata(t *testing.T, data map[string]interface{}) *LazyMetadata {
	encoded, err := core.EncodeMetadata(data)
	require.NoError(t, err)
	return NewLazyMetadata(encoded)
}

func TestAndExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"status": "active", "age": int64(30)})
	
	expr := &AndExpr{
		Conditions: []FilterExpr{
			&EqExpr{Field: "status", Value: "active"},
			&GtExpr{Field: "age", Value: int64(20)},
		},
	}
	assert.True(t, expr.Evaluate(meta))

	exprFalse := &AndExpr{
		Conditions: []FilterExpr{
			&EqExpr{Field: "status", Value: "active"},
			&GtExpr{Field: "age", Value: int64(40)},
		},
	}
	assert.False(t, exprFalse.Evaluate(meta))

	exprEmpty := &AndExpr{}
	assert.True(t, exprEmpty.Evaluate(meta))
}

func TestOrExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"status": "active", "age": int64(30)})
	
	expr := &OrExpr{
		Conditions: []FilterExpr{
			&EqExpr{Field: "status", Value: "inactive"},
			&GtExpr{Field: "age", Value: int64(20)},
		},
	}
	assert.True(t, expr.Evaluate(meta))

	exprFalse := &OrExpr{
		Conditions: []FilterExpr{
			&EqExpr{Field: "status", Value: "inactive"},
			&GtExpr{Field: "age", Value: int64(40)},
		},
	}
	assert.False(t, exprFalse.Evaluate(meta))

	exprEmpty := &OrExpr{}
	assert.True(t, exprEmpty.Evaluate(meta))
}

func TestNotExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"status": "active"})
	
	expr := &NotExpr{
		Condition: &EqExpr{Field: "status", Value: "inactive"},
	}
	assert.True(t, expr.Evaluate(meta))

	exprFalse := &NotExpr{
		Condition: &EqExpr{Field: "status", Value: "active"},
	}
	assert.False(t, exprFalse.Evaluate(meta))
}

func TestEqExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"status": "active"})
	
	expr := &EqExpr{Field: "status", Value: "active"}
	assert.True(t, expr.Evaluate(meta))

	exprFalse := &EqExpr{Field: "status", Value: "inactive"}
	assert.False(t, exprFalse.Evaluate(meta))

	exprMissing := &EqExpr{Field: "missing", Value: "active"}
	assert.False(t, exprMissing.Evaluate(meta))
}

func TestGtExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{
		"int64val": int64(30),
		"float64val": float64(30.5),
	})
	
	assert.True(t, (&GtExpr{Field: "int64val", Value: int64(20)}).Evaluate(meta))
	assert.False(t, (&GtExpr{Field: "int64val", Value: int64(40)}).Evaluate(meta))
	assert.False(t, (&GtExpr{Field: "missing", Value: int64(20)}).Evaluate(meta))

	assert.True(t, (&GtExpr{Field: "float64val", Value: float64(20.5)}).Evaluate(meta))
}

func TestGeExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"int64val": int64(30)})
	
	assert.True(t, (&GeExpr{Field: "int64val", Value: int64(30)}).Evaluate(meta))
	assert.True(t, (&GeExpr{Field: "int64val", Value: int64(20)}).Evaluate(meta))
	assert.False(t, (&GeExpr{Field: "int64val", Value: int64(40)}).Evaluate(meta))
	assert.False(t, (&GeExpr{Field: "missing", Value: int64(20)}).Evaluate(meta))
}

func TestLtExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"int64val": int64(30)})
	
	assert.True(t, (&LtExpr{Field: "int64val", Value: int64(40)}).Evaluate(meta))
	assert.False(t, (&LtExpr{Field: "int64val", Value: int64(20)}).Evaluate(meta))
	assert.False(t, (&LtExpr{Field: "missing", Value: int64(40)}).Evaluate(meta))
}

func TestLeExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"int64val": int64(30)})
	
	assert.True(t, (&LeExpr{Field: "int64val", Value: int64(30)}).Evaluate(meta))
	assert.True(t, (&LeExpr{Field: "int64val", Value: int64(40)}).Evaluate(meta))
	assert.False(t, (&LeExpr{Field: "int64val", Value: int64(20)}).Evaluate(meta))
	assert.False(t, (&LeExpr{Field: "missing", Value: int64(40)}).Evaluate(meta))
}

func TestContainsExpr_Evaluate(t *testing.T) {
	meta := buildMockLazyMetadata(t, map[string]interface{}{"role": "admin_user", "age": int64(30)})
	
	assert.True(t, (&ContainsExpr{Field: "role", Value: "admin"}).Evaluate(meta))
	assert.False(t, (&ContainsExpr{Field: "role", Value: "guest"}).Evaluate(meta))
	assert.False(t, (&ContainsExpr{Field: "age", Value: "30"}).Evaluate(meta)) // not a string
	assert.False(t, (&ContainsExpr{Field: "missing", Value: "admin"}).Evaluate(meta))
}

func TestParseFilter(t *testing.T) {
	// Test empty
	assert.Nil(t, ParseFilter(map[string]interface{}{}))

	// Test $eq
	eqNode := map[string]interface{}{"$eq": map[string]interface{}{"status": "active"}}
	parsedEq := ParseFilter(eqNode)
	require.IsType(t, &EqExpr{}, parsedEq)
	assert.Equal(t, "status", parsedEq.(*EqExpr).Field)

	// Test $gt, $ge, $lt, $le
	assert.IsType(t, &GtExpr{}, ParseFilter(map[string]interface{}{"$gt": map[string]interface{}{"age": 20}}))
	assert.IsType(t, &GeExpr{}, ParseFilter(map[string]interface{}{"$ge": map[string]interface{}{"age": 20}}))
	assert.IsType(t, &LtExpr{}, ParseFilter(map[string]interface{}{"$lt": map[string]interface{}{"age": 20}}))
	assert.IsType(t, &LeExpr{}, ParseFilter(map[string]interface{}{"$le": map[string]interface{}{"age": 20}}))

	// Test $contains
	assert.IsType(t, &ContainsExpr{}, ParseFilter(map[string]interface{}{"$contains": map[string]interface{}{"role": "admin"}}))

	// Test $and
	andNode := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{"$eq": map[string]interface{}{"status": "active"}},
		},
	}
	parsedAnd := ParseFilter(andNode)
	require.IsType(t, &AndExpr{}, parsedAnd)
	assert.Len(t, parsedAnd.(*AndExpr).Conditions, 1)

	// Test $or
	orNode := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{"$eq": map[string]interface{}{"status": "active"}},
		},
	}
	parsedOr := ParseFilter(orNode)
	require.IsType(t, &OrExpr{}, parsedOr)
	assert.Len(t, parsedOr.(*OrExpr).Conditions, 1)

	// Test $not
	notNode := map[string]interface{}{
		"$not": map[string]interface{}{"$eq": map[string]interface{}{"status": "active"}},
	}
	parsedNot := ParseFilter(notNode)
	require.IsType(t, &NotExpr{}, parsedNot)
}

func TestBQEncoder_Rest(t *testing.T) {
	encoder := NewBQEncoder(128)
	require.NotNil(t, encoder)

	// CodeSize
	assert.Equal(t, 2, encoder.CodeSize())

	// HammingDistanceBatch
	q := []uint64{0, 0}
	codes := [][]uint64{{0, 0}, {1, 1}}
	dists := make([]int, 2)
	encoder.HammingDistanceBatch(q, codes, dists)
	assert.Equal(t, int(0), dists[0])
	assert.Greater(t, dists[1], int(0))

	// ScoreToFloat32 / Float32ToHamming
	score := encoder.ScoreToFloat32(10)
	assert.Greater(t, score, float32(0.0))

	dist := encoder.Float32ToHamming(score)
	assert.Equal(t, int(10), dist)

	// Decode
	encoded := []uint64{0, 0}
	decoded := encoder.Decode(encoded)
	assert.Len(t, decoded, 128)
	assert.Equal(t, float32(-1.0), decoded[0])
}
