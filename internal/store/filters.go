package store

import (
	"github.com/23skdu/longbow/internal/store/types"
)

// FilterExpr is the AST node interface for evaluating complex boolean nesting conditions.
type FilterExpr = types.FilterExpr

// Logical Operators
type AndExpr = types.AndExpr
type OrExpr = types.OrExpr
type NotExpr = types.NotExpr

// Comparison Operators
type EqExpr = types.EqExpr
type ContainsExpr = types.ContainsExpr

// ParseFilter converts a generic JSON map structural AST into a typed FilterExpr recursively.
// Ex: {"$and": [{"$eq": {"status": "active"}}, {"$contains": {"role": "admin"}}]}
func ParseFilter(node map[string]interface{}) FilterExpr {
	return types.ParseFilter(node)
}
