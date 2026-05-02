package store

import (
	"github.com/23skdu/longbow/internal/store/types"
)

// FilterExpr is the AST node interface for evaluating complex boolean nesting conditions.
type FilterExpr = types.FilterExpr

// AndExpr represents a logical AND operation between multiple filter expressions.
type AndExpr = types.AndExpr

// OrExpr represents a logical OR operation between multiple filter expressions.
type OrExpr = types.OrExpr

// NotExpr represents a logical NOT operation on a filter expression.
type NotExpr = types.NotExpr

// EqExpr represents an equality comparison filter.
type EqExpr = types.EqExpr

// ContainsExpr represents a containment comparison filter (e.g., for sets or strings).
type ContainsExpr = types.ContainsExpr

// ParseFilter converts a generic JSON map structural AST into a typed FilterExpr recursively.
// Ex: {"$and": [{"$eq": {"status": "active"}}, {"$contains": {"role": "admin"}}]}
func ParseFilter(node map[string]interface{}) FilterExpr {
	return types.ParseFilter(node)
}
