package types

import (
	"strings"
)

// AndExpr Operators
type AndExpr struct {
	Conditions []FilterExpr
}

// Evaluate runs the evaluate operation.
func (a *AndExpr) Evaluate(metadata *LazyMetadata) bool {
	if len(a.Conditions) == 0 {
		return true
	}
	for _, c := range a.Conditions {
		if !c.Evaluate(metadata) {
			return false
		}
	}
	return true
}

// OrExpr represents a or expr value.
type OrExpr struct {
	Conditions []FilterExpr
}

// Evaluate runs the evaluate operation.
func (o *OrExpr) Evaluate(metadata *LazyMetadata) bool {
	if len(o.Conditions) == 0 {
		return true
	}
	for _, c := range o.Conditions {
		if c.Evaluate(metadata) {
			return true
		}
	}
	return false
}

// NotExpr represents a not expr value.
type NotExpr struct {
	Condition FilterExpr
}

// Evaluate runs the evaluate operation.
func (n *NotExpr) Evaluate(metadata *LazyMetadata) bool {
	return !n.Condition.Evaluate(metadata)
}

// EqExpr Operators
type EqExpr struct {
	Field string
	Value interface{}
}

// Evaluate runs the evaluate operation.
func (e *EqExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(e.Field)
	if !ok {
		return false
	}
	return val == e.Value
}

// GtExpr represents a gt expr value.
type GtExpr struct {
	Field string
	Value interface{}
}

// Evaluate runs the evaluate operation.
func (e *GtExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(e.Field)
	if !ok {
		return false
	}
	return compareValues(val, e.Value) > 0
}

// GeExpr represents a ge expr value.
type GeExpr struct {
	Field string
	Value interface{}
}

// Evaluate runs the evaluate operation.
func (e *GeExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(e.Field)
	if !ok {
		return false
	}
	return compareValues(val, e.Value) >= 0
}

// LtExpr represents a lt expr value.
type LtExpr struct {
	Field string
	Value interface{}
}

// Evaluate runs the evaluate operation.
func (e *LtExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(e.Field)
	if !ok {
		return false
	}
	return compareValues(val, e.Value) < 0
}

// LeExpr represents a le expr value.
type LeExpr struct {
	Field string
	Value interface{}
}

// Evaluate runs the evaluate operation.
func (e *LeExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(e.Field)
	if !ok {
		return false
	}
	return compareValues(val, e.Value) <= 0
}

func compareValues(v1, v2 interface{}) int {
	switch a := v1.(type) {
	case int64:
		b, ok := toInt64(v2)
		if !ok {
			return 0
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	case float64:
		b, ok := toFloat64(v2)
		if !ok {
			return 0
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	case int32:
		b, ok := toInt64(v2)
		if !ok {
			return 0
		}
		a64 := int64(a)
		if a64 < b {
			return -1
		}
		if a64 > b {
			return 1
		}
		return 0
	case float32:
		b, ok := toFloat64(v2)
		if !ok {
			return 0
		}
		a64 := float64(a)
		if a64 < b {
			return -1
		}
		if a64 > b {
			return 1
		}
		return 0
	}
	return 0
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float64:
		return int64(val), true
	}
	return 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	}
	return 0, false
}

// ContainsExpr represents a contains expr value.
type ContainsExpr struct {
	Field string
	Value string
}

// Evaluate runs the evaluate operation.
func (c *ContainsExpr) Evaluate(metadata *LazyMetadata) bool {
	val, ok := metadata.GetField(c.Field)
	if !ok {
		return false
	}
	if strVal, ok := val.(string); ok {
		return strings.Contains(strVal, c.Value)
	}
	return false
}

// ParseFilter converts a generic JSON map structural AST into a typed FilterExpr recursively.
// Ex: {"$and": [{"$eq": {"status": "active"}}, {"$contains": {"role": "admin"}}]}
func ParseFilter(node map[string]interface{}) FilterExpr {
	if len(node) == 0 {
		return nil
	}

	for key, val := range node {
		switch key {
		case "$and":
			list, ok := val.([]interface{})
			if !ok {
				continue
			}
			and := &AndExpr{}
			for _, item := range list {
				if childMap, ok := item.(map[string]interface{}); ok {
					if parsed := ParseFilter(childMap); parsed != nil {
						and.Conditions = append(and.Conditions, parsed)
					}
				}
			}
			return and

		case "$or":
			list, ok := val.([]interface{})
			if !ok {
				continue
			}
			or := &OrExpr{}
			for _, item := range list {
				if childMap, ok := item.(map[string]interface{}); ok {
					if parsed := ParseFilter(childMap); parsed != nil {
						or.Conditions = append(or.Conditions, parsed)
					}
				}
			}
			return or

		case "$not":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			if parsed := ParseFilter(childMap); parsed != nil {
				return &NotExpr{Condition: parsed}
			}

		case "$eq":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				return &EqExpr{Field: fKey, Value: fVal}
			}

		case "$gt":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				return &GtExpr{Field: fKey, Value: fVal}
			}
		case "$ge":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				return &GeExpr{Field: fKey, Value: fVal}
			}
		case "$lt":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				return &LtExpr{Field: fKey, Value: fVal}
			}
		case "$le":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				return &LeExpr{Field: fKey, Value: fVal}
			}

		case "$contains":
			childMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			for fKey, fVal := range childMap {
				if strVal, ok := fVal.(string); ok {
					return &ContainsExpr{Field: fKey, Value: strVal}
				}
			}
		}
	}
	return nil
}
