package types

import (
	"strings"
)

// Logical Operators
type AndExpr struct {
	Conditions []FilterExpr
}

func (a *AndExpr) Evaluate(metadata map[string]interface{}) bool {
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

type OrExpr struct {
	Conditions []FilterExpr
}

func (o *OrExpr) Evaluate(metadata map[string]interface{}) bool {
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

type NotExpr struct {
	Condition FilterExpr
}

func (n *NotExpr) Evaluate(metadata map[string]interface{}) bool {
	return !n.Condition.Evaluate(metadata)
}

// Comparison Operators
type EqExpr struct {
	Field string
	Value interface{}
}

func (e *EqExpr) Evaluate(metadata map[string]interface{}) bool {
	val, ok := metadata[e.Field]
	if !ok {
		return false
	}
	return val == e.Value
}

type ContainsExpr struct {
	Field string
	Value string
}

func (c *ContainsExpr) Evaluate(metadata map[string]interface{}) bool {
	val, ok := metadata[c.Field]
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
