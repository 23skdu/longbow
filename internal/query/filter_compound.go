package query

import (
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
)

type compoundFilterOp struct {
	logic    string // "AND", "OR", "NOT"
	children []filterOp
}

func (c *compoundFilterOp) Compound() bool { return true }

func (c *compoundFilterOp) Match(rowIdx int) bool {
	switch c.logic {
	case "AND":
		for _, child := range c.children {
			if !child.Match(rowIdx) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.children {
			if child.Match(rowIdx) {
				return true
			}
		}
		return false
	case "NOT":
		if len(c.children) == 0 {
			return true
		}
		return !c.children[0].Match(rowIdx)
	default:
		return false
	}
}

func (c *compoundFilterOp) MatchBitmap(dst []byte) {
	if len(dst) == 0 {
		return
	}

	temp := make([]byte, len(dst))

	switch c.logic {
	case "AND":
		// Initialize with all 1s
		for i := range dst {
			dst[i] = 1
		}
		for _, child := range c.children {
			child.MatchBitmap(temp)
			_ = simd.AndBytes(dst, temp)
		}
	case "OR":
		// Initialize with all 0s
		for i := range dst {
			dst[i] = 0
		}
		for _, child := range c.children {
			child.MatchBitmap(temp)
			_ = simd.OrBytes(dst, temp)
		}
	case "NOT":
		if len(c.children) > 0 {
			c.children[0].MatchBitmap(dst)
			_ = simd.NotBytes(dst)
		} else {
			for i := range dst {
				dst[i] = 1
			}
		}
	}
}

func (c *compoundFilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}
	switch c.logic {
	case "AND":
		result := indices
		for _, child := range c.children {
			result = child.FilterBatch(result)
			if len(result) == 0 {
				return nil
			}
		}
		return result
	case "OR":
		seen := make(map[int]bool)
		var result []int
		for _, child := range c.children {
			matches := child.FilterBatch(indices)
			for _, idx := range matches {
				if !seen[idx] {
					seen[idx] = true
					result = append(result, idx)
				}
			}
		}
		// Maintain original order
		order := make(map[int]int)
		for i, idx := range indices {
			order[idx] = i
		}
		sorted := make([]int, len(result))
		copy(sorted, result)
		for i := 0; i < len(sorted)-1; i++ {
			for j := i + 1; j < len(sorted); j++ {
				if order[sorted[i]] > order[sorted[j]] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		return sorted
	case "NOT":
		if len(c.children) == 0 {
			return indices
		}
		excluded := c.children[0].FilterBatch(indices)
		excludedMap := make(map[int]bool)
		for _, idx := range excluded {
			excludedMap[idx] = true
		}
		var result []int
		for _, idx := range indices {
			if !excludedMap[idx] {
				result = append(result, idx)
			}
		}
		return result
	default:
		return indices
	}
}

func (c *compoundFilterOp) Bind(col arrow.Array) error {
	// Re-binding compound op is usually handled via Reset(rec)
	return nil
}

func (c *compoundFilterOp) Reset(rec arrow.RecordBatch) error {
	for _, child := range c.children {
		if err := child.Reset(rec); err != nil {
			return err
		}
	}
	return nil
}

func (c *compoundFilterOp) MatchValue(val interface{}) bool {
	switch c.logic {
	case "AND":
		for _, child := range c.children {
			if !child.MatchValue(val) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.children {
			if child.MatchValue(val) {
				return true
			}
		}
		return false
	case "NOT":
		if len(c.children) == 0 {
			return true
		}
		return !c.children[0].MatchValue(val)
	default:
		return false
	}
}

func buildCompoundOp(schema arrow.Schema, rec arrow.RecordBatch, logic string, childFilters []Filter) (filterOp, error) {
	children := make([]filterOp, 0, len(childFilters))
	for i := range childFilters {
		child, err := buildFilterOp(schema, rec, &childFilters[i])
		if err != nil {
			return nil, err
		}
		if child != nil {
			children = append(children, child)
		}
	}
	return &compoundFilterOp{logic: logic, children: children}, nil
}
