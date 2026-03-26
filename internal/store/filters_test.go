package store

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestParseFilterAndEvaluate_AST(t *testing.T) {
	node := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{
				"$eq": map[string]interface{}{"status": "active"},
			},
			map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{
						"$contains": map[string]interface{}{"role": "admin"},
					},
					map[string]interface{}{
						"$eq": map[string]interface{}{"role": "superuser"},
					},
				},
			},
		},
	}

	filter := ParseFilter(node)
	assert.NotNil(t, filter)

	// Test Match 1: contains "admin"
	metadataMatch1 := map[string]interface{}{
		"status": "active",
		"role":   "super_admin_user",
	}
	assert.True(t, filter.Evaluate(metadataMatch1))

	// Test Match 2: eq "superuser"
	metadataMatch2 := map[string]interface{}{
		"status": "active",
		"role":   "superuser",
	}
	assert.True(t, filter.Evaluate(metadataMatch2))

	// Test Non-Match 1: wrong status
	metadataNoMatch1 := map[string]interface{}{
		"status": "inactive",
		"role":   "superuser",
	}
	assert.False(t, filter.Evaluate(metadataNoMatch1))

	// Test Non-Match 2: wrong role
	metadataNoMatch2 := map[string]interface{}{
		"status": "active",
		"role":   "guest",
	}
	assert.False(t, filter.Evaluate(metadataNoMatch2))
	
	// Test NotExpr
	notNode := map[string]interface{}{
		"$not": map[string]interface{}{
			"$eq": map[string]interface{}{"status": "deleted"},
		},
	}
	notFilter := ParseFilter(notNode)
	assert.NotNil(t, notFilter)

	assert.True(t, notFilter.Evaluate(map[string]interface{}{"status": "active"}))
	assert.False(t, notFilter.Evaluate(map[string]interface{}{"status": "deleted"}))
}
