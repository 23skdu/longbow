package store

import (
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestCollectionRegistry_RegisterCollection(t *testing.T) {
	registry := NewCollectionRegistry()

	info := &CollectionInfo{
		Name:        "test-collection",
		Description: "Test collection",
		Dimension:   128,
		VectorCount: 1000,
		Tags:        []string{"test", "demo"},
	}

	err := registry.RegisterCollection(info)
	assert.NoError(t, err)

	retrieved, ok := registry.GetCollection("test-collection")
	assert.True(t, ok)
	assert.Equal(t, "test-collection", retrieved.Name)
	assert.Equal(t, 128, retrieved.Dimension)
}

func TestCollectionRegistry_DuplicateCollection(t *testing.T) {
	registry := NewCollectionRegistry()

	info := &CollectionInfo{
		Name:      "dup-collection",
		Dimension: 128,
	}

	err := registry.RegisterCollection(info)
	assert.NoError(t, err)

	err = registry.RegisterCollection(info)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCollectionRegistry_DeleteCollection(t *testing.T) {
	registry := NewCollectionRegistry()

	info := &CollectionInfo{
		Name:      "to-delete",
		Dimension: 128,
	}
	registry.RegisterCollection(info)

	err := registry.DeleteCollection("to-delete")
	assert.NoError(t, err)

	_, ok := registry.GetCollection("to-delete")
	assert.False(t, ok)
}

func TestCollectionRegistry_ListCollections(t *testing.T) {
	registry := NewCollectionRegistry()

	registry.RegisterCollection(&CollectionInfo{Name: "col1", Dimension: 128})
	registry.RegisterCollection(&CollectionInfo{Name: "col2", Dimension: 256})
	registry.RegisterCollection(&CollectionInfo{Name: "col3", Dimension: 512})

	collections := registry.ListCollections()
	assert.Len(t, collections, 3)
}

func TestCollectionRegistry_RegisterRoutingRule(t *testing.T) {
	registry := NewCollectionRegistry()

	rule := &RoutingRule{
		Tag:        "tag1",
		Collection: "col1",
		Priority:   1,
		Conditions: []string{"env=prod"},
	}

	err := registry.RegisterRoutingRule(rule)
	assert.NoError(t, err)

	retrieved, ok := registry.GetRoutingRule("tag1")
	assert.True(t, ok)
	assert.Equal(t, "col1", retrieved.Collection)
	assert.Equal(t, 1, retrieved.Priority)
}

func TestReciprocalRankFusion(t *testing.T) {
	results1 := []lbtypes.SearchResult{
		{ID: 1, Distance: 1.0, Score: 1.0},
		{ID: 2, Distance: 0.8, Score: 0.8},
		{ID: 3, Distance: 0.6, Score: 0.6},
	}
	results2 := []lbtypes.SearchResult{
		{ID: 1, Distance: 0.9, Score: 0.9},
		{ID: 2, Distance: 0.7, Score: 0.7},
		{ID: 4, Distance: 0.5, Score: 0.5},
	}

	fused := ReciprocalRankFusion("test", results1, results2, 60, 1)

	assert.NotEmpty(t, fused)

	for _, r := range fused {
		assert.NotEmpty(t, r.ID)
	}
}

func TestFederatedQueryRouter_RegisterCollection(t *testing.T) {
	router := NewFederatedQueryRouter()

	info := &CollectionInfo{
		Name:      "test-col",
		Dimension: 128,
	}

	err := router.registry.RegisterCollection(info)
	assert.NoError(t, err)

	coll, ok := router.registry.GetCollection("test-col")
	assert.True(t, ok)
	assert.Equal(t, 128, coll.Dimension)
}

func TestCollectionRegistry_RouteQuery(t *testing.T) {
	registry := NewCollectionRegistry()
	registry.RegisterCollection(&CollectionInfo{
		Name:        "prod-collection",
		Description: "Production data",
		Dimension:   128,
		Tags:        []string{"prod"},
	})
	registry.RegisterCollection(&CollectionInfo{
		Name:        "dev-collection",
		Description: "Dev data",
		Dimension:   128,
		Tags:        []string{"dev"},
	})

	registry.RegisterRoutingRule(&RoutingRule{
		Tag:        "prod",
		Collection: "prod-collection",
		Priority:   1,
	})

	rule, ok := registry.GetRoutingRule("prod")
	assert.True(t, ok)
	assert.Equal(t, "prod-collection", rule.Collection)
}

func TestCollectionRegistry_RouteByTag(t *testing.T) {
	registry := NewCollectionRegistry()
	registry.RegisterCollection(&CollectionInfo{
		Name: "collection1",
		Tags: []string{"tag1", "tag2"},
	})
	registry.RegisterCollection(&CollectionInfo{
		Name: "collection2",
		Tags: []string{"tag2", "tag3"},
	})

	collections := registry.ListCollections()
	assert.Len(t, collections, 2)

	var tag1Collections []string
	for _, c := range collections {
		for _, tag := range c.Tags {
			if tag == "tag1" {
				tag1Collections = append(tag1Collections, c.Name)
			}
		}
	}
	assert.Contains(t, tag1Collections, "collection1")
}
