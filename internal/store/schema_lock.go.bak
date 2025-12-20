package store

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// SchemaCompatibility represents the result of schema comparison
type SchemaCompatibility int

const (
	// SchemaExactMatch indicates schemas are identical
	SchemaExactMatch SchemaCompatibility = iota
	// SchemaEvolution indicates new schema is a compatible superset
	SchemaEvolution
	// SchemaIncompatible indicates schemas are not compatible
	SchemaIncompatible
)

// GetExistingSchema returns the schema of the first record in the dataset.
// Uses dataMu RLock for read-only access to Records.
// Returns nil if dataset has no records.
func (ds *Dataset) GetExistingSchema() *arrow.Schema {
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if len(ds.Records) == 0 {
		return nil
	}
	// Return the latest schema (from the last record)
	return ds.Records[len(ds.Records)-1].Schema()
}

// GetRecordsCount returns the number of records in the dataset.
// Uses dataMu RLock for read-only access.
func (ds *Dataset) GetRecordsCount() int {
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	return len(ds.Records)
}

// CheckSchemaCompatibility compares existing and incoming schemas.
// Returns SchemaExactMatch if identical, SchemaEvolution if incoming
// is a compatible superset (prefix matches), or SchemaIncompatible otherwise.
func CheckSchemaCompatibility(existing, incoming *arrow.Schema) SchemaCompatibility {
	if existing == nil || incoming == nil {
		return SchemaIncompatible
	}

	// Exact match
	if existing.Equal(incoming) {
		return SchemaExactMatch
	}

	// Check for compatible evolution (incoming is superset)
	if len(incoming.Fields()) > len(existing.Fields()) {
		// Verify existing fields are a prefix of incoming
		for i, f := range existing.Fields() {
			if !f.Equal(incoming.Field(i)) {
				return SchemaIncompatible
			}
		}
		return SchemaEvolution
	}

	return SchemaIncompatible
}

// UpgradeSchemaVersion increments the dataset version.
// Uses Lock for exclusive write access.
func (ds *Dataset) UpgradeSchemaVersion() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.Version++
}

// GetVersion returns the current schema version.
// Uses RLock for read-only access.
func (ds *Dataset) GetVersion() int64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.Version
}
