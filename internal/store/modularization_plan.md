# Store Package Modularization Plan

## Overview
The store package currently contains 482+ files with mixed concerns. This plan outlines a systematic approach to break it down into focused, maintainable sub-packages.

## Target Package Structure

```
internal/store/
├── core/           # Core HNSW algorithms and data structures
├── indexing/       # Search and indexing operations
├── compaction/     # Background maintenance and compaction
├── storage/        # Persistence and I/O operations
├── types/          # Common types and interfaces
├── metrics/        # Metrics collection (already separate)
├── query/          # Query processing (already separate)
└── [main files]    # Public API and orchestration
```

## Phase 1: Core Package (High Priority)

### Files to Move:
```
core/
├── arrow_hnsw_graph.go         # Core HNSW graph implementation
├── arrow_hnsw_index.go         # HNSW index operations
├── arrow_hnsw_compute.go       # Distance computations
├── arrow_hnsw_search.go        # Core search algorithms
├── graph_data_access.go        # Graph data structures
├── graph_data_clone.go         # Graph data utilities
├── graph_store.go              # Graph storage interface
├── arrow_bitset.go             # Bitset operations
├── arrow_maxheap.go            # Priority queue
└── [associated test files]
```

### Interfaces to Define:
```go
// Core interfaces that other packages will import
type HNSWGraph interface {
    // Core HNSW operations
}

type GraphData interface {
    // Graph data operations
}
```

## Phase 2: Indexing Package

### Files to Move:
```
indexing/
├── index.go                    # Index interface and base implementations
├── index_build.go              # Index construction
├── index_search.go             # Index search operations
├── hnsw.go                     # HNSW wrapper
├── sharded_hnsw.go             # Sharded HNSW implementation
└── [associated test files]
```

## Phase 3: Compaction Package

### Files to Move:
```
compaction/
├── compaction.go               # Compaction algorithms
├── compaction_store.go         # Compaction worker
├── hnsw_compaction.go          # HNSW-specific compaction
├── auto_compaction_test.go     # Compaction tests
└── [other compaction files]
```

## Phase 4: Storage Package

### Files to Move:
```
storage/
├── disk_vector_store.go        # Disk persistence
├── persistence.go              # Persistence utilities
├── wal_*.go                    # WAL implementations
├── serialization.go            # Data serialization
└── [storage-related tests]
```

## Implementation Strategy

### Step 1: Create Internal Organization
1. Create `internal/store/internal/` subdirectories
2. Move files to logical groupings within store package
3. Update import paths to use internal paths

### Step 2: Define Package Boundaries
1. Identify shared interfaces and types
2. Create `types/` package for common definitions
3. Ensure clean API boundaries between packages

### Step 3: Gradual Package Separation
1. Convert internal directories to separate packages one by one
2. Update all import statements
3. Ensure all tests pass after each conversion

### Step 4: API Cleanup
1. Define clean public APIs for each package
2. Remove internal dependencies from public interfaces
3. Update documentation and examples

## Risk Mitigation

### Testing Strategy
- Run full test suite after each major move
- Use build tags to test internal organization
- Maintain backward compatibility during transition

### Rollback Plan
- Keep git history for easy reversion
- Use feature flags for package boundaries
- Maintain interface compatibility

## Success Criteria

- [ ] Max 50 files per package
- [ ] Clear package responsibilities
- [ ] Clean import dependencies
- [ ] All tests passing
- [ ] No circular dependencies
- [ ] Improved code navigation and maintainability