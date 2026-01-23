# Monolithic File Refactoring Analysis - Part 1

## File Structure Analysis

### metrics_storage.go (1,198 lines)

**Current Structure:**
- 7 major var blocks containing different metric categories
- Simple imports: only prometheus client libraries
- No complex interdependencies within the file

**Block Analysis:**
1. **WAL Metrics** (lines 12-127, 116 lines): Write operations, bytes written, replay duration, buffer pools, fsync timing
2. **io_uring Metrics** (lines 128-158, 31 lines): Submission/completion queue depths, submit latency
3. **Vector Index Metrics** (lines 159-300, 142 lines): Index size, vector norms, search operations, quantization metrics
4. **HNSW Metrics** (lines 301-513, 213 lines): Readers, graph height, node counts, distance computations, graph operations
5. **Arena Metrics** (lines 514-590, 77 lines): Memory allocation, overflow events, reset operations
6. **Compaction Metrics** (lines 591-1101, 511 lines): Operation counts, durations, record processing, garbage collection
7. **Dataset Metrics** (lines 1102-1198, 97 lines): Record batch counts, evictions, record access patterns

**Dependencies:**
- Only depends on prometheus libraries
- No internal package dependencies
- All metrics are standalone

### simd.go (1,429 lines)

**Current Structure:**
- Mixed functions, types, and global variables
- CPU detection, distance functions, batch operations all in one file

**Major Components:**
1. **CPU Detection** (lines 22-106): CPUFeatures struct, detection logic, dispatch initialization
2. **Distance Functions** (lines 352-520): Individual distance calculations for different types
3. **Batch Operations** (lines 352+): Batch processing functions
4. **Dispatch Logic** (lines 163-339): Runtime dispatch and implementation selection

**Dependencies:**
- Imports: float16, sync, sync/atomic, unsafe
- Internal dependencies: None (pure computation library)
- Global state: CPU detection results, dispatch tables

### arrow_hnsw_insert.go (1,645 lines)

**Current Structure:**
- Large monolithic file with multiple concerns
- Functions range from 10-200+ lines each

**Major Components:**
1. **PQ Training** (lines 26-125): Product Quantization training logic
2. **Insertion Core** (lines 126-503): Main insertion algorithms
3. **Connection Management** (lines 856-1151): Neighbor connections and graph updates
4. **Level Generation** (lines 1350-1381): Hierarchical level generation
5. **Search Operations** (lines 584-855): Search during insertion
6. **Pruning Logic** (lines 1152-1349): Connection pruning and optimization
7. **F16 Operations** (lines 1382-1645): Float16-specific insertion operations

**Dependencies:**
- Heavy internal dependencies: store package types, metrics, other HNSW components
- Complex coupling with graph data structures, search contexts, quantization

## Coupling Analysis

### Low Coupling (Easy to Extract)
- **metrics_storage.go**: Already logically separated by metric domains, minimal coupling
- **simd.go CPU detection**: Standalone CPU feature detection logic

### Medium Coupling (Requires Interface Design)
- **simd.go distance functions**: Need consistent API design across types
- **metrics_storage.go compaction metrics**: Large block but cohesive domain

### High Coupling (Complex Extraction)
- **arrow_hnsw_insert.go**: Heavy coupling with HNSW graph structures, search contexts, and quantization systems
- **simd.go batch operations**: May depend on distance function APIs

## Proposed Module Boundaries

### metrics_storage.go → 4 files
1. **wal_metrics.go**: WAL operations, fsync, buffer pools (Block 1 + io_uring Block 2)
2. **compaction_metrics.go**: Compaction operations and garbage collection (Block 6)
3. **storage_metrics.go**: Vector index, HNSW, arena, dataset metrics (Blocks 3, 4, 5, 7)
4. **bulk_metrics.go**: Bulk operation and error metrics (new additions)

### simd.go → 4 files
1. **cpu_detection.go**: CPU feature detection and dispatch initialization
2. **distance_functions.go**: Individual distance calculations (Euclidean, Cosine, DotProduct)
3. **batch_operations.go**: Batch processing functions (F16Batch, etc.)
4. **dispatch.go**: Runtime dispatch logic and implementation selection

### arrow_hnsw_insert.go → 8 files
1. **pq_training.go**: Product Quantization training logic
2. **insertion_core.go**: Core insertion algorithms (Insert, InsertWithVector)
3. **connection_management.go**: Neighbor connections and graph updates (AddConnection*)
4. **level_generation.go**: LevelGenerator struct and generation logic
5. **insert_search.go**: Search operations during insertion (searchLayerForInsert*)
6. **connection_pruning.go**: Pruning logic (PruneConnections, pruneConnectionsLocked)
7. **f16_operations.go**: Float16-specific operations
8. **quantization_integration.go**: Integration points with quantization systems

## Migration Strategy

### Phase 1: Safe Extractions (Low Risk)
1. Extract metrics_storage.go blocks (pure data definitions, no logic)
2. Extract cpu_detection.go from simd.go (standalone functionality)

### Phase 2: Interface Design (Medium Risk)
1. Extract distance_functions.go with consistent API design
2. Extract batch_operations.go with proper abstraction

### Phase 3: Complex Refactoring (High Risk)
1. Extract arrow_hnsw_insert.go modules with careful dependency management
2. Ensure all internal package references are maintained

## Testing Strategy

1. **Unit Tests**: Each extracted module gets focused unit tests
2. **Integration Tests**: End-to-end tests ensure functionality preservation
3. **Performance Tests**: Benchmark tests verify no performance regression
4. **Build Verification**: Ensure all imports resolve correctly

## Risk Assessment

- **Low Risk**: metrics_storage.go extraction (pure data movement)
- **Medium Risk**: simd.go extraction (API consistency required)
- **High Risk**: arrow_hnsw_insert.go extraction (complex internal dependencies)

## Success Criteria

- All builds pass without import errors
- All existing tests continue to pass
- No performance regression in benchmarks
- Code coverage maintained or improved
- Clear module boundaries with single responsibilities