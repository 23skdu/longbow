package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
)

// SchemaCompatibilityMode mirrors Confluent-style evolution guarantees.
type SchemaCompatibilityMode string

const (
	CompatNone     SchemaCompatibilityMode = "NONE"
	CompatBackward SchemaCompatibilityMode = "BACKWARD"
	CompatForward  SchemaCompatibilityMode = "FORWARD"
	CompatFull     SchemaCompatibilityMode = "FULL"
)

// FieldConstraint defines validation & constraint rules for an Arrow column or metadata key.
type FieldConstraint struct {
	Field     string   `json:"field"`
	Type      string   `json:"type,omitempty"` // "string", "number", "int", "bool", "vector"
	NotNull   bool     `json:"not_null,omitempty"`
	Unique    bool     `json:"unique,omitempty"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Regex     string   `json:"regex,omitempty"`
	Enum      []string `json:"enum,omitempty"`
	VectorDim int      `json:"vector_dim,omitempty"`
}

// DatasetSchemaSpec declares versioned field constraints, metadata typing, and evolution compatibility.
type DatasetSchemaSpec struct {
	Dataset        string                  `json:"dataset"`
	Version        int                     `json:"version"`
	Compatibility  SchemaCompatibilityMode `json:"compatibility"`
	Constraints    []FieldConstraint       `json:"constraints"`
	MetadataTypes  map[string]string       `json:"metadata_types,omitempty"`
	StrictMetadata bool                    `json:"strict_metadata,omitempty"`
	UpdatedAt      time.Time               `json:"updated_at"`
}

// SchemaConstraintRegistry enforces field constraints, unique keys, metadata typing,
// evolution compatibility, and dataset migration.
type SchemaConstraintRegistry struct {
	mu          sync.RWMutex
	store       *VectorStore
	specs       map[string][]DatasetSchemaSpec
	regexCache  map[string]*regexp.Regexp
	uniqueIndex map[string]map[string]map[string]struct{} // dataset -> field -> valueKey -> present
}

// NewSchemaConstraintRegistry creates a schema validation & constraint registry.
func NewSchemaConstraintRegistry(s *VectorStore) *SchemaConstraintRegistry {
	return &SchemaConstraintRegistry{
		store:       s,
		specs:       make(map[string][]DatasetSchemaSpec),
		regexCache:  make(map[string]*regexp.Regexp),
		uniqueIndex: make(map[string]map[string]map[string]struct{}),
	}
}

// RegisterSchema checks evolution compatibility against prior versions, pre-compiles regexes,
// and stores the new schema version.
func (r *SchemaConstraintRegistry) RegisterSchema(spec DatasetSchemaSpec) (int, error) {
	if spec.Dataset == "" {
		return 0, errors.New("dataset is required")
	}
	if spec.Compatibility == "" {
		spec.Compatibility = CompatBackward
	}
	compiled := make(map[string]*regexp.Regexp)
	for _, c := range spec.Constraints {
		if c.Field == "" {
			return 0, errors.New("constraint field name cannot be empty")
		}
		if c.Regex != "" {
			rx, err := regexp.Compile(c.Regex)
			if err != nil {
				return 0, fmt.Errorf("invalid regex for field %q: %w", c.Field, err)
			}
			compiled[c.Regex] = rx
		}
		if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
			return 0, fmt.Errorf("field %q min (%v) > max (%v)", c.Field, *c.Min, *c.Max)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for pat, rx := range compiled {
		r.regexCache[pat] = rx
	}
	history := r.specs[spec.Dataset]
	if len(history) > 0 {
		prev := history[len(history)-1]
		if err := CheckEvolutionCompatibility(prev, spec, spec.Compatibility); err != nil {
			metrics.SchemaValidationRejectionsTotal.WithLabelValues(spec.Dataset, "incompatible_evolution").Inc()
			return 0, err
		}
		spec.Version = prev.Version + 1
	} else if spec.Version <= 0 {
		spec.Version = 1
	}
	spec.UpdatedAt = time.Now().UTC()
	r.specs[spec.Dataset] = append(history, spec)
	return spec.Version, nil
}

// LatestSchema returns the current active schema spec for a dataset, if registered.
func (r *SchemaConstraintRegistry) LatestSchema(dataset string) (DatasetSchemaSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := r.specs[dataset]
	if len(h) == 0 {
		return DatasetSchemaSpec{}, false
	}
	return h[len(h)-1], true
}

// CheckEvolutionCompatibility verifies BACKWARD, FORWARD, FULL, or NONE rules between versions.
func CheckEvolutionCompatibility(prev, next DatasetSchemaSpec, mode SchemaCompatibilityMode) error {
	if mode == CompatNone || mode == "" {
		return nil
	}
	prevMap := make(map[string]FieldConstraint, len(prev.Constraints))
	for _, c := range prev.Constraints {
		prevMap[c.Field] = c
	}
	nextMap := make(map[string]FieldConstraint, len(next.Constraints))
	for _, c := range next.Constraints {
		nextMap[c.Field] = c
	}

	checkBackward := mode == CompatBackward || mode == CompatFull
	checkForward := mode == CompatForward || mode == CompatFull

	// Type changes are always incompatible under BACKWARD, FORWARD, and FULL.
	for name, nc := range nextMap {
		if pc, existed := prevMap[name]; existed {
			if pc.Type != "" && nc.Type != "" && pc.Type != nc.Type {
				return fmt.Errorf("incompatible type evolution on %q: %s -> %s", name, pc.Type, nc.Type)
			}
			if pc.VectorDim > 0 && nc.VectorDim > 0 && pc.VectorDim != nc.VectorDim {
				return fmt.Errorf("incompatible vector_dim change on %q: %d -> %d", name, pc.VectorDim, nc.VectorDim)
			}
		} else if checkBackward && nc.NotNull {
			return fmt.Errorf("BACKWARD incompatible: cannot add required NOT NULL field %q", name)
		}
	}
	if checkForward {
		for name, pc := range prevMap {
			if _, stillPresent := nextMap[name]; !stillPresent && pc.NotNull {
				return fmt.Errorf("FORWARD incompatible: cannot drop required field %q", name)
			}
		}
	}
	return nil
}

// ValidateBatch enforces registered schema constraints, metadata types, NOT NULL, UNIQUE,
// and CHECK rules against an Arrow RecordBatch before ingestion.
func (r *SchemaConstraintRegistry) ValidateBatch(ctx context.Context, dataset string, rec arrow.RecordBatch) error {
	_ = ctx
	r.mu.RLock()
	history := r.specs[dataset]
	if len(history) == 0 {
		r.mu.RUnlock()
		return nil
	}
	spec := history[len(history)-1]
	r.mu.RUnlock()

	if rec == nil || rec.NumRows() == 0 {
		return nil
	}

	schema := rec.Schema()
	fieldColIdx := make(map[string]int, schema.NumFields())
	for i, f := range schema.Fields() {
		fieldColIdx[f.Name] = i
	}
	metaColIdx, hasMetaCol := fieldColIdx["metadata"]

	// Validate vector dimensions & column presence
	for _, c := range spec.Constraints {
		if c.VectorDim > 0 && (c.Field == "vector" || c.Field == "embedding") {
			colIdx, ok := fieldColIdx[c.Field]
			if !ok {
				metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "missing_column").Inc()
				return fmt.Errorf("schema violation: missing vector column %q", c.Field)
			}
			if listArr, ok := rec.Column(colIdx).(*array.FixedSizeList); ok {
				gotDim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				if gotDim != c.VectorDim {
					metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "vector_dim").Inc()
					return fmt.Errorf("schema violation on %q: expected vector_dim=%d got %d", c.Field, c.VectorDim, gotDim)
				}
			}
		}
		if strings.HasPrefix(c.Field, "metadata.") {
			continue
		}
		if _, ok := fieldColIdx[c.Field]; !ok && c.NotNull && !hasMetaCol {
			metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "not_null").Inc()
			return fmt.Errorf("schema violation: required field %q is missing", c.Field)
		}
	}

	// Stage unique keys optimistically and commit only if entire batch passes.
	pendingUnique := make(map[string][]string)

	for row := 0; row < int(rec.NumRows()); row++ {
		var rowMeta map[string]any
		if hasMetaCol {
			metaCol := rec.Column(metaColIdx)
			if !metaCol.IsNull(row) {
				if sa, ok := metaCol.(*array.String); ok {
					raw := sa.Value(row)
					if raw != "" {
						if err := json.Unmarshal([]byte(raw), &rowMeta); err != nil {
							metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "invalid_metadata_json").Inc()
							return fmt.Errorf("row %d: invalid metadata JSON: %w", row, err)
						}
					}
				}
			}
		}

		// Metadata type enforcement
		for k, v := range rowMeta {
			if expectedType, ok := spec.MetadataTypes[k]; ok {
				if !matchesValueType(v, expectedType) {
					metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "metadata_type").Inc()
					return fmt.Errorf("row %d: metadata key %q expected type %q", row, k, expectedType)
				}
			} else if spec.StrictMetadata && len(spec.MetadataTypes) > 0 {
				metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "strict_metadata").Inc()
				return fmt.Errorf("row %d: unexpected metadata key %q under strict_metadata mode", row, k)
			}
		}

		for _, c := range spec.Constraints {
			if c.VectorDim > 0 && (c.Field == "vector" || c.Field == "embedding") {
				continue
			}
			val, isNull := r.extractFieldValue(rec, fieldColIdx, c.Field, rowMeta, row)
			if isNull || val == nil {
				if c.NotNull {
					metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "not_null").Inc()
					return fmt.Errorf("row %d: NOT NULL constraint violated on %q", row, c.Field)
				}
				continue
			}
			if c.Type != "" && !matchesValueType(val, c.Type) {
				metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "type_mismatch").Inc()
				return fmt.Errorf("row %d: field %q expected type %s", row, c.Field, c.Type)
			}
			if err := r.checkValueConstraints(c, val, row); err != nil {
				metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "check_constraint").Inc()
				return err
			}
			if c.Unique {
				valKey := fmt.Sprintf("%v", val)
				if r.isDuplicateUnique(dataset, c.Field, valKey, pendingUnique[c.Field]) {
					metrics.SchemaValidationRejectionsTotal.WithLabelValues(dataset, "unique").Inc()
					return fmt.Errorf("row %d: UNIQUE constraint violated on %q for value %q", row, c.Field, valKey)
				}
				pendingUnique[c.Field] = append(pendingUnique[c.Field], valKey)
			}
		}
	}

	if len(pendingUnique) > 0 {
		r.commitUniqueValues(dataset, pendingUnique)
	}
	return nil
}

func (r *SchemaConstraintRegistry) extractFieldValue(rec arrow.RecordBatch, fieldColIdx map[string]int, field string, rowMeta map[string]any, row int) (any, bool) {
	if strings.HasPrefix(field, "metadata.") {
		key := strings.TrimPrefix(field, "metadata.")
		if rowMeta == nil {
			return nil, true
		}
		v, ok := rowMeta[key]
		return v, !ok || v == nil
	}
	if colIdx, ok := fieldColIdx[field]; ok {
		col := rec.Column(colIdx)
		if col.IsNull(row) {
			return nil, true
		}
		switch arr := col.(type) {
		case *array.String:
			return arr.Value(row), false
		case *array.Int64:
			return arr.Value(row), false
		case *array.Int32:
			return arr.Value(row), false
		case *array.Uint32:
			return arr.Value(row), false
		case *array.Uint64:
			return arr.Value(row), false
		case *array.Float64:
			return arr.Value(row), false
		case *array.Float32:
			return arr.Value(row), false
		case *array.Boolean:
			return arr.Value(row), false
		default:
			return fmt.Sprintf("%v", arr.GetOneForMarshal(row)), false
		}
	}
	if rowMeta != nil {
		if v, ok := rowMeta[field]; ok && v != nil {
			return v, false
		}
	}
	return nil, true
}

func (r *SchemaConstraintRegistry) checkValueConstraints(c FieldConstraint, val any, row int) error {
	if c.Min != nil || c.Max != nil {
		num, ok := toFloat64(val)
		if !ok {
			return fmt.Errorf("row %d: CHECK range constraint requires numeric value on %q", row, c.Field)
		}
		if c.Min != nil && num < *c.Min {
			return fmt.Errorf("row %d: CHECK constraint violated on %q: %v < min %v", row, c.Field, num, *c.Min)
		}
		if c.Max != nil && num > *c.Max {
			return fmt.Errorf("row %d: CHECK constraint violated on %q: %v > max %v", row, c.Field, num, *c.Max)
		}
	}
	if c.Regex != "" {
		s := fmt.Sprintf("%v", val)
		r.mu.RLock()
		rx := r.regexCache[c.Regex]
		r.mu.RUnlock()
		if rx == nil {
			var err error
			rx, err = regexp.Compile(c.Regex)
			if err != nil {
				return err
			}
		}
		if !rx.MatchString(s) {
			return fmt.Errorf("row %d: CHECK regex violated on %q: %q does not match %q", row, c.Field, s, c.Regex)
		}
	}
	if len(c.Enum) > 0 {
		s := fmt.Sprintf("%v", val)
		matched := false
		for _, allowed := range c.Enum {
			if s == allowed {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("row %d: CHECK enum violated on %q: %q not in %v", row, c.Field, s, c.Enum)
		}
	}
	return nil
}

func (r *SchemaConstraintRegistry) isDuplicateUnique(dataset, field, valKey string, batchPending []string) bool {
	for _, b := range batchPending {
		if b == valKey {
			return true
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.uniqueIndex[dataset] == nil || r.uniqueIndex[dataset][field] == nil {
		return false
	}
	_, exists := r.uniqueIndex[dataset][field][valKey]
	return exists
}

func (r *SchemaConstraintRegistry) commitUniqueValues(dataset string, pending map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.uniqueIndex[dataset] == nil {
		r.uniqueIndex[dataset] = make(map[string]map[string]struct{})
	}
	for field, vals := range pending {
		if r.uniqueIndex[dataset][field] == nil {
			r.uniqueIndex[dataset][field] = make(map[string]struct{})
		}
		for _, v := range vals {
			r.uniqueIndex[dataset][field][v] = struct{}{}
		}
	}
}

// MigrateDatasetSchema registers the new schema version and backfills default values for newly
// added columns across existing Arrow batches in the Dataset.
func (r *SchemaConstraintRegistry) MigrateDatasetSchema(ds *Dataset, target DatasetSchemaSpec, addStringCols map[string]string) (int, error) {
	if ds == nil {
		return 0, errors.New("dataset is nil")
	}
	ver, err := r.RegisterSchema(target)
	if err != nil {
		return 0, err
	}
	if len(addStringCols) == 0 {
		return ver, nil
	}

	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()
	if ds.SchemaManager != nil {
		for colName := range addStringCols {
			_ = ds.SchemaManager.AddColumn(colName, arrow.BinaryTypes.String)
		}
	}
	records := ds.Records.Read()
	updated := make([]arrow.RecordBatch, 0, len(records))
	for _, rec := range records {
		ext := appendStringColumns(memory.DefaultAllocator, rec, addStringCols)
		updated = append(updated, ext)
		rec.Release()
	}
	ds.Records.Update(updated)
	if len(updated) > 0 {
		ds.Schema = updated[0].Schema()
	}
	return ver, nil
}

func appendStringColumns(alloc memory.Allocator, rec arrow.RecordBatch, addCols map[string]string) arrow.RecordBatch {
	fields := append([]arrow.Field{}, rec.Schema().Fields()...)
	cols := append([]arrow.Array{}, rec.Columns()...)
	for k, defVal := range addCols {
		if len(rec.Schema().FieldIndices(k)) > 0 {
			continue
		}
		fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String, Nullable: true})
		sb := array.NewStringBuilder(alloc)
		for i := 0; i < int(rec.NumRows()); i++ {
			sb.Append(defVal)
		}
		arr := sb.NewArray()
		sb.Release()
		cols = append(cols, arr)
		defer arr.Release()
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
}

func matchesValueType(v any, expected string) bool {
	switch strings.ToLower(expected) {
	case "string", "str":
		_, ok := v.(string)
		return ok
	case "bool", "boolean":
		_, ok := v.(bool)
		return ok
	case "int", "integer":
		switch x := v.(type) {
		case int, int32, int64, uint32, uint64:
			return true
		case float64:
			return x == float64(int64(x))
		default:
			return false
		}
	case "number", "float":
		_, ok := toFloat64(v)
		return ok
	default:
		return true
	}
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func (s *VectorStore) handleSchemaRegistryAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "RegisterSchema":
		var spec DatasetSchemaSpec
		if err := json.Unmarshal(action.Body, &spec); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid schema json: %v", err)
		}
		ver, err := s.GetSchemaRegistry().RegisterSchema(spec)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"dataset": spec.Dataset, "version": ver})
		return true, stream.Send(&flight.Result{Body: body})

	case "GetSchemaRegistry":
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		spec, ok := s.GetSchemaRegistry().LatestSchema(req.Dataset)
		if !ok {
			return true, status.Errorf(codes.NotFound, "no registered schema for %q", req.Dataset)
		}
		body, _ := json.Marshal(spec)
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
