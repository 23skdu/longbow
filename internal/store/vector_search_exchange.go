package store

import (
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PeekedStream wraps DoExchangeServer to inject a pre-read message
type PeekedStream struct {
	flight.FlightService_DoExchangeServer
	firstMsg *flight.FlightData
	read     bool
}

func (p *PeekedStream) Recv() (*flight.FlightData, error) {
	if !p.read && p.firstMsg != nil {
		p.read = true
		return p.firstMsg, nil
	}
	return p.FlightService_DoExchangeServer.Recv()
}

// handleVectorSearchExchange handles binary protocol vector search
func (s *VectorStore) handleVectorSearchExchange(stream flight.FlightService_DoExchangeServer, firstMsg *flight.FlightData) error {
	timer := prometheus.NewTimer(metrics.DoExchangeSearchDuration)
	defer timer.ObserveDuration()
	metrics.DoExchangeSearchTotal.Inc()

	// Wrap stream to reply the first message (which contains Schema + Descriptor)
	peeked := &PeekedStream{
		FlightService_DoExchangeServer: stream,
		firstMsg:                       firstMsg,
	}

	// 1. Read Request Batch
	reader, err := flight.NewRecordReader(peeked)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create record reader: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		if reader.Err() != nil {
			return status.Errorf(codes.Internal, "failed to read record: %v", reader.Err())
		}
		return status.Error(codes.InvalidArgument, "empty search request")
	}

	rec := reader.RecordBatch()

	// 2. Parse Parameters
	if rec.NumRows() == 0 {
		return status.Error(codes.InvalidArgument, "empty search request parameters")
	}

	colIdx := func(name string) int {
		for i, field := range rec.Schema().Fields() {
			if field.Name == name {
				return i
			}
		}
		return -1
	}

	// Extract Dataset
	idxDataset := colIdx("dataset")
	if idxDataset == -1 {
		return status.Error(codes.InvalidArgument, "missing 'dataset' column")
	}
	datasetName := rec.Column(idxDataset).(*array.String).Value(0)

	// Extract K
	idxK := colIdx("k")
	k := 10 // Default
	if idxK != -1 {
		k = int(rec.Column(idxK).(*array.Int32).Value(0))
	}

	// Extract Ef
	idxEf := colIdx("ef")
	ef := -1 // Default
	if idxEf != -1 {
		ef = int(rec.Column(idxEf).(*array.Int32).Value(0))
	}

	// Extract Query Vector (FixedSizeList<Float32> or List<Float32>)
	idxVec := colIdx("query_vector")
	if idxVec == -1 {
		return status.Error(codes.InvalidArgument, "missing 'query_vector' column")
	}

	// Parse float32 slice
	var queryVec []float32
	vecCol := rec.Column(idxVec)
	switch col := vecCol.(type) {
	case *array.FixedSizeList:
		values := col.ListValues().(*array.Float32).Float32Values()
		listSize := int(col.DataType().(*arrow.FixedSizeListType).Len())
		// For row 0:
		start := 0
		end := start + listSize
		if len(values) < end {
			return status.Error(codes.Internal, "invalid fixed size list length")
		}
		queryVec = values[start:end]
	case *array.List:
		values := col.ListValues().(*array.Float32).Float32Values()
		offsets := col.Offsets()
		start := int(offsets[0])
		end := int(offsets[1])
		if len(values) < end {
			return status.Error(codes.Internal, "invalid list length")
		}
		queryVec = values[start:end]
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported query_vector type: %T", vecCol)
	}

	// 3. Execute Search (Logic replicated/shared from actions)
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", datasetName)
	}

	ds.dataMu.RLock()
	// Check eviction
	if ds.evicting.Load() {
		ds.dataMu.RUnlock()
		return status.Errorf(codes.Unavailable, "dataset %s is being evicted", datasetName)
	}
	if ds.Index == nil {
		ds.dataMu.RUnlock()
		return status.Error(codes.FailedPrecondition, "dataset has no index")
	}

	// Validate dims
	if uint32(len(queryVec)) != ds.Index.GetDimension() {
		ds.dataMu.RUnlock()
		return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", ds.Index.GetDimension(), len(queryVec))
	}

	// Perform Search
	// TODO: Use 'ef' if supported by SearchOptions
	searchOpts := SearchOptions{
		IncludeVectors: false,
	}
	// Logic to use ef if > 0 (assuming SearchVectors might use it or we set it globally)
	// For now, suppress unused var
	if ef > 0 {
		// TODO: Pass ef to HNSW search context
		_ = ef
	}

	searchResults, err := ds.Index.SearchVectors(queryVec, k, nil, searchOpts)
	ds.dataMu.RUnlock() // Unlock early

	if err != nil {
		return status.Errorf(codes.Internal, "search failed: %v", err)
	}

	// Map Internal IDs to User IDs (if necessary, SearchVectors returns internal IDs?)
	// MapInternalToUserIDs requires dataset lock? No, it uses `ds.Records`.
	// ds.Records is slice, protected by lock? Yes.
	// We released lock. Unsafe.
	// We should hold lock or acquire RLock inside MapInternalToUserIDs?
	// s.MapInternalToUserIDs iterates existing `ds.Records`.
	// We should map BEFORE unlocking or use `SearchVectors` that returns valid IDs.
	// HNSW index dealing with internal IDs.
	// Let's re-acquire RLock or keep it held.

	ds.dataMu.RLock()
	searchResults = s.mapInternalToUserIDsLocked(ds, searchResults)
	ds.dataMu.RUnlock()

	// 4. Serialize Results
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		},
		nil,
	)

	pool := memory.DefaultAllocator
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.Uint64Builder)
	scoreBuilder := b.Field(1).(*array.Float32Builder)

	idBuilder.Reserve(len(searchResults))
	scoreBuilder.Reserve(len(searchResults))

	for _, res := range searchResults {
		idBuilder.Append(uint64(res.ID))
		scoreBuilder.Append(res.Score)
	}

	resRec := b.NewRecordBatch()
	defer resRec.Release()

	// 5. Write Response
	// NewRecordWriter takes (ipc.MessageWriter, ...options)
	// flight.NewRecordWriter takes (flight.DataStreamWriter, ...options)
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer func() { _ = writer.Close() }()

	if err := writer.Write(resRec); err != nil {
		return status.Errorf(codes.Internal, "failed to write response: %v", err)
	}

	return nil
}
