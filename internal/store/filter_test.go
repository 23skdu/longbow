
package store

import (
	"io"
"context"
"encoding/json"
"testing"


"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/ipc"
"github.com/apache/arrow-go/v18/arrow/memory"
"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
)

func TestFiltering(t *testing.T) {
_, _, dialer := setupServer(t)
ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough:///bufnet",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer func() { _ = client.Close() }()

// 1. Create Data with various types
schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "score", Type: arrow.PrimitiveTypes.Float64},
{Name: "category", Type: arrow.BinaryTypes.String},
},
nil,
)

mem := memory.NewGoAllocator()
b := array.NewRecordBuilder(mem, schema)
defer b.Release()

b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
b.Field(1).(*array.Float64Builder).AppendValues([]float64{10.5, 20.0, 30.5, 40.0, 50.5}, nil)
b.Field(2).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "B"}, nil)
rec := b.NewRecord()
defer rec.Release()

// 2. Upload Data
stream, err := client.DoPut(ctx)
if err != nil {
t.Fatalf("DoPut failed: %v", err)
}

desc := &flight.FlightDescriptor{
Type: flight.DescriptorPATH,
Path: []string{"filter_test"},
}

w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
w.SetFlightDescriptor(desc)

if err := w.Write(rec); err != nil {
t.Fatalf("Write failed: %v", err)
}
_ = w.Close()
if err := stream.CloseSend(); err != nil {
	t.Fatalf("CloseSend failed: %v", err)
}
if _, err := stream.Recv(); err != io.EOF {
	t.Fatalf("Recv expected EOF, got: %v", err)
}

// 3. Test Cases
tests := []struct {
name string
filters []Filter
expectedCount int
}{
{
name: "Filter ID > 3",
filters: []Filter{{Field: "id", Operator: ">", Value: "3"}},
expectedCount: 2, // 4, 5
},
{
name: "Filter Category = A",
filters: []Filter{{Field: "category", Operator: "=", Value: "A"}},
expectedCount: 2, // 1, 3
},
{
name: "Filter Score <= 20.0",
filters: []Filter{{Field: "score", Operator: "<=", Value: "20.0"}},
expectedCount: 2, // 10.5, 20.0
},
{
name: "Combined Filter: Category = B AND ID > 2",
filters: []Filter{
{Field: "category", Operator: "=", Value: "B"},
{Field: "id", Operator: ">", Value: "2"},
},
expectedCount: 1, // 5 (2 is B but id=2 not > 2)
},
}

for _, tc := range tests {
t.Run(tc.name, func(t *testing.T) {
query := TicketQuery{
Name: "filter_test",
Filters: tc.filters,
}
ticketBytes, _ := json.Marshal(query)
ticket := &flight.Ticket{Ticket: ticketBytes}

rStream, err := client.DoGet(ctx, ticket)
if err != nil {
t.Fatalf("DoGet failed: %v", err)
}

r, err := flight.NewRecordReader(rStream)
if err != nil {
t.Fatalf("NewRecordReader failed: %v", err)
}
defer r.Release()

count := 0
for r.Next() {
count += int(r.Record().NumRows())
}
if r.Err() != nil {
t.Fatalf("Reader error: %v", r.Err())
}

if count != tc.expectedCount {
t.Errorf("Expected %d rows, got %d", tc.expectedCount, count)
}
})
}
}


func TestListFlightsFiltering(t *testing.T) {
_, _, dialer := setupServer(t)
ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough:///bufnet",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer func() { _ = client.Close() }()

// 1. Create Datasets
// Dataset A: "alpha", 10 rows
// Dataset B: "beta", 20 rows
// Dataset C: "gamma", 30 rows
datasets := []struct {
name string
rows int
}{
{"alpha", 10},
{"beta", 20},
{"gamma", 30},
}

schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
mem := memory.NewGoAllocator()

for _, ds := range datasets {
b := array.NewRecordBuilder(mem, schema)
for i := 0; i < ds.rows; i++ {
b.Field(0).(*array.Int64Builder).Append(int64(i))
}
rec := b.NewRecord()
b.Release()

stream, _ := client.DoPut(ctx)
w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{ds.name}})
if err := w.Write(rec); err != nil {
	t.Fatalf("Write failed: %v", err)
}
_ = w.Close()
if err := stream.CloseSend(); err != nil {
	t.Fatalf("CloseSend failed: %v", err)
}
if _, err := stream.Recv(); err != io.EOF {
	t.Fatalf("Recv expected EOF, got: %v", err)
}
rec.Release()
}

// 2. Test Cases
tests := []struct {
name string
filters []Filter
expected []string
}{
{
name: "Name Contains 'a'",
filters: []Filter{{Field: "name", Operator: "contains", Value: "a"}},
expected: []string{"alpha", "beta", "gamma"},
},
{
name: "Name Contains 'mm'",
filters: []Filter{{Field: "name", Operator: "contains", Value: "mm"}},
expected: []string{"gamma"},
},
{
name: "Rows > 15",
filters: []Filter{{Field: "rows", Operator: ">", Value: "15"}},
expected: []string{"beta", "gamma"},
},
{
name: "Rows <= 10",
filters: []Filter{{Field: "rows", Operator: "<=", Value: "10"}},
expected: []string{"alpha"},
},
{
name: "Combined: Name contains 'a' AND Rows > 20",
filters: []Filter{
{Field: "name", Operator: "contains", Value: "a"},
{Field: "rows", Operator: ">", Value: "20"},
},
expected: []string{"gamma"},
},
}

for _, tc := range tests {
t.Run(tc.name, func(t *testing.T) {
query := TicketQuery{
Filters: tc.filters,
}
criteriaBytes, _ := json.Marshal(query)
criteria := &flight.Criteria{Expression: criteriaBytes}

stream, err := client.ListFlights(ctx, criteria)
if err != nil {
t.Fatalf("ListFlights failed: %v", err)
}

	var found []string
	for {
		info, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		found = append(found, info.FlightDescriptor.Path[0])
	}

// Check length
if len(found) != len(tc.expected) {
t.Errorf("Expected %d flights, got %d (%v)", len(tc.expected), len(found), found)
}

// Check content (order might vary, so use map)
expectMap := make(map[string]bool)
for _, e := range tc.expected {
expectMap[e] = true
}
for _, f := range found {
if !expectMap[f] {
t.Errorf("Unexpected flight found: %s", f)
}
}
})
}
}
