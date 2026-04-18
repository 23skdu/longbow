package store

import (
	"context"
	"fmt"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
    "github.com/apache/arrow-go/v18/arrow/ipc"
    "github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAdvancedSQL_E2E(t *testing.T) {
	vs, _, dialer := setupServer(t)
	ctx := context.Background()
	client, err := flight.NewClientWithMiddleware(
		"passthrough:///bufnet",
		nil,
		nil,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	mem := memory.NewGoAllocator()

	// 1. Prepare Data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "status", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Users dataset
	b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{101, 102, 103}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"active", "inactive", "active"}, nil)
	usersRec := b.NewRecordBatch()
	defer usersRec.Release()

	// 2. DoPut Users
	putStream, err := client.DoPut(ctx)
	require.NoError(t, err)
	w := flight.NewRecordWriter(putStream, ipc.WithSchema(schema))
	w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"users"}})
	err = w.Write(usersRec)
	require.NoError(t, err)
	w.Close()
	putStream.CloseSend()
	_, _ = putStream.Recv() // Wait

	fmt.Println("Waiting for indexing...")
	_, ok := vs.getDataset("users")
	require.True(t, ok, "dataset users should exist")
	vs.WaitForIndexing("users")
	fmt.Println("Indexing complete")

	t.Run("Subquery_IN", func(t *testing.T) {
		// Query with subquery: select * from users where id IN (select id from users where status == "active")
		// (A recursive example on same dataset)
		ticketJSON := `{
			"name": "users",
			"filters": [
				{
					"field": "id",
					"operator": "in",
					"subquery": {
						"search": {
							"dataset": "users",
							"k": 10,
							"filters": [{"field": "status", "operator": "==", "value": "active"}]
						}
					}
				}
			]
		}`
		ticket := &flight.Ticket{Ticket: []byte(ticketJSON)}
		rStream, err := client.DoGet(ctx, ticket)
		require.NoError(t, err)

		r, err := flight.NewRecordReader(rStream)
		require.NoError(t, err)
		defer r.Release()

		var ids []uint64
		for r.Next() {
			rec := r.RecordBatch()
			idCol := rec.Column(0).(*array.Uint64)
			for i := 0; i < int(rec.NumRows()); i++ {
				ids = append(ids, idCol.Value(i))
			}
		}
		
		// Expected IDs: 101, 103 (the active ones)
		assert.ElementsMatch(t, []uint64{101, 103}, ids)
	})

	t.Run("CTE_WITH", func(t *testing.T) {
		// select * from active_users
		ticketJSON := `{
			"with": [
				{
					"name": "active_users",
					"search": {
						"dataset": "users",
						"k": 10,
						"filters": [{"field": "status", "operator": "==", "value": "active"}]
					}
				}
			],
			"name": "active_users"
		}`
		ticket := &flight.Ticket{Ticket: []byte(ticketJSON)}
		rStream, err := client.DoGet(ctx, ticket)
		require.NoError(t, err)

		r, err := flight.NewRecordReader(rStream)
		require.NoError(t, err)
		defer r.Release()

		var ids []uint64
		for r.Next() {
			rec := r.RecordBatch()
			idCol := rec.Column(0).(*array.Uint64)
			for i := 0; i < int(rec.NumRows()); i++ {
				ids = append(ids, idCol.Value(i))
			}
		}
		
		assert.ElementsMatch(t, []uint64{101, 103}, ids)
	})
}
