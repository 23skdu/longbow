# Arrow Protocol Specification

Longbow adheres to the Apache Arrow Flight protocol version 18.

## Endpoints

### ListFlights

Returns a stream of `FlightInfo` objects representing stored datasets. Supported on the **Meta Server**.

#### Criteria Filtering

Clients can filter results by providing a JSON-serialized `TicketQuery` in the `expression` field.

Supported Filters:

- `name`: Match by dataset name (operator: `contains`).
- `rows`: Match by row count (operators: `==`, `>`, `<=`).

### GetFlightInfo

Returns schema and metadata for a specific dataset path.

### DoGet

Retrieves data or performs searches via a `Ticket` containing a JSON `TicketQuery`.

#### Standard Fetch

```json
{"name": "my_dataset", "filters": [{"field": "id", "op": "=", "value": "123"}]}
```

#### Search (Integrated)

```json
{
  "search": {
    "dataset": "my_data",
    "vector": [0.1, 0.2, ...],
    "k": 10
  }
}
```

### DoPut

Streams Arrow Record Batches to the server. Supports **AppMetadata** in `PutResult` for backpressure signaling (`slow_down`).

### DoExchange

Bidirectional stream for advanced mesh operations.

- **VectorSearch**: High-performance bidirectional search.
- **sync**: Delta synchronization using WAL sequence numbers.
- **merkle_node**: Merkle tree navigation for data consistency checks.

## Actions (`DoAction`)

Longbow uses `DoAction` for control plane operations and specific search modes.

### VectorSearch

**Type**: `VectorSearch` (on Meta Server)
**Input**: JSON payload containing `dataset`, `vector`, `k`, and optional `filters`.
**Output**: JSON stream of search results.

### VectorSearchByID

**Type**: `VectorSearchByID`
**Input**: `{"dataset": "...", "id": "...", "k": 10}`
**Output**: Similar vectors based on an existing ID.
