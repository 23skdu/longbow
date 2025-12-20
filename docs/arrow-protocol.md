# Arrow Protocol Specification

Longbow adheres to the Apache Arrow Flight protocol version 18.

## Endpoints

### Handshake

Standard gRPC handshake.

### ListFlights

Returns a stream of `FlightInfo` objects representing stored vectors.

Clients can filter the results by providing a JSON-serialized
in the  argument.

#### TicketQuery Structure

```json
{
 "name": "optional_name_match",
 "limit": 100,
 "filters": [
 {"field": "name", "operator": "contains", "value": "test"},
 {"field": "rows", "operator": ">", "value": "1000"}
 ]
}
```

#### Supported Filters

| Field | Supported Operators | Description |
| :--- | :--- | :--- |
| `name` | `=`, `!=`, `contains` | Filter by dataset name. |
| `rows` | `=`, `>`, `<`, `>=`, `<=` | Filter by total row count. |

### GetFlightInfo

**Input**: `FlightDescriptor` (Path)
**Output**: `FlightInfo` (Schema, TotalRecords, TotalBytes)

### DoGet

**Input**: `Ticket` containing a JSON-serialized `TicketQuery`.
**Output**: Stream of Arrow Record Batches.

#### TicketQuery Structure

```json
{
  "name": "dataset_name",
  "filters": [
    {"field": "id", "op": ">", "value": "50"},
    {"field": "category", "op": "=", "value": "A"}
  ]
}
```

### DoPut

**Input**: Stream of Arrow Record Batches
**Output**: Stream of `PutResult`

### DoExchange

**Input**: Bidirectional stream of `FlightData`
**Output**: Bidirectional stream (used for sync/replication)

- **Foundation**: Currently implements a basic ping/ack and dataset fetch mechanism using `cmd` in descriptor.

## Actions

### VectorSearch

**Type**: `VectorSearch`
**Input**: JSON payload
**Output**: JSON stream of results

**Payload**:

```json
{
  "dataset": "my_data",
  "vector": [0.1, 0.2, ...],
  "k": 10,
  "filters": [
    {"field": "tag", "op": "=", "value": "important"}
  ]
}
```
