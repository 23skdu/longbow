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

**Input**: `Ticket` (containing the vector ID)
**Output**: Stream of Arrow Record Batches

### DoPut

**Input**: Stream of Arrow Record Batches
**Output**: Stream of `PutResult`
