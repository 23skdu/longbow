# Arrow Protocol Specification

Longbow adheres to the Apache Arrow Flight protocol version 18.

## Endpoints

### Handshake

Standard gRPC handshake.

### ListFlights

Returns a stream of `FlightInfo` objects representing stored vectors.

### GetFlightInfo

**Input**: `FlightDescriptor` (Path)
**Output**: `FlightInfo` (Schema, TotalRecords, TotalBytes)

### DoGet

**Input**: `Ticket` (containing the vector ID)
**Output**: Stream of Arrow Record Batches

### DoPut

**Input**: Stream of Arrow Record Batches
**Output**: Stream of `PutResult`
