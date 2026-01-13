# Admin API

Longbow provides management capabilities via the **Meta Server** (`DoAction` and `ListFlights`).

## Operations

Operations are performed by sending a `DoAction` request with a JSON body.

### Namespace Management

#### `CreateNamespace`

Creates a new namespace for vector data.

- **Type**: `CreateNamespace`
- **Body**: `{"name": "my_namespace"}`
- **Response**: `{"status": "created"}`

#### `DeleteNamespace`

Deletes an entire namespace and its associated datasets.

- **Type**: `DeleteNamespace`
- **Body**: `{"name": "my_namespace"}`
- **Response**: `{"status": "deleted"}`

#### `ListNamespaces`

Returns a list of all active namespaces.

- **Type**: `ListNamespaces`
- **Body**: (empty)
- **Response**: `{"namespaces": ["ns1", "ns2"], "count": 2}`

### Dataset Management

#### `delete-dataset`

Removes a dataset from memory.

- **Type**: `delete-dataset`
- **Body**: `{"dataset": "my_dataset"}`
- **Response**: `"deleted"` (string)

#### `delete-vector`

Deletes a specific vector by its internal `VectorID`.

- **Type**: `delete-vector`
- **Body**: `{"dataset": "my_dataset", "vector_id": 123}`
- **Response**: `"deleted"` (string)

### Mesh & Cluster Status

#### `MeshStatus`

Retrieves the status of the gossip mesh and connected members.

- **Type**: `MeshStatus`
- **Response**: List of member objects including ID, Addr, and Status.

#### `cluster-status`

Retrieves cluster-level health and member identity.

- **Type**: `cluster-status`
- **Response**: JSON object containing `self` identity and `members` list.

## Backpressure Monitoring

The Data Server (`DoPut`) monitors the Write-Ahead Log (WAL) queue depth. If the queue exceeds **80% capacity**, the server applies backpressure:

1. Server logs a `wal_pressure` warning.
2. `DoPut` responses include metadata: `{"status": "slow_down", "reason": "wal_pressure"}`.

Clients (including the Python SDK) monitor this metadata and should implement backoff or throttling to avoid overloading the persistence layer.
