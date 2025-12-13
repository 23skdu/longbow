# Usage Guide

## Installation

```bash
helm install longbow ./helm/longbow
```

## Client Example (Python)

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:3000")

# List flights
for info in client.list_flights():
    print(info.descriptor.path)

# Get data
reader = client.do_get(flight.Ticket("vector-id"))
table = reader.read_all()
```
