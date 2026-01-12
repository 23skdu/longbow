"""Debug test to trace search request flow."""
import time
import pandas as pd
import numpy as np
from longbow import LongbowClient
import pyarrow.flight as flight
import json

client = LongbowClient(uri="grpc://localhost:3000")
dataset_name = "debug_search_test"

np.random.seed(42)
docs = [{"id": 1, "text": "Test", "category": "tech"}]
embeddings = [np.random.randn(384).tolist()]

df = pd.DataFrame(docs)
df['vector'] = embeddings

try:
    client.delete_namespace(dataset_name)
    time.sleep(1)
except:
    pass

print("Inserting data...")
client.insert(dataset_name, df)
time.sleep(3)

print("\\nAttempting search...")
query_vec = embeddings[0]

# Build the search request manually to see what's being sent
req = {
    "dataset": dataset_name,
    "vector": query_vec,
    "k": 5,
    "filters": [{"field": "category", "op": "Eq", "value": "tech"}]
}

ticket_bytes = json.dumps({"search": req}).encode("utf-8")
ticket = flight.Ticket(ticket_bytes)

print(f"Search ticket: {ticket_bytes[:200]}...")
print(f"Ticket length: {len(ticket_bytes)} bytes")

try:
    print("Calling do_get...")
    reader = client._data_client.do_get(ticket, options=client._get_call_options())
    print("do_get succeeded, reading results...")
    table = reader.read_all()
    print(f"Got table with {table.num_rows} rows")
    print(table.to_pandas())
except Exception as e:
    print(f"Error during search: {e}")
    import traceback
    traceback.print_exc()
