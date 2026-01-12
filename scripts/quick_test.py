"""Quick test to verify indexing delay hypothesis."""
import time
import pandas as pd
import numpy as np
from longbow import LongbowClient

# Setup
client = LongbowClient(uri="grpc://localhost:3000")
dataset_name = "quick_test_dataset"

# Create test data
np.random.seed(42)
docs = [
    {"id": 1, "text": "Test document", "category": "tech"},
    {"id": 2, "text": "Another test", "category": "ai"},
]
embeddings = [np.random.randn(384).tolist() for _ in range(2)]

df = pd.DataFrame(docs)
df['vector'] = embeddings

# Clean up
try:
    client.delete_namespace(dataset_name)
    time.sleep(1)
except:
    pass

# Insert
print("Inserting data...")
client.insert(dataset_name, df)

# Wait for indexing
print("Waiting 5 seconds for indexing...")
time.sleep(5)

# Search without filters
print("Searching without filters...")
results = client.search(dataset_name, vector=embeddings[0], k=5).compute()
print(f"Results: {len(results)} rows")
print(results)

# Search with filter
print("\nSearching with filter (category=tech)...")
results_filtered = client.search(
    dataset_name, 
    vector=embeddings[0], 
    k=5,
    filters=[{"field": "category", "op": "eq", "value": "tech"}]
).compute()
print(f"Filtered results: {len(results_filtered)} rows")
print(results_filtered)

# Cleanup
client.delete_namespace(dataset_name)
