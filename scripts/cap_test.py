"""Test with capitalized operator."""
import time
import pandas as pd
import numpy as np
from longbow import LongbowClient

client = LongbowClient(uri="grpc://localhost:3000")
dataset_name = "cap_test_dataset"

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

client.insert(dataset_name, df)
time.sleep(3)

# Test with capitalized "Eq"
print("Testing with Eq (capitalized)...")
results = client.search(
    dataset_name, 
    vector=embeddings[0], 
    k=5,
    filters=[{"field": "category", "op": "Eq", "value": "tech"}]
).compute()
print(f"Results: {len(results)}")
print(results)
