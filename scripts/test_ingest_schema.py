
import pandas as pd
import pyarrow as pa
from longbow.ingest import to_arrow_table

def test_ingest():
    df = pd.DataFrame([
        {"id": 1, "vector": [0.1, 0.2], "text": "hello", "category": "A", "timestamp": pd.Timestamp.now()}
    ])
    table = to_arrow_table(df)
    print("Schema:")
    print(table.schema)
    
    if "category" in table.schema.names:
        print("PASS: category found")
    else:
        print("FAIL: category missing")

if __name__ == "__main__":
    test_ingest()
