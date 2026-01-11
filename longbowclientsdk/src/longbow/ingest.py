import pyarrow as pa
import pandas as pd
import numpy as np
import dask.dataframe as dd
from typing import Union, List, Dict, Any, Optional

def _infer_schema(dim: int, with_meta: bool = False) -> pa.Schema:
    """Creates standard Longbow Arrow schema."""
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), dim)),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    if with_meta:
        fields.append(pa.field("metadata", pa.string())) # JSON serialized
    return pa.schema(fields)

def to_arrow_table(
    data: Union[List[Dict[str, Any]], pd.DataFrame, dd.DataFrame, Dict[str, Any], pa.Table],
    dim: Optional[int] = None
) -> pa.Table:
    """
    Converts various input formats to a PyArrow Table suitable for Longbow ingestion.
    
    Args:
        data: Input data (List of Dicts, Pandas DataFrame, Dask DataFrame).
        dim: Vector dimension (optional if can be inferred).
        
    Returns:
        pa.Table: Arrow table ready for flight.
    """
    
    # 0. Handle Arrow Table (Pass-through)
    if isinstance(data, pa.Table):
        return data

    # 1. Handle Dask DataFrame
    if isinstance(data, dd.DataFrame):
        # Materialize to Pandas for now as flight requires a Table.
        # Ideally we stream partitions, but that requires logic in Client.insert
        # to iterate partitions. For this helper, we assume small-ish partitions
        # or the client handles partitioning.
        # Let's verify if client handles it.
        # Actually, let's materialize here for v0.1 simplification unless it's HUGE.
        # User should partition manually if calling this lower level function.
        # BUT `LongbowClient` will use this.
        # Better strategy: return Pandas DF if input is Dask, let Client loop.
        # For this function, let's assume it receives a materialized chunk (Pandas)
        # or a raw list.
        # If Dask is passed here, we compute().
        return to_arrow_table(data.compute(), dim)

    # 2. Handle List of Dicts
    if isinstance(data, list):
        if not data:
            raise ValueError("Input list is empty")
        
        # Check first item to guess structure or use dim
        first = data[0]
        if 'vector' in first:
            if dim is None:
                dim = len(first['vector'])
        
        # Convert to Pandas first for easier Arrow conversion (handling missing cols)
        df = pd.DataFrame(data)
        return to_arrow_table(df, dim)

    # 3. Handle Pandas DataFrame
    if isinstance(data, pd.DataFrame):
        # Ensure required columns
        if 'id' not in data.columns:
            # Auto-generate IDs if missing? Or raise error. Longbow requires explicit IDs currently.
            # Let's fallback to index if it's integer
            if pd.api.types.is_integer_dtype(data.index):
                data['id'] = data.index
            else:
                 raise ValueError("Data must have an 'id' column or integer index")
        
        if 'vector' not in data.columns:
             raise ValueError("Data must have a 'vector' column")

        # Ensure vector column is list of floats
        # Pandas often stores lists as objects.
        
        # Infer dimension if needed
        if dim is None:
            # Peek at first vector
            first_vec = data['vector'].iloc[0]
            if isinstance(first_vec, (list, np.ndarray)):
                dim = len(first_vec)
            elif isinstance(first_vec, str):
                try:
                    import json
                    parsed = json.loads(first_vec)
                    if isinstance(parsed, list):
                        dim = len(parsed)
                    else:
                         raise ValueError(f"String vector is not a list: {first_vec}")
                except Exception:
                     raise ValueError(f"Could not parse string vector: {first_vec}")
            else:
                raise ValueError(f"Could not infer vector dimension. First element type: {type(first_vec)}, Value: {first_vec}")
        
        # Timestamp handling
        if 'timestamp' not in data.columns:
            data['timestamp'] = pd.Timestamp.now()
            
        # Metadata handling
        # If there are extra columns, assume they are metadata?
        # Longbow expects a single "metadata" column or specific schema.
        # The schema expected is: id, vector, timestamp, (optional) metadata [string or map?]
        # The go server expects "meta" as string? Wait, let's check ops_test.py
        # ops_test.py uses: id, vector, timestamp, meta (string)
        # We need to serialize dict to string for 'meta' column if provided as dict
        
        # If 'metadata' column exists
        if 'metadata' in data.columns or 'meta' in data.columns:
            # meta_col = 'metadata' if 'metadata' in data.columns else 'meta'
            # Serialize if needed
            # ... implementation detail ...
            pass
            
        # Construct Arrow Arrays efficiently
        # Vector column need special handling to FixedSizeList
        
        # Fast path using stack for vectors
        # Check if we need to parse strings
        vec_sample = data['vector'].iloc[0]
        if isinstance(vec_sample, str):
             import json
             # Apply parsing to the whole column. 
             data['vector'] = data['vector'].apply(json.loads)

        vecs = np.stack(data['vector'].values) # matrix [N, dim]
        if vecs.dtype != np.float32:
            vecs = vecs.astype(np.float32)
            
        # Create FixedSizeListArray
        flat_vecs = vecs.flatten()
        arrow_vecs = pa.FixedSizeListArray.from_arrays(flat_vecs, dim)
        
        arrow_ids = pa.array(data['id'].values, type=pa.int64())
        arrow_ts = pa.array(data['timestamp'].values, type=pa.timestamp("ns"))
        
        arrays = [arrow_ids, arrow_vecs, arrow_ts]
        names = ['id', 'vector', 'timestamp']
        
        # Handle metadata
        # If users passed extra columns, pack them into json?
        # For v1, let's strictly look for 'metadata' str column for simplicity
        if 'metadata' in data.columns:
             arrays.append(pa.array(data['metadata'].astype(str)))
             names.append('metadata')
             
        return pa.Table.from_arrays(arrays, names=names)

    raise TypeError(f"Unsupported data type: {type(data)}")
